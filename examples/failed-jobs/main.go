// Example failed-jobs: persist exhausted / non-retryable jobs via WithFailHandler.
//
// This is the adapter-side contract. In production, wire the handler to
// go-failedjobs (GetFailedJobHandler + POST /retry-task) instead of the
// in-memory store below.
//
//	docker compose -f examples/docker-compose.yml up -d
//	go run ./failed-jobs
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

const (
	queueName mq.QueueName = "example.failed-jobs"
	httpAddr               = "127.0.0.1:8080"
)

type storedJob struct {
	ID    int    `json:"id"`
	Queue string `json:"queue"`
	Body  string `json:"body"`
	Err   string `json:"err"`
}

type memoryStore struct {
	mu   sync.Mutex
	next int
	jobs []storedJob
}

func (s *memoryStore) add(job mq.FailedJob) storedJob {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.next++

	errMsg := ""
	if job.Err != nil {
		errMsg = job.Err.Error()
	}

	item := storedJob{
		ID:    s.next,
		Queue: string(job.Queue),
		Body:  string(job.Body),
		Err:   errMsg,
	}
	s.jobs = append(s.jobs, item)

	return item
}

func (s *memoryStore) list() []storedJob {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]storedJob, len(s.jobs))
	copy(out, s.jobs)

	return out
}

func (s *memoryStore) take(id int) (storedJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, job := range s.jobs {
		if job.ID == id {
			s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)

			return job, true
		}
	}

	return storedJob{}, false
}

func main() {
	logger := slog.Default()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := &memoryStore{}
	stored := make(chan storedJob, 8)
	var recoveredOnce sync.Once
	recovered := make(chan struct{})

	queue, err := mq.New(mq.Config{
		URL:            amqpURL(),
		ReconnectDelay: time.Second,
		ReInitDelay:    time.Second,
		ResendDelay:    time.Second,
		QueueParams: map[mq.QueueName]mq.QueueItem{
			queueName: {
				Retry: &mq.RetryConfig{
					Delay:       time.Second,
					MaxDuration: 4 * time.Second,
				},
			},
		},
	},
		mq.WithLogger(logger),
		mq.WithFailHandler(func(job mq.FailedJob) error {
			// Production (go-failedjobs):
			//   save := failedSrv.GetFailedJobHandler()
			//   errMsg := ""
			//   if job.Err != nil { errMsg = job.Err.Error() }
			//   return save(string(job.Queue), string(job.Body), errMsg)
			item := store.add(job)
			logger.Warn("failed job stored", "id", item.ID, "body", item.Body, "err", item.Err)
			stored <- item

			return nil
		}),
	)
	if err != nil {
		logger.Error("create queue", "error", err)
		os.Exit(1)
	}
	defer shutdown(logger, queue)

	err = queue.AddConsumer(ctx, queueName, func(_ context.Context, body []byte) error {
		msg := string(body)
		logger.Info("consume", "body", msg)

		switch msg {
		case "hello-from-retry":
			logger.Info("processed OK after retry", "body", msg)
			recoveredOnce.Do(func() { close(recovered) })

			return nil
		case "fail:boom":
			return errors.New("permanent demo failure")
		default:
			return mq.Retry(errors.New("temporary demo failure"))
		}
	})
	if err != nil {
		logger.Error("AddConsumer", "error", err)
		os.Exit(1)
	}

	if err = queue.InitConsumer(ctx); err != nil {
		logger.Error("InitConsumer", "error", err)
		os.Exit(1)
	}

	httpSrv := startHTTP(logger, store, queue)
	defer func() {
		shutdownHTTP, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownHTTP)
	}()

	if err = queue.Publish(ctx, queueName, mq.PublishMessage{Body: []byte("fail:boom")}); err != nil {
		logger.Error("Publish", "error", err)
		os.Exit(1)
	}

	var job storedJob
	select {
	case job = <-stored:
	case <-ctx.Done():
		return
	}

	logger.Info("stored; republish via HTTP or auto-retry", "id", job.ID)
	logger.Info("list: curl http://" + httpAddr + "/failed")
	logger.Info("retry: curl -X POST 'http://" + httpAddr + "/retry?id=" + strconv.Itoa(job.ID) + "'")

	// go-failedjobs republishes the stored payload as-is. This demo rewrites it
	// to a payload the handler accepts, analogue of fixing the job then queue:retry.
	if err = republish(ctx, store, queue, job.ID, []byte("hello-from-retry")); err != nil {
		logger.Error("auto retry", "error", err)
		os.Exit(1)
	}

	select {
	case <-recovered:
		logger.Info("done")
	case <-ctx.Done():
		logger.Info("interrupted")
	}
}

func startHTTP(logger *slog.Logger, store *memoryStore, queue mq.Queue) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /failed", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(store.list())
	})
	mux.HandleFunc("POST /retry", func(w http.ResponseWriter, r *http.Request) {
		id, convErr := strconv.Atoi(r.URL.Query().Get("id"))
		if convErr != nil || id <= 0 {
			http.Error(w, "id must be a positive integer", http.StatusBadRequest)

			return
		}

		err := republish(r.Context(), store, queue, id, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)

			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("HTTP listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http", "error", err)
		}
	}()

	return srv
}

func republish(ctx context.Context, store *memoryStore, queue mq.Queue, id int, body []byte) error {
	job, ok := store.take(id)
	if !ok {
		return errors.New("failed job not found")
	}

	if len(body) == 0 {
		body = []byte(job.Body)
	}

	return queue.Publish(ctx, mq.QueueName(job.Queue), mq.PublishMessage{Body: body})
}

func shutdown(logger *slog.Logger, queue mq.Queue) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := queue.Shutdown(ctx); err != nil {
		logger.Error("Shutdown", "error", err)
	}
}

func amqpURL() string {
	if v := os.Getenv("AMQP_URL"); v != "" {
		return v
	}

	return "amqp://guest:guest@localhost:5672/"
}
