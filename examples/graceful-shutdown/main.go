// Example graceful-shutdown: Shutdown stops intake, then waits for in-flight handlers.
//
// Connections are closed first so no new deliveries arrive. Then Shutdown waits
// on inflight until handlers return or the context deadline hits.
//
//	docker compose -f examples/docker-compose.yml up -d
//	go run ./graceful-shutdown
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

const (
	queueName    mq.QueueName = "example.shutdown"
	workers                   = 2
	jobs                      = 2
	handlerDelay              = 3 * time.Second
)

func main() {
	logger := slog.Default()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	queue, err := mq.New(mq.Config{
		URL:            amqpURL(),
		ReconnectDelay: time.Second,
		ReInitDelay:    time.Second,
		ResendDelay:    time.Second,
		QueueParams: map[mq.QueueName]mq.QueueItem{
			queueName: {},
		},
	}, mq.WithLogger(logger))
	if err != nil {
		logger.Error("create queue", "error", err)
		os.Exit(1)
	}
	defer shutdown(logger, queue)

	var started atomic.Int64
	var finished atomic.Int64

	err = queue.AddConsumerN(ctx, queueName, workers, func(_ context.Context, body []byte) error {
		n := started.Add(1)
		logger.Info("handler start", "body", string(body), "running", n)
		time.Sleep(handlerDelay)
		logger.Info("handler finish", "body", string(body), "elapsed", handlerDelay)
		finished.Add(1)

		return nil
	})
	if err != nil {
		logger.Error("AddConsumerN", "error", err)
		os.Exit(1)
	}

	if err = queue.InitConsumer(ctx); err != nil {
		logger.Error("InitConsumer", "error", err)
		os.Exit(1)
	}

	for i := 1; i <= jobs; i++ {
		body := []byte("slow-" + strconv.Itoa(i))
		if err = queue.Publish(ctx, queueName, mq.PublishMessage{Body: body}); err != nil {
			logger.Error("Publish", "error", err)
			os.Exit(1)
		}
	}

	logger.Info("published slow jobs; waiting until handlers are in-flight", "jobs", jobs)

	if !waitStarted(ctx, logger, &started, jobs) {
		return
	}

	logger.Info("Shutdown: close connections first, then wait for in-flight handlers")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()
	if err = queue.Shutdown(shutdownCtx); err != nil {
		logger.Error("Shutdown", "error", err)
		os.Exit(1)
	}

	logger.Info("Shutdown returned",
		"elapsed", time.Since(start),
		"started", started.Load(),
		"finished", finished.Load(),
	)
}

func shutdown(logger *slog.Logger, queue mq.Queue) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := queue.Shutdown(ctx); err != nil {
		logger.Error("Shutdown", "error", err)
	}
}

func waitStarted(ctx context.Context, logger *slog.Logger, started *atomic.Int64, want int) bool {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.NewTimer(15 * time.Second)
	defer timeout.Stop()

	for {
		if started.Load() >= int64(want) {
			return true
		}

		select {
		case <-ctx.Done():
			logger.Info("interrupted before Shutdown")

			return false
		case <-timeout.C:
			logger.Error("handlers did not start in time", "started", started.Load(), "want", want)

			return false
		case <-ticker.C:
		}
	}
}

func amqpURL() string {
	if v := os.Getenv("AMQP_URL"); v != "" {
		return v
	}

	return "amqp://guest:guest@localhost:5672/"
}
