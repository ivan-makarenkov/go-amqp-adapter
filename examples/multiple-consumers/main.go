// Example multiple-consumers: AddConsumerN opens N AMQP connections/workers.
//
// Prefetch is QoS=1 per channel. AddConsumer / AddConsumerN must run before
// InitConsumer. This example also registers a second queue with a single worker.
//
//	docker compose -f examples/docker-compose.yml up -d
//	go run ./multiple-consumers
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
	emailQueue   mq.QueueName = "example.emails"
	smsQueue     mq.QueueName = "example.sms"
	emailWorkers              = 4
	emailJobs                 = 8
	workTime                  = 400 * time.Millisecond
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
			emailQueue: {},
			smsQueue:   {},
		},
	}, mq.WithLogger(logger))
	if err != nil {
		logger.Error("create queue", "error", err)
		os.Exit(1)
	}
	defer shutdown(logger, queue)

	var emailsDone atomic.Int64
	var smsDone atomic.Int64
	finished := make(chan struct{})

	mark := func() {
		if emailsDone.Load() == emailJobs && smsDone.Load() == 1 {
			select {
			case <-finished:
			default:
				close(finished)
			}
		}
	}

	err = queue.AddConsumerN(ctx, emailQueue, emailWorkers, func(_ context.Context, body []byte) error {
		start := time.Now()
		logger.Info("email start", "body", string(body))
		time.Sleep(workTime)
		logger.Info("email done", "body", string(body), "elapsed", time.Since(start))
		emailsDone.Add(1)
		mark()

		return nil
	})
	if err != nil {
		logger.Error("AddConsumerN", "error", err)
		os.Exit(1)
	}

	err = queue.AddConsumer(ctx, smsQueue, func(_ context.Context, body []byte) error {
		logger.Info("sms", "body", string(body))
		smsDone.Add(1)
		mark()

		return nil
	})
	if err != nil {
		logger.Error("AddConsumer", "error", err)
		os.Exit(1)
	}

	if err = queue.InitConsumer(ctx); err != nil {
		logger.Error("InitConsumer", "error", err)
		os.Exit(1)
	}

	for i := 1; i <= emailJobs; i++ {
		body := []byte("email-" + strconv.Itoa(i))
		if err = queue.Publish(ctx, emailQueue, mq.PublishMessage{Body: body}); err != nil {
			logger.Error("Publish emails", "error", err)
			os.Exit(1)
		}
	}

	if err = queue.Publish(ctx, smsQueue, mq.PublishMessage{Body: []byte("sms-1")}); err != nil {
		logger.Error("Publish sms", "error", err)
		os.Exit(1)
	}

	logger.Info("published", "emails", emailJobs, "workers", emailWorkers)

	select {
	case <-finished:
		logger.Info("done: overlapping email start logs mean workers ran in parallel")
	case <-ctx.Done():
		logger.Info("interrupted")
	}
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
