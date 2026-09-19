// Example retry: delay-queue retries via mq.Retry(err), then WithFailHandler.
//
// Return mq.Retry(err) to requeue after Retry.Delay until MaxDuration.
// Return a plain error for a permanent failure (fail handler, then ack).
// WithFailHandler is required whenever QueueItem.Retry is set.
//
//	docker compose -f examples/docker-compose.yml up -d
//	go run ./retry
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

const queueName mq.QueueName = "example.retry"

func main() {
	logger := slog.Default()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var retryThenOKAttempts atomic.Int64
	done := make(chan struct{}, 3)

	queue, err := mq.New(mq.Config{
		URL:            amqpURL(),
		ReconnectDelay: time.Second,
		ReInitDelay:    time.Second,
		ResendDelay:    time.Second,
		QueueParams: map[mq.QueueName]mq.QueueItem{
			queueName: {
				Retry: &mq.RetryConfig{
					Delay:       2 * time.Second,
					MaxDuration: 8 * time.Second,
				},
			},
		},
	},
		mq.WithLogger(logger),
		mq.WithFailHandler(func(job mq.FailedJob) error {
			logger.Warn("permanent failure",
				"queue", job.Queue,
				"body", string(job.Body),
				"err", job.Err,
			)
			done <- struct{}{}

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

		switch {
		case msg == "ok":
			done <- struct{}{}

			return nil
		case strings.HasPrefix(msg, "retry-then-ok:"):
			n := retryThenOKAttempts.Add(1)
			logger.Info("retry-then-ok attempt", "n", n)
			if n < 3 {
				return mq.Retry(errors.New("temporary failure"))
			}

			logger.Info("recovered after retry", "body", msg)
			done <- struct{}{}

			return nil
		case strings.HasPrefix(msg, "fail:"):
			return errors.New("permanent demo failure")
		default:
			return mq.Retry(errors.New("keep retrying until MaxDuration"))
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

	messages := []string{"ok", "retry-then-ok:job", "fail:boom"}
	for _, msg := range messages {
		if err = queue.Publish(ctx, queueName, mq.PublishMessage{Body: []byte(msg)}); err != nil {
			logger.Error("Publish", "error", err, "body", msg)
			os.Exit(1)
		}
	}

	logger.Info("waiting for ok, recovered retry, and permanent fail")

	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-ctx.Done():
			logger.Info("interrupted")

			return
		}
	}

	logger.Info("done")
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
