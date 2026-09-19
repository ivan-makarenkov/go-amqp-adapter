// Example basic-consumer: declare a queue, consume one message, publish, then exit.
//
//	docker compose -f examples/docker-compose.yml up -d
//	go run ./basic-consumer
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

const queueName mq.QueueName = "example.basic"

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

	got := make(chan struct{})

	err = queue.AddConsumer(ctx, queueName, func(_ context.Context, body []byte) error {
		logger.Info("consumed", "body", string(body))
		close(got)

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

	err = queue.Publish(ctx, queueName, mq.PublishMessage{Body: []byte("hello")})
	if err != nil {
		logger.Error("Publish", "error", err)
		os.Exit(1)
	}

	select {
	case <-got:
		logger.Info("done")
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
