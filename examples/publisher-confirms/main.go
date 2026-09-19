// Example publisher-confirms: Publish blocks until the broker confirms the message.
//
// Confirms are always on. There is one in-flight publish per queue connection;
// concurrent Publish calls on the same queue are serialized. On nack or publish
// error the adapter waits ResendDelay and retries until the context is canceled.
//
//	docker compose -f examples/docker-compose.yml up -d
//	go run ./publisher-confirms
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

const queueName mq.QueueName = "example.confirms"

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

	logger.Info("sequential publishes: each Publish returns after broker ack")

	for i := 1; i <= 3; i++ {
		start := time.Now()
		err = queue.Publish(ctx, queueName, mq.PublishMessage{
			Body:        []byte("seq-" + strconv.Itoa(i)),
			ContentType: mq.DefaultContentType,
		})
		if err != nil {
			logger.Error("Publish", "error", err, "n", i)
			os.Exit(1)
		}

		logger.Info("confirmed", "n", i, "elapsed", time.Since(start))
	}

	logger.Info("concurrent publishes: one connection, so confirms are serialized")

	var wg sync.WaitGroup

	for i := 1; i <= 4; i++ {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			start := time.Now()
			pubErr := queue.Publish(ctx, queueName, mq.PublishMessage{
				Body: []byte("par-" + strconv.Itoa(n)),
			})
			logger.Info("goroutine confirmed", "n", n, "elapsed", time.Since(start), "error", pubErr)
		}(i)
	}

	wg.Wait()
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
