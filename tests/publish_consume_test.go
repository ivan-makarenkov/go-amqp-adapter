package functional

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

func TestPublishConsume_SingleMessage(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "pubsub")
	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {},
	}, testQueueOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64
	var lastBody atomic.Value

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, data []byte) error {
		lastBody.Store(string(data))
		processed.Add(1)

		return nil
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	wantBody := "functional-test-message"
	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte(wantBody)}, 15*time.Second)

	waitUntil(t, 15*time.Second, func() bool {
		return processed.Load() == 1
	}, "single message processed")

	if got := lastBody.Load(); got != wantBody {
		t.Fatalf("message body = %q, want %q", got, wantBody)
	}
}

func TestPublishConsume_ParallelWorkers(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "parallel")
	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {},
	}, testQueueOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		workers = 3
		total   = 12
	)

	var processed atomic.Int64

	err := queue.AddConsumerN(ctx, queueName, workers, func(_ context.Context, _ []byte) error {
		processed.Add(1)

		return nil
	})
	if err != nil {
		t.Fatalf("AddConsumerN() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	for i := range total {
		body := []byte{byte('a' + i)}
		publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: body}, 15*time.Second)
	}

	waitUntil(t, 20*time.Second, func() bool {
		return processed.Load() == total
	}, "all messages processed by parallel workers")

	if got := processed.Load(); got != total {
		t.Fatalf("processed %d messages, want %d", got, total)
	}
}
