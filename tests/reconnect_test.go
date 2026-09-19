package functional

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

func TestReconnect_AfterRabbitMQRestart(t *testing.T) {
	requireFunctional(t)
	requireDocker(t)

	queueName := uniqueQueueName(t, "reconnect")
	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {},
	}, testQueueOptions{
		// failHandler not needed — retry disabled
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, data []byte) error {
		t.Logf("received message: %q", data)
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

	// Message before stopping RabbitMQ.
	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte("before-stop")}, 20*time.Second)

	waitUntil(t, 15*time.Second, func() bool {
		return processed.Load() >= 1
	}, "message processed before RabbitMQ stop")

	t.Log("stopping RabbitMQ...")
	runCompose(t, "stop", "rabbitmq")

	time.Sleep(2 * time.Second)

	t.Log("starting RabbitMQ...")
	runCompose(t, "start", "rabbitmq")
	waitRabbitReady(t, 90*time.Second)

	// Give the client time to reconnect.
	time.Sleep(3 * time.Second)

	// Message after RabbitMQ recovery.
	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte("after-start")}, 60*time.Second)

	waitUntil(t, 60*time.Second, func() bool {
		return processed.Load() >= 2
	}, "message processed after reconnect")

	if got := processed.Load(); got < 2 {
		t.Fatalf("processed %d messages, want >= 2", got)
	}
}
