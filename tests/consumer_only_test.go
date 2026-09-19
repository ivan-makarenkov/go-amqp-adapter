package functional

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConsumerOnly_ConsumeWithoutPublish(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "consumer_only")
	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {ConsumerOnly: true},
	}, testQueueOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, data []byte) error {
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

	err = queue.Publish(ctx, queueName, mq.PublishMessage{Body: []byte("must-fail")})
	if err == nil {
		t.Fatal("Publish() error = nil, want ErrQueueConsumerOnly")
	}

	if !errors.Is(err, mq.ErrQueueConsumerOnly) {
		t.Fatalf("Publish() error = %v, want ErrQueueConsumerOnly", err)
	}

	conn, err := amqp.Dial(rabbitURL())
	if err != nil {
		t.Fatalf("amqp dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(string(queueName), true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}

	err = ch.PublishWithContext(ctx, "", string(queueName), false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("external"),
	})
	if err != nil {
		t.Fatalf("PublishWithContext: %v", err)
	}

	waitUntil(t, 15*time.Second, func() bool {
		return processed.Load() == 1
	}, "message processed on ConsumerOnly queue")
}
