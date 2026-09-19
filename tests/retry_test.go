package functional

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

func TestRetry_MessageRetriedUntilSuccess(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "retry_ok")
	const retryDelay = 2 * time.Second

	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {
			Retry: &mq.RetryConfig{
				Delay:       retryDelay,
				MaxDuration: 2 * time.Minute,
			},
		},
	}, testQueueOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const wantSuccessOnAttempt = 3

	var attempts atomic.Int64
	var succeeded atomic.Bool

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, data []byte) error {
		attempt := attempts.Add(1)
		t.Logf("processing attempt #%d, body=%q", attempt, data)

		if attempt < wantSuccessOnAttempt {
			return mq.Retry(errors.New("temporary error"))
		}

		succeeded.Store(true)

		return nil
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte("retry-me")}, 15*time.Second)

	// Two retry delays plus processing slack.
	waitUntil(t, retryDelay*time.Duration(wantSuccessOnAttempt)+10*time.Second, func() bool {
		return succeeded.Load()
	}, "successful processing after retry")

	if got := attempts.Load(); got < wantSuccessOnAttempt {
		t.Fatalf("processing attempts %d, want >= %d", got, wantSuccessOnAttempt)
	}
}

func TestRetry_PermanentErrorCallsFailHandler(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "retry_permanent")
	const retryDelay = 1 * time.Second

	var failedJobs atomic.Int64

	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {
			Retry: &mq.RetryConfig{
				Delay:       retryDelay,
				MaxDuration: 5 * time.Second,
			},
		},
	}, testQueueOptions{
		failHandler: func(job mq.FailedJob) error {
			failedJobs.Add(1)
			t.Logf("fail handler: queue=%s body=%q err=%v", job.Queue, job.Body, job.Err)

			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, _ []byte) error {
		// Non-retryable error — go straight to fail handler.
		return errors.New("permanent error")
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{Body: []byte("will-fail")}, 15*time.Second)

	waitUntil(t, 10*time.Second, func() bool {
		return failedJobs.Load() >= 1
	}, "fail handler called for non-retryable error")

	if got := failedJobs.Load(); got < 1 {
		t.Fatalf("fail handler called %d times, want >= 1", got)
	}
}

func TestRetry_ExhaustedCallsFailHandler(t *testing.T) {
	requireFunctional(t)

	queueName := uniqueQueueName(t, "retry_fail")
	const retryDelay = 1 * time.Second

	var failedJobs atomic.Int64

	queue := newTestQueue(t, map[mq.QueueName]mq.QueueItem{
		queueName: {
			Retry: &mq.RetryConfig{
				Delay:       retryDelay,
				MaxDuration: 5 * time.Second,
			},
		},
	}, testQueueOptions{
		failHandler: func(job mq.FailedJob) error {
			failedJobs.Add(1)
			t.Logf("fail handler: queue=%s body=%q err=%v", job.Queue, job.Body, job.Err)

			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := queue.AddConsumer(ctx, queueName, func(_ context.Context, _ []byte) error {
		return mq.Retry(errors.New("persistent error"))
	})
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err = queue.InitConsumer(ctx)
	if err != nil {
		t.Fatalf("InitConsumer() error = %v", err)
	}

	maxRetry := 3 * time.Second
	publishUntilSuccess(t, ctx, queue, queueName, mq.PublishMessage{
		Body:             []byte("will-fail"),
		MaxRetryDuration: &maxRetry,
	}, 15*time.Second)

	// Wait for MaxRetryDuration plus a few delay-queue cycles.
	waitUntil(t, 20*time.Second, func() bool {
		return failedJobs.Load() >= 1
	}, "fail handler called after retries exhausted")

	if got := failedJobs.Load(); got < 1 {
		t.Fatalf("fail handler called %d times, want >= 1", got)
	}
}
