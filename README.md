# amqp-adapter

Go adapter over [amqp091-go](https://github.com/rabbitmq/amqp091-go): publish and consume RabbitMQ messages with publisher confirms, auto-reconnect, and optional retry via a delayed queue / DLX.

```bash
go get github.com/ivan-makarenkov/amqp-adapter
```

Package: `amqpadapter`.

## Together with go-failedjobs

This library and [go-failedjobs](https://github.com/ivan-makarenkov/go-failedjobs) were designed to be used together as a replacement for Laravel's retry-through-queue mechanism (`tries` / delayed retries → `failed_jobs` → `php artisan queue:retry`).

- **amqp-adapter** handles in-broker retries (delay queue / DLX) — the analogue of Laravel's automatic job retries.
- When retries are exhausted (or the handler returns a non-retryable error), `WithFailHandler` persists the payload instead of Laravel's `failed_jobs` table. Wire it to `go-failedjobs` via `GetFailedJobHandler`.
- **go-failedjobs** stores those rows in MySQL/Postgres and republishes selected IDs to RabbitMQ (`POST /retry-task`) — the analogue of `php artisan queue:retry`.

## Features

- Lazy publisher connect on first `Publish`
- Publisher confirms (one in-flight publish per connection)
- Automatic reconnect on connection/channel loss
- Consumers with parallelism (`AddConsumerN`)
- Retry via delay queue and `expired` header (requires `WithFailHandler`)
- Header/context plumbing (`WithPublishHeadersBuilder` / `WithConsumeHeadersExtractor`)
- [`otel`](otel/) subpackage for correlation ID and OpenTelemetry propagation

## Quick start

```go
package main

import (
	"context"
	"log"
	"time"

	mq "github.com/ivan-makarenkov/amqp-adapter"
)

func main() {
	ctx := context.Background()

	q, err := mq.New(mq.Config{
		URL:            "amqp://guest:guest@localhost:5672/",
		ReconnectDelay: time.Second,
		ReInitDelay:    time.Second,
		ResendDelay:    time.Second,
		QueueParams: map[mq.QueueName]mq.QueueItem{
			"jobs": {},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = q.Shutdown(shutdownCtx)
	}()

	err = q.AddConsumer(ctx, "jobs", func(ctx context.Context, body []byte) error {
		log.Printf("got: %s", body)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if err = q.InitConsumer(ctx); err != nil {
		log.Fatal(err)
	}

	err = q.Publish(ctx, "jobs", mq.PublishMessage{Body: []byte("hello")})
	if err != nil {
		log.Fatal(err)
	}

	select {}
}
```

## Configuration

| Field | Meaning |
|------|--------|
| `URL` | AMQP URL |
| `ReconnectDelay` | Pause between dial attempts |
| `ReInitDelay` | Pause between channel/queue re-init attempts |
| `ResendDelay` | Pause before republish after error/nack |
| `QueueParams` | Per-queue parameters by name |

`QueueItem`:

- `Retry` — `nil` = no retry; otherwise delay/DLX topology
- `ConsumerOnly` — consume only, no publisher connection

## Retry

When `Retry != nil`, the library declares exchanges and a delay queue. For reprocessing, the handler must return `amqpadapter.Retry(err)`.

After `MaxDuration` is exhausted (`expired` header) or a non-retryable error, `WithFailHandler` is called and the message is acked (removed from the queue).

```go
q, err := mq.New(conf,
	mq.WithFailHandler(func(job mq.FailedJob) error {
		log.Printf("failed %s: %v", job.Queue, job.Err)
		return nil
	}),
)

// in handler:
return mq.Retry(err) // delay and retry
return err           // final fail (in retry mode)
```

`WithFailHandler` is required if at least one queue has `Retry` set.

## Consumers

```go
_ = q.AddConsumer(ctx, "jobs", handler)      // 1 worker
_ = q.AddConsumerN(ctx, "jobs", 4, handler) // 4 connections/workers
_ = q.InitConsumer(ctx)                     // start read loops
```

`AddConsumer*` is only allowed before `InitConsumer`. Prefetch: QoS = 1 per channel.

`AddConsumerN(N)` creates **N separate AMQP connections and channels** (one client per worker). That gives good parallelism and failure isolation, at the cost of more TCP connections to RabbitMQ compared to a single connection with N goroutines reading one channel.

## Shutdown

`Shutdown` closes connections first (stops intake), then waits for in-flight handlers (`inflight`) within the context deadline.

## Options

- `WithLogger` — custom logger (otherwise noop); `*slog.Logger` satisfies the interface:

```go
import "log/slog"

q, err := mq.New(conf, mq.WithLogger(slog.Default()))
```

- `WithFailHandler` — permanent failures in retry mode
- `WithPublishHeadersBuilder` — headers from `context` on publish
- `WithConsumeHeadersExtractor` — restore `context` from headers

Example with otel:

```go
import "github.com/ivan-makarenkov/amqp-adapter/otel"

cfg := otel.PropagationConfig{
	CorrelationIDKey: "x-correlation-id",
	TraceKeys:        []string{"traceparent", "tracestate"},
	// GetCorrelationID / SetCorrelationID / Propagator — as needed
}

q, err := mq.New(conf,
	mq.WithPublishHeadersBuilder(otel.NewPublishHeadersBuilder(cfg)),
	mq.WithConsumeHeadersExtractor(otel.NewConsumeHeadersExtractor(cfg)),
)
```

## Tests

```bash
go test ./...                  # unit
go test -short ./...           # skip integration
# functional (needs RabbitMQ), see tests/ and Makefile
```
