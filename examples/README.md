# Examples

Runnable programs for [go-amqp-adapter](https://github.com/ivan-makarenkov/go-amqp-adapter). Each directory is a standalone `main` you can copy into an application.

## Run

RabbitMQ:

```bash
docker compose -f examples/docker-compose.yml up -d
```

From this directory:

```bash
go run ./basic-consumer
go run ./publisher-confirms
go run ./retry
go run ./multiple-consumers
go run ./failed-jobs
go run ./graceful-shutdown
```

Override the broker with `AMQP_URL` (default `amqp://guest:guest@localhost:5672/`).

## What each example shows

| Directory | Topic |
|-----------|--------|
| [basic-consumer](basic-consumer) | `New` → `AddConsumer` → `InitConsumer` → `Publish` |
| [publisher-confirms](publisher-confirms) | `Publish` waits for broker ack; one in-flight confirm per connection |
| [retry](retry) | `RetryConfig`, `mq.Retry(err)` vs a plain error, `WithFailHandler` |
| [multiple-consumers](multiple-consumers) | `AddConsumerN` (N connections) plus a second queue |
| [failed-jobs](failed-jobs) | `WithFailHandler` persist + republish (in-memory stand-in for go-failedjobs) |
| [graceful-shutdown](graceful-shutdown) | `Shutdown` stops intake, then waits for in-flight handlers |

## failed-jobs and go-failedjobs

The adapter calls `WithFailHandler` when retries are exhausted or the handler returns a non-retryable error. The example keeps those jobs in memory and exposes `GET /failed` / `POST /retry?id=`.

In production, pass the same payload to [go-failedjobs](https://github.com/ivan-makarenkov/go-failedjobs):

```go
save := failedSrv.GetFailedJobHandler()

mq.WithFailHandler(func(job mq.FailedJob) error {
    errMsg := ""
    if job.Err != nil {
        errMsg = job.Err.Error()
    }
    return save(string(job.Queue), string(job.Body), errMsg)
})
```

`POST /retry-task?task=<id>` on the Echo route registered by go-failedjobs republishes stored rows to RabbitMQ.
