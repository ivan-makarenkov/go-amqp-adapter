package functional

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultRabbitURL = "amqp://guest:guest@127.0.0.1:5672/"

var composeDir = func() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}

	return filepath.Dir(file)
}()

func rabbitURL() string {
	if url := os.Getenv("MQ_TEST_URL"); url != "" {
		return url
	}

	return defaultRabbitURL
}

func requireFunctional(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping functional test in short mode")
	}

	conn, err := amqp.Dial(rabbitURL())
	if err != nil {
		t.Fatalf("RabbitMQ unavailable (%s): %v", rabbitURL(), err)
	}

	_ = conn.Close()
}

func requireDocker(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found in PATH")
	}
}

func uniqueQueueName(t *testing.T, suffix string) mq.QueueName {
	t.Helper()

	safe := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())

	return mq.QueueName(fmt.Sprintf("func_%s_%s_%d", safe, suffix, time.Now().UnixNano()))
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("wait timeout: %s", msg)
}

func waitRabbitReady(t *testing.T, timeout time.Duration) {
	t.Helper()

	waitUntil(t, timeout, func() bool {
		conn, err := amqp.Dial(rabbitURL())
		if err != nil {
			return false
		}

		_ = conn.Close()

		return true
	}, "RabbitMQ readiness")
}

type tLogger struct {
	t *testing.T
}

func (l tLogger) Info(msg string, args ...any)                                 { l.t.Logf("INFO: "+msg, args...) }
func (l tLogger) Warn(msg string, args ...any)                                 { l.t.Logf("WARN: "+msg, args...) }
func (l tLogger) Debug(msg string, args ...any)                                { l.t.Logf("DEBUG: "+msg, args...) }
func (l tLogger) Error(msg string, args ...any)                                { l.t.Logf("ERROR: "+msg, args...) }
func (l tLogger) InfoContext(_ context.Context, msg string, args ...any)       { l.Info(msg, args...) }
func (l tLogger) WarnContext(_ context.Context, msg string, args ...any)       { l.Warn(msg, args...) }
func (l tLogger) DebugContext(_ context.Context, msg string, args ...any)      { l.Debug(msg, args...) }
func (l tLogger) ErrorContext(_ context.Context, msg string, args ...any)      { l.Error(msg, args...) }

type testQueueOptions struct {
	failHandler mq.FailJobHandler
}

func newTestQueue(t *testing.T, queues map[mq.QueueName]mq.QueueItem, opts testQueueOptions) mq.Queue {
	t.Helper()

	failHandler := opts.failHandler
	if failHandler == nil {
		failHandler = func(job mq.FailedJob) error {
			t.Logf("fail job: queue=%s body=%q err=%v", job.Queue, job.Body, job.Err)

			return nil
		}
	}

	needsFailHandler := false
	for _, item := range queues {
		if item.Retry != nil {
			needsFailHandler = true
			break
		}
	}

	mqOpts := []mq.Option{mq.WithLogger(tLogger{t: t})}
	if needsFailHandler {
		mqOpts = append(mqOpts, mq.WithFailHandler(failHandler))
	}

	conf := mq.Config{
		URL:            rabbitURL(),
		ReconnectDelay: 500 * time.Millisecond,
		ReInitDelay:    200 * time.Millisecond,
		ResendDelay:    100 * time.Millisecond,
		QueueParams:    queues,
	}

	queue, err := mq.New(conf, mqOpts...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := queue.Shutdown(shutdownCtx); err != nil {
			t.Logf("Shutdown() error = %v", err)
		}
	})

	return queue
}

func publishUntilSuccess(t *testing.T, ctx context.Context, queue mq.Queue, name mq.QueueName, msg mq.PublishMessage, timeout time.Duration) {
	t.Helper()

	var lastErr error

	waitUntil(t, timeout, func() bool {
		lastErr = queue.Publish(ctx, name, msg)
		return lastErr == nil
	}, "successful publish")

	if lastErr != nil {
		t.Fatalf("Publish() failed: %v", lastErr)
	}
}

func runCompose(t *testing.T, args ...string) {
	t.Helper()

	cmd := exec.Command("docker", append([]string{"compose", "-f", "docker-compose.yml"}, args...)...)
	cmd.Dir = composeDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func composeUp() error {
	cmd := exec.Command("docker", "compose", "-f", "docker-compose.yml", "up", "-d", "--wait")
	cmd.Dir = composeDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}

	return nil
}

func composeDown() error {
	cmd := exec.Command("docker", "compose", "-f", "docker-compose.yml", "down", "-v")
	cmd.Dir = composeDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}

	return nil
}
