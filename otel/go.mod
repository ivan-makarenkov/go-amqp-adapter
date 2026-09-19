module github.com/ivan-makarenkov/go-amqp-adapter/otel

go 1.22.0

require (
	github.com/ivan-makarenkov/go-amqp-adapter v0.0.0
	go.opentelemetry.io/otel v1.35.0
)

require (
	github.com/rabbitmq/amqp091-go v1.14.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
)

replace github.com/ivan-makarenkov/go-amqp-adapter => ../
