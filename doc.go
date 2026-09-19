// Package amqpadapter provides a production-oriented RabbitMQ adapter for Go.
//
// It provides publishing and consuming of RabbitMQ messages with automatic
// connection recovery, publisher confirms, concurrent consumers, delayed
// retries using RabbitMQ delay queues or dead-letter exchanges, graceful
// shutdown, context propagation, correlation IDs and OpenTelemetry support.
//
// The package is intended for Go services that need reliable RabbitMQ
// message processing without implementing connection recovery and retry
// infrastructure in application code.
package amqpadapter
