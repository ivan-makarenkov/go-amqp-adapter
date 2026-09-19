.DEFAULT_GOAL := help

.PHONY: help
help: ## Available commands
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[0;33m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""

##@ Targets

.PHONY: test
test: ## Run unit tests (short mode)
	go test -v -short ./...

.PHONY: test-functional
test-functional: ## Start RabbitMQ (docker compose) and run functional tests
	@set -e; \
	COMPOSE_STARTED=0; \
	if ! docker compose -f tests/docker-compose.yml ps --status running 2>/dev/null | grep -q rabbitmq; then \
		docker compose -f tests/docker-compose.yml up -d --wait; \
		COMPOSE_STARTED=1; \
	fi; \
	trap 'if [ "$$COMPOSE_STARTED" = "1" ]; then docker compose -f tests/docker-compose.yml down -v; fi' EXIT; \
	MQ_TEST_SKIP_COMPOSE=1 go test -C tests -v -count=1 -p 1 ./...

.PHONY: test-functional-down
test-functional-down: ## Stop RabbitMQ from tests/docker-compose.yml
	docker compose -f tests/docker-compose.yml down -v

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./... --config .golangci.yml

.PHONY: examples
examples: ## Build example programs
	@set -e; \
	for p in basic-consumer publisher-confirms retry multiple-consumers failed-jobs graceful-shutdown; do \
		go -C examples build -o /dev/null ./$$p; \
	done

.PHONY: format
format: ## Format with goimports
	goimports -l -w .
