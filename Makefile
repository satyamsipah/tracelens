COMPOSE := docker compose -f deploy/docker-compose.yml
GO      ?= go

# Container-backed tests need a real ClickHouse and a real Redpanda, so they
# are slow by design. -short skips them for a fast inner loop.
TEST_TIMEOUT ?= 20m

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## Bring the whole stack up and wait for health
	$(COMPOSE) up -d --build --wait
	@echo
	@echo "collector   OTLP gRPC :4317  OTLP HTTP :4318  metrics :9464"
	@echo "assembler   metrics   :9465"
	@echo "clickhouse  http      :8123  native    :9000"
	@echo "prometheus  :9090     grafana :3000 (anonymous admin)"

.PHONY: down
down: ## Tear the stack down and remove volumes
	$(COMPOSE) down -v --remove-orphans

.PHONY: logs
logs: ## Follow logs for the tracelens services
	$(COMPOSE) logs -f collector assembler

.PHONY: test
test: ## Run unit tests only (no containers)
	$(GO) test -short ./...

.PHONY: test-race
test-race: ## Run the full suite under the race detector, with containers
	$(GO) test -race -timeout $(TEST_TIMEOUT) ./...

.PHONY: bench
bench: ## Run hot-path benchmarks with allocation counts
	$(GO) test -run '^$$' -bench . -benchmem ./internal/ingest/...

.PHONY: lint
lint: ## Run golangci-lint (and gofmt as a fallback if it is absent)
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; running gofmt + vet instead"; \
		test -z "$$(gofmt -l ./cmd ./demo ./internal)" || { gofmt -l ./cmd ./demo ./internal; exit 1; }; \
		$(GO) vet ./...; \
	fi

.PHONY: migrate
migrate: ## Apply ClickHouse migrations against a running stack
	TRACELENS_CLICKHOUSE_ADDR=localhost:9000 \
	TRACELENS_CLICKHOUSE_PASSWORD=tracelens \
	$(GO) run ./cmd/migrate

.PHONY: loadgen
loadgen: ## Generate synthetic OTLP traffic against the running collector
	TRACELENS_OTLP_ENDPOINT=localhost:4317 \
	$(GO) run ./cmd/loadgen -rate $(or $(RATE),2000) -duration $(or $(DURATION),20s)

.PHONY: spans
spans: ## Show what has landed in ClickHouse
	@docker exec tracelens-clickhouse clickhouse-client --password tracelens --query \
		"SELECT service_name, count() AS spans, round(avg(duration_ns)/1e6, 2) AS avg_ms \
		 FROM tracelens.spans GROUP BY service_name ORDER BY spans DESC FORMAT PrettyCompact"

.PHONY: compression
compression: ## Report the measured compression ratio per table
	@docker exec tracelens-clickhouse clickhouse-client --password tracelens --query \
		"SELECT table, \
		        sum(rows) AS total_rows, \
		        formatReadableSize(sum(data_uncompressed_bytes)) AS raw, \
		        formatReadableSize(sum(data_compressed_bytes))   AS compressed, \
		        round(sum(data_uncompressed_bytes) / \
		              greatest(sum(data_compressed_bytes), 1), 2) AS ratio, \
		        round(sum(data_compressed_bytes) / greatest(sum(rows), 1), 2) AS bytes_per_row \
		 FROM system.parts \
		 WHERE database='tracelens' AND active \
		 GROUP BY table HAVING total_rows > 0 \
		 ORDER BY sum(data_uncompressed_bytes) DESC FORMAT PrettyCompact"
	@echo
	@echo "Per-column breakdown (reports zeros on builds that do not populate"
	@echo "column-level accounting -- the table-level figures above are authoritative):"
	@docker exec tracelens-clickhouse clickhouse-client --password tracelens --query \
		"SELECT column, \
		        formatReadableSize(sum(column_data_uncompressed_bytes)) AS raw, \
		        formatReadableSize(sum(column_data_compressed_bytes))   AS compressed \
		 FROM system.parts_columns \
		 WHERE database='tracelens' AND table='spans' AND active \
		 GROUP BY column \
		 ORDER BY sum(column_data_uncompressed_bytes) DESC LIMIT 10 FORMAT PrettyCompact"

.PHONY: build
build: ## Build every binary
	$(GO) build ./...
