.PHONY: lint test build-consumer build-spark build help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint: ## Run ruff linter
	ruff check .

test: ## Run all tests
	pytest -v

build-consumer: ## Build Kafka→ES consumer Docker image
	docker build -t videostreamingplatform-kafka-es-consumer:latest kafka-es-consumer/

build-spark: ## Build Spark ingestion Docker image
	docker build -t videostreamingplatform-spark-ingest:latest spark/

build: build-consumer build-spark ## Build all Docker images
