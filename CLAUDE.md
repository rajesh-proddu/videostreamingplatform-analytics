# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests (from repo root)
pytest -v

# Run tests for a single component
pytest -v kafka-es-consumer/tests/
pytest -v watch-history-consumer/tests/
pytest -v catalog-admin/tests/

# Run a single test file
pytest -v kafka-es-consumer/tests/test_consumer.py

# Lint (ruff, line-length=120, rules E/F/I/W)
make lint        # ruff check .

# Build Docker images
make build-consumer        # kafka-es-consumer image
make build-watch-history   # watch-history-consumer image
```

## Python Import Quirk

Directory names use hyphens (`kafka-es-consumer`, `watch-history-consumer`, `catalog-admin`) which are not valid Python identifiers. The root `conftest.py` registers these as importable under underscored names at test time:

| Directory | Importable as |
|-----------|--------------|
| `kafka-es-consumer/` | `kafka_es_consumer` |
| `watch-history-consumer/` | `watch_history_consumer` |
| `catalog-admin/` | `catalog_admin` |

Tests in `kafka-es-consumer/tests/` import via the full mapped path (e.g. `from kafka_es_consumer.src.consumer import VideoEventConsumer`). Tests in `watch-history-consumer/tests/` use a relative path (`from src.consumer import ...`) because `watch-history-consumer/src/` has `__init__.py`. Keep both patterns consistent when adding new tests.

## Architecture

### Three Components

**1. `kafka-es-consumer/`** — Syncs video lifecycle events to Elasticsearch.
- Reads `video-events` Kafka topic using `confluent-kafka` (not kafka-go).
- Handles `video.created` / `VIDEO_CREATED`, `video.updated` / `VIDEO_UPDATED`, `video.deleted` / `VIDEO_DELETED` (both snake_case and UPPER variants are accepted).
- **Exactly-once**: `enable.auto.commit=False`, commits manually after each successful ES write.
- Single entry point: `VideoEventConsumer` class + `main()` in `src/consumer.py`.

**2. `watch-history-consumer/`** — Writes watch events to Apache Iceberg via AWS Glue catalog.
- Reads `watch-events` Kafka topic; only processes events with `version == "1.0"` (others skipped).
- Buffers records in `self.batch` and flushes every `BATCH_SIZE=100` records (or on idle poll) using PyArrow + PyIceberg `table.append()`.
- Commits Kafka offsets after each successful flush (batch-level exactly-once).
- Local dev vs AWS is determined by `GLUE_ENDPOINT` env var (LocalStack when set, real AWS Glue when absent).
- Table must already exist before the consumer starts — created by `catalog-admin`.

**3. `catalog-admin/`** — Iceberg table lifecycle CLI. Run as a K8s Job (initial setup) or CronJob (compaction).
- `python catalog-admin/admin.py create-tables` — creates `analytics.watch_history` if absent.
- `python catalog-admin/admin.py compact analytics.watch_history` — rewrites small parquet files (weekly CronJob in k8s).
- `python catalog-admin/admin.py expire-snapshots analytics.watch_history` — prunes old snapshots.
- `python catalog-admin/admin.py list` / `describe <table>` — inspection.
- Table: `analytics.watch_history`, parquet/zstd, 128MB target files, partitioned by `DayTransform(event_ts)`.

### Environment Variables

| Variable | Default | Used by |
|----------|---------|---------|
| `KAFKA_BROKERS` | `localhost:9092` | both consumers |
| `KAFKA_VIDEO_TOPIC` | `video-events` | kafka-es-consumer |
| `KAFKA_WATCH_TOPIC` | `watch-events` | watch-history-consumer |
| `KAFKA_GROUP_ID` | component-specific | both consumers |
| `ELASTICSEARCH_URL` | `http://localhost:9200` | kafka-es-consumer |
| `ES_VIDEO_INDEX` | `videos` | kafka-es-consumer |
| `ICEBERG_CATALOG_NAME` | `glue` | watch-history-consumer, catalog-admin |
| `ICEBERG_WAREHOUSE` | `s3://iceberg-warehouse/` | both iceberg components |
| `ICEBERG_TABLE` | `analytics.watch_history` | watch-history-consumer |
| `GLUE_ENDPOINT` | `` (empty=AWS) | both iceberg components |
| `S3_ENDPOINT` | `` (empty=AWS) | both iceberg components |
| `AWS_REGION` | `us-east-1` | both iceberg components |

### Infrastructure Dependency

Start shared infra first (from sibling repo):
```bash
cd ../videostreamingplatform-infra && make up
```
This brings up Kafka (KRaft), LocalStack (Glue + S3), and pgvector.

### Kubernetes

`k8s/` contains three manifests:
- `catalog-admin.yaml` — K8s Job for `create-tables` + weekly CronJob for compaction.
- `kafka-es-consumer.yaml` — Deployment in `analytics` namespace.
- `watch-history-consumer.yaml` — Deployment in `analytics` namespace.

The `catalog-admin` Job must complete successfully before either consumer Deployment starts (Iceberg table must exist).
