# Video Streaming Platform — Analytics

Data pipelines for the video streaming platform.

## Components

### Kafka → Elasticsearch Consumer
Reads video lifecycle events from the `video-events` Kafka topic and syncs them to the Elasticsearch `videos` index.

- **Events handled**: `video.created` (index), `video.updated` (upsert), `video.deleted` (delete)
- **Consumer group**: `kafka-es-consumer`
- **Exactly-once**: Manual commit after successful ES write

### Spark Watch History Ingestion
PySpark Structured Streaming job that reads watch events from the `watch-events` Kafka topic and persists them to Apache Iceberg.

- **Events handled**: `watch.started`, `watch.completed`
- **Output**: Iceberg table `watch_history`, partitioned by `days(event_ts)`
- **Semantics**: Append mode with checkpointing

## Development

### Prerequisites
- Python 3.11+
- Docker (for Kafka, ES, Spark)
- Start shared infra: `cd ../videostreamingplatform-infra && make up`

### Install dependencies
```bash
pip install -r kafka-es-consumer/requirements.txt
pip install -r spark/requirements.txt
pip install ruff pytest
```

### Run tests
```bash
make test
```

### Lint
```bash
make lint
```

### Build Docker images
```bash
make build
```

## Configuration

### Kafka→ES Consumer
| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_BROKERS | localhost:9092 | Kafka bootstrap servers |
| KAFKA_VIDEO_TOPIC | video-events | Topic to consume from |
| KAFKA_GROUP_ID | kafka-es-consumer | Consumer group ID |
| ELASTICSEARCH_URL | http://localhost:9200 | Elasticsearch URL |
| ES_VIDEO_INDEX | videos | ES index name |

### Spark Ingestion
| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_BROKERS | localhost:9092 | Kafka bootstrap servers |
| KAFKA_WATCH_TOPIC | watch-events | Topic to consume from |
| ICEBERG_CATALOG | hadoop_catalog | Iceberg catalog name |
| ICEBERG_WAREHOUSE | /tmp/iceberg-warehouse | Warehouse path |
