"""
Kafka → Iceberg consumer for watch history events.

Reads from the 'watch-events' Kafka topic and appends records
to an Apache Iceberg watch_history table via AWS Glue Catalog.

Local dev: uses LocalStack Glue + S3 (set GLUE_ENDPOINT, S3_ENDPOINT)
AWS:       uses real AWS Glue + S3 (IAM credentials via IRSA/instance profile)

Prerequisites:
  - Glue database + Iceberg table created via catalog-admin
  - S3 warehouse bucket exists
"""

import json
import logging
import os
import signal
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, KafkaException
from pyiceberg.catalog import load_catalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("watch-history-consumer")

ARROW_SCHEMA = pa.schema([
    ("event_type", pa.string()),
    ("video_id", pa.string()),
    ("user_id", pa.string()),
    ("session_id", pa.string()),
    ("bytes_read", pa.int64()),
    ("event_ts", pa.timestamp("us", tz="UTC")),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])

BATCH_SIZE = 100


def _build_catalog(catalog_name: str) -> object:
    """Build an Iceberg catalog from environment variables.

    Supports two catalog types:
      - "rest"  — Iceberg REST catalog (local dev via tabulario/iceberg-rest + MinIO)
      - "glue"  — AWS Glue catalog (production, or LocalStack Pro for local dev)

    Set ICEBERG_CATALOG_TYPE to select. Defaults to "glue" for backward compat.
    """
    catalog_type = os.getenv("ICEBERG_CATALOG_TYPE", "glue")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse/")
    s3_endpoint = os.getenv("S3_ENDPOINT", "")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    if catalog_type == "rest":
        rest_uri = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
        catalog_props = {
            "type": "rest",
            "uri": rest_uri,
            "warehouse": warehouse,
            "s3.region": aws_region,
        }
        if s3_endpoint:
            catalog_props["s3.endpoint"] = s3_endpoint
            catalog_props["s3.access-key-id"] = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
            catalog_props["s3.secret-access-key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
            catalog_props["s3.path-style-access"] = "true"
        return load_catalog(catalog_name, **catalog_props)

    # Glue catalog (default)
    glue_endpoint = os.getenv("GLUE_ENDPOINT", "")
    catalog_props = {
        "type": "glue",
        "warehouse": warehouse,
        "glue.region": aws_region,
    }

    if glue_endpoint:
        catalog_props["glue.endpoint"] = glue_endpoint
        catalog_props["glue.access-key-id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
        catalog_props["glue.secret-access-key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    if s3_endpoint:
        catalog_props["s3.endpoint"] = s3_endpoint
        catalog_props["s3.access-key-id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
        catalog_props["s3.secret-access-key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
        catalog_props["s3.path-style-access"] = "true"

    return load_catalog(catalog_name, **catalog_props)


class WatchHistoryConsumer:
    """Consumes watch events from Kafka and writes to an Iceberg table."""

    def __init__(
        self,
        kafka_brokers: str,
        kafka_topic: str,
        kafka_group_id: str,
        catalog_name: str,
        table_name: str,
    ):
        self.topic = kafka_topic
        self.table_name = table_name
        self.running = True
        self.batch: list[dict] = []

        self.consumer = Consumer({
            "bootstrap.servers": kafka_brokers,
            "group.id": kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

        self.catalog = _build_catalog(catalog_name)

        # Table must already exist (created by catalog-admin)
        self.table = self.catalog.load_table(self.table_name)
        logger.info(f"Loaded table: {self.table_name} (location: {self.table.metadata.location})")

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received, flushing batch and stopping...")
        self.running = False

    def start(self):
        """Subscribe and start consuming events."""
        self.consumer.subscribe([self.topic])
        logger.info(f"Subscribed to topic: {self.topic}")

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    if self.batch:
                        self._flush_batch()
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    record = self._parse_message(msg)
                    if record:
                        self.batch.append(record)
                    if len(self.batch) >= BATCH_SIZE:
                        self._flush_batch()
                        self.consumer.commit(asynchronous=False)
                except Exception:
                    logger.exception(f"Failed to process message at offset {msg.offset()}")
        finally:
            if self.batch:
                self._flush_batch()
            self.consumer.close()
            logger.info("Consumer closed")

    def _parse_message(self, msg) -> dict | None:
        """Parse a Kafka message into a record dict."""
        value = json.loads(msg.value().decode("utf-8"))
        version = value.get("version", "")
        if version != "1.0":
            logger.debug(f"Skipping event with version {version}")
            return None

        event_type = value.get("type", "")
        payload = value.get("payload", {})
        video_id = payload.get("video_id", "")
        user_id = payload.get("user_id", "")

        if not video_id or not user_id:
            logger.warning(f"Skipping event missing video_id or user_id: {value}")
            return None

        event_ts = datetime.fromisoformat(
            value.get("timestamp", datetime.now(timezone.utc).isoformat())
        )

        return {
            "event_type": event_type,
            "video_id": video_id,
            "user_id": user_id,
            "session_id": payload.get("session_id", ""),
            "bytes_read": payload.get("bytes_read", 0),
            "event_ts": event_ts,
            "ingested_at": datetime.now(timezone.utc),
        }

    def _flush_batch(self):
        """Write accumulated records to the Iceberg table."""
        if not self.batch:
            return

        table = pa.Table.from_pylist(self.batch, schema=ARROW_SCHEMA)
        self.table.append(table)
        logger.info(f"Flushed {len(self.batch)} records to {self.table_name}")
        self.batch.clear()


def main():
    consumer = WatchHistoryConsumer(
        kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_WATCH_TOPIC", "watch-events"),
        kafka_group_id=os.getenv("KAFKA_GROUP_ID", "watch-history-consumer"),
        catalog_name=os.getenv("ICEBERG_CATALOG_NAME", "glue"),
        table_name=os.getenv("ICEBERG_TABLE", "analytics.watch_history"),
    )
    consumer.start()


if __name__ == "__main__":
    main()
