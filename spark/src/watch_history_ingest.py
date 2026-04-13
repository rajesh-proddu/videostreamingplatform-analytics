"""
Spark Structured Streaming job that reads watch events from Kafka
and writes them to an Apache Iceberg watch_history table.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

WATCH_EVENT_SCHEMA = StructType([
    StructField("version", StringType(), False),
    StructField("type", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("payload", StructType([
        StructField("video_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("bytes_read", LongType(), False),
    ]), False),
])


def create_spark_session(app_name: str = "WatchHistoryIngest") -> SparkSession:
    """Create a Spark session with Iceberg catalog configuration."""
    catalog = os.getenv("ICEBERG_CATALOG", "local")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "/tmp/iceberg-warehouse")

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .getOrCreate()
    )


def ensure_table_exists(spark: SparkSession, catalog: str, table: str) -> None:
    """Create the watch_history Iceberg table if it does not exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{table} (
            event_type    STRING,
            video_id      STRING,
            user_id       STRING,
            session_id    STRING,
            bytes_read    BIGINT,
            event_ts      TIMESTAMP,
            ingested_at   TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
    """)


def run(
    kafka_brokers: str | None = None,
    kafka_topic: str | None = None,
    catalog: str | None = None,
    table: str = "default.watch_history",
    checkpoint: str | None = None,
) -> None:
    """Main entry point for the streaming job."""
    kafka_brokers = kafka_brokers or os.getenv("KAFKA_BROKERS", "localhost:9092")
    kafka_topic = kafka_topic or os.getenv("KAFKA_WATCH_TOPIC", "watch-events")
    catalog = catalog or os.getenv("ICEBERG_CATALOG", "local")
    checkpoint = checkpoint or os.getenv(
        "CHECKPOINT_DIR", "/tmp/watch-history-checkpoint"
    )

    spark = create_spark_session()
    ensure_table_exists(spark, catalog, table)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), WATCH_EVENT_SCHEMA).alias("evt"))
        .select(
            col("evt.type").alias("event_type"),
            col("evt.payload.video_id").alias("video_id"),
            col("evt.payload.user_id").alias("user_id"),
            col("evt.payload.session_id").alias("session_id"),
            col("evt.payload.bytes_read").alias("bytes_read"),
            to_timestamp(col("evt.timestamp")).alias("event_ts"),
            current_timestamp().alias("ingested_at"),
        )
        .filter(col("evt.version") == "1.0")
    )

    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .toTable(f"{catalog}.{table}")
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
