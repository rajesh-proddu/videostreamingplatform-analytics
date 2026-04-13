"""Unit tests for watch_history_ingest event parsing logic."""

import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp

from src.watch_history_ingest import WATCH_EVENT_SCHEMA


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-watch-ingest")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _make_event(event_type: str, video_id: str = "v1", user_id: str = "u1",
                session_id: str = "s1", bytes_read: int = 0,
                version: str = "1.0") -> str:
    return json.dumps({
        "version": version,
        "type": event_type,
        "timestamp": "2025-01-15T10:30:00Z",
        "payload": {
            "video_id": video_id,
            "user_id": user_id,
            "session_id": session_id,
            "bytes_read": bytes_read,
        },
    })


class TestEventDeserialization:
    def test_started_event(self, spark: SparkSession):
        raw = spark.createDataFrame(
            [(_make_event("watch.started"),)], ["json_str"]
        )
        parsed = (
            raw.select(from_json(col("json_str"), WATCH_EVENT_SCHEMA).alias("evt"))
            .select(
                col("evt.type").alias("event_type"),
                col("evt.payload.video_id").alias("video_id"),
                col("evt.payload.bytes_read").alias("bytes_read"),
            )
        )
        row = parsed.collect()[0]
        assert row["event_type"] == "watch.started"
        assert row["video_id"] == "v1"
        assert row["bytes_read"] == 0

    def test_completed_event(self, spark: SparkSession):
        raw = spark.createDataFrame(
            [(_make_event("watch.completed", bytes_read=1048576),)], ["json_str"]
        )
        parsed = (
            raw.select(from_json(col("json_str"), WATCH_EVENT_SCHEMA).alias("evt"))
            .select(
                col("evt.type").alias("event_type"),
                col("evt.payload.bytes_read").alias("bytes_read"),
            )
        )
        row = parsed.collect()[0]
        assert row["event_type"] == "watch.completed"
        assert row["bytes_read"] == 1048576

    def test_version_filter(self, spark: SparkSession):
        events = [
            (_make_event("watch.started", version="1.0"),),
            (_make_event("watch.started", version="2.0"),),
        ]
        raw = spark.createDataFrame(events, ["json_str"])
        parsed = (
            raw.select(from_json(col("json_str"), WATCH_EVENT_SCHEMA).alias("evt"))
            .filter(col("evt.version") == "1.0")
        )
        assert parsed.count() == 1
