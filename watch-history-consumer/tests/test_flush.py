"""Tests for WatchHistoryConsumer._flush_batch."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from src.consumer import WatchHistoryConsumer


@pytest.fixture
def consumer(monkeypatch):
    """Create a WatchHistoryConsumer with __init__ bypassed."""
    monkeypatch.setattr(
        "src.consumer.WatchHistoryConsumer.__init__",
        lambda self, **kwargs: None,
    )
    c = WatchHistoryConsumer.__new__(WatchHistoryConsumer)
    c.table = MagicMock()
    c.table_name = "analytics.watch_history"
    c.batch = []
    c.consumer = MagicMock()
    c.running = True
    return c


def _sample_record():
    return {
        "event_type": "watch.started",
        "video_id": "v1",
        "user_id": "u1",
        "session_id": "s1",
        "bytes_read": 0,
        "event_ts": datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc),
        "ingested_at": datetime.now(timezone.utc),
    }


class TestFlushBatch:
    def test_flush_batch_appends_to_iceberg(self, consumer):
        consumer.batch = [_sample_record()]
        consumer._flush_batch()
        consumer.table.append.assert_called_once()
        arrow_table = consumer.table.append.call_args[0][0]
        assert arrow_table.num_rows == 1

    def test_flush_batch_clears_batch(self, consumer):
        consumer.batch = [_sample_record(), _sample_record()]
        consumer._flush_batch()
        assert consumer.batch == []

    def test_flush_batch_empty_noop(self, consumer):
        consumer.batch = []
        consumer._flush_batch()
        consumer.table.append.assert_not_called()
