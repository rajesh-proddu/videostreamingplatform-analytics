"""Unit tests for WatchHistoryConsumer parsing logic."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from src.consumer import WatchHistoryConsumer


def _make_event(
    event_type: str = "watch.started",
    video_id: str = "v1",
    user_id: str = "u1",
    session_id: str = "s1",
    bytes_read: int = 0,
    version: str = "1.0",
) -> str:
    return json.dumps({
        "version": version,
        "type": event_type,
        "timestamp": "2025-01-15T10:30:00+00:00",
        "payload": {
            "video_id": video_id,
            "user_id": user_id,
            "session_id": session_id,
            "bytes_read": bytes_read,
        },
    })


def _mock_msg(value: str):
    msg = MagicMock()
    msg.value.return_value = value.encode("utf-8")
    msg.offset.return_value = 0
    msg.error.return_value = None
    return msg


class TestParseMessage:
    """Tests for _parse_message without Kafka/Iceberg connections."""

    @pytest.fixture
    def consumer(self, monkeypatch):
        monkeypatch.setattr(
            "src.consumer.WatchHistoryConsumer.__init__",
            lambda self, **kwargs: None,
        )
        c = WatchHistoryConsumer.__new__(WatchHistoryConsumer)
        c.running = True
        c.batch = []
        return c

    def test_started_event(self, consumer):
        msg = _mock_msg(_make_event("watch.started"))
        record = consumer._parse_message(msg)
        assert record is not None
        assert record["event_type"] == "watch.started"
        assert record["video_id"] == "v1"
        assert record["user_id"] == "u1"
        assert record["bytes_read"] == 0

    def test_completed_event_with_bytes(self, consumer):
        msg = _mock_msg(_make_event("watch.completed", bytes_read=1048576))
        record = consumer._parse_message(msg)
        assert record["event_type"] == "watch.completed"
        assert record["bytes_read"] == 1048576

    def test_skips_wrong_version(self, consumer):
        msg = _mock_msg(_make_event(version="2.0"))
        record = consumer._parse_message(msg)
        assert record is None

    def test_skips_missing_video_id(self, consumer):
        msg = _mock_msg(_make_event(video_id=""))
        record = consumer._parse_message(msg)
        assert record is None

    def test_skips_missing_user_id(self, consumer):
        msg = _mock_msg(_make_event(user_id=""))
        record = consumer._parse_message(msg)
        assert record is None

    def test_event_timestamp_parsed(self, consumer):
        msg = _mock_msg(_make_event())
        record = consumer._parse_message(msg)
        assert record["event_ts"] == datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc)

    def test_ingested_at_is_now(self, consumer):
        msg = _mock_msg(_make_event())
        record = consumer._parse_message(msg)
        assert isinstance(record["ingested_at"], datetime)
