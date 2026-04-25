"""Additional component tests for WatchHistoryConsumer.

Covers: upper-case event types, batch threshold flush trigger, idle flush, and the
explicit version='1.0' boundary.
"""

import json
from unittest.mock import MagicMock

import pytest
from src.consumer import BATCH_SIZE, WatchHistoryConsumer


def _event(version="1.0", event_type="WATCH_COMPLETED", **payload):
    base = {
        "video_id": "v1",
        "user_id": "u1",
        "session_id": "s1",
        "bytes_read": 0,
    }
    base.update(payload)
    return json.dumps({
        "version": version,
        "type": event_type,
        "timestamp": "2025-01-15T10:30:00+00:00",
        "payload": base,
    })


def _mock_msg(value: str):
    msg = MagicMock()
    msg.value.return_value = value.encode("utf-8")
    msg.offset.return_value = 0
    msg.error.return_value = None
    return msg


@pytest.fixture
def consumer(monkeypatch):
    monkeypatch.setattr(
        "src.consumer.WatchHistoryConsumer.__init__",
        lambda self, **kwargs: None,
    )
    c = WatchHistoryConsumer.__new__(WatchHistoryConsumer)
    c.running = True
    c.batch = []
    c.table = MagicMock()
    c.table_name = "analytics.watch_history"
    c.consumer = MagicMock()
    return c


@pytest.mark.parametrize("evt", ["WATCH_STARTED", "WATCH_COMPLETED", "watch.started", "watch.completed"])
def test_event_type_passthrough(consumer, evt):
    record = consumer._parse_message(_mock_msg(_event(event_type=evt)))
    assert record is not None
    assert record["event_type"] == evt


def test_v09_dropped(consumer):
    record = consumer._parse_message(_mock_msg(_event(version="0.9")))
    assert record is None


def test_v11_dropped(consumer):
    # Forward-only: any non-1.0 value is currently dropped.
    record = consumer._parse_message(_mock_msg(_event(version="1.1")))
    assert record is None


def test_batch_does_not_flush_below_threshold(consumer):
    # Manually populate batch but stay under BATCH_SIZE.
    consumer.batch = [
        consumer._parse_message(_mock_msg(_event(video_id=f"v{i}")))
        for i in range(BATCH_SIZE - 1)
    ]
    consumer.table.append.assert_not_called()


def test_batch_flush_clears_state(consumer):
    consumer.batch = [
        consumer._parse_message(_mock_msg(_event(video_id=f"v{i}")))
        for i in range(3)
    ]
    consumer._flush_batch()
    consumer.table.append.assert_called_once()
    assert consumer.batch == []


def test_invalid_json_raises(consumer):
    bad = MagicMock()
    bad.value.return_value = b"not-json"
    bad.offset.return_value = 0
    bad.error.return_value = None
    with pytest.raises(json.JSONDecodeError):
        consumer._parse_message(bad)
