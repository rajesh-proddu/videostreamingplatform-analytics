"""Tests for WatchHistoryConsumer lifecycle: start loop, shutdown, _build_glue_catalog, main."""

import json
from unittest.mock import MagicMock, patch

import pytest
from src.consumer import WatchHistoryConsumer, _build_glue_catalog, main


def _make_msg(event_type="watch.started", video_id="v1", user_id="u1"):
    msg = MagicMock()
    msg.value.return_value = json.dumps({
        "version": "1.0",
        "type": event_type,
        "timestamp": "2025-01-15T10:30:00+00:00",
        "payload": {
            "video_id": video_id,
            "user_id": user_id,
            "session_id": "s1",
            "bytes_read": 0,
        },
    }).encode("utf-8")
    msg.error.return_value = None
    msg.offset.return_value = 0
    return msg


@pytest.fixture
def consumer(monkeypatch):
    monkeypatch.setattr(
        "src.consumer.WatchHistoryConsumer.__init__",
        lambda self, **kwargs: None,
    )
    c = WatchHistoryConsumer.__new__(WatchHistoryConsumer)
    c.topic = "watch-events"
    c.table_name = "analytics.watch_history"
    c.running = True
    c.batch = []
    c.table = MagicMock()
    c.consumer = MagicMock()
    return c


class TestShutdown:
    def test_shutdown_sets_running_false(self, consumer):
        assert consumer.running is True
        consumer._shutdown(15, None)
        assert consumer.running is False


class TestStartLoop:
    def test_start_processes_messages_and_flushes(self, consumer):
        msg = _make_msg()

        call_count = [0]

        def poll_side_effect(timeout=1.0):
            call_count[0] += 1
            if call_count[0] <= 100:  # fill batch to BATCH_SIZE
                return msg
            consumer.running = False
            return None

        consumer.consumer.poll.side_effect = poll_side_effect
        consumer.start()

        consumer.consumer.subscribe.assert_called_once_with(["watch-events"])
        consumer.table.append.assert_called()
        consumer.consumer.commit.assert_called()
        consumer.consumer.close.assert_called_once()

    def test_start_flushes_on_idle_poll(self, consumer):
        msg = _make_msg()
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            call_count[0] += 1
            if call_count[0] == 1:
                return msg
            if call_count[0] == 2:
                return None  # idle — should trigger flush
            consumer.running = False
            return None

        consumer.consumer.poll.side_effect = poll_side_effect
        consumer.start()

        # Should have flushed the 1-message batch on idle poll
        consumer.table.append.assert_called()

    def test_start_flushes_remaining_on_shutdown(self, consumer):
        msg = _make_msg()
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            call_count[0] += 1
            if call_count[0] == 1:
                return msg
            consumer.running = False
            return None

        consumer.consumer.poll.side_effect = poll_side_effect
        consumer.start()

        # finally block should flush remaining batch
        consumer.table.append.assert_called()
        consumer.consumer.close.assert_called_once()

    def test_start_handles_partition_eof(self, consumer):
        from confluent_kafka import KafkaError

        eof_msg = MagicMock()
        eof_error = MagicMock()
        eof_error.code.return_value = KafkaError._PARTITION_EOF
        eof_msg.error.return_value = eof_error

        call_count = [0]

        def poll_side_effect(timeout=1.0):
            call_count[0] += 1
            if call_count[0] == 1:
                return eof_msg
            consumer.running = False
            return None

        consumer.consumer.poll.side_effect = poll_side_effect
        consumer.start()  # should not raise
        consumer.consumer.close.assert_called_once()

    def test_start_raises_on_kafka_error(self, consumer):
        from confluent_kafka import KafkaError, KafkaException

        err_msg = MagicMock()
        err = MagicMock()
        err.code.return_value = KafkaError._ALL_BROKERS_DOWN
        err_msg.error.return_value = err

        consumer.consumer.poll.return_value = err_msg

        with pytest.raises(KafkaException):
            consumer.start()

        consumer.consumer.close.assert_called_once()

    def test_start_handles_parse_exception(self, consumer):
        """Invalid JSON should be caught and logged, not crash the loop."""
        bad_msg = MagicMock()
        bad_msg.error.return_value = None
        bad_msg.value.return_value = b"not json"
        bad_msg.offset.return_value = 42

        call_count = [0]

        def poll_side_effect(timeout=1.0):
            call_count[0] += 1
            if call_count[0] == 1:
                return bad_msg
            consumer.running = False
            return None

        consumer.consumer.poll.side_effect = poll_side_effect
        consumer.start()  # should not raise
        consumer.consumer.close.assert_called_once()


class TestBuildGlueCatalog:
    @patch("src.consumer.load_catalog")
    def test_local_dev_with_endpoints(self, mock_load, monkeypatch):
        monkeypatch.setenv("GLUE_ENDPOINT", "http://localhost:4566")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:4566")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse/")

        _build_glue_catalog("test-catalog")

        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["glue.endpoint"] == "http://localhost:4566"
        assert call_kwargs["s3.endpoint"] == "http://localhost:4566"
        assert call_kwargs["s3.path-style-access"] == "true"

    @patch("src.consumer.load_catalog")
    def test_aws_without_endpoints(self, mock_load, monkeypatch):
        monkeypatch.delenv("GLUE_ENDPOINT", raising=False)
        monkeypatch.delenv("S3_ENDPOINT", raising=False)

        _build_glue_catalog("test-catalog")

        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]
        assert "glue.endpoint" not in call_kwargs
        assert "s3.endpoint" not in call_kwargs


class TestMain:
    @patch("src.consumer.WatchHistoryConsumer")
    def test_main_creates_and_starts_consumer(self, mock_cls, monkeypatch):
        monkeypatch.setenv("KAFKA_BROKERS", "broker:9092")
        monkeypatch.setenv("KAFKA_WATCH_TOPIC", "test-topic")
        monkeypatch.setenv("KAFKA_GROUP_ID", "test-grp")
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test-cat")
        monkeypatch.setenv("ICEBERG_TABLE", "test.table")

        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        main()

        mock_cls.assert_called_once_with(
            kafka_brokers="broker:9092",
            kafka_topic="test-topic",
            kafka_group_id="test-grp",
            catalog_name="test-cat",
            table_name="test.table",
        )
        mock_instance.start.assert_called_once()
