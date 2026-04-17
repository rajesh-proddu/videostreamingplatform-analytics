"""Tests for VideoEventConsumer lifecycle: start loop, shutdown, main entry point."""

import json
from unittest.mock import MagicMock, patch, call

import pytest
from kafka_es_consumer.src.consumer import VideoEventConsumer, main


class TestConsumerLifecycle:
    """Tests for the consumer start loop and signal handling."""

    @pytest.fixture
    def consumer(self):
        with patch("kafka_es_consumer.src.consumer.Consumer") as mock_kafka, \
             patch("kafka_es_consumer.src.consumer.Elasticsearch"):
            c = VideoEventConsumer(
                kafka_brokers="localhost:9092",
                kafka_topic="video-events",
                kafka_group_id="test-group",
                es_url="http://localhost:9200",
                es_index="videos",
            )
            c.es = MagicMock()
            c.consumer = mock_kafka.return_value
            return c

    def test_shutdown_sets_running_false(self, consumer):
        assert consumer.running is True
        consumer._shutdown(15, None)  # SIGTERM
        assert consumer.running is False

    def test_start_polls_and_processes(self, consumer):
        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = json.dumps({
            "type": "video.created",
            "payload": {"id": "vid-1", "title": "Test"},
        }).encode("utf-8")
        msg.offset.return_value = 0

        # Return one message then stop
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            call_count[0] += 1
            if call_count[0] == 1:
                return msg
            consumer.running = False
            return None

        consumer.consumer.poll.side_effect = poll_side_effect

        consumer.start()

        consumer.consumer.subscribe.assert_called_once_with(["video-events"])
        consumer.consumer.commit.assert_called_once_with(asynchronous=False)
        consumer.consumer.close.assert_called_once()
        consumer.es.index.assert_called_once()

    def test_start_skips_none_poll(self, consumer):
        call_count = [0]

        def poll_side_effect(timeout=1.0):
            call_count[0] += 1
            if call_count[0] >= 2:
                consumer.running = False
            return None

        consumer.consumer.poll.side_effect = poll_side_effect
        consumer.start()
        consumer.es.index.assert_not_called()
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

    def test_start_handles_process_exception(self, consumer):
        """Processing failure should be logged, not crash the loop."""
        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = b"invalid json"
        msg.offset.return_value = 42

        call_count = [0]

        def poll_side_effect(timeout=1.0):
            call_count[0] += 1
            if call_count[0] == 1:
                return msg
            consumer.running = False
            return None

        consumer.consumer.poll.side_effect = poll_side_effect
        consumer.start()  # should not raise

        # Commit should NOT be called for failed message
        consumer.consumer.commit.assert_not_called()
        consumer.consumer.close.assert_called_once()

    def test_upper_case_event_types(self, consumer):
        """Verify VIDEO_CREATED and VIDEO_DELETED uppercase variants work."""
        created_msg = MagicMock()
        created_msg.error.return_value = None
        created_msg.value.return_value = json.dumps({
            "type": "VIDEO_CREATED",
            "payload": {"id": "vid-1", "title": "Upper"},
        }).encode("utf-8")
        created_msg.offset.return_value = 0

        consumer._process_message(created_msg)
        consumer.es.index.assert_called_once()

        consumer.es.reset_mock()

        deleted_msg = MagicMock()
        deleted_msg.error.return_value = None
        deleted_msg.value.return_value = json.dumps({
            "type": "VIDEO_DELETED",
            "payload": {"id": "vid-2"},
        }).encode("utf-8")
        deleted_msg.offset.return_value = 1

        consumer._process_message(deleted_msg)
        consumer.es.delete.assert_called_once_with(index="videos", id="vid-2")


class TestMain:
    @patch("kafka_es_consumer.src.consumer.VideoEventConsumer")
    def test_main_creates_and_starts_consumer(self, mock_consumer_class, monkeypatch):
        monkeypatch.setenv("KAFKA_BROKERS", "broker:9092")
        monkeypatch.setenv("KAFKA_VIDEO_TOPIC", "test-topic")
        monkeypatch.setenv("KAFKA_GROUP_ID", "test-grp")
        monkeypatch.setenv("ELASTICSEARCH_URL", "http://es:9200")
        monkeypatch.setenv("ES_VIDEO_INDEX", "test-videos")

        mock_instance = MagicMock()
        mock_consumer_class.return_value = mock_instance

        main()

        mock_consumer_class.assert_called_once_with(
            kafka_brokers="broker:9092",
            kafka_topic="test-topic",
            kafka_group_id="test-grp",
            es_url="http://es:9200",
            es_index="test-videos",
        )
        mock_instance.start.assert_called_once()
