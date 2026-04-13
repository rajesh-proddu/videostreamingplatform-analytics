"""Tests for the Kafka→ES consumer."""

import json
from unittest.mock import MagicMock, patch

import pytest

from kafka_es_consumer.src.consumer import VideoEventConsumer


class TestVideoEventConsumer:
    """Test event processing logic."""

    @pytest.fixture
    def consumer(self):
        with patch("kafka_es_consumer.src.consumer.Consumer"), \
             patch("kafka_es_consumer.src.consumer.Elasticsearch"):
            c = VideoEventConsumer(
                kafka_brokers="localhost:9092",
                kafka_topic="video-events",
                kafka_group_id="test-group",
                es_url="http://localhost:9200",
                es_index="videos",
            )
            c.es = MagicMock()
            return c

    def _make_msg(self, event_type: str, payload: dict) -> MagicMock:
        msg = MagicMock()
        msg.value.return_value = json.dumps({
            "version": "1.0",
            "type": event_type,
            "timestamp": "2024-01-01T00:00:00Z",
            "payload": payload,
        }).encode("utf-8")
        msg.error.return_value = None
        msg.offset.return_value = 0
        return msg

    def test_index_on_created(self, consumer):
        msg = self._make_msg("video.created", {
            "id": "vid-1",
            "title": "Test Video",
            "description": "A test",
        })
        consumer._process_message(msg)
        consumer.es.index.assert_called_once()
        call_kwargs = consumer.es.index.call_args
        assert call_kwargs.kwargs["id"] == "vid-1"
        assert call_kwargs.kwargs["index"] == "videos"

    def test_index_on_updated(self, consumer):
        msg = self._make_msg("video.updated", {
            "id": "vid-2",
            "title": "Updated Video",
        })
        consumer._process_message(msg)
        consumer.es.index.assert_called_once()

    def test_delete_on_deleted(self, consumer):
        msg = self._make_msg("video.deleted", {"id": "vid-3"})
        consumer._process_message(msg)
        consumer.es.delete.assert_called_once_with(index="videos", id="vid-3")

    def test_skip_no_video_id(self, consumer):
        msg = self._make_msg("video.created", {})
        consumer._process_message(msg)
        consumer.es.index.assert_not_called()

    def test_unknown_event_type(self, consumer):
        msg = self._make_msg("video.unknown", {"id": "vid-4"})
        consumer._process_message(msg)
        consumer.es.index.assert_not_called()
        consumer.es.delete.assert_not_called()
