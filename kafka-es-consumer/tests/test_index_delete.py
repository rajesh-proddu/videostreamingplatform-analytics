"""Tests for VideoEventConsumer._index_video and _delete_video."""

from unittest.mock import MagicMock, patch

import pytest
from kafka_es_consumer.src.consumer import VideoEventConsumer


class TestIndexDelete:
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

    def test_index_video_calls_es_index(self, consumer):
        consumer._index_video("vid-1", {"title": "Test", "description": "Hello"})
        consumer.es.index.assert_called_once()
        call_kwargs = consumer.es.index.call_args.kwargs
        assert call_kwargs["index"] == "videos"
        assert call_kwargs["id"] == "vid-1"
        assert call_kwargs["document"]["title"] == "Test"
        assert call_kwargs["document"]["description"] == "Hello"

    def test_delete_video_calls_es_delete(self, consumer):
        consumer._delete_video("vid-1")
        consumer.es.delete.assert_called_once_with(index="videos", id="vid-1")

    def test_delete_video_not_found(self, consumer):
        consumer.es.delete.side_effect = Exception("NotFoundError")
        # Should not raise
        consumer._delete_video("vid-1")
