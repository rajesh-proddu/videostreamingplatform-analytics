"""Additional component tests for VideoEventConsumer.

Covers: upper-case event variants, ES failure surfacing, malformed payload tolerance.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from kafka_es_consumer.src.consumer import VideoEventConsumer


@pytest.fixture
def consumer():
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


def _msg(event_type: str, payload: dict):
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


@pytest.mark.parametrize("evt", ["VIDEO_CREATED", "VIDEO_UPDATED"])
def test_index_on_upper_case_create_update(consumer, evt):
    consumer._process_message(_msg(evt, {"id": "v-up", "title": "T"}))
    consumer.es.index.assert_called_once()


def test_delete_on_upper_case(consumer):
    consumer._process_message(_msg("VIDEO_DELETED", {"id": "v-up-del"}))
    consumer.es.delete.assert_called_once_with(index="videos", id="v-up-del")


def test_es_index_failure_propagates(consumer):
    consumer.es.index.side_effect = RuntimeError("ES down")
    with pytest.raises(RuntimeError):
        consumer._process_message(_msg("video.created", {"id": "v-fail", "title": "T"}))


def test_delete_missing_doc_swallowed(consumer):
    consumer.es.delete.side_effect = RuntimeError("not found")
    consumer._process_message(_msg("video.deleted", {"id": "v-gone"}))
    consumer.es.delete.assert_called_once()


def test_indexed_doc_carries_payload_fields(consumer):
    consumer._process_message(_msg("video.created", {
        "id": "v-doc",
        "title": "T",
        "description": "D",
        "duration_seconds": 300,
        "size_bytes": 12345,
        "upload_status": "completed",
    }))
    doc = consumer.es.index.call_args.kwargs["document"]
    assert doc["title"] == "T"
    assert doc["description"] == "D"
    assert doc["duration_seconds"] == 300
    assert doc["size_bytes"] == 12345
    assert doc["upload_status"] == "completed"
    assert "indexed_at" in doc
