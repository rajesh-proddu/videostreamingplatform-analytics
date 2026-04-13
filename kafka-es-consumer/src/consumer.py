"""
Kafka → Elasticsearch consumer for video events.

Reads from the 'video-events' Kafka topic, deserializes events,
and indexes/deletes documents in the Elasticsearch 'videos' index.
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException
from elasticsearch import Elasticsearch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("kafka-es-consumer")


class VideoEventConsumer:
    """Consumes video events from Kafka and syncs to Elasticsearch."""

    def __init__(
        self,
        kafka_brokers: str,
        kafka_topic: str,
        kafka_group_id: str,
        es_url: str,
        es_index: str,
    ):
        self.topic = kafka_topic
        self.es_index = es_index
        self.running = True

        self.consumer = Consumer({
            "bootstrap.servers": kafka_brokers,
            "group.id": kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

        self.es = Elasticsearch(es_url)

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received, stopping consumer...")
        self.running = False

    def start(self):
        """Subscribe and start consuming events."""
        self.consumer.subscribe([self.topic])
        logger.info(f"Subscribed to topic: {self.topic}")

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    self._process_message(msg)
                    self.consumer.commit(asynchronous=False)
                except Exception:
                    logger.exception(f"Failed to process message at offset {msg.offset()}")
        finally:
            self.consumer.close()
            logger.info("Consumer closed")

    def _process_message(self, msg):
        """Process a single Kafka message."""
        value = json.loads(msg.value().decode("utf-8"))
        event_type = value.get("type", "")
        payload = value.get("payload", {})
        video_id = payload.get("id", "")

        if not video_id:
            logger.warning(f"Skipping event with no video ID: {value}")
            return

        if event_type in ("video.created", "VIDEO_CREATED"):
            self._index_video(video_id, payload)
        elif event_type in ("video.updated", "VIDEO_UPDATED"):
            self._index_video(video_id, payload)
        elif event_type in ("video.deleted", "VIDEO_DELETED"):
            self._delete_video(video_id)
        else:
            logger.warning(f"Unknown event type: {event_type}")

    def _index_video(self, video_id: str, payload: dict):
        """Index or update a video document in Elasticsearch."""
        doc = {
            "title": payload.get("title", ""),
            "description": payload.get("description", ""),
            "duration_seconds": payload.get("duration_seconds"),
            "size_bytes": payload.get("size_bytes"),
            "upload_status": payload.get("upload_status", ""),
            "created_at": payload.get("created_at"),
            "updated_at": payload.get("updated_at"),
            "indexed_at": datetime.now(timezone.utc).isoformat(),
        }
        self.es.index(index=self.es_index, id=video_id, document=doc)
        logger.info(f"Indexed video {video_id}")

    def _delete_video(self, video_id: str):
        """Delete a video document from Elasticsearch."""
        try:
            self.es.delete(index=self.es_index, id=video_id)
            logger.info(f"Deleted video {video_id}")
        except Exception:
            logger.warning(f"Video {video_id} not found in ES, skipping delete")


def main():
    consumer = VideoEventConsumer(
        kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_VIDEO_TOPIC", "video-events"),
        kafka_group_id=os.getenv("KAFKA_GROUP_ID", "kafka-es-consumer"),
        es_url=os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
        es_index=os.getenv("ES_VIDEO_INDEX", "videos"),
    )
    consumer.start()


if __name__ == "__main__":
    main()
