"""
Kafka service for event streaming.
Publishes events for video uploads, views, searches, etc.
"""
from confluent_kafka import Producer
import json
from app.config import get_settings
from typing import Dict, Any
from datetime import datetime

settings = get_settings()


class KafkaService:
    """Service for publishing events to Kafka."""

    def __init__(self):
        """Initialize Kafka producer."""
        try:
            self.producer = Producer({
                'bootstrap.servers': settings.kafka_bootstrap_servers,
                'client.id': 'entertainmenttime-backend'
            })
            print("✅ Connected to Kafka")
        except Exception as e:
            print(f"❌ Error connecting to Kafka: {e}")
            raise

    def publish_video_uploaded(self, video_id: int, video_data: Dict[str, Any]):
        """
        Publish 'video_uploaded' event.

        Args:
            video_id: ID of uploaded video
            video_data: Video metadata (title, content_type, etc.)

        Event consumers will:
        - Index video in Elasticsearch
        - Update cache
        - Send notifications
        """
        event = {
            "event_type": "video_uploaded",
            "video_id": video_id,
            "timestamp": datetime.utcnow().isoformat(),
            "data": video_data
        }
        self._publish("video-events", str(video_id), event)

    def publish_video_viewed(self, video_id: int, user_id: str = None):
        """
        Publish 'video_viewed' event.

        Args:
            video_id: ID of viewed video
            user_id: Optional user identifier

        Event consumers will:
        - Increment view count in Redis
        - Update analytics
        - Generate recommendations
        """
        import uuid
        event = {
            "event_type": "video_viewed",
            "event_id": f"evt_{uuid.uuid4().hex[:16]}",  # Unique ID for idempotency
            "video_id": video_id,
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        self._publish("video-events", str(video_id), event)

    def publish_video_searched(self, query: str, results_count: int):
        """
        Publish 'video_searched' event.

        Args:
            query: Search query
            results_count: Number of results returned

        Use case: Track popular search terms
        """
        event = {
            "event_type": "video_searched",
            "query": query,
            "results_count": results_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        self._publish("search-events", query, event)

    def _publish(self, topic: str, key: str, event: Dict[str, Any]):
        """
        Internal method to publish event to Kafka.

        Args:
            topic: Kafka topic name
            key: Message key (for partitioning)
            event: Event data
        """
        try:
            # Serialize event to JSON
            value = json.dumps(event).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None

            # Produce message
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value,
                callback=self._delivery_callback
            )

            # Trigger delivery (non-blocking)
            self.producer.poll(0)

            print(f"📤 Published event: {event['event_type']} to topic: {topic}")
        except Exception as e:
            print(f"❌ Error publishing to Kafka: {e}")
            # In production, you might want to:
            # - Retry
            # - Log to dead letter queue
            # - Alert monitoring system

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            print(f"❌ Message delivery failed: {err}")
        else:
            print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            # Wait for any outstanding messages to be delivered
            self.producer.flush()


# Singleton instance
_kafka_service = None


def get_kafka_service() -> KafkaService:
    """
    Get Kafka service singleton.

    Returns:
        KafkaService: Initialized Kafka service

    Usage:
        kafka = get_kafka_service()
        kafka.publish_video_uploaded(video_id, video_data)
    """
    global _kafka_service
    if _kafka_service is None:
        _kafka_service = KafkaService()
    return _kafka_service
