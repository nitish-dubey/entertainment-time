"""
Kafka consumer for processing video events.
Handles video_viewed events to update Redis analytics.
"""
from confluent_kafka import Consumer, KafkaError
import json
import logging
from datetime import datetime
from typing import Dict

from app.services.redis_service import RedisService
from app.services.elasticsearch_service import ElasticsearchService
from app.database import SessionLocal
from app.models import View
from app.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()


class VideoEventConsumer:
    """
    Consumes video events from Kafka and updates Redis analytics.

    Processes:
    - video_viewed: Record view in Redis sorted set
    - video_uploaded: Could trigger initial indexing (future)
    """

    def __init__(self):
        self.redis = RedisService()
        self.es = ElasticsearchService()

        # Kafka consumer configuration
        self.consumer = Consumer({
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'group.id': 'video-analytics-group',
            'auto.offset.reset': 'earliest',  # Start from beginning if no offset
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        })

        # Subscribe to video-events topic
        self.consumer.subscribe(['video-events'])
        logger.info("Video event consumer initialized and subscribed to 'video-events'")

    def process_video_viewed(self, event: Dict):
        """
        Process video_viewed event.

        Dual-write strategy:
        1. Write to Redis (primary, fast)
        2. Write to PostgreSQL (backup, reliable)

        Args:
            event: {
                "event_type": "video_viewed",
                "video_id": 123,
                "user_id": "user_456",
                "timestamp": "2024-01-15T10:30:00"
            }
        """
        try:
            video_id = event.get('video_id')
            user_id = event.get('user_id')
            timestamp_str = event.get('timestamp')

            if not video_id:
                logger.error(f"Missing video_id in event: {event}")
                return

            # Parse timestamp or use current time
            if timestamp_str:
                try:
                    viewed_at = datetime.fromisoformat(timestamp_str)
                except:
                    viewed_at = datetime.now()
            else:
                viewed_at = datetime.now()

            # 1. Write to Redis (primary) with idempotency
            redis_success = False
            try:
                # Check for duplicate events (idempotency)
                event_id = event.get('event_id')
                if event_id and self.redis.client.exists(f"processed:{event_id}"):
                    logger.info(f"Event {event_id} already processed, skipping")
                    return

                # Record view
                self.redis.record_view(video_id, user_id)

                # Mark event as processed (7 day TTL)
                if event_id:
                    self.redis.client.setex(f"processed:{event_id}", 604800, "1")

                redis_success = True
                logger.debug(f"✓ Recorded view in Redis for video {video_id}")
            except Exception as e:
                logger.warning(f"Failed to record view in Redis: {e}")

            # 2. Write to PostgreSQL (backup)
            db = SessionLocal()
            try:
                view = View(
                    video_id=video_id,
                    user_id=user_id,
                    viewed_at=viewed_at
                )
                db.add(view)
                db.commit()
                logger.debug(f"✓ Recorded view in PostgreSQL for video {video_id}")
            except Exception as e:
                logger.error(f"Failed to record view in PostgreSQL: {e}")
                db.rollback()
            finally:
                db.close()

            # Log success
            if redis_success:
                logger.info(f"Recorded view for video {video_id} (Redis + PostgreSQL)")
            else:
                logger.warning(f"Recorded view for video {video_id} (PostgreSQL only - Redis failed)")

        except Exception as e:
            logger.error(f"Error processing video_viewed event: {e}", exc_info=True)

    def process_video_uploaded(self, event: Dict):
        """
        Process video_uploaded event.

        Indexes video in Elasticsearch for search.

        Args:
            event: {
                "event_type": "video_uploaded",
                "video_id": 123,
                "content_type": "movie",
                "title": "Avatar",
                "description": "...",
                "show_title": null,
                "season_number": null,
                "episode_number": null,
                "genre": "Sci-Fi",
                "release_year": 2009,
                "rating": "PG-13",
                "view_count": 0,
                "created_at": "2024-01-15T10:30:00"
            }
        """
        try:
            video_id = event.get('video_id')

            if not video_id:
                logger.error(f"Missing video_id in event: {event}")
                return

            # Index in Elasticsearch
            self.es.index_video(video_id, {
                "video_id": video_id,
                "content_type": event.get('content_type'),
                "title": event.get('title'),
                "description": event.get('description'),
                "show_title": event.get('show_title'),
                "season_number": event.get('season_number'),
                "episode_number": event.get('episode_number'),
                "genre": event.get('genre'),
                "release_year": event.get('release_year'),
                "rating": event.get('rating'),
                "view_count": event.get('view_count', 0),
                "created_at": event.get('created_at')
            })

            logger.info(f"Indexed video {video_id} in Elasticsearch: {event.get('title')}")

        except Exception as e:
            logger.error(f"Error processing video_uploaded event: {e}", exc_info=True)

    def process_event(self, event: Dict):
        """
        Route event to appropriate handler based on event_type.
        """
        event_type = event.get('event_type')

        if event_type == 'video_viewed':
            self.process_video_viewed(event)
        elif event_type == 'video_uploaded':
            self.process_video_uploaded(event)
        else:
            logger.warning(f"Unknown event type: {event_type}")

    def run(self):
        """
        Main consumer loop.
        Continuously polls for messages and processes them.
        """
        logger.info("Starting video event consumer...")

        try:
            while True:
                # Poll for messages (1 second timeout)
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue

                # Parse message
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    logger.debug(f"Received event: {event}")

                    # Process event
                    self.process_event(event)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")

        finally:
            # Close consumer
            logger.info("Closing consumer...")
            self.consumer.close()

    def close(self):
        """Clean up resources."""
        self.consumer.close()


def main():
    """Entry point for running consumer as standalone process."""
    consumer = VideoEventConsumer()
    consumer.run()


if __name__ == '__main__':
    main()
