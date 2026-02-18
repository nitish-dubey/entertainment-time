"""
Transcoding worker - processes video uploads and transcodes them.

Listens to Kafka for video_uploaded events and triggers transcoding jobs.
"""
from confluent_kafka import Consumer, KafkaError
import json
import logging
from datetime import datetime

from app.database import SessionLocal
from app.models import TranscodingJob, VideoVariant, TranscodingStatus, VideoQuality
from app.services.transcoding_service import TranscodingService
from app.services.minio_service import MinIOService
from app.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()


class TranscodingWorker:
    """
    Background worker for video transcoding.

    Consumes video_uploaded events and creates multiple quality variants.
    """

    def __init__(self):
        self.transcoding_service = TranscodingService()
        self.minio = MinIOService()

        # Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'group.id': 'transcoding-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })

        self.consumer.subscribe(['video-events'])
        logger.info("Transcoding worker initialized")

    def process_video_uploaded(self, event: dict):
        """
        Process video_uploaded event - trigger transcoding.

        Args:
            event: {
                "video_id": 123,
                "file_path": "videos/2024/movie.mp4",
                ...
            }
        """
        try:
            video_id = event.get('video_id')
            file_path = event.get('file_path')

            if not video_id or not file_path:
                logger.error(f"Missing video_id or file_path in event: {event}")
                return

            logger.info(f"Starting transcoding for video {video_id}")

            # Create transcoding job
            db = SessionLocal()
            try:
                job = TranscodingJob(
                    video_id=video_id,
                    status=TranscodingStatus.PROCESSING,
                    started_at=datetime.now()
                )
                db.add(job)
                db.commit()
                db.refresh(job)

                # Transcode to multiple qualities
                qualities = ['1080p', '720p', '480p', '360p']
                variants = self.transcoding_service.transcode_to_hls(
                    video_id,
                    file_path,
                    qualities
                )

                # Save variants to database
                for quality, variant_info in variants.items():
                    if quality == 'master':
                        continue  # Skip master playlist entry

                    variant = VideoVariant(
                        video_id=video_id,
                        quality=VideoQuality[f'Q_{quality.upper()}'],
                        file_path=variant_info['playlist_path'],
                        file_size=variant_info['file_size'],
                        width=variant_info['width'],
                        height=variant_info['height'],
                        bitrate=variant_info['bitrate'],
                        codec='h264',
                        playlist_path=variant_info['playlist_path'],
                        is_ready=True
                    )
                    db.add(variant)

                # Mark job as completed
                job.status = TranscodingStatus.COMPLETED
                job.completed_at = datetime.now()
                job.progress = 100.0

                db.commit()

                logger.info(f"✓ Transcoding complete for video {video_id}: {len(variants)} variants")

            except Exception as e:
                logger.error(f"Transcoding failed for video {video_id}: {e}", exc_info=True)

                # Mark job as failed
                job.status = TranscodingStatus.FAILED
                job.error_message = str(e)
                job.retry_count += 1
                db.commit()

            finally:
                db.close()

        except Exception as e:
            logger.error(f"Error processing video_uploaded event: {e}", exc_info=True)

    def process_event(self, event: dict):
        """Route event to appropriate handler."""
        event_type = event.get('event_type')

        if event_type == 'video_uploaded':
            self.process_video_uploaded(event)
        else:
            # Not interested in other events
            pass

    def run(self):
        """Main worker loop."""
        logger.info("Starting transcoding worker...")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue

                # Parse and process event
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    self.process_event(event)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Transcoding worker interrupted")
        finally:
            self.consumer.close()


def main():
    """Entry point."""
    worker = TranscodingWorker()
    worker.run()


if __name__ == '__main__':
    main()
