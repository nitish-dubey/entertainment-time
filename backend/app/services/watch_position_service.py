"""
Watch position service with Redis write-through cache.

Strategy:
1. Write to Redis immediately (fast)
2. Async write to PostgreSQL (background)
3. Best of both worlds: speed + persistence
"""
import logging
from datetime import datetime
from typing import Optional

from app.services.redis_service import RedisService
from app.database import SessionLocal
from app.models import WatchHistory
from sqlalchemy import and_

logger = logging.getLogger(__name__)


class WatchPositionService:
    """
    Manages watch position with Redis caching.
    
    Write path:
    1. Write to Redis (instant)
    2. Return success immediately
    3. Async flush to PostgreSQL (background job)
    
    Read path:
    1. Try Redis (fast)
    2. Fall back to PostgreSQL
    3. Populate Redis cache
    """

    def __init__(self, redis: RedisService = None):
        self.redis = redis or RedisService()

    def save_position_fast(
        self,
        user_id: str,
        video_id: int,
        position_seconds: int,
        duration_seconds: Optional[int] = None
    ) -> bool:
        """
        Save watch position to Redis (instant).
        
        Background job will flush to PostgreSQL later.
        
        Returns:
            True if saved successfully
        """
        try:
            # Build Redis key
            position_key = f"watch_position:{user_id}:{video_id}"
            metadata_key = f"watch_metadata:{user_id}:{video_id}"

            # Save position (7 day TTL)
            self.redis.client.setex(position_key, 604800, position_seconds)

            # Save metadata as hash
            metadata = {
                'position': position_seconds,
                'duration': duration_seconds or 0,
                'updated_at': datetime.now().isoformat(),
                'dirty': '1'  # Flag for background flush
            }
            
            self.redis.client.hset(metadata_key, mapping=metadata)
            self.redis.client.expire(metadata_key, 604800)

            # Add to flush queue (sorted set by timestamp)
            flush_key = f"watch_position:flush_queue"
            self.redis.client.zadd(
                flush_key,
                {f"{user_id}:{video_id}": datetime.now().timestamp()}
            )

            logger.debug(f"Saved position to Redis: {user_id}:{video_id} = {position_seconds}s")
            return True

        except Exception as e:
            logger.error(f"Failed to save position to Redis: {e}")
            return False

    def get_position_fast(
        self,
        user_id: str,
        video_id: int
    ) -> Optional[dict]:
        """
        Get watch position from Redis (instant).
        
        Falls back to PostgreSQL if not in cache.
        """
        try:
            # Try Redis first
            metadata_key = f"watch_metadata:{user_id}:{video_id}"
            metadata = self.redis.client.hgetall(metadata_key)

            if metadata:
                return {
                    'position_seconds': int(metadata.get('position', 0)),
                    'duration_seconds': int(metadata.get('duration', 0)),
                    'updated_at': metadata.get('updated_at')
                }

            # Fall back to PostgreSQL
            db = SessionLocal()
            try:
                watch_history = db.query(WatchHistory).filter(
                    and_(
                        WatchHistory.user_id == user_id,
                        WatchHistory.video_id == video_id
                    )
                ).first()

                if watch_history:
                    # Populate Redis cache
                    self.save_position_fast(
                        user_id,
                        video_id,
                        watch_history.position_seconds,
                        watch_history.duration_seconds
                    )

                    return {
                        'position_seconds': watch_history.position_seconds,
                        'duration_seconds': watch_history.duration_seconds,
                        'updated_at': watch_history.updated_at.isoformat()
                    }

            finally:
                db.close()

        except Exception as e:
            logger.error(f"Failed to get position: {e}")

        return None

    def flush_to_database(self, batch_size: int = 100):
        """
        Flush dirty positions from Redis to PostgreSQL.
        
        Called by background job every 30 seconds.
        """
        db = SessionLocal()
        try:
            flush_key = "watch_position:flush_queue"
            
            # Get batch of entries to flush
            entries = self.redis.client.zrange(flush_key, 0, batch_size - 1)

            if not entries:
                return

            logger.info(f"Flushing {len(entries)} watch positions to PostgreSQL")

            for entry in entries:
                user_id, video_id = entry.split(':')
                video_id = int(video_id)

                # Get metadata from Redis
                metadata_key = f"watch_metadata:{user_id}:{video_id}"
                metadata = self.redis.client.hgetall(metadata_key)

                if not metadata or metadata.get('dirty') != '1':
                    # Remove from queue
                    self.redis.client.zrem(flush_key, entry)
                    continue

                position = int(metadata.get('position', 0))
                duration = int(metadata.get('duration', 0))

                # Calculate progress
                progress_percent = 0.0
                if duration > 0:
                    progress_percent = (position / duration) * 100

                completed = progress_percent >= 90.0

                # UPSERT to PostgreSQL
                watch_history = db.query(WatchHistory).filter(
                    and_(
                        WatchHistory.user_id == user_id,
                        WatchHistory.video_id == video_id
                    )
                ).first()

                if watch_history:
                    # Update existing
                    watch_history.position_seconds = position
                    watch_history.duration_seconds = duration
                    watch_history.progress_percent = progress_percent
                    watch_history.completed = completed
                    watch_history.last_watched_at = datetime.now()
                else:
                    # Insert new
                    watch_history = WatchHistory(
                        user_id=user_id,
                        video_id=video_id,
                        position_seconds=position,
                        duration_seconds=duration,
                        progress_percent=progress_percent,
                        completed=completed,
                        last_watched_at=datetime.now()
                    )
                    db.add(watch_history)

                # Clear dirty flag
                self.redis.client.hset(metadata_key, 'dirty', '0')

                # Remove from flush queue
                self.redis.client.zrem(flush_key, entry)

            db.commit()
            logger.info(f"✓ Flushed {len(entries)} positions to PostgreSQL")

        except Exception as e:
            logger.error(f"Flush failed: {e}", exc_info=True)
            db.rollback()

        finally:
            db.close()
