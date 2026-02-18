"""
Background scheduler for refreshing Redis leaderboards.
Runs every 5 minutes to update global top K leaderboards.
"""
import time
import logging
from datetime import datetime
from typing import Dict, List

from app.services.redis_service import RedisService
from app.database import SessionLocal
from app.models import Video
from app.schemas import Timeframe

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LeaderboardScheduler:
    """
    Background job that refreshes global leaderboards.

    Strategy:
    1. Calculate view counts for each timeframe
    2. Build new leaderboard in temporary key
    3. Atomically swap using RENAME
    4. Cleanup old data beyond 30 days

    Runs every 5 minutes.
    """

    def __init__(self, refresh_interval_seconds: int = 300):
        """
        Args:
            refresh_interval_seconds: How often to refresh (default: 300 = 5 minutes)
        """
        self.redis = RedisService()
        self.refresh_interval = refresh_interval_seconds

    def get_all_video_ids(self) -> List[int]:
        """Get all video IDs from database."""
        db = SessionLocal()
        try:
            videos = db.query(Video.id).all()
            return [v.id for v in videos]
        finally:
            db.close()

    def refresh_leaderboard(self, timeframe: Timeframe):
        """
        Refresh leaderboard for a specific timeframe.

        Uses RENAME for atomic swap to prevent race conditions.

        Args:
            timeframe: Which timeframe to refresh (hour, day, week, month, year, all_time)
        """
        try:
            logger.info(f"Refreshing {timeframe.value} leaderboard...")

            # Get timeframe in seconds
            timeframe_seconds_map = {
                Timeframe.HOUR: 3600,
                Timeframe.DAY: 86400,
                Timeframe.WEEK: 604800,
                Timeframe.MONTH: 2592000,
                Timeframe.YEAR: 31536000,
                Timeframe.ALL_TIME: None
            }
            timeframe_seconds = timeframe_seconds_map[timeframe]

            # Get all video IDs
            video_ids = self.get_all_video_ids()

            if not video_ids:
                logger.warning("No videos found in database")
                return

            # Calculate view counts for all videos
            video_scores = {}
            for video_id in video_ids:
                view_count = self.redis.get_view_count(video_id, timeframe_seconds)
                if view_count > 0:
                    video_scores[str(video_id)] = view_count

            if not video_scores:
                logger.info(f"No views found for {timeframe.value} timeframe")
                # Still create empty leaderboard
                video_scores = {}

            # Build new leaderboard in temporary key
            temp_key = f"global:top_videos:{timeframe.value}:temp"
            final_key = f"global:top_videos:{timeframe.value}"

            # Delete temp key if exists
            self.redis.client.delete(temp_key)

            # Add all scores to temp leaderboard
            if video_scores:
                self.redis.client.zadd(temp_key, video_scores)

            # Atomically swap: rename temp to final
            # This is atomic - no race condition!
            try:
                self.redis.client.rename(temp_key, final_key)
                logger.info(
                    f"✓ Refreshed {timeframe.value} leaderboard: "
                    f"{len(video_scores)} videos with views"
                )
            except Exception as e:
                # If temp key doesn't exist, just create empty final key
                logger.warning(f"Could not rename {temp_key}: {e}")
                self.redis.client.delete(final_key)
                if video_scores:
                    self.redis.client.zadd(final_key, video_scores)

        except Exception as e:
            logger.error(f"Error refreshing {timeframe.value} leaderboard: {e}", exc_info=True)

    def cleanup_old_views(self):
        """
        Cleanup views older than 30 days to save memory.

        Removes old entries from video:{video_id}:views sorted sets.
        """
        try:
            logger.info("Cleaning up views older than 30 days...")

            now = datetime.now().timestamp()
            cutoff = now - (30 * 24 * 60 * 60)  # 30 days ago

            video_ids = self.get_all_video_ids()
            total_removed = 0

            for video_id in video_ids:
                key = f"video:{video_id}:views"
                # Remove all views with score (timestamp) < cutoff
                removed = self.redis.client.zremrangebyscore(key, 0, cutoff)
                if removed > 0:
                    total_removed += removed

            logger.info(f"✓ Cleaned up {total_removed} old view entries")

        except Exception as e:
            logger.error(f"Error cleaning up old views: {e}", exc_info=True)

    def refresh_all_leaderboards(self):
        """Refresh all timeframe leaderboards."""
        logger.info("=" * 60)
        logger.info(f"Starting leaderboard refresh at {datetime.now()}")

        # Refresh each timeframe
        for timeframe in Timeframe:
            self.refresh_leaderboard(timeframe)

        # Cleanup old data
        self.cleanup_old_views()

        logger.info(f"Finished leaderboard refresh at {datetime.now()}")
        logger.info("=" * 60)

    def run(self):
        """
        Main scheduler loop.
        Refreshes leaderboards every N seconds.
        """
        logger.info(
            f"Starting leaderboard scheduler (refresh interval: {self.refresh_interval}s)"
        )

        # Do initial refresh
        self.refresh_all_leaderboards()

        try:
            while True:
                # Sleep for refresh interval
                time.sleep(self.refresh_interval)

                # Refresh all leaderboards
                self.refresh_all_leaderboards()

        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user")


def main():
    """Entry point for running scheduler as standalone process."""
    scheduler = LeaderboardScheduler(refresh_interval_seconds=300)  # 5 minutes
    scheduler.run()


if __name__ == '__main__':
    main()
