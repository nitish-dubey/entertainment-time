"""
Redis rebuild service - recovers analytics data from PostgreSQL.

Use this when Redis crashes or loses data.
Rebuilds from PostgreSQL Views table.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

from app.database import SessionLocal
from app.models import View, Video
from app.services.redis_service import RedisService
from sqlalchemy import func

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RedisRebuildService:
    """
    Rebuilds Redis analytics data from PostgreSQL.
    
    Strategy:
    1. Query Views table for last 30 days
    2. Populate Redis sorted sets (video:X:views)
    3. Build total view counters
    4. Trigger leaderboard refresh
    """

    def __init__(self):
        self.redis = RedisService()
        self.db = SessionLocal()

    def rebuild_all(self, days_back: int = 30, batch_size: int = 10000):
        """
        Full rebuild of Redis from PostgreSQL.
        
        Args:
            days_back: How many days to rebuild (default: 30)
            batch_size: Process views in batches (for memory efficiency)
        """
        logger.info("=" * 60)
        logger.info(f"Starting Redis rebuild from PostgreSQL")
        logger.info(f"Rebuilding last {days_back} days of data")
        logger.info("=" * 60)

        try:
            # Step 1: Clear existing Redis data (optional - be careful!)
            # self._clear_redis_analytics()

            # Step 2: Rebuild individual views
            self._rebuild_views(days_back, batch_size)

            # Step 3: Rebuild total counters
            self._rebuild_total_counters()

            # Step 4: Leaderboards will be rebuilt by scheduler (runs every 5 min)
            logger.info("\n✅ Redis rebuild complete!")
            logger.info("Note: Leaderboards will be refreshed by scheduler in ~5 minutes")

        except Exception as e:
            logger.error(f"Rebuild failed: {e}", exc_info=True)
            raise
        finally:
            self.db.close()

    def _clear_redis_analytics(self):
        """
        Clear existing analytics data in Redis.
        
        ⚠️  WARNING: This deletes all analytics data!
        Only use during full rebuild.
        """
        logger.warning("⚠️  Clearing existing Redis analytics data...")
        
        # Delete all video view sorted sets
        pattern = "video:*:views"
        cursor = 0
        deleted = 0
        
        while True:
            cursor, keys = self.redis.client.scan(cursor, match=pattern, count=1000)
            if keys:
                self.redis.client.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        
        # Delete all total view counters
        pattern = "video:*:total_views"
        cursor = 0
        
        while True:
            cursor, keys = self.redis.client.scan(cursor, match=pattern, count=1000)
            if keys:
                self.redis.client.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        
        # Delete all leaderboards
        leaderboards = [
            "global:top_videos:hour",
            "global:top_videos:day",
            "global:top_videos:week",
            "global:top_videos:month",
            "global:top_videos:year",
            "global:top_videos:all_time"
        ]
        self.redis.client.delete(*leaderboards)
        
        logger.info(f"✓ Cleared {deleted} Redis keys")

    def _rebuild_views(self, days_back: int, batch_size: int):
        """
        Rebuild individual view sorted sets from PostgreSQL.
        
        Queries Views table and populates Redis sorted sets.
        """
        logger.info(f"\n[1/3] Rebuilding individual views...")
        
        cutoff = datetime.now() - timedelta(days=days_back)
        
        # Count total views to rebuild
        total_views = self.db.query(func.count(View.id))\
            .filter(View.viewed_at >= cutoff)\
            .scalar()
        
        logger.info(f"Found {total_views:,} views to rebuild")
        
        # Process in batches
        offset = 0
        processed = 0
        
        while offset < total_views:
            # Fetch batch
            views = self.db.query(View)\
                .filter(View.viewed_at >= cutoff)\
                .order_by(View.id)\
                .limit(batch_size)\
                .offset(offset)\
                .all()
            
            if not views:
                break
            
            # Add to Redis (batch operation for performance)
            for view in views:
                timestamp = view.viewed_at.timestamp()
                view_id = f"{view.user_id}:{timestamp}" if view.user_id else f"anon:{timestamp}"
                
                # Add to sorted set
                self.redis.client.zadd(
                    f"video:{view.video_id}:views",
                    {view_id: timestamp}
                )
            
            processed += len(views)
            offset += batch_size
            
            # Progress update
            progress = (processed / total_views) * 100
            logger.info(f"Progress: {processed:,}/{total_views:,} ({progress:.1f}%)")
        
        logger.info(f"✓ Rebuilt {processed:,} individual views")

    def _rebuild_total_counters(self):
        """
        Rebuild total view counters from PostgreSQL.
        
        Gets count of all views per video (not just last 30 days).
        """
        logger.info(f"\n[2/3] Rebuilding total view counters...")
        
        # Get view counts per video (all time)
        results = self.db.query(
            View.video_id,
            func.count(View.id).label('total_views')
        ).group_by(View.video_id).all()
        
        # Set counters in Redis
        for video_id, total_views in results:
            self.redis.client.set(f"video:{video_id}:total_views", total_views)
        
        logger.info(f"✓ Rebuilt {len(results)} total view counters")

    def rebuild_single_video(self, video_id: int, days_back: int = 30):
        """
        Rebuild Redis data for a single video.
        
        Useful for fixing inconsistencies without full rebuild.
        
        Args:
            video_id: Video to rebuild
            days_back: How many days to rebuild
        """
        logger.info(f"Rebuilding Redis data for video {video_id}...")
        
        cutoff = datetime.now() - timedelta(days=days_back)
        
        # Get views for this video
        views = self.db.query(View)\
            .filter(View.video_id == video_id)\
            .filter(View.viewed_at >= cutoff)\
            .all()
        
        # Clear existing data for this video
        self.redis.client.delete(f"video:{video_id}:views")
        
        # Rebuild
        for view in views:
            timestamp = view.viewed_at.timestamp()
            view_id = f"{view.user_id}:{timestamp}" if view.user_id else f"anon:{timestamp}"
            
            self.redis.client.zadd(
                f"video:{video_id}:views",
                {view_id: timestamp}
            )
        
        # Rebuild total counter
        total_views = self.db.query(func.count(View.id))\
            .filter(View.video_id == video_id)\
            .scalar()
        
        self.redis.client.set(f"video:{video_id}:total_views", total_views)
        
        logger.info(f"✓ Rebuilt video {video_id}: {len(views)} recent views, {total_views} total")

    def verify_rebuild(self) -> dict:
        """
        Verify rebuild by comparing Redis and PostgreSQL counts.
        
        Returns:
            dict: Verification statistics
        """
        logger.info("\n[3/3] Verifying rebuild...")
        
        cutoff = datetime.now() - timedelta(days=30)
        
        # Get sample of videos
        sample_videos = self.db.query(Video.id).limit(10).all()
        
        mismatches = 0
        checked = 0
        
        for (video_id,) in sample_videos:
            # PostgreSQL count (last 30 days)
            pg_count = self.db.query(func.count(View.id))\
                .filter(View.video_id == video_id)\
                .filter(View.viewed_at >= cutoff)\
                .scalar()
            
            # Redis count (last 30 days)
            now = datetime.now().timestamp()
            cutoff_ts = cutoff.timestamp()
            redis_count = self.redis.client.zcount(
                f"video:{video_id}:views",
                cutoff_ts,
                now
            )
            
            checked += 1
            
            if pg_count != redis_count:
                mismatches += 1
                logger.warning(
                    f"Mismatch for video {video_id}: "
                    f"PostgreSQL={pg_count}, Redis={redis_count}"
                )
            else:
                logger.debug(f"✓ Video {video_id}: {pg_count} views (match)")
        
        result = {
            "checked": checked,
            "mismatches": mismatches,
            "success_rate": ((checked - mismatches) / checked * 100) if checked > 0 else 0
        }
        
        if mismatches == 0:
            logger.info(f"✅ Verification passed: {checked}/{checked} videos match")
        else:
            logger.warning(
                f"⚠️  Verification found issues: {mismatches}/{checked} videos mismatch"
            )
        
        return result


def rebuild_redis_from_postgres(days_back: int = 30, clear_first: bool = False):
    """
    Convenience function to rebuild Redis.
    
    Args:
        days_back: How many days of data to rebuild
        clear_first: Whether to clear Redis first (dangerous!)
    """
    rebuilder = RedisRebuildService()
    
    if clear_first:
        logger.warning("⚠️  Will clear existing Redis data first!")
        response = input("Are you sure? Type 'yes' to confirm: ")
        if response.lower() != 'yes':
            logger.info("Rebuild cancelled")
            return
        rebuilder._clear_redis_analytics()
    
    rebuilder.rebuild_all(days_back=days_back)
    rebuilder.verify_rebuild()


if __name__ == '__main__':
    # Run rebuild
    import sys
    
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    clear = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else False
    
    rebuild_redis_from_postgres(days_back=days, clear_first=clear)
