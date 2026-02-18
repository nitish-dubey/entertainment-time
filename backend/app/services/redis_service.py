"""
Redis service for caching and real-time analytics.
Handles view counts, top K videos, and caching.
"""
import redis
from app.config import get_settings
from typing import List, Tuple
from datetime import datetime, timedelta

settings = get_settings()


class RedisService:
    """Service for caching and analytics using Redis."""

    def __init__(self):
        """Initialize Redis client."""
        try:
            self.client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                decode_responses=True  # Automatically decode bytes to strings
            )
            # Test connection
            self.client.ping()
            print("✅ Connected to Redis")
        except redis.RedisError as e:
            print(f"❌ Error connecting to Redis: {e}")
            raise

    # ========== View Counting (Sliding Window) ==========

    def record_view(self, video_id: int, user_id: str = None):
        """
        Record a video view with timestamp (for sliding window analytics).

        Args:
            video_id: ID of viewed video
            user_id: Optional user identifier

        Storage: Redis Sorted Set
        Key: video:{video_id}:views
        Score: timestamp
        Member: unique view identifier
        """
        timestamp = datetime.now().timestamp()
        view_id = f"{user_id}:{timestamp}" if user_id else f"anon:{timestamp}"

        # Add to sorted set (score = timestamp)
        self.client.zadd(f"video:{video_id}:views", {view_id: timestamp})

        # Also increment total view count
        self.client.incr(f"video:{video_id}:total_views")

    def get_view_count(self, video_id: int, timeframe_seconds: int = None) -> int:
        """
        Get view count for a video.

        Args:
            video_id: Video ID
            timeframe_seconds: Optional sliding window (e.g., 3600 for last hour)
                              If None, returns total view count

        Returns:
            int: Number of views

        Examples:
            # Total views
            total = redis.get_view_count(123)

            # Views in last hour (sliding window)
            last_hour = redis.get_view_count(123, timeframe_seconds=3600)
        """
        if timeframe_seconds is None:
            # Return total view count
            count = self.client.get(f"video:{video_id}:total_views")
            return int(count) if count else 0
        else:
            # Return sliding window count
            now = datetime.now().timestamp()
            cutoff = now - timeframe_seconds
            return self.client.zcount(f"video:{video_id}:views", cutoff, now)

    def cleanup_old_views(self, video_id: int, older_than_days: int = 30):
        """
        Remove old view records to save memory.

        Args:
            video_id: Video ID
            older_than_days: Remove views older than this many days
        """
        cutoff = (datetime.now() - timedelta(days=older_than_days)).timestamp()
        self.client.zremrangebyscore(f"video:{video_id}:views", 0, cutoff)

    # ========== Top K Videos ==========

    def get_top_videos(
        self,
        k: int,
        timeframe_seconds: int = None,
        video_ids: List[int] = None
    ) -> List[Tuple[int, int]]:
        """
        Get top K most watched videos (sliding window).

        Args:
            k: Number of top videos to return
            timeframe_seconds: Sliding window in seconds (None = all time)
            video_ids: Optional list of video IDs to check (if None, checks all)

        Returns:
            List of (video_id, view_count) tuples, sorted descending

        Example:
            # Top 10 videos in last hour
            top_10 = redis.get_top_videos(k=10, timeframe_seconds=3600)
            # Returns: [(video_id, view_count), ...]
        """
        if video_ids is None:
            # Get all video IDs (in production, you'd get this from database)
            # For now, scan for all video:*:views keys
            video_ids = self._get_all_video_ids()

        # Count views for each video in timeframe
        video_counts = []
        for video_id in video_ids:
            count = self.get_view_count(video_id, timeframe_seconds)
            if count > 0:
                video_counts.append((video_id, count))

        # Sort by view count (descending) and return top K
        video_counts.sort(key=lambda x: x[1], reverse=True)
        return video_counts[:k]

    def _get_all_video_ids(self) -> List[int]:
        """
        Get all video IDs that have view records.

        Returns:
            List of video IDs

        Note: In production, you'd maintain a separate set of video IDs
        """
        keys = self.client.keys("video:*:views")
        video_ids = []
        for key in keys:
            # Extract video_id from "video:123:views"
            parts = key.split(":")
            if len(parts) == 3:
                try:
                    video_ids.append(int(parts[1]))
                except ValueError:
                    continue
        return video_ids

    # ========== Caching ==========

    def cache_set(self, key: str, value: str, ttl_seconds: int = 3600):
        """
        Cache a value with expiration.

        Args:
            key: Cache key
            value: Value to cache (string)
            ttl_seconds: Time to live (default 1 hour)

        Example:
            redis.cache_set("video:123:metadata", json.dumps(metadata), ttl=300)
        """
        self.client.setex(key, ttl_seconds, value)

    def cache_get(self, key: str) -> str:
        """
        Get cached value.

        Args:
            key: Cache key

        Returns:
            str: Cached value or None if not found/expired
        """
        return self.client.get(key)

    def cache_delete(self, key: str):
        """Delete cached value."""
        self.client.delete(key)

    # ========== Leaderboard (Timeframe-based Top K) ==========

    def get_leaderboard_top_k(self, leaderboard_key: str, k: int) -> List[Tuple[int, int]]:
        """
        Get top K from a specific leaderboard.

        Args:
            leaderboard_key: Redis key for the leaderboard (e.g., "global:top_videos:day")
            k: Number of top videos to return

        Returns:
            List of (video_id, view_count) tuples

        Examples:
            redis.get_leaderboard_top_k("global:top_videos:day", 10)
            redis.get_leaderboard_top_k("global:top_videos:hour", 5)
        """
        # ZREVRANGE returns highest scores first
        results = self.client.zrevrange(
            leaderboard_key,
            0,
            k - 1,
            withscores=True
        )
        return [(int(video_id), int(score)) for video_id, score in results]


# Singleton instance
_redis_service = None


def get_redis_service() -> RedisService:
    """
    Get Redis service singleton.

    Returns:
        RedisService: Initialized Redis service
    """
    global _redis_service
    if _redis_service is None:
        _redis_service = RedisService()
    return _redis_service
