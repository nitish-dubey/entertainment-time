"""
Analytics service with PostgreSQL fallback.
Provides analytics when Redis is down.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
import logging

from app.models import View, Video, VideoStatsHourly, VideoStatsDaily
from app.schemas import Timeframe

logger = logging.getLogger(__name__)


class AnalyticsService:
    """
    PostgreSQL-based analytics fallback.
    
    Used when Redis is unavailable.
    Slower than Redis but reliable.
    """

    @staticmethod
    def get_timeframe_cutoff(timeframe: Timeframe) -> Optional[datetime]:
        """
        Get cutoff datetime for timeframe.
        
        Args:
            timeframe: Timeframe enum
            
        Returns:
            Cutoff datetime or None for all_time
        """
        if timeframe == Timeframe.ALL_TIME:
            return None
        
        timeframe_map = {
            Timeframe.HOUR: timedelta(hours=1),
            Timeframe.DAY: timedelta(days=1),
            Timeframe.WEEK: timedelta(weeks=1),
            Timeframe.MONTH: timedelta(days=30),
            Timeframe.YEAR: timedelta(days=365),
        }
        
        delta = timeframe_map.get(timeframe)
        if delta:
            return datetime.now() - delta
        return None

    @staticmethod
    def get_top_videos_from_aggregates(
        db: Session,
        k: int,
        timeframe: Timeframe
    ) -> List[Tuple[int, int]]:
        """
        Get top K videos from pre-aggregated tables (FAST fallback).

        Uses hourly/daily aggregates instead of individual views.
        Much faster than querying Views table directly.

        Args:
            db: Database session
            k: Number of top videos
            timeframe: Time window

        Returns:
            List of (video_id, view_count) tuples

        Performance:
        - Hour: Query 1 hour of hourly stats → ~1000 videos max
        - Day: Query 24 hours of hourly stats → ~24K rows max
        - Week: Query 7 days of daily stats → ~7K rows max
        - Much faster than millions of individual views!
        """
        try:
            cutoff = AnalyticsService.get_timeframe_cutoff(timeframe)

            # Choose aggregation level based on timeframe
            if timeframe in [Timeframe.HOUR, Timeframe.DAY]:
                # Use hourly aggregates (more granular)
                if cutoff:
                    results = db.query(
                        VideoStatsHourly.video_id,
                        func.sum(VideoStatsHourly.view_count).label('view_count')
                    ).filter(
                        VideoStatsHourly.hour_start >= cutoff
                    ).group_by(VideoStatsHourly.video_id)\
                     .order_by(func.sum(VideoStatsHourly.view_count).desc())\
                     .limit(k)\
                     .all()
                else:
                    # All time - sum all hourly stats
                    results = db.query(
                        VideoStatsHourly.video_id,
                        func.sum(VideoStatsHourly.view_count).label('view_count')
                    ).group_by(VideoStatsHourly.video_id)\
                     .order_by(func.sum(VideoStatsHourly.view_count).desc())\
                     .limit(k)\
                     .all()

            else:
                # Use daily aggregates (faster for longer timeframes)
                if cutoff:
                    results = db.query(
                        VideoStatsDaily.video_id,
                        func.sum(VideoStatsDaily.view_count).label('view_count')
                    ).filter(
                        VideoStatsDaily.date >= cutoff
                    ).group_by(VideoStatsDaily.video_id)\
                     .order_by(func.sum(VideoStatsDaily.view_count).desc())\
                     .limit(k)\
                     .all()
                else:
                    # All time - sum all daily stats
                    results = db.query(
                        VideoStatsDaily.video_id,
                        func.sum(VideoStatsDaily.view_count).label('view_count')
                    ).group_by(VideoStatsDaily.video_id)\
                     .order_by(func.sum(VideoStatsDaily.view_count).desc())\
                     .limit(k)\
                     .all()

            return [(video_id, view_count) for video_id, view_count in results]

        except Exception as e:
            logger.error(f"Error getting top videos from aggregates: {e}", exc_info=True)
            return []

    @staticmethod
    def get_top_videos_from_db(
        db: Session,
        k: int,
        timeframe: Timeframe
    ) -> List[Tuple[int, int]]:
        """
        Get top K videos from PostgreSQL Views table.
        
        Slower than Redis but works when Redis is down.
        
        Args:
            db: Database session
            k: Number of top videos
            timeframe: Time window
            
        Returns:
            List of (video_id, view_count) tuples
            
        Performance:
        - Uses indexed query on viewed_at
        - GROUP BY and COUNT aggregation
        - O(N) where N = views in timeframe
        """
        try:
            cutoff = AnalyticsService.get_timeframe_cutoff(timeframe)
            
            # Build query
            query = db.query(
                View.video_id,
                func.count(View.id).label('view_count')
            )
            
            # Apply timeframe filter
            if cutoff:
                query = query.filter(View.viewed_at >= cutoff)
            
            # Group, order, limit
            results = query.group_by(View.video_id)\
                          .order_by(func.count(View.id).desc())\
                          .limit(k)\
                          .all()
            
            return [(video_id, view_count) for video_id, view_count in results]
            
        except Exception as e:
            logger.error(f"Error getting top videos from DB: {e}", exc_info=True)
            return []

    @staticmethod
    def get_video_view_count(
        db: Session,
        video_id: int,
        timeframe_seconds: Optional[int] = None
    ) -> int:
        """
        Get view count for a specific video from PostgreSQL.
        
        Args:
            db: Database session
            video_id: Video ID
            timeframe_seconds: Seconds to look back (None = all time)
            
        Returns:
            View count
        """
        try:
            query = db.query(func.count(View.id)).filter(View.video_id == video_id)
            
            if timeframe_seconds:
                cutoff = datetime.now() - timedelta(seconds=timeframe_seconds)
                query = query.filter(View.viewed_at >= cutoff)
            
            return query.scalar() or 0
            
        except Exception as e:
            logger.error(f"Error getting view count from DB: {e}", exc_info=True)
            return 0


# Singleton instance
_analytics_service = None


def get_analytics_service() -> AnalyticsService:
    """Get analytics service singleton."""
    global _analytics_service
    if _analytics_service is None:
        _analytics_service = AnalyticsService()
    return _analytics_service
