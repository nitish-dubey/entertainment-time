"""
Pre-aggregation service for analytics.

Maintains hourly and daily view count tables for fast fallback queries.
Runs periodically to aggregate from Views table.
"""
import logging
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from app.models import View, VideoStatsHourly, VideoStatsDaily

logger = logging.getLogger(__name__)


class AggregationService:
    """
    Aggregates view data into hourly/daily buckets.
    
    Strategy:
    - Run every hour to aggregate previous hour
    - Aggregate Views → VideoStatsHourly
    - Aggregate VideoStatsHourly → VideoStatsDaily
    """

    @staticmethod
    def aggregate_last_hour(db: Session):
        """
        Aggregate views from last hour into VideoStatsHourly.
        
        Called every hour by background job.
        """
        logger.info("Aggregating last hour...")
        
        # Calculate hour range (previous full hour)
        now = datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        hour_end = hour_start + timedelta(hours=1)
        
        logger.info(f"Aggregating views from {hour_start} to {hour_end}")
        
        # Query: Group views by video_id for this hour
        results = db.query(
            View.video_id,
            func.count(View.id).label('view_count')
        ).filter(
            and_(
                View.viewed_at >= hour_start,
                View.viewed_at < hour_end
            )
        ).group_by(View.video_id).all()
        
        # Upsert into VideoStatsHourly
        for video_id, view_count in results:
            # Check if record exists
            existing = db.query(VideoStatsHourly).filter(
                and_(
                    VideoStatsHourly.video_id == video_id,
                    VideoStatsHourly.hour_start == hour_start
                )
            ).first()
            
            if existing:
                # Update existing
                existing.view_count = view_count
            else:
                # Insert new
                stat = VideoStatsHourly(
                    video_id=video_id,
                    hour_start=hour_start,
                    view_count=view_count
                )
                db.add(stat)
        
        db.commit()
        
        logger.info(f"✓ Aggregated {len(results)} videos for hour {hour_start}")
        return len(results)

    @staticmethod
    def aggregate_last_day(db: Session):
        """
        Aggregate hourly stats into daily stats.
        
        Called once per day.
        """
        logger.info("Aggregating last day...")
        
        # Calculate day range (previous full day)
        now = datetime.now()
        day_start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        
        logger.info(f"Aggregating day from {day_start} to {day_end}")
        
        # Query: Sum hourly stats for this day
        results = db.query(
            VideoStatsHourly.video_id,
            func.sum(VideoStatsHourly.view_count).label('view_count')
        ).filter(
            and_(
                VideoStatsHourly.hour_start >= day_start,
                VideoStatsHourly.hour_start < day_end
            )
        ).group_by(VideoStatsHourly.video_id).all()
        
        # Upsert into VideoStatsDaily
        for video_id, view_count in results:
            existing = db.query(VideoStatsDaily).filter(
                and_(
                    VideoStatsDaily.video_id == video_id,
                    VideoStatsDaily.date == day_start
                )
            ).first()
            
            if existing:
                existing.view_count = view_count
            else:
                stat = VideoStatsDaily(
                    video_id=video_id,
                    date=day_start,
                    view_count=view_count
                )
                db.add(stat)
        
        db.commit()
        
        logger.info(f"✓ Aggregated {len(results)} videos for day {day_start}")
        return len(results)

    @staticmethod
    def backfill_hourly(db: Session, days_back: int = 7):
        """
        Backfill hourly aggregations for past N days.
        
        Use this to populate historical data.
        """
        logger.info(f"Backfilling hourly aggregations for last {days_back} days...")
        
        end_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=days_back)
        
        current_hour = start_time
        aggregated = 0
        
        while current_hour < end_time:
            next_hour = current_hour + timedelta(hours=1)
            
            # Aggregate this hour
            results = db.query(
                View.video_id,
                func.count(View.id).label('view_count')
            ).filter(
                and_(
                    View.viewed_at >= current_hour,
                    View.viewed_at < next_hour
                )
            ).group_by(View.video_id).all()
            
            # Insert (skip if exists)
            for video_id, view_count in results:
                existing = db.query(VideoStatsHourly).filter(
                    and_(
                        VideoStatsHourly.video_id == video_id,
                        VideoStatsHourly.hour_start == current_hour
                    )
                ).first()
                
                if not existing:
                    stat = VideoStatsHourly(
                        video_id=video_id,
                        hour_start=current_hour,
                        view_count=view_count
                    )
                    db.add(stat)
                    aggregated += 1
            
            if aggregated % 100 == 0:
                db.commit()  # Commit in batches
                logger.info(f"Progress: {current_hour}")
            
            current_hour = next_hour
        
        db.commit()
        logger.info(f"✓ Backfilled {aggregated} hourly records")

    @staticmethod
    def cleanup_old_aggregates(db: Session, keep_days: int = 90):
        """
        Delete old aggregation records to save space.
        
        Args:
            keep_days: Keep this many days of aggregates
        """
        logger.info(f"Cleaning up aggregates older than {keep_days} days...")
        
        cutoff = datetime.now() - timedelta(days=keep_days)
        
        # Delete old hourly stats
        deleted_hourly = db.query(VideoStatsHourly).filter(
            VideoStatsHourly.hour_start < cutoff
        ).delete()
        
        # Delete old daily stats
        deleted_daily = db.query(VideoStatsDaily).filter(
            VideoStatsDaily.date < cutoff
        ).delete()
        
        db.commit()
        
        logger.info(f"✓ Deleted {deleted_hourly} hourly + {deleted_daily} daily records")
