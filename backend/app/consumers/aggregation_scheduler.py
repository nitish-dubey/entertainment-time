"""
Background scheduler for aggregating view data.
Maintains hourly and daily pre-aggregated tables.
"""
import time
import logging
from datetime import datetime

from app.database import SessionLocal
from app.services.aggregation_service import AggregationService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AggregationScheduler:
    """
    Runs aggregation jobs periodically.
    
    Schedule:
    - Every hour: Aggregate last hour into VideoStatsHourly
    - Every day (at midnight): Aggregate yesterday into VideoStatsDaily
    - Every week: Cleanup old data
    """

    def __init__(self):
        self.service = AggregationService()
        self.last_hourly_run = None
        self.last_daily_run = None
        self.last_cleanup_run = None

    def should_run_hourly(self) -> bool:
        """Check if it's time to run hourly aggregation."""
        now = datetime.now()
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        
        if self.last_hourly_run is None:
            return True
        
        return current_hour > self.last_hourly_run

    def should_run_daily(self) -> bool:
        """Check if it's time to run daily aggregation (midnight)."""
        now = datetime.now()
        current_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if self.last_daily_run is None:
            return False  # Don't run on first start
        
        return current_day > self.last_daily_run and now.hour == 0

    def should_run_cleanup(self) -> bool:
        """Check if it's time to run cleanup (once a week)."""
        if self.last_cleanup_run is None:
            return False
        
        days_since_cleanup = (datetime.now() - self.last_cleanup_run).days
        return days_since_cleanup >= 7

    def run_hourly_aggregation(self):
        """Run hourly aggregation job."""
        logger.info("=" * 60)
        logger.info(f"Running hourly aggregation at {datetime.now()}")
        logger.info("=" * 60)
        
        db = SessionLocal()
        try:
            count = self.service.aggregate_last_hour(db)
            logger.info(f"✓ Hourly aggregation complete: {count} videos")
            self.last_hourly_run = datetime.now().replace(minute=0, second=0, microsecond=0)
        except Exception as e:
            logger.error(f"Hourly aggregation failed: {e}", exc_info=True)
        finally:
            db.close()

    def run_daily_aggregation(self):
        """Run daily aggregation job."""
        logger.info("=" * 60)
        logger.info(f"Running daily aggregation at {datetime.now()}")
        logger.info("=" * 60)
        
        db = SessionLocal()
        try:
            count = self.service.aggregate_last_day(db)
            logger.info(f"✓ Daily aggregation complete: {count} videos")
            self.last_daily_run = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        except Exception as e:
            logger.error(f"Daily aggregation failed: {e}", exc_info=True)
        finally:
            db.close()

    def run_cleanup(self):
        """Run cleanup job."""
        logger.info("=" * 60)
        logger.info(f"Running cleanup at {datetime.now()}")
        logger.info("=" * 60)
        
        db = SessionLocal()
        try:
            self.service.cleanup_old_aggregates(db, keep_days=90)
            logger.info(f"✓ Cleanup complete")
            self.last_cleanup_run = datetime.now()
        except Exception as e:
            logger.error(f"Cleanup failed: {e}", exc_info=True)
        finally:
            db.close()

    def run(self):
        """
        Main scheduler loop.
        Checks every minute if jobs should run.
        """
        logger.info("Starting aggregation scheduler...")
        logger.info("Schedule:")
        logger.info("  - Hourly: Aggregate views into hourly stats")
        logger.info("  - Daily: Aggregate hourly stats into daily stats (midnight)")
        logger.info("  - Weekly: Cleanup old data")
        
        # Initialize timestamps
        self.last_hourly_run = datetime.now().replace(minute=0, second=0, microsecond=0)
        self.last_daily_run = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.last_cleanup_run = datetime.now()
        
        try:
            while True:
                # Check hourly aggregation
                if self.should_run_hourly():
                    self.run_hourly_aggregation()
                
                # Check daily aggregation
                if self.should_run_daily():
                    self.run_daily_aggregation()
                
                # Check cleanup
                if self.should_run_cleanup():
                    self.run_cleanup()
                
                # Sleep for 1 minute
                time.sleep(60)
                
        except KeyboardInterrupt:
            logger.info("Aggregation scheduler stopped")


def main():
    """Entry point for running scheduler as standalone process."""
    scheduler = AggregationScheduler()
    scheduler.run()


if __name__ == '__main__':
    main()
