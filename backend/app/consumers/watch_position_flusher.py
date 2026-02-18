"""
Background job to flush watch positions from Redis to PostgreSQL.

Runs every 30 seconds to persist cached positions.
"""
import time
import logging

from app.services.watch_position_service import WatchPositionService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WatchPositionFlusher:
    """
    Periodically flushes watch positions from Redis to PostgreSQL.
    
    Strategy:
    - Every 30 seconds
    - Batch flush up to 1000 positions
    - Reduces database load
    """

    def __init__(self, interval_seconds: int = 30):
        self.service = WatchPositionService()
        self.interval = interval_seconds

    def run(self):
        """Main loop."""
        logger.info(f"Starting watch position flusher (interval: {self.interval}s)")

        try:
            while True:
                self.service.flush_to_database(batch_size=1000)
                time.sleep(self.interval)

        except KeyboardInterrupt:
            logger.info("Flusher stopped")


def main():
    """Entry point."""
    flusher = WatchPositionFlusher(interval_seconds=30)
    flusher.run()


if __name__ == '__main__':
    main()
