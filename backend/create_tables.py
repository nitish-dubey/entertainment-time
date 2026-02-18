"""
Create database tables.
"""
import logging
from app.database import engine, Base
from app.models import Video, View, VideoStatsHourly, VideoStatsDaily, TranscodingJob, VideoVariant, WatchHistory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_tables():
    """Create all database tables."""
    logger.info('Creating database tables...')

    # Import all models to register them with Base
    # This ensures all tables are created

    Base.metadata.create_all(bind=engine)
    logger.info('✅ Database tables created successfully')

    # Verify tables exist
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    logger.info(f'Tables created: {sorted(tables)}')

    return tables


if __name__ == '__main__':
    tables = create_tables()
    print(f"\n✓ Created {len(tables)} tables:")
    for table in sorted(tables):
        print(f"  - {table}")
