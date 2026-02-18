"""
Analytics API endpoints.
Handles top K videos with Redis primary and PostgreSQL fallback.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import redis
import logging

from app.database import get_db
from app.models import Video
from app.schemas import TopVideosResponse, Timeframe
from app.services.redis_service import get_redis_service, RedisService
from app.services.analytics_service import get_analytics_service, AnalyticsService

logger = logging.getLogger(__name__)

router = APIRouter()


# Timeframe to seconds mapping
TIMEFRAME_SECONDS = {
    Timeframe.HOUR: 3600,           # 60 * 60
    Timeframe.DAY: 86400,           # 24 * 60 * 60
    Timeframe.WEEK: 604800,         # 7 * 24 * 60 * 60
    Timeframe.MONTH: 2592000,       # 30 * 24 * 60 * 60
    Timeframe.YEAR: 31536000,       # 365 * 24 * 60 * 60
    Timeframe.ALL_TIME: None        # No time limit
}


@router.get("/top", response_model=TopVideosResponse)
async def get_top_videos(
    k: int = 10,
    timeframe: Timeframe = Timeframe.DAY,
    db: Session = Depends(get_db),
    redis: RedisService = Depends(get_redis_service),
    analytics: AnalyticsService = Depends(get_analytics_service)
):
    """
    Get top K most watched videos with Redis primary and PostgreSQL fallback.

    Strategy:
    1. Try Redis leaderboards (fast, pre-calculated, refreshed every 5 min)
    2. If Redis fails, fall back to PostgreSQL Views table (slower but reliable)

    Args:
        k: Number of top videos to return (1-100)
        timeframe: Time window (hour, day, week, month, year, all_time)

    Returns:
        List of top K videos sorted by view count

    Examples:
        GET /api/analytics/top?k=10&timeframe=hour
        GET /api/analytics/top?k=20&timeframe=week
    """
    # Validate k
    if k < 1 or k > 100:
        raise HTTPException(status_code=400, detail="k must be between 1 and 100")

    top_video_tuples = []
    source = "redis"

    # Try Redis first (primary, fast)
    try:
        leaderboard_key = f"global:top_videos:{timeframe.value}"
        top_video_tuples = redis.get_leaderboard_top_k(leaderboard_key, k)

        if not top_video_tuples:
            # Empty leaderboard, might be new system or no views
            logger.info(f"Redis leaderboard '{leaderboard_key}' is empty")

    except redis.RedisError as e:
        # Redis is down, try 3-level fallback
        logger.warning(f"Redis unavailable, falling back to PostgreSQL: {e}")

        # Level 2: Try pre-aggregated tables (fast)
        try:
            logger.info("Trying pre-aggregated tables...")
            top_video_tuples = analytics.get_top_videos_from_aggregates(db, k, timeframe)
            source = "aggregates"

            if not top_video_tuples:
                # Aggregates might not exist yet, fall back further
                raise Exception("No aggregated data available")

        except Exception as agg_error:
            # Level 3: Fall back to raw Views table (slow but reliable)
            logger.warning(f"Aggregates unavailable, using raw views: {agg_error}")
            source = "views"

            try:
                top_video_tuples = analytics.get_top_videos_from_db(db, k, timeframe)
            except Exception as db_error:
                logger.error(f"All fallbacks failed: {db_error}")
                raise HTTPException(
                    status_code=503,
                    detail="Analytics service temporarily unavailable"
                )

    except Exception as e:
        # Unexpected error, try 3-level fallback
        logger.error(f"Unexpected Redis error, trying fallbacks: {e}")

        # Level 2: Try aggregates
        try:
            top_video_tuples = analytics.get_top_videos_from_aggregates(db, k, timeframe)
            source = "aggregates"

            if not top_video_tuples:
                raise Exception("No aggregated data")

        except Exception as agg_error:
            # Level 3: Raw views
            logger.warning(f"Aggregates failed, using raw views: {agg_error}")
            source = "views"

            try:
                top_video_tuples = analytics.get_top_videos_from_db(db, k, timeframe)
            except Exception as db_error:
                logger.error(f"All fallbacks failed: {db_error}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to get top videos: {str(e)}"
                )

    # If no results, return empty
    if not top_video_tuples:
        logger.info(f"No top videos found for timeframe: {timeframe.value} (source: {source})")
        return TopVideosResponse(videos=[])

    # Extract video IDs
    video_ids = [video_id for video_id, _ in top_video_tuples]

    # Fetch full video details from PostgreSQL
    videos = db.query(Video).filter(Video.id.in_(video_ids)).all()

    # Sort videos to match order
    video_dict = {v.id: v for v in videos}
    sorted_videos = [video_dict[vid] for vid in video_ids if vid in video_dict]

    logger.info(f"Returned top {len(sorted_videos)} videos from {source} for {timeframe.value}")
    return TopVideosResponse(videos=sorted_videos)


@router.get("/videos/{video_id}/stats")
async def get_video_stats(
    video_id: int,
    db: Session = Depends(get_db),
    redis: RedisService = Depends(get_redis_service),
    analytics: AnalyticsService = Depends(get_analytics_service)
):
    """
    Get detailed statistics for a video across different timeframes.

    Returns view counts for:
    - Last hour
    - Last day
    - Last week
    - Last month
    - All time

    Strategy:
    1. Try Redis (fast, real-time sliding windows)
    2. If Redis fails, fall back to PostgreSQL Views table
    """
    # Check if video exists
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    source = "redis"

    try:
        # Try Redis first
        stats = {
            "video_id": video_id,
            "title": video.title,
            "total_views": redis.get_view_count(video_id),
            "views_last_hour": redis.get_view_count(
                video_id, TIMEFRAME_SECONDS[Timeframe.HOUR]
            ),
            "views_last_day": redis.get_view_count(
                video_id, TIMEFRAME_SECONDS[Timeframe.DAY]
            ),
            "views_last_week": redis.get_view_count(
                video_id, TIMEFRAME_SECONDS[Timeframe.WEEK]
            ),
            "views_last_month": redis.get_view_count(
                video_id, TIMEFRAME_SECONDS[Timeframe.MONTH]
            ),
        }

    except redis.RedisError as e:
        # Redis is down, fall back to PostgreSQL
        logger.warning(f"Redis unavailable for stats, using PostgreSQL: {e}")
        source = "postgresql"

        try:
            stats = {
                "video_id": video_id,
                "title": video.title,
                "total_views": analytics.get_video_view_count(db, video_id),
                "views_last_hour": analytics.get_video_view_count(
                    db, video_id, TIMEFRAME_SECONDS[Timeframe.HOUR]
                ),
                "views_last_day": analytics.get_video_view_count(
                    db, video_id, TIMEFRAME_SECONDS[Timeframe.DAY]
                ),
                "views_last_week": analytics.get_video_view_count(
                    db, video_id, TIMEFRAME_SECONDS[Timeframe.WEEK]
                ),
                "views_last_month": analytics.get_video_view_count(
                    db, video_id, TIMEFRAME_SECONDS[Timeframe.MONTH]
                ),
            }
        except Exception as db_error:
            logger.error(f"PostgreSQL fallback failed: {db_error}")
            raise HTTPException(
                status_code=503,
                detail="Analytics service temporarily unavailable"
            )

    except Exception as e:
        # Unexpected error, try PostgreSQL
        logger.error(f"Unexpected error getting stats from Redis: {e}")
        source = "postgresql"

        try:
            stats = {
                "video_id": video_id,
                "title": video.title,
                "total_views": analytics.get_video_view_count(db, video_id),
                "views_last_hour": analytics.get_video_view_count(
                    db, video_id, TIMEFRAME_SECONDS[Timeframe.HOUR]
                ),
                "views_last_day": analytics.get_video_view_count(
                    db, video_id, TIMEFRAME_SECONDS[Timeframe.DAY]
                ),
                "views_last_week": analytics.get_video_view_count(
                    db, video_id, TIMEFRAME_SECONDS[Timeframe.WEEK]
                ),
                "views_last_month": analytics.get_video_view_count(
                    db, video_id, TIMEFRAME_SECONDS[Timeframe.MONTH]
                ),
            }
        except Exception as db_error:
            logger.error(f"PostgreSQL fallback failed: {db_error}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get stats: {str(e)}"
            )

    logger.info(f"Returned stats for video {video_id} from {source}")
    return stats
