"""
Watch position and history API endpoints.

Tracks user's watch progress for resume playback.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import and_
from datetime import datetime
from typing import Optional

from app.database import get_db
from app.models import WatchHistory, Video
from app.schemas import (
    WatchPositionUpdate,
    WatchPositionResponse,
    WatchHistoryResponse,
    WatchHistoryItem
)
from app.services.redis_service import RedisService, get_redis_service

router = APIRouter()


@router.post("/videos/{video_id}/position", response_model=WatchPositionResponse)
async def update_watch_position(
    video_id: int,
    user_id: str,  # In production, get from JWT token
    position: WatchPositionUpdate,
    db: Session = Depends(get_db),
    redis: RedisService = Depends(get_redis_service)
):
    """
    Update user's watch position for a video.

    Called periodically (every 10s) from video player.
    Enables resume playback.

    Args:
        video_id: Video being watched
        user_id: User identifier (from auth token)
        position: Current position and duration

    Example:
        POST /api/videos/123/position?user_id=user_456
        {
            "position_seconds": 150,
            "duration_seconds": 3600
        }
    """
    # Verify video exists
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    # Calculate progress percentage
    progress_percent = 0.0
    if position.duration_seconds and position.duration_seconds > 0:
        progress_percent = (position.position_seconds / position.duration_seconds) * 100

    # Mark as completed if watched > 90%
    completed = progress_percent >= 90.0

    # Update or create watch history
    watch_history = db.query(WatchHistory).filter(
        and_(
            WatchHistory.user_id == user_id,
            WatchHistory.video_id == video_id
        )
    ).first()

    if watch_history:
        # Update existing
        watch_history.position_seconds = position.position_seconds
        watch_history.duration_seconds = position.duration_seconds
        watch_history.progress_percent = progress_percent
        watch_history.completed = completed
        watch_history.last_watched_at = datetime.now()

        # Increment watch count if restarting completed video
        if watch_history.completed and position.position_seconds < 60:
            watch_history.watch_count += 1
            watch_history.completed = False

    else:
        # Create new
        watch_history = WatchHistory(
            user_id=user_id,
            video_id=video_id,
            position_seconds=position.position_seconds,
            duration_seconds=position.duration_seconds,
            progress_percent=progress_percent,
            completed=completed,
            watch_count=1,
            last_watched_at=datetime.now()
        )
        db.add(watch_history)

    db.commit()
    db.refresh(watch_history)

    # Cache in Redis for fast access (7 day TTL)
    try:
        redis_key = f"watch_position:{user_id}:{video_id}"
        redis.client.setex(
            redis_key,
            604800,  # 7 days
            position.position_seconds
        )
    except Exception as e:
        # Redis failure shouldn't break the request
        pass

    return watch_history


@router.get("/videos/{video_id}/position", response_model=WatchPositionResponse)
async def get_watch_position(
    video_id: int,
    user_id: str,  # In production, get from JWT token
    db: Session = Depends(get_db),
    redis: RedisService = Depends(get_redis_service)
):
    """
    Get user's saved watch position for a video.

    Returns position to resume playback.

    Example:
        GET /api/videos/123/position?user_id=user_456

        Response:
        {
            "position_seconds": 150,
            "duration_seconds": 3600,
            "progress_percent": 4.17,
            "completed": false
        }
    """
    # Try Redis first (fast)
    try:
        redis_key = f"watch_position:{user_id}:{video_id}"
        cached_position = redis.client.get(redis_key)

        if cached_position:
            # Get full details from database
            watch_history = db.query(WatchHistory).filter(
                and_(
                    WatchHistory.user_id == user_id,
                    WatchHistory.video_id == video_id
                )
            ).first()

            if watch_history:
                return watch_history

    except Exception as e:
        # Fall through to database
        pass

    # Get from database
    watch_history = db.query(WatchHistory).filter(
        and_(
            WatchHistory.user_id == user_id,
            WatchHistory.video_id == video_id
        )
    ).first()

    if not watch_history:
        raise HTTPException(
            status_code=404,
            detail="No watch history found for this video"
        )

    return watch_history


@router.post("/videos/{video_id}/mark-complete", status_code=status.HTTP_204_NO_CONTENT)
async def mark_video_complete(
    video_id: int,
    user_id: str,
    db: Session = Depends(get_db)
):
    """
    Mark video as completed (watched to the end).

    Example:
        POST /api/videos/123/mark-complete?user_id=user_456
    """
    watch_history = db.query(WatchHistory).filter(
        and_(
            WatchHistory.user_id == user_id,
            WatchHistory.video_id == video_id
        )
    ).first()

    if watch_history:
        watch_history.completed = True
        watch_history.progress_percent = 100.0
        watch_history.last_watched_at = datetime.now()
        db.commit()

    return None


@router.get("/users/{user_id}/history", response_model=WatchHistoryResponse)
async def get_watch_history(
    user_id: str,
    limit: int = 20,
    db: Session = Depends(get_db)
):
    """
    Get user's watch history.

    Returns:
    - Continue watching: Incomplete videos (< 90% watched)
    - Completed: Finished videos (>= 90% watched)

    Sorted by last_watched_at (most recent first).

    Example:
        GET /api/users/user_456/history?limit=20
    """
    # Get all watch history for user
    history_records = db.query(WatchHistory, Video).join(
        Video, WatchHistory.video_id == Video.id
    ).filter(
        WatchHistory.user_id == user_id
    ).order_by(
        WatchHistory.last_watched_at.desc()
    ).limit(limit).all()

    # Split into continue watching and completed
    continue_watching = []
    completed = []

    for watch_history, video in history_records:
        item = WatchHistoryItem(
            video_id=video.id,
            video_title=video.title,
            position_seconds=watch_history.position_seconds,
            duration_seconds=watch_history.duration_seconds,
            progress_percent=watch_history.progress_percent,
            completed=watch_history.completed,
            watch_count=watch_history.watch_count,
            last_watched_at=watch_history.last_watched_at
        )

        if watch_history.completed:
            completed.append(item)
        else:
            continue_watching.append(item)

    return WatchHistoryResponse(
        user_id=user_id,
        total_count=len(history_records),
        continue_watching=continue_watching,
        completed=completed
    )


@router.delete("/users/{user_id}/history/{video_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_watch_history(
    user_id: str,
    video_id: int,
    db: Session = Depends(get_db),
    redis: RedisService = Depends(get_redis_service)
):
    """
    Delete watch history for a specific video.

    Removes from "Continue Watching" list.

    Example:
        DELETE /api/users/user_456/history/123
    """
    watch_history = db.query(WatchHistory).filter(
        and_(
            WatchHistory.user_id == user_id,
            WatchHistory.video_id == video_id
        )
    ).first()

    if watch_history:
        db.delete(watch_history)
        db.commit()

        # Clear Redis cache
        try:
            redis_key = f"watch_position:{user_id}:{video_id}"
            redis.client.delete(redis_key)
        except:
            pass

    return None
