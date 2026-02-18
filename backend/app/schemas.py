"""
Pydantic schemas for request/response validation.
These define the shape of data coming in and going out of our API.
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from enum import Enum


class ContentType(str, Enum):
    """Types of content in the platform."""
    MOVIE = "movie"
    EPISODE = "episode"
    SHORT = "short"
    DOCUMENTARY = "documentary"


class Timeframe(str, Enum):
    """Enum for timeframe options in top videos query."""
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL_TIME = "all_time"


class VideoBase(BaseModel):
    """Base video/content schema with common fields."""
    content_type: ContentType = Field(default=ContentType.MOVIE, description="Type of content")
    title: str = Field(..., min_length=1, max_length=255, description="Movie/episode title")
    description: Optional[str] = None

    # TV Show specific fields (only used when content_type = EPISODE)
    show_title: Optional[str] = Field(None, max_length=255, description="TV show title (for episodes)")
    season_number: Optional[int] = Field(None, ge=1, description="Season number (for episodes)")
    episode_number: Optional[int] = Field(None, ge=1, description="Episode number (for episodes)")

    # Additional metadata
    genre: Optional[str] = Field(None, max_length=100, description="Genre (e.g., Action, Drama)")
    release_year: Optional[int] = Field(None, ge=1900, le=2100, description="Release year")
    rating: Optional[str] = Field(None, max_length=10, description="Content rating (e.g., PG-13, TV-MA)")

    uploaded_by: Optional[str] = None


class VideoCreate(VideoBase):
    """Schema for creating a new video (used in upload)."""
    pass


class VideoResponse(VideoBase):
    """Schema for video responses (returned to client)."""
    id: int
    filename: str
    file_size: int
    duration: Optional[int]
    view_count: int
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True  # Allows converting SQLAlchemy models to Pydantic


class VideoSearchResult(BaseModel):
    """Schema for search results."""
    total: int
    videos: list[VideoResponse]


class TopVideosResponse(BaseModel):
    """Schema for top K most watched videos."""
    videos: list[VideoResponse]


class VideoSearchRequest(BaseModel):
    """Request schema for searching videos."""
    query: str = Field(..., min_length=1, max_length=200, description="Search query")
    limit: int = Field(default=10, ge=1, le=100, description="Max results to return")
    offset: int = Field(default=0, ge=0, description="Offset for pagination")

    class Config:
        json_schema_extra = {
            "example": {
                "query": "inception",
                "limit": 10,
                "offset": 0
            }
        }


class TopVideosRequest(BaseModel):
    """Request schema for getting top K videos."""
    k: int = Field(default=10, ge=1, le=100, description="Number of top videos to return")
    timeframe: Timeframe = Field(default=Timeframe.DAY, description="Timeframe for top videos")
    category: Optional[str] = Field(None, max_length=50, description="Filter by category")

    class Config:
        json_schema_extra = {
            "example": {
                "k": 10,
                "timeframe": "day",
                "category": "action"
            }
        }


# ========== Watch Position & History ==========

class WatchPositionUpdate(BaseModel):
    """Request to update watch position."""
    position_seconds: int = Field(..., ge=0, description="Current position in seconds")
    duration_seconds: Optional[int] = Field(None, ge=0, description="Total video duration in seconds")

    class Config:
        json_schema_extra = {
            "example": {
                "position_seconds": 150,
                "duration_seconds": 3600
            }
        }


class WatchPositionResponse(BaseModel):
    """Response with watch position info."""
    user_id: str
    video_id: int
    position_seconds: int
    duration_seconds: Optional[int]
    progress_percent: float
    completed: bool
    last_watched_at: datetime

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "user_id": "user_123",
                "video_id": 456,
                "position_seconds": 150,
                "duration_seconds": 3600,
                "progress_percent": 4.17,
                "completed": False,
                "last_watched_at": "2024-01-15T14:30:00Z"
            }
        }


class WatchHistoryItem(BaseModel):
    """Single item in watch history."""
    video_id: int
    video_title: str
    position_seconds: int
    duration_seconds: Optional[int]
    progress_percent: float
    completed: bool
    watch_count: int
    last_watched_at: datetime

    class Config:
        from_attributes = True


class WatchHistoryResponse(BaseModel):
    """Response with user's watch history."""
    user_id: str
    total_count: int
    continue_watching: list[WatchHistoryItem] = Field(
        default_factory=list,
        description="Incomplete videos, sorted by last watched"
    )
    completed: list[WatchHistoryItem] = Field(
        default_factory=list,
        description="Completed videos"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user_123",
                "total_count": 15,
                "continue_watching": [
                    {
                        "video_id": 456,
                        "video_title": "Inception",
                        "position_seconds": 1800,
                        "duration_seconds": 8880,
                        "progress_percent": 20.3,
                        "completed": False,
                        "watch_count": 1,
                        "last_watched_at": "2024-01-15T14:30:00Z"
                    }
                ],
                "completed": []
            }
        }
