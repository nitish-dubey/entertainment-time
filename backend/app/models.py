"""
Database models using SQLAlchemy ORM.
"""
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Text, Enum as SQLEnum, ForeignKey, Index, Boolean, Float
from sqlalchemy.sql import func
from app.database import Base
import enum


class ContentType(enum.Enum):
    """Types of content in the platform."""
    MOVIE = "movie"
    EPISODE = "episode"
    SHORT = "short"
    DOCUMENTARY = "documentary"


class TranscodingStatus(enum.Enum):
    """Status of video transcoding job."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class VideoQuality(enum.Enum):
    """Video quality/resolution options."""
    ORIGINAL = "original"  # Original uploaded quality
    Q_1080P = "1080p"      # Full HD
    Q_720P = "720p"        # HD
    Q_480P = "480p"        # SD
    Q_360P = "360p"        # Mobile
    Q_240P = "240p"        # Low bandwidth


class Video(Base):
    """
    Video/Content metadata stored in PostgreSQL.
    Supports movies, TV show episodes, shorts, etc.
    The actual video file is stored in MinIO.
    """
    __tablename__ = "videos"

    id = Column(Integer, primary_key=True, index=True)

    # Content type and metadata
    content_type = Column(SQLEnum(ContentType), nullable=False, default=ContentType.MOVIE, index=True)
    title = Column(String(255), nullable=False, index=True)  # Movie title or episode title
    description = Column(Text, nullable=True)

    # TV Show specific fields (only used when content_type = EPISODE)
    show_title = Column(String(255), nullable=True, index=True)  # e.g., "Breaking Bad"
    season_number = Column(Integer, nullable=True)  # e.g., 1, 2, 3
    episode_number = Column(Integer, nullable=True)  # e.g., 1, 2, 3

    # Additional metadata
    genre = Column(String(100), nullable=True, index=True)  # e.g., "Action", "Drama"
    release_year = Column(Integer, nullable=True)
    rating = Column(String(10), nullable=True)  # e.g., "PG-13", "TV-MA"

    # File information
    filename = Column(String(255), nullable=False)  # Original filename
    file_path = Column(String(500), nullable=False)  # Path in MinIO bucket
    file_size = Column(BigInteger, nullable=False)  # Size in bytes
    duration = Column(Integer, nullable=True)  # Duration in seconds

    # User engagement
    uploaded_by = Column(String(100), nullable=True)  # User who uploaded
    view_count = Column(BigInteger, default=0)  # Total views

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        if self.content_type == ContentType.EPISODE:
            return f"<Video(id={self.id}, type=EPISODE, show='{self.show_title}' S{self.season_number}E{self.episode_number})>"
        return f"<Video(id={self.id}, type={self.content_type.value}, title='{self.title}')>"


class View(Base):
    """
    Individual view event record.

    Stores each video view with timestamp for analytics fallback.
    Used when Redis is down or for long-term analytics.
    Indexed for fast timeframe queries.
    """
    __tablename__ = "views"

    id = Column(Integer, primary_key=True, index=True)
    video_id = Column(Integer, ForeignKey('videos.id', ondelete='CASCADE'), nullable=False)
    user_id = Column(String(255), nullable=True)  # Optional user identifier
    viewed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Indexes for performance
    __table_args__ = (
        # Fast lookup: views for specific video in timeframe
        Index('idx_view_video_viewed_at', 'video_id', 'viewed_at'),
        # Fast lookup: all views in timeframe (for top K)
        Index('idx_view_viewed_at', 'viewed_at'),
        # Fast count: views per video
        Index('idx_view_video_id', 'video_id'),
    )

    def __repr__(self):
        return f"<View(video_id={self.video_id}, viewed_at={self.viewed_at})>"


class VideoStatsHourly(Base):
    """
    Pre-aggregated hourly view counts.

    Much faster than querying individual views for analytics fallback.
    Maintained by background aggregation job.

    Example:
    - video_id=123, hour_start='2024-01-15 14:00:00', view_count=1500
    - Means video 123 got 1500 views between 2pm-3pm on Jan 15
    """
    __tablename__ = "video_stats_hourly"

    id = Column(Integer, primary_key=True, index=True)
    video_id = Column(Integer, ForeignKey('videos.id', ondelete='CASCADE'), nullable=False)
    hour_start = Column(DateTime(timezone=True), nullable=False)  # Start of hour (e.g., 14:00:00)
    view_count = Column(Integer, nullable=False, default=0)

    # Composite index for fast timeframe queries
    __table_args__ = (
        Index('idx_video_hour', 'video_id', 'hour_start'),
        Index('idx_hour', 'hour_start'),
        # Ensure uniqueness: one record per video per hour
        Index('idx_unique_video_hour', 'video_id', 'hour_start', unique=True),
    )

    def __repr__(self):
        return f"<VideoStatsHourly(video_id={self.video_id}, hour={self.hour_start}, count={self.view_count})>"


class VideoStatsDaily(Base):
    """
    Pre-aggregated daily view counts.

    Even faster for weekly/monthly queries.

    Example:
    - video_id=123, date='2024-01-15', view_count=35000
    - Means video 123 got 35K views on Jan 15
    """
    __tablename__ = "video_stats_daily"

    id = Column(Integer, primary_key=True, index=True)
    video_id = Column(Integer, ForeignKey('videos.id', ondelete='CASCADE'), nullable=False)
    date = Column(DateTime(timezone=True), nullable=False)  # Date (e.g., 2024-01-15 00:00:00)
    view_count = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index('idx_video_date', 'video_id', 'date'),
        Index('idx_date', 'date'),
        Index('idx_unique_video_date', 'video_id', 'date', unique=True),
    )

    def __repr__(self):
        return f"<VideoStatsDaily(video_id={self.video_id}, date={self.date}, count={self.view_count})>"


class TranscodingJob(Base):
    """
    Transcoding job for converting videos to multiple resolutions.

    Tracks progress of FFmpeg transcoding jobs.
    """
    __tablename__ = "transcoding_jobs"

    id = Column(Integer, primary_key=True, index=True)
    video_id = Column(Integer, ForeignKey('videos.id', ondelete='CASCADE'), nullable=False)
    status = Column(SQLEnum(TranscodingStatus), nullable=False, default=TranscodingStatus.PENDING)

    # Progress tracking
    progress = Column(Float, default=0.0)  # 0.0 to 100.0
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Error handling
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index('idx_transcoding_status', 'status'),
        Index('idx_transcoding_video_id', 'video_id'),
    )

    def __repr__(self):
        return f"<TranscodingJob(video_id={self.video_id}, status={self.status.value})>"


class VideoVariant(Base):
    """
    Represents a transcoded version of a video at specific quality.

    Each video has multiple variants (1080p, 720p, 480p, etc.).
    Used for adaptive bitrate streaming.
    """
    __tablename__ = "video_variants"

    id = Column(Integer, primary_key=True, index=True)
    video_id = Column(Integer, ForeignKey('videos.id', ondelete='CASCADE'), nullable=False)
    quality = Column(SQLEnum(VideoQuality), nullable=False)

    # File information
    file_path = Column(String(500), nullable=False)  # Path in MinIO
    file_size = Column(BigInteger, nullable=False)  # Size in bytes

    # Video properties
    width = Column(Integer, nullable=True)  # e.g., 1920
    height = Column(Integer, nullable=True)  # e.g., 1080
    bitrate = Column(Integer, nullable=True)  # bits per second
    codec = Column(String(50), nullable=True)  # e.g., "h264"

    # HLS/DASH
    playlist_path = Column(String(500), nullable=True)  # .m3u8 file path
    is_ready = Column(Boolean, default=False)  # Ready for streaming

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index('idx_video_quality', 'video_id', 'quality'),
        Index('idx_video_ready', 'video_id', 'is_ready'),
        # Ensure one variant per quality per video
        Index('idx_unique_video_quality', 'video_id', 'quality', unique=True),
    )

    def __repr__(self):
        return f"<VideoVariant(video_id={self.video_id}, quality={self.quality.value}, ready={self.is_ready})>"


class WatchHistory(Base):
    """
    Track user's watch position for resume playback.

    Stores current position, completion status, and watch history.
    Enables "Continue Watching" feature.
    """
    __tablename__ = "watch_history"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(255), nullable=False)  # User identifier
    video_id = Column(Integer, ForeignKey('videos.id', ondelete='CASCADE'), nullable=False)

    # Position tracking
    position_seconds = Column(Integer, nullable=False, default=0)  # Current position (e.g., 150s)
    duration_seconds = Column(Integer, nullable=True)  # Total video duration
    progress_percent = Column(Float, nullable=False, default=0.0)  # 0.0 to 100.0

    # Completion tracking
    completed = Column(Boolean, default=False)  # Watched to end?
    watch_count = Column(Integer, default=1)  # How many times watched

    # Timestamps
    last_watched_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        # One record per user per video
        Index('idx_unique_user_video', 'user_id', 'video_id', unique=True),
        # Fast lookup: user's watch history
        Index('idx_user_last_watched', 'user_id', 'last_watched_at'),
        # Fast lookup: incomplete videos (for "Continue Watching")
        Index('idx_user_incomplete', 'user_id', 'completed', 'last_watched_at'),
    )

    def __repr__(self):
        return f"<WatchHistory(user={self.user_id}, video={self.video_id}, position={self.position_seconds}s, {self.progress_percent:.1f}%)>"
