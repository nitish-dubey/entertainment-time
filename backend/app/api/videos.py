"""
Video API endpoints.
Handles upload, streaming, search, and video management.
"""
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional
import os
from datetime import datetime

from app.database import get_db
from app.models import Video, ContentType, VideoVariant
from app.schemas import (
    VideoCreate, VideoResponse, VideoSearchRequest, VideoSearchResult
)
from app.services.minio_service import get_minio_service, MinIOService
from app.services.kafka_service import get_kafka_service, KafkaService
from app.services.elasticsearch_service import get_elasticsearch_service, ElasticsearchService

router = APIRouter()


@router.post("/upload", response_model=VideoResponse, status_code=status.HTTP_201_CREATED)
async def upload_video(
    # File upload
    file: UploadFile = File(..., description="Video file to upload"),

    # Video metadata
    content_type: ContentType = Form(..., description="Type of content (movie, episode, etc.)"),
    title: str = Form(..., description="Video/episode title"),
    description: Optional[str] = Form(None),

    # TV Show fields (optional)
    show_title: Optional[str] = Form(None, description="TV show title (for episodes)"),
    season_number: Optional[int] = Form(None, description="Season number (for episodes)"),
    episode_number: Optional[int] = Form(None, description="Episode number (for episodes)"),

    # Additional metadata
    genre: Optional[str] = Form(None),
    release_year: Optional[int] = Form(None),
    rating: Optional[str] = Form(None),
    uploaded_by: Optional[str] = Form(None),

    # Dependencies
    db: Session = Depends(get_db),
    minio: MinIOService = Depends(get_minio_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    """
    Upload a video file with metadata.

    Supports:
    - Movies
    - TV show episodes
    - Short videos
    - Documentaries

    Process:
    1. Upload file to MinIO
    2. Save metadata to PostgreSQL
    3. Publish event to Kafka (consumer will index in Elasticsearch)
    """
    try:
        # Validate file type
        if not file.content_type.startswith('video/'):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type: {file.content_type}. Must be a video file."
            )

        # Generate file path in MinIO
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_extension = os.path.splitext(file.filename)[1]
        file_path = f"videos/{datetime.now().year}/{timestamp}_{file.filename}"

        # Get file size
        file.file.seek(0, 2)  # Seek to end
        file_size = file.file.tell()
        file.file.seek(0)  # Seek back to start

        # Upload to MinIO
        minio.upload_video(file_path, file.file, file_size)

        # Create database record
        video = Video(
            content_type=content_type,
            title=title,
            description=description,
            show_title=show_title,
            season_number=season_number,
            episode_number=episode_number,
            genre=genre,
            release_year=release_year,
            rating=rating,
            filename=file.filename,
            file_path=file_path,
            file_size=file_size,
            uploaded_by=uploaded_by
        )

        db.add(video)
        db.commit()
        db.refresh(video)

        # Prepare video data for events
        video_data = {
            "video_id": video.id,
            "content_type": video.content_type.value,
            "title": video.title,
            "description": video.description,
            "show_title": video.show_title,
            "season_number": video.season_number,
            "episode_number": video.episode_number,
            "genre": video.genre,
            "release_year": video.release_year,
            "rating": video.rating,
            "view_count": video.view_count,
            "created_at": video.created_at.isoformat(),
            "file_path": video.file_path
        }

        # Publish event to Kafka (consumer will handle Elasticsearch indexing)
        kafka.publish_video_uploaded(video.id, video_data)

        return video

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.get("/{video_id}/stream")
async def stream_video(
    video_id: int,
    db: Session = Depends(get_db),
    minio: MinIOService = Depends(get_minio_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    """
    Stream a video file.

    Supports HTTP Range requests for seeking.
    Records view for analytics.
    """
    # Get video from database
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    try:
        # Get video stream from MinIO
        response = minio.get_video_stream(video.file_path)

        # Publish view event to Kafka (consumer will update Redis)
        kafka.publish_video_viewed(video_id)

        # Stream response
        def iterfile():
            try:
                while True:
                    chunk = response.read(8192)  # 8KB chunks
                    if not chunk:
                        break
                    yield chunk
            finally:
                response.close()
                response.release_conn()

        return StreamingResponse(
            iterfile(),
            media_type="video/mp4",
            headers={
                "Content-Disposition": f'inline; filename="{video.filename}"',
                "Accept-Ranges": "bytes"
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Streaming failed: {str(e)}")


@router.get("/{video_id}/hls/master.m3u8")
async def stream_hls_master(
    video_id: int,
    db: Session = Depends(get_db),
    minio: MinIOService = Depends(get_minio_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    """
    Stream HLS master playlist for adaptive bitrate streaming.

    Returns .m3u8 playlist that references all quality variants.
    Client player (video.js, etc.) picks appropriate quality.

    Example:
        GET /api/videos/123/hls/master.m3u8
        Returns:
            #EXTM3U
            #EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
            1080p/playlist.m3u8
            #EXT-X-STREAM-INF:BANDWIDTH=2800000,RESOLUTION=1280x720
            720p/playlist.m3u8
            ...
    """
    # Check if video exists
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    # Check if HLS variants are available
    variants = db.query(VideoVariant).filter(
        VideoVariant.video_id == video_id,
        VideoVariant.is_ready == True
    ).all()

    if not variants:
        raise HTTPException(
            status_code=404,
            detail="HLS variants not yet available. Transcoding may still be in progress."
        )

    try:
        # Get master playlist from MinIO
        master_path = f"videos/{video_id}/hls/master.m3u8"
        response = minio.client.get_object(
            minio.bucket_name,
            master_path
        )

        # Record view
        kafka.publish_video_viewed(video_id)

        # Return playlist
        content = response.read().decode('utf-8')

        return StreamingResponse(
            iter([content]),
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Content-Disposition": f'inline; filename="master.m3u8"',
                "Access-Control-Allow-Origin": "*"
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stream HLS: {str(e)}"
        )


@router.get("/{video_id}/hls/{quality}/playlist.m3u8")
async def stream_hls_variant(
    video_id: int,
    quality: str,
    db: Session = Depends(get_db),
    minio: MinIOService = Depends(get_minio_service)
):
    """
    Stream HLS variant playlist for specific quality.

    Returns .m3u8 playlist for a specific quality level.
    """
    try:
        playlist_path = f"videos/{video_id}/hls/{quality}/playlist.m3u8"
        response = minio.client.get_object(
            minio.bucket_name,
            playlist_path
        )

        content = response.read().decode('utf-8')

        return StreamingResponse(
            iter([content]),
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*"
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=404,
            detail=f"Playlist not found: {str(e)}"
        )


@router.get("/{video_id}/hls/{quality}/{segment}")
async def stream_hls_segment(
    video_id: int,
    quality: str,
    segment: str,
    minio: MinIOService = Depends(get_minio_service)
):
    """
    Stream HLS video segment (.ts file).

    Serves individual video chunks for HLS streaming.
    """
    try:
        segment_path = f"videos/{video_id}/hls/{quality}/{segment}"
        response = minio.client.get_object(
            minio.bucket_name,
            segment_path
        )

        def iterfile():
            try:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    yield chunk
            finally:
                response.close()
                response.release_conn()

        return StreamingResponse(
            iterfile(),
            media_type="video/mp2t",
            headers={
                "Access-Control-Allow-Origin": "*"
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=404,
            detail=f"Segment not found: {str(e)}"
        )


@router.get("/search", response_model=VideoSearchResult)
async def search_videos(
    query: str,
    limit: int = 10,
    offset: int = 0,
    content_type: Optional[str] = None,
    genre: Optional[str] = None,
    db: Session = Depends(get_db),
    es: ElasticsearchService = Depends(get_elasticsearch_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    """
    Search for videos by query.

    Searches across:
    - Title
    - Description
    - Show title (for episodes)

    Supports fuzzy matching for typos.
    """
    try:
        # Search in Elasticsearch
        results = es.search_videos(
            query=query,
            limit=limit,
            offset=offset,
            content_type=content_type,
            genre=genre
        )

        # Get full video details from database
        video_ids = [r["video_id"] for r in results["results"]]
        videos = db.query(Video).filter(Video.id.in_(video_ids)).all()

        # Sort videos to match search result order
        video_dict = {v.id: v for v in videos}
        sorted_videos = [video_dict[vid] for vid in video_ids if vid in video_dict]

        # Publish search event
        kafka.publish_video_searched(query, results["total"])

        return VideoSearchResult(
            total=results["total"],
            videos=sorted_videos
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.get("/{video_id}", response_model=VideoResponse)
async def get_video(
    video_id: int,
    db: Session = Depends(get_db)
):
    """Get video details by ID."""
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")
    return video


@router.delete("/{video_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_video(
    video_id: int,
    db: Session = Depends(get_db),
    minio: MinIOService = Depends(get_minio_service),
    es: ElasticsearchService = Depends(get_elasticsearch_service)
):
    """Delete a video."""
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    try:
        # Delete from MinIO
        minio.delete_video(video.file_path)

        # Delete from Elasticsearch
        es.delete_video(video_id)

        # Delete from database
        db.delete(video)
        db.commit()

        return None

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
