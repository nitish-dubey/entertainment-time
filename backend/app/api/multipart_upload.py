"""
Multipart upload endpoints for large video files.
Supports resume, parallel uploads, and better reliability.
"""
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Dict
from datetime import datetime
import os

from app.database import get_db
from app.models import Video, ContentType
from app.schemas import VideoResponse
from app.services.minio_service import get_minio_service, MinIOService
from app.services.kafka_service import get_kafka_service, KafkaService

router = APIRouter()


@router.post("/initiate")
async def initiate_multipart_upload(
    filename: str = Form(...),
    file_size: int = Form(...),
    content_type: ContentType = Form(...),
    title: str = Form(...),
    description: str = Form(None),
    show_title: str = Form(None),
    season_number: int = Form(None),
    episode_number: int = Form(None),
    genre: str = Form(None),
    release_year: int = Form(None),
    rating: str = Form(None),
    minio: MinIOService = Depends(get_minio_service)
):
    """
    Step 1: Initiate multipart upload.

    Returns:
        - upload_id: Unique ID for this upload session
        - file_path: Where file will be stored in MinIO
        - part_size: Size of each part (5MB)
        - total_parts: Total number of parts to upload

    Example response:
    {
        "upload_id": "abc123",
        "file_path": "videos/2024/movie.mp4",
        "part_size": 5242880,
        "total_parts": 410
    }
    """
    try:
        # Generate file path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"videos/{datetime.now().year}/{timestamp}_{filename}"

        # Calculate part size and number of parts
        # MinIO default: 5MB per part
        part_size = 5 * 1024 * 1024  # 5MB
        total_parts = (file_size + part_size - 1) // part_size  # Ceiling division

        # Initiate multipart upload in MinIO
        # MinIO's internal API for multipart upload
        upload_id = minio.client._create_multipart_upload(
            bucket_name=minio.bucket_name,
            object_name=file_path,
            headers={
                'Content-Type': 'video/mp4'
            }
        )

        return {
            "upload_id": upload_id,
            "file_path": file_path,
            "part_size": part_size,
            "total_parts": total_parts,
            "metadata": {
                "content_type": content_type,
                "title": title,
                "description": description,
                "show_title": show_title,
                "season_number": season_number,
                "episode_number": episode_number,
                "genre": genre,
                "release_year": release_year,
                "rating": rating
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initiate upload: {str(e)}"
        )


@router.post("/part")
async def upload_part(
    upload_id: str = Form(...),
    file_path: str = Form(...),
    part_number: int = Form(...),
    part: UploadFile = File(...),
    minio: MinIOService = Depends(get_minio_service)
):
    """
    Step 2: Upload a single part.

    Client can upload parts in parallel for faster speeds!
    Client can retry failed parts without re-uploading entire file!

    Args:
        upload_id: Upload ID from initiate
        file_path: File path from initiate
        part_number: Part number (1-indexed)
        part: The chunk data (5MB)

    Returns:
        - part_number: Part number uploaded
        - etag: ETag for this part (needed for complete)
        - size: Size of uploaded part

    Example:
        Upload part 1 of 410:
        POST /api/multipart/part
        Form data:
            upload_id=abc123
            file_path=videos/2024/movie.mp4
            part_number=1
            part=[5MB binary data]
    """
    try:
        # Get part size
        part.file.seek(0, 2)  # Seek to end
        part_size = part.file.tell()
        part.file.seek(0)  # Seek back to start

        # Upload part to MinIO
        response = minio.client._upload_part(
            bucket_name=minio.bucket_name,
            object_name=file_path,
            upload_id=upload_id,
            part_number=part_number,
            data=part.file,
            headers={'Content-Length': str(part_size)}
        )

        return {
            "part_number": part_number,
            "etag": response.etag,
            "size": part_size
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload part {part_number}: {str(e)}"
        )


@router.post("/complete", response_model=VideoResponse)
async def complete_multipart_upload(
    upload_id: str = Form(...),
    file_path: str = Form(...),
    parts: str = Form(...),  # JSON string of parts
    filename: str = Form(...),
    file_size: int = Form(...),

    # Metadata
    content_type: ContentType = Form(...),
    title: str = Form(...),
    description: str = Form(None),
    show_title: str = Form(None),
    season_number: int = Form(None),
    episode_number: int = Form(None),
    genre: str = Form(None),
    release_year: int = Form(None),
    rating: str = Form(None),
    uploaded_by: str = Form(None),

    # Dependencies
    db: Session = Depends(get_db),
    minio: MinIOService = Depends(get_minio_service),
    kafka: KafkaService = Depends(get_kafka_service)
):
    """
    Step 3: Complete multipart upload.

    MinIO will assemble all parts into the final video file.
    Then we create the database record and index it.

    Args:
        upload_id: Upload ID from initiate
        file_path: File path from initiate
        parts: JSON string like '[{"part_number": 1, "etag": "..."}, ...]'
        + all metadata fields

    Returns:
        Complete video record
    """
    try:
        import json

        # Parse parts JSON
        parts_list = json.loads(parts)

        # Sort by part number (required by MinIO)
        parts_list.sort(key=lambda x: x['part_number'])

        # Complete multipart upload in MinIO
        # This tells MinIO to assemble all parts into final file
        minio.client._complete_multipart_upload(
            bucket_name=minio.bucket_name,
            object_name=file_path,
            upload_id=upload_id,
            parts=parts_list
        )

        # Now create database record
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
            filename=filename,
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
        raise HTTPException(
            status_code=500,
            detail=f"Failed to complete upload: {str(e)}"
        )


@router.post("/abort")
async def abort_multipart_upload(
    upload_id: str = Form(...),
    file_path: str = Form(...),
    minio: MinIOService = Depends(get_minio_service)
):
    """
    Abort multipart upload.

    Cleans up partially uploaded parts.
    Use this if user cancels upload or it fails completely.

    Args:
        upload_id: Upload ID from initiate
        file_path: File path from initiate
    """
    try:
        minio.client._abort_multipart_upload(
            bucket_name=minio.bucket_name,
            object_name=file_path,
            upload_id=upload_id
        )

        return {"message": "Upload aborted successfully"}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to abort upload: {str(e)}"
        )


@router.get("/list-parts")
async def list_uploaded_parts(
    upload_id: str,
    file_path: str,
    minio: MinIOService = Depends(get_minio_service)
):
    """
    List already uploaded parts.

    Use this to implement resume:
    1. Check which parts are already uploaded
    2. Only upload missing parts

    Args:
        upload_id: Upload ID from initiate
        file_path: File path from initiate

    Returns:
        List of uploaded parts: [{"part_number": 1, "etag": "...", "size": 5242880}, ...]
    """
    try:
        parts = minio.client._list_parts(
            bucket_name=minio.bucket_name,
            object_name=file_path,
            upload_id=upload_id
        )

        return {
            "uploaded_parts": [
                {
                    "part_number": part.part_number,
                    "etag": part.etag,
                    "size": part.size
                }
                for part in parts
            ]
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list parts: {str(e)}"
        )
