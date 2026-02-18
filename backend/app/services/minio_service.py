"""
MinIO service for video storage and streaming.
Handles upload, download, and multipart upload operations.
"""
from minio import Minio
from minio.error import S3Error
from app.config import get_settings
import io
from typing import BinaryIO

settings = get_settings()


class MinIOService:
    """Service for interacting with MinIO object storage."""

    def __init__(self):
        """Initialize MinIO client."""
        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=False  # Use HTTP (not HTTPS) for local development
        )
        self.bucket_name = settings.minio_bucket
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                print(f"✅ Created MinIO bucket: {self.bucket_name}")
            else:
                print(f"✅ MinIO bucket exists: {self.bucket_name}")
        except S3Error as e:
            print(f"❌ Error checking/creating bucket: {e}")
            raise

    def upload_video(self, file_path: str, file_data: BinaryIO, file_size: int) -> str:
        """
        Upload a video file to MinIO.

        Args:
            file_path: Path in bucket (e.g., "videos/2024/movie.mp4")
            file_data: File-like object with video data
            file_size: Size of file in bytes

        Returns:
            str: Path of uploaded file in MinIO

        Example:
            with open("video.mp4", "rb") as f:
                path = minio_service.upload_video("videos/video.mp4", f, file_size)
        """
        try:
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=file_path,
                data=file_data,
                length=file_size,
                content_type="video/mp4"
            )
            return file_path
        except S3Error as e:
            print(f"❌ Error uploading to MinIO: {e}")
            raise

    def get_video(self, file_path: str) -> bytes:
        """
        Download a video file from MinIO.

        Args:
            file_path: Path in bucket

        Returns:
            bytes: Video file data
        """
        try:
            response = self.client.get_object(self.bucket_name, file_path)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            print(f"❌ Error downloading from MinIO: {e}")
            raise

    def get_video_stream(self, file_path: str):
        """
        Get video as a stream for efficient serving.

        Args:
            file_path: Path in bucket

        Returns:
            HTTPResponse: Streamable response object

        Example:
            stream = minio_service.get_video_stream("videos/movie.mp4")
            # Can stream chunks without loading entire file into memory
        """
        try:
            return self.client.get_object(self.bucket_name, file_path)
        except S3Error as e:
            print(f"❌ Error streaming from MinIO: {e}")
            raise

    def delete_video(self, file_path: str):
        """Delete a video file from MinIO."""
        try:
            self.client.remove_object(self.bucket_name, file_path)
        except S3Error as e:
            print(f"❌ Error deleting from MinIO: {e}")
            raise

    def get_presigned_url(self, file_path: str, expires_seconds: int = 3600) -> str:
        """
        Generate a presigned URL for temporary access to a video.

        Args:
            file_path: Path in bucket
            expires_seconds: URL expiration time (default 1 hour)

        Returns:
            str: Presigned URL

        Use case: Give frontend a temporary link to stream video
        """
        try:
            url = self.client.presigned_get_object(
                self.bucket_name,
                file_path,
                expires=expires_seconds
            )
            return url
        except S3Error as e:
            print(f"❌ Error generating presigned URL: {e}")
            raise


# Singleton instance
_minio_service = None


def get_minio_service() -> MinIOService:
    """
    Get MinIO service singleton.

    Returns:
        MinIOService: Initialized MinIO service

    Usage in FastAPI:
        @app.post("/upload")
        def upload(minio: MinIOService = Depends(get_minio_service)):
            minio.upload_video(...)
    """
    global _minio_service
    if _minio_service is None:
        _minio_service = MinIOService()
    return _minio_service
