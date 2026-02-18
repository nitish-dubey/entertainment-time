"""
Video transcoding service using FFmpeg.

Converts videos to multiple resolutions for adaptive streaming.
Generates HLS playlists for each quality level.
"""
import subprocess
import os
import logging
from typing import List, Dict, Optional
from pathlib import Path
import tempfile

from app.services.minio_service import MinIOService

logger = logging.getLogger(__name__)


class TranscodingService:
    """
    Handles video transcoding using FFmpeg.
    
    Features:
    - Multiple resolution outputs (1080p, 720p, 480p, 360p, 240p)
    - HLS streaming format (.m3u8 playlists + .ts segments)
    - Adaptive bitrate streaming
    - Progress tracking
    """

    # Quality presets with encoding settings
    QUALITY_PRESETS = {
        '1080p': {
            'resolution': '1920x1080',
            'video_bitrate': '5000k',
            'audio_bitrate': '192k',
            'width': 1920,
            'height': 1080
        },
        '720p': {
            'resolution': '1280x720',
            'video_bitrate': '2800k',
            'audio_bitrate': '128k',
            'width': 1280,
            'height': 720
        },
        '480p': {
            'resolution': '854x480',
            'video_bitrate': '1400k',
            'audio_bitrate': '128k',
            'width': 854,
            'height': 480
        },
        '360p': {
            'resolution': '640x360',
            'video_bitrate': '800k',
            'audio_bitrate': '96k',
            'width': 640,
            'height': 360
        },
        '240p': {
            'resolution': '426x240',
            'video_bitrate': '400k',
            'audio_bitrate': '64k',
            'width': 426,
            'height': 240
        }
    }

    def __init__(self, minio_service: Optional[MinIOService] = None):
        self.minio = minio_service or MinIOService()

    def transcode_to_hls(
        self,
        video_id: int,
        input_path: str,
        qualities: List[str] = None
    ) -> Dict[str, Dict]:
        """
        Transcode video to multiple HLS variants.

        Args:
            video_id: Video ID
            input_path: Path to source video in MinIO
            qualities: List of quality levels (default: all)

        Returns:
            Dict of quality -> variant info

        Example:
            {
                '1080p': {
                    'playlist_path': 'videos/123/1080p/playlist.m3u8',
                    'file_size': 123456789,
                    'width': 1920,
                    'height': 1080
                },
                '720p': {...},
                ...
            }
        """
        if qualities is None:
            qualities = ['1080p', '720p', '480p', '360p']

        logger.info(f"Starting transcoding for video {video_id} to {qualities}")

        # Download source video from MinIO
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_input = os.path.join(temp_dir, "input.mp4")
            self._download_from_minio(input_path, temp_input)

            # Transcode each quality
            variants = {}
            for quality in qualities:
                try:
                    variant_info = self._transcode_quality(
                        video_id,
                        temp_input,
                        quality,
                        temp_dir
                    )
                    variants[quality] = variant_info
                    logger.info(f"✓ Completed {quality} for video {video_id}")

                except Exception as e:
                    logger.error(f"Failed to transcode {quality}: {e}")
                    # Continue with other qualities

            # Generate master playlist
            if variants:
                master_path = self._generate_master_playlist(video_id, variants)
                variants['master'] = {'playlist_path': master_path}

            logger.info(f"Transcoding complete for video {video_id}: {len(variants)} variants")
            return variants

    def _transcode_quality(
        self,
        video_id: int,
        input_file: str,
        quality: str,
        temp_dir: str
    ) -> Dict:
        """
        Transcode video to specific quality using FFmpeg.

        Returns variant info dict.
        """
        preset = self.QUALITY_PRESETS[quality]

        # Create output directory
        output_dir = os.path.join(temp_dir, quality)
        os.makedirs(output_dir, exist_ok=True)

        # Output HLS playlist and segments
        playlist_file = os.path.join(output_dir, "playlist.m3u8")

        # FFmpeg command for HLS transcoding
        cmd = [
            'ffmpeg',
            '-i', input_file,
            '-vf', f"scale={preset['resolution']}:force_original_aspect_ratio=decrease,pad={preset['resolution']}:(ow-iw)/2:(oh-ih)/2",
            '-c:v', 'libx264',  # H.264 codec
            '-b:v', preset['video_bitrate'],
            '-c:a', 'aac',  # AAC audio
            '-b:a', preset['audio_bitrate'],
            '-hls_time', '10',  # 10 second segments
            '-hls_playlist_type', 'vod',
            '-hls_segment_filename', os.path.join(output_dir, 'segment_%03d.ts'),
            '-f', 'hls',
            playlist_file
        ]

        logger.info(f"Transcoding {quality}: {' '.join(cmd)}")

        # Run FFmpeg
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise Exception(f"FFmpeg failed: {result.stderr}")

        # Upload to MinIO
        minio_playlist_path = f"videos/{video_id}/hls/{quality}/playlist.m3u8"
        total_size = 0

        # Upload playlist
        self._upload_to_minio(playlist_file, minio_playlist_path)

        # Upload all segments
        for ts_file in Path(output_dir).glob("*.ts"):
            segment_path = f"videos/{video_id}/hls/{quality}/{ts_file.name}"
            self._upload_to_minio(str(ts_file), segment_path)
            total_size += ts_file.stat().st_size

        return {
            'playlist_path': minio_playlist_path,
            'file_size': total_size,
            'width': preset['width'],
            'height': preset['height'],
            'bitrate': int(preset['video_bitrate'].replace('k', '000'))
        }

    def _generate_master_playlist(
        self,
        video_id: int,
        variants: Dict[str, Dict]
    ) -> str:
        """
        Generate HLS master playlist that references all quality variants.

        Returns MinIO path to master playlist.
        """
        # Build master playlist content
        lines = ['#EXTM3U']

        for quality, info in sorted(variants.items(), key=lambda x: x[1]['width'], reverse=True):
            if quality == 'master':
                continue

            bandwidth = info['bitrate']
            resolution = f"{info['width']}x{info['height']}"

            lines.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION={resolution}')
            lines.append(f'{quality}/playlist.m3u8')

        content = '\n'.join(lines) + '\n'

        # Upload master playlist
        with tempfile.NamedTemporaryFile(mode='w', suffix='.m3u8', delete=False) as f:
            f.write(content)
            temp_master = f.name

        master_path = f"videos/{video_id}/hls/master.m3u8"
        self._upload_to_minio(temp_master, master_path)

        os.unlink(temp_master)

        logger.info(f"Generated master playlist: {master_path}")
        return master_path

    def _download_from_minio(self, minio_path: str, local_path: str):
        """Download file from MinIO to local path."""
        logger.debug(f"Downloading {minio_path} to {local_path}")
        response = self.minio.client.get_object(
            self.minio.bucket_name,
            minio_path
        )
        with open(local_path, 'wb') as f:
            f.write(response.read())

    def _upload_to_minio(self, local_path: str, minio_path: str):
        """Upload file from local path to MinIO."""
        logger.debug(f"Uploading {local_path} to {minio_path}")
        file_size = os.path.getsize(local_path)
        with open(local_path, 'rb') as f:
            self.minio.client.put_object(
                self.minio.bucket_name,
                minio_path,
                f,
                file_size
            )

    def get_video_info(self, file_path: str) -> Dict:
        """
        Get video metadata using FFprobe.

        Returns:
            Dict with width, height, duration, codec, bitrate
        """
        cmd = [
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            '-show_streams',
            file_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"FFprobe failed: {result.stderr}")

        import json
        data = json.loads(result.stdout)

        # Extract video stream info
        video_stream = next(
            (s for s in data.get('streams', []) if s['codec_type'] == 'video'),
            None
        )

        if not video_stream:
            raise Exception("No video stream found")

        return {
            'width': video_stream.get('width'),
            'height': video_stream.get('height'),
            'duration': float(data.get('format', {}).get('duration', 0)),
            'codec': video_stream.get('codec_name'),
            'bitrate': int(data.get('format', {}).get('bit_rate', 0))
        }
