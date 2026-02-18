"""
Configuration settings for EntertainmentTime backend.
Loads environment variables and provides typed settings.
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # App
    app_name: str = "EntertainmentTime"
    debug: bool = True

    # Database
    database_url: str

    # MinIO
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str

    # Kafka
    kafka_bootstrap_servers: str

    # Elasticsearch
    elasticsearch_host: str

    # Redis
    redis_host: str
    redis_port: int

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    Using lru_cache ensures we only load .env once.
    """
    return Settings()
