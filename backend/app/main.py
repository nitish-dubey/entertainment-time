"""
Main FastAPI application entry point.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import get_settings
from app.database import engine, Base

settings = get_settings()

# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="A Netflix-like video streaming platform",
    version="1.0.0"
)

# CORS middleware - allows frontend to call our API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create database tables
Base.metadata.create_all(bind=engine)


@app.get("/")
async def root():
    """Root endpoint - health check."""
    return {
        "message": f"Welcome to {settings.app_name}!",
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    TODO: Check connections to MinIO, Kafka, Elasticsearch, Redis, PostgreSQL
    """
    return {
        "status": "healthy",
        "services": {
            "api": "ok",
            # We'll add service checks later
        }
    }


# Include API routers
from app.api import videos, analytics, multipart_upload, watch_position
app.include_router(videos.router, prefix="/api/videos", tags=["videos"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["analytics"])
app.include_router(multipart_upload.router, prefix="/api/multipart", tags=["multipart-upload"])
app.include_router(watch_position.router, prefix="/api", tags=["watch-position"])
