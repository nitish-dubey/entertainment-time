# EntertainmentTime

A Netflix-like video streaming platform with distributed systems architecture.

## Features

### Core Streaming
- 🎬 Video upload (simple + S3-style multipart for large files)
- 📺 Adaptive bitrate streaming (HLS with multiple quality levels)
- 🎥 Progressive download streaming (HTTP Range support)
- 🔄 Automatic video transcoding (FFmpeg to 1080p, 720p, 480p, 360p, 240p)
- ⏯️ Resume playback from where you left off
- 📺 TV show support (seasons/episodes)
- 🎥 Movie support

### Search & Discovery
- 🔍 Full-text search with fuzzy matching (Elasticsearch)
- 🔎 Search across title, description, and show titles
- 🏷️ Filter by content type and genre

### Analytics & Performance
- 📊 Real-time top K analytics (Redis sorted sets)
- ⏱️ Sliding time windows (hour, day, week, month, year, all-time)
- 🚀 3-level fallback for reliability (Redis → Aggregates → Raw data)
- 📈 Pre-aggregated statistics (hourly/daily)
- ⚡ Fast queries: O(log k) for top K, 1-2ms with caching

### Watch History
- 📝 Watch position tracking (auto-save every 10 seconds)
- 🎯 Continue watching section
- ✅ Completed videos tracking
- 💾 Write-through cache (Redis + PostgreSQL)
- 🔄 Background flush (batch updates every 30 seconds)

## Architecture

### Infrastructure
- **MinIO**: S3-compatible object storage for video files
- **PostgreSQL**: Primary database for video metadata
- **Kafka**: Event streaming for analytics
- **Elasticsearch**: Full-text search
- **Redis**: Real-time analytics and leaderboards
- **FastAPI**: Backend API server

### Components
1. **API Server**: FastAPI endpoints for upload, streaming, search, analytics, watch position
2. **Kafka Consumer**: Processes video_viewed events with idempotency
3. **Leaderboard Scheduler**: Refreshes top K leaderboards every 5 minutes (atomic RENAME)
4. **Aggregation Scheduler**: Pre-aggregates hourly and daily statistics
5. **Transcoding Worker**: Converts videos to multiple resolutions using FFmpeg
6. **Watch Position Flusher**: Batches position updates from Redis to PostgreSQL every 30s

## Quick Start

### 1. Start Infrastructure

```bash
cd /Users/nitish/Programming/EntertainmentTime
docker-compose up -d
```

This starts:
- MinIO (S3): http://localhost:9001 (admin/adminpass)
- PostgreSQL: localhost:5432
- Kafka: localhost:9092
- Elasticsearch: http://localhost:9200
- Redis: localhost:6379

### 2. Install Dependencies

```bash
cd backend
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -r requirements.txt
```

### 3. Run Backend Services

**Option A: Run all services together (recommended)**
```bash
python run_all.py
```

This starts:
- FastAPI server on http://localhost:8000
- Kafka consumer
- Leaderboard scheduler

**Option B: Run services separately**

Terminal 1 - API Server:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Terminal 2 - Kafka Consumer:
```bash
python -m app.consumers.video_consumer
```

Terminal 3 - Leaderboard Scheduler:
```bash
python -m app.consumers.leaderboard_scheduler
```

### 4. Access API

- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## API Endpoints

### Video Management

#### Upload Video (Simple)
```bash
POST /api/videos/upload
Content-Type: multipart/form-data

Fields:
- file: video file
- content_type: movie|episode|short|documentary
- title: string
- description: string (optional)
- show_title: string (for episodes)
- season_number: int (for episodes)
- episode_number: int (for episodes)
- genre: string
- release_year: int
- rating: string
```

#### Multipart Upload (for large files)
```bash
# Step 1: Initiate
POST /api/multipart/initiate

# Step 2: Upload parts (can be done in parallel)
POST /api/multipart/part

# Step 3: Complete
POST /api/multipart/complete

# Optional: List uploaded parts (for resume)
GET /api/multipart/list-parts

# Optional: Abort upload
POST /api/multipart/abort
```

See `frontend_multipart_example.html` for a working example.

#### Stream Video
```bash
GET /api/videos/{video_id}/stream
```

Supports HTTP Range requests for seeking.

#### Search Videos
```bash
GET /api/videos/search?query=avatar&limit=10&offset=0
```

#### Get Video Details
```bash
GET /api/videos/{video_id}
```

#### Delete Video
```bash
DELETE /api/videos/{video_id}
```

### Analytics

#### Get Top K Videos
```bash
GET /api/analytics/top?k=10&timeframe=day

Timeframes:
- hour: Last hour
- day: Last 24 hours (default)
- week: Last 7 days
- month: Last 30 days
- year: Last 365 days
- all_time: All time
```

#### Get Video Stats
```bash
GET /api/analytics/videos/{video_id}/stats

Returns:
- total_views
- views_last_hour
- views_last_day
- views_last_week
- views_last_month
```

## How It Works

### Video Upload Flow
1. Client uploads video to API
2. API stores video in MinIO
3. API creates record in PostgreSQL
4. API publishes `video_uploaded` event to Kafka
5. API indexes video in Elasticsearch

### Video Transcoding Flow
1. Video uploaded to MinIO
2. Transcoding job created (status: PENDING)
3. Transcoding worker picks up job
4. FFmpeg transcodes to multiple resolutions in parallel
5. Each quality generates HLS playlist (.m3u8) and segments (.ts)
6. Segments stored in MinIO: `videos/{id}/hls/{quality}/`
7. Job status updated to COMPLETED
8. VideoVariant records created for each quality

### Video Streaming Flow
1. Client requests HLS master playlist
2. API returns master.m3u8 with all quality options
3. Video.js player selects quality based on bandwidth
4. Client requests quality-specific playlist
5. Client downloads video segments (.ts files)
6. API records view in Redis and publishes `video_viewed` event to Kafka
7. Player automatically switches quality based on network conditions

### Watch Position Flow
1. **Save**: User watching video
   - Auto-save every 10 seconds to Redis (1-2ms)
   - Position added to flush queue
   - Return success immediately
2. **Background Flush**: Every 30 seconds
   - Read up to 1000 positions from Redis queue
   - Batch UPSERT to PostgreSQL (single transaction)
   - Clear dirty flags and remove from queue
3. **Resume**: User returns to video
   - Check Redis first (cache hit: 1-2ms)
   - Fallback to PostgreSQL if not in Redis (10-20ms)
   - Show "Continue Watching" banner if position > 30s

### Analytics Flow
1. Kafka consumer processes `video_viewed` events
2. Consumer updates Redis sorted sets (for sliding windows)
3. Leaderboard scheduler runs every 5 minutes:
   - Calculates view counts for each timeframe
   - Builds new leaderboard in temp key
   - Atomically swaps using RENAME
   - Cleans up views older than 30 days
4. API queries pre-calculated leaderboards (O(1) lookup)

### Redis Data Structure

**Individual Views** (for sliding window queries):
```
Key: video:{video_id}:views
Type: Sorted Set
Score: timestamp
Member: view_id (user_id:timestamp or anon:timestamp)
TTL: 30 days

Example:
video:123:views = {
  "user_456:1705315800.123": 1705315800.123,
  "anon:1705316000.456": 1705316000.456
}
```

**Total View Counts**:
```
Key: video:{video_id}:total_views
Type: String (counter)
Value: total view count
```

**Global Leaderboards** (for fast top K):
```
Key: global:top_videos:{timeframe}
Type: Sorted Set
Score: view_count
Member: video_id

Example:
global:top_videos:day = {
  "123": 1500,
  "456": 1200,
  "789": 800
}
```

### Multipart Upload

For large files (>5MB), use the multipart upload API:

1. **Initiate**: Get upload_id and calculate total parts
2. **Upload Parts**: Upload 5MB chunks (can be done in parallel)
3. **Complete**: MinIO assembles all parts into final file

Benefits:
- **Resume capability**: If upload fails, only retry failed parts
- **Parallel uploads**: Upload multiple parts simultaneously
- **Progress tracking**: Update UI as each part completes

## Testing

### Upload a Test Video
```bash
curl -X POST http://localhost:8000/api/videos/upload \
  -F "file=@test_video.mp4" \
  -F "content_type=movie" \
  -F "title=Test Movie" \
  -F "description=A test movie" \
  -F "genre=Action" \
  -F "release_year=2024"
```

### Search Videos
```bash
curl "http://localhost:8000/api/videos/search?query=test&limit=10"
```

### Get Top 10 Videos (Last Day)
```bash
curl "http://localhost:8000/api/analytics/top?k=10&timeframe=day"
```

### Stream Video
```bash
# In browser
http://localhost:8000/api/videos/1/stream
```

## Monitoring

### Check Kafka Topics
```bash
docker exec -it entertainment-kafka kafka-topics \
  --list --bootstrap-server localhost:9092
```

### Check Redis Data
```bash
docker exec -it entertainment-redis redis-cli

# View count for video 123
> GET video:123:total_views

# Views in last hour for video 123
> ZCOUNT video:123:views <start_timestamp> <end_timestamp>

# Top 10 from day leaderboard
> ZREVRANGE global:top_videos:day 0 9 WITHSCORES
```

### Check Elasticsearch
```bash
curl http://localhost:9200/videos/_search?pretty
```

## Stopping Services

```bash
# Stop backend services (if using run_all.py)
Ctrl+C

# Stop infrastructure
docker-compose down

# Stop infrastructure and remove volumes
docker-compose down -v
```

## Development

### Project Structure
```
EntertainmentTime/
├── docker-compose.yml          # Infrastructure
├── backend/
│   ├── app/
│   │   ├── api/                # API endpoints
│   │   │   ├── videos.py
│   │   │   ├── analytics.py
│   │   │   └── multipart_upload.py
│   │   ├── consumers/          # Kafka consumers & jobs
│   │   │   ├── video_consumer.py
│   │   │   └── leaderboard_scheduler.py
│   │   ├── services/           # External services
│   │   │   ├── minio_service.py
│   │   │   ├── kafka_service.py
│   │   │   ├── elasticsearch_service.py
│   │   │   └── redis_service.py
│   │   ├── models.py           # Database models
│   │   ├── schemas.py          # Pydantic schemas
│   │   ├── config.py           # Configuration
│   │   ├── database.py         # Database connection
│   │   └── main.py             # FastAPI app
│   ├── run_all.py              # Run all services
│   └── requirements.txt
└── frontend_multipart_example.html  # Example frontend
```

## Already Implemented ✅

- ✅ **PostgreSQL fallback for analytics** - 3-level fallback (Redis → Pre-aggregated tables → Raw views)
- ✅ **Views table** - Complete historical analytics with VideoStatsHourly and VideoStatsDaily
- ✅ **Video transcoding** - FFmpeg transcoding to multiple resolutions (1080p, 720p, 480p, 360p, 240p)
- ✅ **Adaptive bitrate streaming** - HLS with automatic quality switching
- ✅ **Watch position tracking** - Redis write-through cache with background flush to PostgreSQL
- ✅ **Resume playback** - Continue watching from where user left off
- ✅ **Watch history** - Continue watching + Completed videos
- ✅ **Background workers** - Transcoding, flushing, aggregation, leaderboard refresh
- ✅ **Idempotent event processing** - Event IDs prevent duplicate processing
- ✅ **Atomic operations** - RENAME for race-condition-free leaderboard updates

## Future Enhancements

- [ ] Implement user authentication (JWT)
- [ ] Add thumbnail generation from videos
- [ ] Build proper frontend (React/Vue/Next.js)
- [ ] Add rate limiting (per user/IP)
- [ ] Add video recommendations (ML-based)
- [ ] Add user watchlist/favorites
- [ ] Add content moderation
- [ ] Add CDN integration (CloudFront/CloudFlare)
- [ ] Add home screen with featured/trending content
- [ ] Add TV shows catalog organization
- [ ] Add browse by genre endpoints
- [ ] Add user profiles
- [ ] Add comments/reviews
- [ ] Add subtitles/closed captions support

## License

MIT
