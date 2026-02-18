#!/bin/bash

echo "================================================"
echo "EntertainmentTime System Test"
echo "================================================"
echo ""

API="http://localhost:8000/api"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test 1: Health Check
echo -e "${BLUE}[1/6] Testing Health Check...${NC}"
HEALTH=$(curl -s ${API}/../health)
if echo $HEALTH | grep -q "healthy"; then
    echo -e "${GREEN}✓ API is healthy${NC}"
else
    echo -e "${RED}✗ API health check failed${NC}"
    exit 1
fi
echo ""

# Test 2: Create a test video entry (simulated upload)
echo -e "${BLUE}[2/6] Creating test video...${NC}"
cat > /tmp/test_video.json << 'JSON'
{
    "content_type": "movie",
    "title": "Test Movie - Inception",
    "description": "A mind-bending thriller",
    "genre": "Sci-Fi",
    "release_year": 2010,
    "rating": "PG-13"
}
JSON

# We'll insert directly into database since we don't have actual video file
cd /Users/nitish/Programming/EntertainmentTime/backend && source .venv/bin/activate && python << PYTHON
from app.database import SessionLocal
from app.models import Video, ContentType

db = SessionLocal()

# Check if test video exists
existing = db.query(Video).filter(Video.title == "Test Movie - Inception").first()

if not existing:
    video = Video(
        content_type=ContentType.MOVIE,
        title="Test Movie - Inception",
        description="A mind-bending thriller",
        genre="Sci-Fi",
        release_year=2010,
        rating="PG-13",
        filename="test_inception.mp4",
        file_path="videos/test/inception.mp4",
        file_size=1000000,
        duration=8880  # 2h 28m
    )
    db.add(video)
    db.commit()
    print(f"Created video with ID: {video.id}")
else:
    print(f"Video already exists with ID: {existing.id}")
    
db.close()
PYTHON

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Test video created${NC}"
else
    echo -e "${RED}✗ Failed to create test video${NC}"
fi
echo ""

# Get video ID
VIDEO_ID=$(cd /Users/nitish/Programming/EntertainmentTime/backend && source .venv/bin/activate && python -c "from app.database import SessionLocal; from app.models import Video; db = SessionLocal(); v = db.query(Video).filter(Video.title == 'Test Movie - Inception').first(); print(v.id if v else 1); db.close()")

echo -e "Using Video ID: ${VIDEO_ID}"
echo ""

# Test 3: Get video details
echo -e "${BLUE}[3/6] Testing Get Video Details...${NC}"
VIDEO=$(curl -s ${API}/videos/${VIDEO_ID})
if echo $VIDEO | grep -q "Test Movie"; then
    echo -e "${GREEN}✓ Video details retrieved${NC}"
    echo $VIDEO | python3 -m json.tool | head -10
else
    echo -e "${RED}✗ Failed to get video details${NC}"
fi
echo ""

# Test 4: Save watch position
echo -e "${BLUE}[4/6] Testing Watch Position Save...${NC}"
SAVE_RESPONSE=$(curl -s -X POST "${API}/videos/${VIDEO_ID}/position?user_id=test_user" \
    -H "Content-Type: application/json" \
    -d '{"position_seconds": 150, "duration_seconds": 3600}')

if echo $SAVE_RESPONSE | grep -q "test_user"; then
    echo -e "${GREEN}✓ Watch position saved${NC}"
    echo "Position: 150s / 3600s"
else
    echo -e "${RED}✗ Failed to save position${NC}"
    echo $SAVE_RESPONSE
fi
echo ""

# Test 5: Get watch position
echo -e "${BLUE}[5/6] Testing Watch Position Retrieval...${NC}"
GET_POSITION=$(curl -s "${API}/videos/${VIDEO_ID}/position?user_id=test_user")
if echo $GET_POSITION | grep -q "position_seconds"; then
    echo -e "${GREEN}✓ Watch position retrieved${NC}"
    echo $GET_POSITION | python3 -m json.tool
else
    echo -e "${RED}✗ Failed to get position${NC}"
fi
echo ""

# Test 6: Get watch history
echo -e "${BLUE}[6/6] Testing Watch History...${NC}"
HISTORY=$(curl -s "${API}/users/test_user/history?limit=10")
if echo $HISTORY | grep -q "continue_watching"; then
    echo -e "${GREEN}✓ Watch history retrieved${NC}"
    echo $HISTORY | python3 -m json.tool | head -20
else
    echo -e "${RED}✗ Failed to get history${NC}"
fi
echo ""

# Summary
echo "================================================"
echo -e "${GREEN}System Test Complete!${NC}"
echo "================================================"
echo ""
echo "Next steps:"
echo "1. Open http://localhost:8000/docs - API documentation"
echo "2. Test video streaming (need actual video file)"
echo "3. Open frontend_resume_playback.html in browser"
echo ""
