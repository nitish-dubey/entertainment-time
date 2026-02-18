"""
Elasticsearch service for video search and indexing.
Provides fast full-text search across video metadata.
"""
from elasticsearch import Elasticsearch
from app.config import get_settings
from typing import List, Dict, Any

settings = get_settings()


class ElasticsearchService:
    """Service for searching and indexing videos."""

    def __init__(self):
        """Initialize Elasticsearch client."""
        try:
            self.client = Elasticsearch([settings.elasticsearch_host])
            self.index_name = "videos"
            self._ensure_index_exists()
            print("✅ Connected to Elasticsearch")
        except Exception as e:
            print(f"❌ Error connecting to Elasticsearch: {e}")
            raise

    def _ensure_index_exists(self):
        """
        Create index with mapping if it doesn't exist.

        Mapping defines how fields are indexed and searched.
        """
        if not self.client.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "video_id": {"type": "integer"},
                        "content_type": {"type": "keyword"},  # Exact match
                        "title": {
                            "type": "text",  # Full-text search
                            "analyzer": "standard"
                        },
                        "description": {
                            "type": "text",
                            "analyzer": "standard"
                        },
                        "show_title": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword"}  # For exact/filter
                            }
                        },
                        "season_number": {"type": "integer"},
                        "episode_number": {"type": "integer"},
                        "genre": {"type": "keyword"},
                        "release_year": {"type": "integer"},
                        "rating": {"type": "keyword"},
                        "view_count": {"type": "long"},
                        "created_at": {"type": "date"}
                    }
                }
            }
            self.client.indices.create(index=self.index_name, body=mapping)
            print(f"✅ Created Elasticsearch index: {self.index_name}")

    def index_video(self, video_id: int, video_data: Dict[str, Any]):
        """
        Index a video for search.

        Args:
            video_id: Unique video ID
            video_data: Video metadata (title, description, etc.)

        Example:
            es.index_video(123, {
                "title": "Inception",
                "description": "A dream within a dream...",
                "genre": "Sci-Fi",
                ...
            })
        """
        try:
            video_data["video_id"] = video_id
            self.client.index(
                index=self.index_name,
                id=video_id,
                document=video_data
            )
            print(f"📝 Indexed video {video_id} in Elasticsearch")
        except Exception as e:
            print(f"❌ Error indexing video: {e}")
            raise

    def search_videos(
        self,
        query: str,
        limit: int = 10,
        offset: int = 0,
        content_type: str = None,
        genre: str = None
    ) -> Dict[str, Any]:
        """
        Search videos by query.

        Args:
            query: Search query (searches title, description, show_title)
            limit: Max results to return
            offset: Offset for pagination
            content_type: Filter by content type (movie, episode, etc.)
            genre: Filter by genre

        Returns:
            Dict with 'total' count and 'results' list

        Example:
            results = es.search_videos("inception", limit=10)
            # Returns movies/shows matching "inception"
        """
        try:
            # Build search query
            must_clauses = [
                {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^3", "description", "show_title^2"],
                        # ^ boosts: title is 3x more important than description
                        "type": "best_fields",
                        "fuzziness": "AUTO"  # Handles typos
                    }
                }
            ]

            # Add filters
            filter_clauses = []
            if content_type:
                filter_clauses.append({"term": {"content_type": content_type}})
            if genre:
                filter_clauses.append({"term": {"genre": genre}})

            search_body = {
                "query": {
                    "bool": {
                        "must": must_clauses,
                        "filter": filter_clauses
                    }
                },
                "from": offset,
                "size": limit,
                "sort": [
                    {"_score": "desc"},  # Relevance first
                    {"view_count": "desc"}  # Then popularity
                ]
            }

            response = self.client.search(index=self.index_name, body=search_body)

            return {
                "total": response["hits"]["total"]["value"],
                "results": [hit["_source"] for hit in response["hits"]["hits"]]
            }
        except Exception as e:
            print(f"❌ Error searching Elasticsearch: {e}")
            raise

    def delete_video(self, video_id: int):
        """Remove video from search index."""
        try:
            self.client.delete(index=self.index_name, id=video_id)
        except Exception as e:
            print(f"❌ Error deleting from Elasticsearch: {e}")
            raise

    def update_view_count(self, video_id: int, view_count: int):
        """Update view count for a video in search results."""
        try:
            self.client.update(
                index=self.index_name,
                id=video_id,
                body={"doc": {"view_count": view_count}}
            )
        except Exception as e:
            print(f"❌ Error updating view count: {e}")


# Singleton instance
_es_service = None


def get_elasticsearch_service() -> ElasticsearchService:
    """
    Get Elasticsearch service singleton.

    Returns:
        ElasticsearchService: Initialized Elasticsearch service
    """
    global _es_service
    if _es_service is None:
        _es_service = ElasticsearchService()
    return _es_service
