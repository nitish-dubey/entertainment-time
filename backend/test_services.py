"""
Quick test script to verify all services connect properly.
"""
import sys
sys.path.insert(0, 'app')

from app.services.minio_service import get_minio_service
from app.services.kafka_service import get_kafka_service
from app.services.elasticsearch_service import get_elasticsearch_service
from app.services.redis_service import get_redis_service


def test_services():
    """Test connection to all services."""
    print("\n🧪 Testing Service Connections...\n")

    # Test MinIO
    try:
        minio = get_minio_service()
        print("✅ MinIO: Connected")
    except Exception as e:
        print(f"❌ MinIO: Failed - {e}")

    # Test Kafka
    try:
        kafka = get_kafka_service()
        print("✅ Kafka: Connected")
    except Exception as e:
        print(f"❌ Kafka: Failed - {e}")

    # Test Elasticsearch
    try:
        es = get_elasticsearch_service()
        print("✅ Elasticsearch: Connected")
    except Exception as e:
        print(f"❌ Elasticsearch: Failed - {e}")

    # Test Redis
    try:
        redis = get_redis_service()
        # Test basic operation
        redis.client.set("test_key", "test_value")
        val = redis.client.get("test_key")
        redis.client.delete("test_key")
        assert val == "test_value"
        print("✅ Redis: Connected and working")
    except Exception as e:
        print(f"❌ Redis: Failed - {e}")

    print("\n✨ Service connection tests complete!\n")


if __name__ == "__main__":
    test_services()
