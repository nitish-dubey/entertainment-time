#!/usr/bin/env python
"""
Redis Rebuild CLI

Usage:
    python rebuild_redis.py                    # Rebuild last 30 days
    python rebuild_redis.py --days 7           # Rebuild last 7 days
    python rebuild_redis.py --clear            # Clear Redis first (dangerous!)
    python rebuild_redis.py --video 123        # Rebuild single video
    python rebuild_redis.py --verify           # Just verify, don't rebuild
"""
import argparse
from app.services.redis_rebuild import RedisRebuildService


def main():
    parser = argparse.ArgumentParser(description='Rebuild Redis from PostgreSQL')
    
    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='Number of days to rebuild (default: 30)'
    )
    
    parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear existing Redis data before rebuild (DANGEROUS!)'
    )
    
    parser.add_argument(
        '--video',
        type=int,
        help='Rebuild single video by ID'
    )
    
    parser.add_argument(
        '--verify',
        action='store_true',
        help='Only verify Redis data, don\'t rebuild'
    )
    
    args = parser.parse_args()
    
    rebuilder = RedisRebuildService()
    
    # Single video rebuild
    if args.video:
        rebuilder.rebuild_single_video(args.video, days_back=args.days)
        return
    
    # Verification only
    if args.verify:
        result = rebuilder.verify_rebuild()
        print(f"\nVerification Results:")
        print(f"  Checked: {result['checked']} videos")
        print(f"  Mismatches: {result['mismatches']}")
        print(f"  Success Rate: {result['success_rate']:.1f}%")
        return
    
    # Full rebuild
    if args.clear:
        print("⚠️  WARNING: This will clear all existing Redis analytics data!")
        response = input("Are you sure? Type 'yes' to confirm: ")
        if response.lower() != 'yes':
            print("Rebuild cancelled")
            return
        rebuilder._clear_redis_analytics()
    
    rebuilder.rebuild_all(days_back=args.days)
    rebuilder.verify_rebuild()


if __name__ == '__main__':
    main()
