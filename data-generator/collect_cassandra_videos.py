#!/usr/bin/env python3
"""
Collect YouTube videos from DataStax Developers channel matching search criteria.

Fetches video metadata and transcripts, saving each video as a JSON file
in the staging directory for further processing.

Usage:
    uv run python collect_cassandra_videos.py
    uv run python collect_cassandra_videos.py --filter "cassandra"
    uv run python collect_cassandra_videos.py --config custom_config.yaml

    # Resume from where you left off (skips existing files)
    uv run python collect_cassandra_videos.py --resume

    # Process in batches with pauses to avoid rate limiting
    uv run python collect_cassandra_videos.py --batch-size 50 --batch-pause 300
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

import yaml
from tqdm import tqdm

from src.youtube_collector import YouTubeCollector
from src.transcript_collector import TranscriptCollector


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def filter_videos(videos: List[Dict[str, Any]], filter_term: str) -> List[Dict[str, Any]]:
    """
    Filter videos by search term in title or description.

    Args:
        videos: List of video metadata dictionaries
        filter_term: Term to search for (case-insensitive)

    Returns:
        Filtered list of videos
    """
    pattern = re.compile(re.escape(filter_term), re.IGNORECASE)

    filtered = []
    for video in videos:
        title = video.get('title', '')
        description = video.get('description', '')

        if pattern.search(title) or pattern.search(description):
            filtered.append(video)

    return filtered


def create_video_json(video: Dict[str, Any], transcript: Dict[str, Any],
                      channel_id: str) -> Dict[str, Any]:
    """
    Create the full JSON structure for a video.

    Args:
        video: Video metadata from YouTube API
        transcript: Transcript data from TranscriptCollector
        channel_id: YouTube channel ID

    Returns:
        Complete video JSON structure
    """
    return {
        "video_id": video['video_id'],
        "title": video['title'],
        "description": video['description'],
        "published_at": video['published_at'],
        "thumbnail": video['thumbnail'],
        "channel_title": video['channel_title'],
        "channel_id": channel_id,
        "tags": list(video.get('tags', [])),
        "url": f"https://www.youtube.com/watch?v={video['video_id']}",
        "transcript": transcript,
        "collected_at": datetime.now(timezone.utc).isoformat()
    }


def create_manifest(videos: List[Dict[str, Any]], filter_term: str,
                    output_dir: Path) -> Dict[str, Any]:
    """
    Create a manifest summary of collected videos.

    Args:
        videos: List of collected video data
        filter_term: Search filter used
        output_dir: Output directory path

    Returns:
        Manifest dictionary
    """
    transcript_stats = {
        "total": len(videos),
        "with_transcript": sum(1 for v in videos if v['transcript']['available']),
        "without_transcript": sum(1 for v in videos if not v['transcript']['available']),
        "auto_generated": sum(1 for v in videos
                             if v['transcript']['available'] and v['transcript']['is_generated']),
        "manual": sum(1 for v in videos
                     if v['transcript']['available'] and not v['transcript']['is_generated'])
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "filter_term": filter_term,
        "total_videos": len(videos),
        "transcript_stats": transcript_stats,
        "videos": [
            {
                "video_id": v['video_id'],
                "title": v['title'],
                "published_at": v['published_at'],
                "has_transcript": v['transcript']['available'],
                "file": f"{v['video_id']}.json"
            }
            for v in videos
        ]
    }


def get_existing_video_ids(output_dir: Path) -> set:
    """Get set of video IDs that already have JSON files."""
    existing = set()
    for json_file in output_dir.glob("*.json"):
        if json_file.name != "_manifest.json":
            existing.add(json_file.stem)
    return existing


def load_existing_videos(output_dir: Path) -> List[Dict[str, Any]]:
    """Load all existing video JSON files."""
    videos = []
    for json_file in output_dir.glob("*.json"):
        if json_file.name != "_manifest.json":
            with open(json_file, 'r', encoding='utf-8') as f:
                videos.append(json.load(f))
    return videos


def main():
    parser = argparse.ArgumentParser(
        description='Collect YouTube videos with transcripts'
    )
    parser.add_argument(
        '--config', '-c',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--filter', '-f',
        default='cassandra',
        help='Filter term for video title/description (default: cassandra)'
    )
    parser.add_argument(
        '--output', '-o',
        default='output/staging',
        help='Output directory for JSON files (default: output/staging)'
    )
    parser.add_argument(
        '--skip-transcripts',
        action='store_true',
        help='Skip fetching transcripts (faster, metadata only)'
    )
    parser.add_argument(
        '--max-videos',
        type=int,
        default=None,
        help='Maximum number of videos to collect (default: all matching)'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume collection, skipping videos that already have JSON files'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=20,
        help='Number of videos to process before pausing (default: 20)'
    )
    parser.add_argument(
        '--batch-pause',
        type=int,
        default=180,
        help='Seconds to pause between batches to avoid rate limiting (default: 180)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=5.0,
        help='Base delay between transcript requests in seconds (default: 5.0)'
    )
    parser.add_argument(
        '--proxy-http',
        type=str,
        default=None,
        help='HTTP proxy URL (e.g., http://user:pass@proxy:port)'
    )
    parser.add_argument(
        '--proxy-https',
        type=str,
        default=None,
        help='HTTPS proxy URL (e.g., https://user:pass@proxy:port)'
    )
    parser.add_argument(
        '--webshare-user',
        type=str,
        default=None,
        help='Webshare proxy username (alternative to generic proxy)'
    )
    parser.add_argument(
        '--webshare-pass',
        type=str,
        default=None,
        help='Webshare proxy password'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Just check if YouTube transcript API is accessible (not blocked)'
    )

    args = parser.parse_args()

    # Handle --check flag
    if args.check:
        print("Checking YouTube transcript API accessibility...")
        from youtube_transcript_api import YouTubeTranscriptApi
        test_video = 'BHnW2WTDTag'  # Known video with transcript
        try:
            api = YouTubeTranscriptApi()
            transcript_list = api.list(test_video)
            print("✓ List transcripts: OK")
            for t in transcript_list:
                fetched = t.fetch()
                segments = list(fetched)
                print(f"✓ Fetch transcript: OK ({len(segments)} segments)")
                print("\n✅ YouTube API is accessible - you can run collection!")
                sys.exit(0)
        except Exception as e:
            error_name = type(e).__name__
            if 'Blocked' in error_name or 'blocked' in str(e).lower():
                print(f"✗ Fetch transcript: BLOCKED")
                print(f"\n❌ IP is still blocked. Try again later.")
            else:
                print(f"✗ Error: {error_name}: {str(e)[:100]}")
            sys.exit(1)

    # Load configuration
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}")
        sys.exit(1)

    config = load_config(config_path)

    # Get YouTube API settings
    youtube_config = config.get('youtube', {})
    api_key = youtube_config.get('api_key')
    channel_id = youtube_config.get('channel_id', 'UCAIQY251avaMv7bBv5PCo-A')

    if not api_key or api_key == "YOUR_API_KEY":
        print("Error: Please set your YouTube API key in config.yaml")
        sys.exit(1)

    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print("YouTube Video Collector with Transcripts")
    print(f"{'='*60}")
    print(f"Channel ID: {channel_id}")
    print(f"Filter: '{args.filter}'")
    print(f"Output: {output_dir}")
    print(f"Batch size: {args.batch_size}, Pause: {args.batch_pause}s")
    print(f"Request delay: {args.delay}s")
    if args.resume:
        print("Mode: RESUME (skipping existing files)")
    print(f"{'='*60}\n")

    # Step 1: Fetch all channel videos
    print("Step 1: Fetching videos from channel...")
    youtube = YouTubeCollector(api_key)
    all_videos = youtube.get_channel_videos(channel_id)

    if not all_videos:
        print("No videos found in channel")
        sys.exit(1)

    print(f"Found {len(all_videos)} total videos in channel\n")

    # Step 2: Filter videos
    print(f"Step 2: Filtering videos containing '{args.filter}'...")
    filtered_videos = filter_videos(all_videos, args.filter)

    if not filtered_videos:
        print(f"No videos found matching filter '{args.filter}'")
        sys.exit(0)

    # Apply max limit if specified
    if args.max_videos and len(filtered_videos) > args.max_videos:
        filtered_videos = filtered_videos[:args.max_videos]

    print(f"Found {len(filtered_videos)} videos matching filter\n")

    # Check for resume mode
    existing_ids = set()
    if args.resume:
        existing_ids = get_existing_video_ids(output_dir)
        if existing_ids:
            print(f"Found {len(existing_ids)} existing video files, will skip these\n")

    # Step 3: Collect transcripts and save JSON files
    print("Step 3: Collecting transcripts and saving JSON files...")

    if not args.skip_transcripts:
        # Build proxy config if provided
        proxy_config = None
        if args.webshare_user and args.webshare_pass:
            proxy_config = {
                'webshare_username': args.webshare_user,
                'webshare_password': args.webshare_pass
            }
            print(f"Using Webshare proxy")
        elif args.proxy_http or args.proxy_https:
            proxy_config = {
                'http_url': args.proxy_http,
                'https_url': args.proxy_https or args.proxy_http
            }
            print(f"Using proxy: {args.proxy_http}")

        # Use configured delay + jitter to avoid rate limiting
        transcript_collector = TranscriptCollector(
            rate_limit_delay=args.delay,
            max_retries=3,
            proxy_config=proxy_config
        )

        # Check proxy support
        if proxy_config and not TranscriptCollector.has_proxy_support():
            print("WARNING: Proxy config provided but proxy support not available in installed library")
            print("         Install youtube-transcript-api>=1.0 for proxy support")

    collected_videos = []
    transcript_success = 0
    transcript_failed = 0
    ip_blocked_count = 0
    skipped = 0
    batch_count = 0

    for i, video in enumerate(tqdm(filtered_videos, desc="Processing videos", unit="video")):
        video_id = video['video_id']

        # Skip if already exists and in resume mode
        if args.resume and video_id in existing_ids:
            skipped += 1
            continue

        # Get transcript (or empty if skipped)
        if args.skip_transcripts:
            transcript = {
                "available": False,
                "language": None,
                "language_code": None,
                "is_generated": None,
                "text": "",
                "segments": [],
                "error": "Transcript collection skipped"
            }
        else:
            transcript = transcript_collector.get_transcript(video_id)

        if transcript['available']:
            transcript_success += 1
        else:
            transcript_failed += 1
            if transcript.get('error_type') == 'ip_blocked':
                ip_blocked_count += 1

        # Create full video JSON
        video_json = create_video_json(video, transcript, channel_id)
        collected_videos.append(video_json)

        # Save individual JSON file
        json_path = output_dir / f"{video_id}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(video_json, f, indent=2, ensure_ascii=False)

        # Batch pause logic
        batch_count += 1
        if not args.skip_transcripts and batch_count >= args.batch_size:
            remaining = len(filtered_videos) - i - 1 - skipped
            if remaining > 0:
                print(f"\n⏸️  Processed {batch_count} videos. Pausing {args.batch_pause}s to avoid rate limiting...")
                print(f"   {remaining} videos remaining...")
                time.sleep(args.batch_pause)
                batch_count = 0

    # Step 4: Create and save manifest (include previously collected videos if resuming)
    print("\nStep 4: Creating manifest...")

    if args.resume and existing_ids:
        # Load existing videos and merge
        existing_videos = load_existing_videos(output_dir)
        all_collected = existing_videos + collected_videos
        # Remove duplicates by video_id
        seen = set()
        unique_videos = []
        for v in all_collected:
            if v['video_id'] not in seen:
                seen.add(v['video_id'])
                unique_videos.append(v)
        manifest = create_manifest(unique_videos, args.filter, output_dir)
    else:
        manifest = create_manifest(collected_videos, args.filter, output_dir)

    manifest_path = output_dir / "_manifest.json"
    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    # Summary
    print(f"\n{'='*60}")
    print("Collection Complete!")
    print(f"{'='*60}")
    print(f"Videos processed this run: {len(collected_videos)}")
    if args.resume:
        print(f"Videos skipped (existing): {skipped}")
    print(f"Transcripts available:     {transcript_success}")
    print(f"Transcripts missing:       {transcript_failed}")
    if ip_blocked_count > 0:
        print(f"  - IP blocked:            {ip_blocked_count} (try again later or use proxy)")
    print(f"Total in manifest:         {manifest['total_videos']}")
    print(f"Output directory:          {output_dir}")
    print(f"Manifest file:             {manifest_path}")
    print(f"{'='*60}\n")

    # Suggest re-run if IP blocked
    if ip_blocked_count > 0:
        print("TIP: Use --resume to retry IP-blocked videos later, or use --proxy-http to bypass blocks")


if __name__ == "__main__":
    main()
