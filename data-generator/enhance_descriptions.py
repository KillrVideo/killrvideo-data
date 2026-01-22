#!/usr/bin/env python3
"""
Generate enhanced video descriptions using AI from video transcripts.

Reads staged video JSON files, extracts transcripts, and uses a local Ollama
server to generate more informative descriptions that focus on actual content
rather than marketing language.

Usage:
    uv run python enhance_descriptions.py
    uv run python enhance_descriptions.py --model mistral
    uv run python enhance_descriptions.py --resume

Prerequisites:
    - Ollama must be running locally (ollama serve)
    - A model must be available (e.g., ollama pull llama3.2)
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

from tqdm import tqdm

from src.ollama_client import OllamaClient, generate_enhanced_description


def load_staged_videos(input_dir: Path) -> List[Dict[str, Any]]:
    """
    Load all staged video JSON files.

    Args:
        input_dir: Directory containing staged JSON files

    Returns:
        List of (video_data, file_path) tuples
    """
    videos = []
    for json_file in sorted(input_dir.glob("*.json")):
        if json_file.name == "_manifest.json":
            continue
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                videos.append((data, json_file))
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Failed to load {json_file.name}: {e}")
    return videos


def get_existing_video_ids(output_dir: Path) -> set:
    """Get set of video IDs that already have enhanced JSON files."""
    existing = set()
    for json_file in output_dir.glob("*.json"):
        if json_file.name != "_manifest.json":
            existing.add(json_file.stem)
    return existing


def create_manifest(videos: List[Dict[str, Any]], output_dir: Path) -> Dict[str, Any]:
    """
    Create a manifest summary of enhanced videos.

    Args:
        videos: List of enhanced video data
        output_dir: Output directory path

    Returns:
        Manifest dictionary
    """
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_videos": len(videos),
        "videos": [
            {
                "video_id": v['video_id'],
                "title": v['title'],
                "has_enhanced_description": 'enhanced_description' in v,
                "file": f"{v['video_id']}.json"
            }
            for v in videos
        ]
    }


def main():
    parser = argparse.ArgumentParser(
        description='Generate enhanced video descriptions using Ollama'
    )
    parser.add_argument(
        '--input', '-i',
        default='output/staging',
        help='Input directory with staged video JSON files (default: output/staging)'
    )
    parser.add_argument(
        '--output', '-o',
        default='output/enhanced',
        help='Output directory for enhanced JSON files (default: output/enhanced)'
    )
    parser.add_argument(
        '--model', '-m',
        default='llama3.2',
        help='Ollama model name (default: llama3.2)'
    )
    parser.add_argument(
        '--ollama-url',
        default='http://localhost:11434',
        help='Ollama server URL (default: http://localhost:11434)'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Skip videos that already exist in output directory'
    )
    parser.add_argument(
        '--max-videos',
        type=int,
        default=None,
        help='Maximum number of videos to process (default: all)'
    )
    parser.add_argument(
        '--max-words',
        type=int,
        default=6000,
        help='Maximum words from transcript to send to model (default: 6000)'
    )

    args = parser.parse_args()

    # Setup paths
    input_dir = Path(args.input)
    output_dir = Path(args.output)

    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print("Enhanced Description Generator")
    print(f"{'='*60}")
    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Model:  {args.model}")
    print(f"Ollama: {args.ollama_url}")
    if args.resume:
        print("Mode:   RESUME (skipping existing files)")
    print(f"{'='*60}\n")

    # Initialize Ollama client
    print("Connecting to Ollama...")
    client = OllamaClient(
        base_url=args.ollama_url,
        model=args.model
    )

    if not client.health_check():
        print(f"Error: Cannot connect to Ollama at {args.ollama_url}")
        print("Make sure Ollama is running: ollama serve")
        sys.exit(1)

    # Check if model is available
    available_models = client.list_models()
    if available_models:
        model_names = [m.split(':')[0] for m in available_models]
        if args.model not in model_names and args.model not in available_models:
            print(f"Warning: Model '{args.model}' may not be available.")
            print(f"Available models: {', '.join(available_models)}")
            print(f"Try: ollama pull {args.model}")

    print(f"Connected to Ollama (model: {args.model})\n")

    # Load staged videos
    print("Loading staged videos...")
    staged_videos = load_staged_videos(input_dir)

    if not staged_videos:
        print("No staged videos found")
        sys.exit(0)

    print(f"Found {len(staged_videos)} staged videos\n")

    # Filter to videos with transcripts
    videos_with_transcripts = [
        (v, p) for v, p in staged_videos
        if v.get('transcript', {}).get('available', False)
    ]

    videos_without = len(staged_videos) - len(videos_with_transcripts)
    if videos_without > 0:
        print(f"Skipping {videos_without} videos without transcripts")

    if not videos_with_transcripts:
        print("No videos with transcripts to process")
        sys.exit(0)

    # Check for resume mode
    existing_ids = set()
    if args.resume:
        existing_ids = get_existing_video_ids(output_dir)
        if existing_ids:
            print(f"Found {len(existing_ids)} existing enhanced files, will skip these")

    # Apply filters
    to_process = [
        (v, p) for v, p in videos_with_transcripts
        if not args.resume or v['video_id'] not in existing_ids
    ]

    if args.max_videos and len(to_process) > args.max_videos:
        to_process = to_process[:args.max_videos]

    if not to_process:
        print("No new videos to process")
        sys.exit(0)

    print(f"\nProcessing {len(to_process)} videos...\n")

    # Process videos
    enhanced_videos = []
    success_count = 0
    error_count = 0

    for video_data, source_path in tqdm(to_process, desc="Enhancing descriptions", unit="video"):
        video_id = video_data['video_id']
        title = video_data['title']
        transcript_text = video_data['transcript'].get('text', '')

        if not transcript_text:
            tqdm.write(f"Skipping {video_id}: Empty transcript")
            error_count += 1
            continue

        try:
            # Generate enhanced description
            enhanced_desc = generate_enhanced_description(
                client,
                transcript_text,
                title,
                max_words=args.max_words
            )

            # Add to video data
            video_data['enhanced_description'] = enhanced_desc
            video_data['enhancement_metadata'] = {
                'model': args.model,
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'transcript_words_used': min(len(transcript_text.split()), args.max_words)
            }

            # Save enhanced video
            output_path = output_dir / f"{video_id}.json"
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(video_data, f, indent=2, ensure_ascii=False)

            enhanced_videos.append(video_data)
            success_count += 1

        except ConnectionError as e:
            tqdm.write(f"Connection error: {e}")
            print("\nOllama connection lost. Exiting.")
            break
        except Exception as e:
            tqdm.write(f"Error processing {video_id}: {e}")
            error_count += 1
            continue

    # If resuming, load previously enhanced videos for manifest
    if args.resume and existing_ids:
        for json_file in output_dir.glob("*.json"):
            if json_file.name != "_manifest.json" and json_file.stem not in [v['video_id'] for v in enhanced_videos]:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        enhanced_videos.append(json.load(f))
                except (json.JSONDecodeError, IOError):
                    pass

    # Create manifest
    manifest = create_manifest(enhanced_videos, output_dir)
    manifest_path = output_dir / "_manifest.json"
    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    # Summary
    print(f"\n{'='*60}")
    print("Enhancement Complete!")
    print(f"{'='*60}")
    print(f"Videos processed: {success_count}")
    print(f"Errors:           {error_count}")
    print(f"Total enhanced:   {len(enhanced_videos)}")
    print(f"Output directory: {output_dir}")
    print(f"Manifest file:    {manifest_path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
