"""Load enhanced video metadata from JSON files"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from tqdm import tqdm


def load_enhanced_videos(enhanced_dir: str, verbose: bool = True) -> List[Dict[str, Any]]:
    """
    Load all enhanced video JSON files from directory.

    Args:
        enhanced_dir: Path to directory containing enhanced JSON files
        verbose: Whether to print progress information

    Returns:
        List of video dictionaries with enhanced metadata
    """
    enhanced_path = Path(enhanced_dir)

    if not enhanced_path.exists():
        raise FileNotFoundError(f"Enhanced videos directory not found: {enhanced_dir}")

    # Find all JSON files except manifest
    json_files = [f for f in enhanced_path.glob("*.json") if f.name != "_manifest.json"]

    if not json_files:
        raise ValueError(f"No JSON files found in {enhanced_dir}")

    if verbose:
        print(f"\n📂 Loading {len(json_files)} enhanced video files from {enhanced_dir}")

    videos = []
    skipped = 0

    iterator = tqdm(json_files) if verbose else json_files

    for json_file in iterator:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                video_data = json.load(f)

            # Validate required fields
            if not _validate_video(video_data):
                skipped += 1
                continue

            videos.append(video_data)

        except json.JSONDecodeError as e:
            if verbose:
                print(f"\n⚠️  Failed to parse {json_file.name}: {e}")
            skipped += 1
        except Exception as e:
            if verbose:
                print(f"\n⚠️  Error loading {json_file.name}: {e}")
            skipped += 1

    if verbose:
        print(f"✅ Loaded {len(videos)} videos ({skipped} skipped)")

    return videos


def _validate_video(video: Dict[str, Any]) -> bool:
    """
    Validate that a video has required fields.

    Args:
        video: Video dictionary to validate

    Returns:
        True if valid, False otherwise
    """
    required_fields = ['video_id', 'title', 'published_at']

    for field in required_fields:
        if field not in video or not video[field]:
            return False

    # Must have either enhanced_description or description
    if not video.get('enhanced_description') and not video.get('description'):
        return False

    return True


def get_video_description(video: Dict[str, Any]) -> str:
    """
    Get the best available description for a video.

    Prefers enhanced_description, falls back to original description.

    Args:
        video: Video dictionary

    Returns:
        Description text
    """
    if video.get('enhanced_description'):
        return video['enhanced_description']
    return video.get('description', '')


def load_manifest(enhanced_dir: str) -> Optional[Dict[str, Any]]:
    """
    Load the manifest file if it exists.

    Args:
        enhanced_dir: Path to enhanced videos directory

    Returns:
        Manifest dictionary or None if not found
    """
    manifest_path = Path(enhanced_dir) / "_manifest.json"

    if not manifest_path.exists():
        return None

    try:
        with open(manifest_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None
