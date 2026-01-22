#!/usr/bin/env python3
"""
KillrVideo Data Generator - Enhanced Version

Generates CSV datasets using enhanced video metadata from JSON files
(with AI-generated descriptions from transcripts) instead of fetching from YouTube API.
"""

import sys
import yaml
from pathlib import Path

from src.enhanced_loader import load_enhanced_videos, load_manifest
from src.data_generator import DataGenerator
from src.csv_writer import CSVWriter, KILLRVIDEO_SCHEMAS
from src.relationships import RelationshipTracker


def load_config(config_path: str = 'config.yaml') -> dict:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"⚠️  Config file not found: {config_path}")
        print("   Using default configuration")
        return get_default_config()
    except yaml.YAMLError as e:
        print(f"❌ Error parsing config file: {e}")
        sys.exit(1)


def get_default_config() -> dict:
    """Return default configuration for enhanced generation"""
    return {
        'dataset': {
            'num_users': 150,
            'num_comments_min': 500,
            'num_comments_max': 2000,
            'rating_probability': 0.7,
            'popular_video_threshold': 0.2,
            'random_seed': 42
        },
        'output': {
            'directory': './output'
        }
    }


def main():
    """Main execution flow"""
    print("=" * 70)
    print("🎬 KillrVideo Data Generator (Enhanced Version)")
    print("=" * 70)
    print("\nUsing enhanced video descriptions from JSON files")

    # Load configuration
    print("\n📋 Loading configuration...")
    config = load_config()

    # Determine enhanced videos directory
    enhanced_dir = Path('./output/enhanced')
    if not enhanced_dir.exists():
        print(f"❌ Enhanced videos directory not found: {enhanced_dir}")
        print("   Run enhance_descriptions.py first to generate enhanced metadata")
        sys.exit(1)

    # Load manifest if available
    manifest = load_manifest(str(enhanced_dir))
    if manifest:
        print(f"📊 Manifest: {manifest.get('total_videos', 'unknown')} videos available")

    # Initialize components
    tracker = RelationshipTracker()
    data_generator = DataGenerator(config, tracker)

    # Step 1: Load enhanced videos from JSON files
    print("\n" + "=" * 70)
    print("STEP 1: Loading Enhanced Video Metadata")
    print("=" * 70)

    enhanced_videos = load_enhanced_videos(str(enhanced_dir))

    if not enhanced_videos:
        print("❌ No enhanced videos loaded. Check your JSON files.")
        sys.exit(1)

    print(f"✅ Loaded {len(enhanced_videos)} enhanced videos")

    # Step 2: Generate users
    print("\n" + "=" * 70)
    print("STEP 2: Generating Users")
    print("=" * 70)

    num_users = config.get('dataset', {}).get('num_users', 150)
    users, credentials = data_generator.generate_users(num_users)

    # Step 3: Process videos with enhanced descriptions
    print("\n" + "=" * 70)
    print("STEP 3: Processing Videos with Enhanced Descriptions")
    print("=" * 70)

    videos = data_generator.process_enhanced_videos(enhanced_videos, users)
    latest_videos = data_generator.generate_latest_videos(videos)

    # Step 4: Generate tags
    print("\n" + "=" * 70)
    print("STEP 4: Generating Tags")
    print("=" * 70)

    tags = data_generator.generate_tags(videos)
    tag_counts = data_generator.generate_tag_counts(tags, videos)

    # Step 5: Generate comments
    print("\n" + "=" * 70)
    print("STEP 5: Generating Comments")
    print("=" * 70)

    comments, comments_by_user = data_generator.generate_comments(videos, users)

    # Step 6: Generate ratings
    print("\n" + "=" * 70)
    print("STEP 6: Generating Ratings")
    print("=" * 70)

    ratings_by_user, video_ratings = data_generator.generate_ratings(videos, users)

    # Step 7: Generate playback stats
    print("\n" + "=" * 70)
    print("STEP 7: Generating Playback Stats")
    print("=" * 70)

    playback_stats = data_generator.generate_playback_stats(videos)

    # Step 8: Generate user preferences
    print("\n" + "=" * 70)
    print("STEP 8: Generating User Preferences")
    print("=" * 70)

    user_preferences = data_generator.generate_user_preferences(users, videos, ratings_by_user)

    # Step 9: Validate relationships
    print("\n" + "=" * 70)
    print("STEP 9: Validating Relationships")
    print("=" * 70)

    errors = tracker.validate()
    if errors:
        print("❌ Referential integrity errors found:")
        for entity_type, error_list in errors.items():
            print(f"\n  {entity_type}:")
            for error in error_list[:5]:
                print(f"    - {error}")
            if len(error_list) > 5:
                print(f"    ... and {len(error_list) - 5} more")
        sys.exit(1)
    else:
        print("✅ All relationships are valid!")

    # Print statistics
    stats = tracker.get_stats()
    print(f"\n📊 Dataset Statistics:")
    print(f"   Users:                {stats['users']}")
    print(f"   Videos:               {stats['videos']}")
    print(f"   Comments:             {stats['comments']}")
    print(f"   Tags:                 {stats['tags']}")
    print(f"   Ratings:              {len(ratings_by_user)}")
    print(f"   Videos per user:      {stats['videos_by_user']['avg']:.1f} avg")
    print(f"   Comments per video:   {stats['comments_by_video']['avg']:.1f} avg")

    # Step 10: Write CSV files
    print("\n" + "=" * 70)
    print("STEP 10: Writing CSV Files")
    print("=" * 70)

    output_dir = config.get('output', {}).get('directory', './output')
    writer = CSVWriter(output_dir)

    all_data = {
        'users': users,
        'user_credentials': credentials,
        'videos': videos,
        'latest_videos': latest_videos,
        'tags': tags,
        'tag_counts': tag_counts,
        'comments': comments,
        'comments_by_user': comments_by_user,
        'video_ratings': video_ratings,
        'video_ratings_by_user': ratings_by_user,
        'video_playback_stats': playback_stats,
        'user_preferences': user_preferences
    }

    writer.write_all_tables(all_data, KILLRVIDEO_SCHEMAS)

    # Success!
    print("\n" + "=" * 70)
    print("🎉 SUCCESS!")
    print("=" * 70)
    print(f"\n✅ Generated {len(all_data)} CSV files in: {output_dir}/")
    print(f"✅ Used enhanced descriptions from {len(enhanced_videos)} videos")
    print("\n📦 Next steps:")
    print("   1. Review the generated CSV files")
    print("   2. Load into Astra: ./scripts/load_to_astra.sh")
    print("   3. Load into Cassandra: ./scripts/load_to_cassandra.sh")
    print()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Generation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
