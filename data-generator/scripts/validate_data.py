#!/usr/bin/env python3
"""
Validate generated CSV files for referential integrity before loading into Cassandra.
"""

import csv
import sys
from pathlib import Path
from collections import defaultdict


def load_csv_ids(filepath, id_column):
    """Load a set of IDs from a CSV file"""
    ids = set()
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if id_column in row:
                ids.add(row[id_column])
    return ids


def validate_foreign_keys(output_dir='./output'):
    """Validate all foreign key relationships"""
    output_path = Path(output_dir)
    errors = defaultdict(list)

    print("=" * 70)
    print("🔍 Validating Data Integrity")
    print("=" * 70)

    # Load primary keys
    print("\n📋 Loading primary keys...")

    try:
        user_ids = load_csv_ids(output_path / 'users.csv', 'userid')
        print(f"   ✓ Loaded {len(user_ids)} user IDs")
    except FileNotFoundError:
        print(f"   ✗ users.csv not found")
        return False

    try:
        user_emails = load_csv_ids(output_path / 'users.csv', 'email')
        print(f"   ✓ Loaded {len(user_emails)} user emails")
    except FileNotFoundError:
        print(f"   ✗ users.csv not found")
        return False

    try:
        video_ids = load_csv_ids(output_path / 'videos.csv', 'videoid')
        print(f"   ✓ Loaded {len(video_ids)} video IDs")
    except FileNotFoundError:
        print(f"   ✗ videos.csv not found")
        return False

    # Validate user_credentials
    print("\n🔑 Validating user_credentials...")
    try:
        with open(output_path / 'user_credentials.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if row['email'] not in user_emails:
                    errors['user_credentials'].append(f"Row {i}: email {row['email']} not in users")
                if row['userid'] not in user_ids:
                    errors['user_credentials'].append(f"Row {i}: userid {row['userid']} not in users")

        if errors['user_credentials']:
            print(f"   ✗ Found {len(errors['user_credentials'])} errors")
        else:
            print(f"   ✓ All foreign keys valid")
    except FileNotFoundError:
        print(f"   ⚠ user_credentials.csv not found")

    # Validate videos
    print("\n🎬 Validating videos...")
    try:
        with open(output_path / 'videos.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if row['userid'] not in user_ids:
                    errors['videos'].append(f"Row {i}: userid {row['userid']} not in users")

        if errors['videos']:
            print(f"   ✗ Found {len(errors['videos'])} errors")
        else:
            print(f"   ✓ All foreign keys valid")
    except FileNotFoundError:
        print(f"   ⚠ videos.csv not found")

    # Validate comments
    print("\n💬 Validating comments...")
    try:
        with open(output_path / 'comments.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if row['videoid'] not in video_ids:
                    errors['comments'].append(f"Row {i}: videoid {row['videoid']} not in videos")
                if row['userid'] not in user_ids:
                    errors['comments'].append(f"Row {i}: userid {row['userid']} not in users")

        if errors['comments']:
            print(f"   ✗ Found {len(errors['comments'])} errors")
        else:
            print(f"   ✓ All foreign keys valid")
    except FileNotFoundError:
        print(f"   ⚠ comments.csv not found")

    # Validate comments_by_user
    print("\n💬 Validating comments_by_user...")
    try:
        with open(output_path / 'comments_by_user.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if row['videoid'] not in video_ids:
                    errors['comments_by_user'].append(f"Row {i}: videoid {row['videoid']} not in videos")
                if row['userid'] not in user_ids:
                    errors['comments_by_user'].append(f"Row {i}: userid {row['userid']} not in users")

        if errors['comments_by_user']:
            print(f"   ✗ Found {len(errors['comments_by_user'])} errors")
        else:
            print(f"   ✓ All foreign keys valid")
    except FileNotFoundError:
        print(f"   ⚠ comments_by_user.csv not found")

    # Validate video_ratings_by_user
    print("\n⭐ Validating video_ratings_by_user...")
    try:
        with open(output_path / 'video_ratings_by_user.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if row['videoid'] not in video_ids:
                    errors['video_ratings_by_user'].append(f"Row {i}: videoid {row['videoid']} not in videos")
                if row['userid'] not in user_ids:
                    errors['video_ratings_by_user'].append(f"Row {i}: userid {row['userid']} not in users")

        if errors['video_ratings_by_user']:
            print(f"   ✗ Found {len(errors['video_ratings_by_user'])} errors")
        else:
            print(f"   ✓ All foreign keys valid")
    except FileNotFoundError:
        print(f"   ⚠ video_ratings_by_user.csv not found")

    # Validate user_preferences
    print("\n🎯 Validating user_preferences...")
    try:
        with open(output_path / 'user_preferences.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if row['userid'] not in user_ids:
                    errors['user_preferences'].append(f"Row {i}: userid {row['userid']} not in users")

        if errors['user_preferences']:
            print(f"   ✗ Found {len(errors['user_preferences'])} errors")
        else:
            print(f"   ✓ All foreign keys valid")
    except FileNotFoundError:
        print(f"   ⚠ user_preferences.csv not found")

    # Summary
    print("\n" + "=" * 70)
    if errors:
        print("❌ VALIDATION FAILED")
        print("=" * 70)
        print(f"\nFound errors in {len(errors)} table(s):\n")
        for table, error_list in errors.items():
            print(f"  {table}:")
            for error in error_list[:5]:
                print(f"    - {error}")
            if len(error_list) > 5:
                print(f"    ... and {len(error_list) - 5} more errors")
        return False
    else:
        print("✅ VALIDATION SUCCESSFUL")
        print("=" * 70)
        print("\nAll referential integrity constraints are satisfied!")
        print("Data is ready to be loaded into Cassandra.")
        return True


if __name__ == '__main__':
    output_dir = sys.argv[1] if len(sys.argv) > 1 else './output'

    success = validate_foreign_keys(output_dir)
    sys.exit(0 if success else 1)
