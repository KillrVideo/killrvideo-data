#!/usr/bin/env python3
"""
Data loading script for KillrVideo using native CQL driver.
Loads CSV files from data/ directory into Astra DB CQL tables.
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel
import os, sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Data directory
DATA_DIR = Path(__file__).parent.parent / "../data/csv"

# Get Astra DB credentials from environment
ASTRA_DB_ID = os.getenv("ASTRA_DB_ID", "d32f1a9d-7395-4344-94e0-310ca9a6a96d")
ASTRA_DB_REGION = os.getenv("ASTRA_DB_REGION", "us-east1")
ASTRA_DB_APPLICATION_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE", "killrvideo")

# Secure connect bundle path (we'll generate the endpoint)
SECURE_CONNECT_BUNDLE = os.getenv("ASTRA_SCB_PATH")

# Batch size for inserts
BATCH_SIZE = 20


def get_session():
    """Create Cassandra session using Astra DB secure connect bundle or cloud endpoint."""

    if not ASTRA_DB_APPLICATION_TOKEN:
        raise ValueError("ASTRA_DB_APPLICATION_TOKEN environment variable is required")

    cloud_config = (
        {"secure_connect_bundle": SECURE_CONNECT_BUNDLE}
        if SECURE_CONNECT_BUNDLE
        else None
    )

    auth_provider = PlainTextAuthProvider("token", ASTRA_DB_APPLICATION_TOKEN)

    if cloud_config:
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    else:
        # Use direct CQL endpoint
        contact_points = [f"{ASTRA_DB_ID}-{ASTRA_DB_REGION}.db.astra.datastax.com"]
        cluster = Cluster(
            contact_points=contact_points,
            port=29042,
            auth_provider=auth_provider,
            protocol_version=4,
        )

    session = cluster.connect(ASTRA_DB_KEYSPACE)
    return session, cluster


def parse_uuid(value: str) -> Optional[UUID]:
    """Parse UUID string."""
    if not value or value == "":
        return None
    try:
        return UUID(value)
    except ValueError:
        logger.warning(f"Invalid UUID: {value}")
        return None


def parse_timestamp(value: str) -> Optional[datetime]:
    """Parse ISO timestamp string."""
    if not value or value == "":
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        logger.warning(f"Invalid timestamp: {value}")
        return None


def parse_vector(value: str) -> Optional[List[float]]:
    """Parse vector embedding from string representation."""
    if not value or value == "" or value == "null":
        return None
    try:
        vector = json.loads(value)
        if isinstance(vector, list):
            return [float(x) for x in vector]
        return None
    except (json.JSONDecodeError, ValueError, TypeError):
        return None


def parse_set(value: str) -> Optional[set]:
    """Parse set from string representation."""
    if not value or value == "" or value == "null":
        return None
    try:
        if value.startswith("["):
            items = json.loads(value)
        elif value.startswith("{") and value.endswith("}"):
            items_str = value[1:-1]
            items = [
                item.strip().strip('"').strip("'")
                for item in items_str.split(",")
                if item.strip()
            ]
        else:
            return None
        return set(items) if items else None
    except (json.JSONDecodeError, ValueError):
        return None


def parse_map(value: str) -> Optional[Dict[str, Any]]:
    """Parse map from JSON string."""
    if not value or value == "" or value == "null":
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError):
        return None


def load_users(session):
    """Load users.csv into users table."""
    csv_file = DATA_DIR / "users.csv"
    if not csv_file.exists():
        logger.warning(f"{csv_file} not found, skipping")
        return 0

    logger.info(f"Loading {csv_file.name}...")

    insert_stmt = session.prepare("""
        INSERT INTO users (userid, created_date, email, firstname, lastname, account_status, last_login_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    loaded = 0
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                session.execute(
                    insert_stmt,
                    [
                        parse_uuid(row["userid"]),
                        parse_timestamp(row["created_date"]),
                        row["email"],
                        row["firstname"],
                        row["lastname"],
                        row["account_status"],
                        parse_timestamp(row["last_login_date"]),
                    ],
                )
                loaded += 1
                if loaded % 20 == 0:
                    logger.info(f"  Loaded {loaded} users...")
            except Exception as e:
                logger.error(f"Error loading user row: {e}")

    logger.info(f"✓ Loaded {loaded} users")
    return loaded


def load_user_credentials(session):
    """Load user_credentials.csv."""
    csv_file = DATA_DIR / "user_credentials.csv"
    if not csv_file.exists():
        return 0

    logger.info(f"Loading {csv_file.name}...")

    insert_stmt = session.prepare("""
        INSERT INTO user_credentials (email, password, userid, account_locked)
        VALUES (?, ?, ?, ?)
    """)

    loaded = 0
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                session.execute(
                    insert_stmt,
                    [
                        row["email"],
                        row["password"],
                        parse_uuid(row["userid"]),
                        row.get("account_locked", "false").lower() == "true",
                    ],
                )
                loaded += 1
            except Exception as e:
                logger.error(f"Error loading user_credentials row: {e}")

    logger.info(f"✓ Loaded {loaded} user_credentials")
    return loaded


def load_videos(session):
    """Load videos_cleaned.csv or videos.csv."""
    #csv_file = DATA_DIR / "videos_cleaned.csv"
    #if not csv_file.exists():
    csv_file = DATA_DIR / "videos.csv"

    logger.info(f"csv_file: {csv_file}")
    if not csv_file.exists():
        logger.warning("No videos CSV found, skipping")
        return 0

    logger.info(f"Loading {csv_file.name}...")

    insert_stmt = session.prepare("""
        INSERT INTO videos (videoid, added_date, description, location, location_type, name,
                           preview_image_location, tags, content_features, userid,
                           content_rating, category, language)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    loaded = 0
    errors = 0
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                session.execute(
                    insert_stmt,
                    [
                        parse_uuid(row["videoid"]),
                        parse_timestamp(row["added_date"]),
                        row["description"],
                        row["location"],
                        int(row["location_type"]) if row["location_type"] else None,
                        row["name"],
                        row["preview_image_location"],
                        parse_set(row["tags"]),
                        parse_vector(row["content_features"]),
                        parse_uuid(row["userid"]),
                        row["content_rating"],
                        row["category"],
                        row["language"],
                    ],
                )
                loaded += 1
                if loaded % 100 == 0:
                    logger.info(f"  Loaded {loaded} videos...")
            except Exception as e:
                errors += 1
                if errors <= 5:  # Only log first 5 errors
                    logger.error(f"Error loading video row: {e}")

    logger.info(f"✓ Loaded {loaded} videos ({errors} errors)")
    return loaded


def load_tags(session):
    """Load tags_cleaned.csv or tags.csv."""
    csv_file = DATA_DIR / "tags_cleaned.csv"
    if not csv_file.exists():
        csv_file = DATA_DIR / "tags.csv"
    if not csv_file.exists():
        return 0

    logger.info(f"Loading {csv_file.name}...")

    insert_stmt = session.prepare("""
        INSERT INTO tags (tag, tag_vector, related_tags, category)
        VALUES (?, ?, ?, ?)
    """)

    loaded = 0
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                session.execute(
                    insert_stmt,
                    [
                        row["tag"],
                        parse_vector(row["tag_vector"]),
                        parse_set(row["related_tags"]),
                        row["category"],
                    ],
                )
                loaded += 1
            except Exception as e:
                logger.error(f"Error loading tag row: {e}")

    logger.info(f"✓ Loaded {loaded} tags")
    return loaded


def load_comments(session):
    """Load comments.csv."""
    csv_file = DATA_DIR / "comments.csv"
    if not csv_file.exists():
        return 0

    logger.info(f"Loading {csv_file.name}...")

    insert_stmt = session.prepare("""
        INSERT INTO comments (videoid, commentid, comment, userid, sentiment_score)
        VALUES (?, ?, ?, ?, ?)
    """)

    loaded = 0
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                sentiment = row.get("sentiment_score")
                session.execute(
                    insert_stmt,
                    [
                        parse_uuid(row["videoid"]),
                        parse_uuid(row["commentid"]),
                        row["comment"],
                        parse_uuid(row["userid"]),
                        float(sentiment) if sentiment and sentiment != "" else None,
                    ],
                )
                loaded += 1
                if loaded % 100 == 0:
                    logger.info(f"  Loaded {loaded} comments...")
            except Exception as e:
                logger.error(f"Error loading comment row: {e}")

    logger.info(f"✓ Loaded {loaded} comments")
    return loaded


def main():
    """Main loading function."""
    logger.info("=== Starting KillrVideo CQL Data Load ===")
    logger.info(f"Keyspace: {ASTRA_DB_KEYSPACE}")

    # Note: Native CQL driver with Astra requires secure connect bundle
    # Since we may not have it, let's inform the user
    if not SECURE_CONNECT_BUNDLE:
        logger.error("""
ERROR: This script requires the Astra DB Secure Connect Bundle.

To download it:
1. Go to https://astra.datastax.com
2. Select your database 'killrvideo'
3. Click 'Connect' tab
4. Download the Secure Connect Bundle
5. Set environment variable: export ASTRA_SCB_PATH=/path/to/secure-connect-killrvideo.zip

Alternatively, use DSBulk for bulk loading:
https://docs.datastax.com/en/astra-serverless/docs/manage/dsbulk.html
        """)
        return

    table = sys.argv[1]

    try:
        session, cluster = get_session()
        logger.info("✓ Connected to Astra DB")

        total = 0

        if not table:
            total += load_users(session)
            total += load_user_credentials(session)
            total += load_videos(session)
            total += load_tags(session)
            total += load_comments(session)
        elif table == "users":
            total += load_users(session)
        elif table == "user_credentials":
            total += load_user_credentials(session)
        elif table == "videos":
            total += load_videos(session)
        elif table == "tags":
            total += load_tags(session)
        elif table == "comments":
            total += load_comments(session)

        logger.info("=" * 60)
        logger.info(f"✓ COMPLETE: Loaded {total} total records")
        logger.info("=" * 60)

        cluster.shutdown()

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
