#!/usr/bin/env python3
"""
KillrVideo Data Loader - Python Implementation for OSS Cassandra 5.0+

This script loads CSV data into OSS Cassandra using the cassandra-driver.
It provides more flexibility than DSBulk for custom transformations and
error handling.

Prerequisites:
    - Cassandra 5.0+ cluster running
    - Schema loaded from data/schemas/schema-v5.cql
    - Python 3.8+
    - cassandra-driver installed (pip install cassandra-driver)

Usage:
    python load_with_python.py --host 127.0.0.1 --keyspace killrvideo

    # With authentication
    python load_with_python.py --host 127.0.0.1 --keyspace killrvideo \\
        --username myuser --password mypass

    # Custom CSV directory
    python load_with_python.py --host 127.0.0.1 --keyspace killrvideo \\
        --csv-dir /path/to/csv
"""

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from uuid import UUID

try:
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
    from cassandra.query import SimpleStatement, ConsistencyLevel
    from cassandra import InvalidRequest, Unavailable, Timeout, ReadTimeout, WriteTimeout
except ImportError:
    print("ERROR: cassandra-driver not installed")
    print("Install with: pip install cassandra-driver")
    sys.exit(1)

# ANSI color codes
class Colors:
    BLUE = '\033[0;34m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    BOLD = '\033[1m'
    NC = '\033[0m'  # No Color

def log_info(msg: str):
    print(f"{Colors.BLUE}[INFO]{Colors.NC} {msg}")

def log_success(msg: str):
    print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {msg}")

def log_warning(msg: str):
    print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {msg}")

def log_error(msg: str):
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}", file=sys.stderr)

def log_section(msg: str):
    print()
    print(f"{Colors.BOLD}{'=' * 70}{Colors.NC}")
    print(f"{Colors.BOLD}  {msg}{Colors.NC}")
    print(f"{Colors.BOLD}{'=' * 70}{Colors.NC}")
    print()

class CassandraLoader:
    """Loader for KillrVideo data into Cassandra"""

    def __init__(self, hosts: List[str], keyspace: str, username: Optional[str] = None,
                 password: Optional[str] = None, datacenter: str = 'datacenter1'):
        """
        Initialize Cassandra connection

        Args:
            hosts: List of contact points
            keyspace: Target keyspace name
            username: Username for authentication (optional)
            password: Password for authentication (optional)
            datacenter: Local datacenter name
        """
        self.keyspace = keyspace
        self.hosts = hosts

        # Set up authentication if provided
        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            log_info(f"Using authentication (username: {username})")

        # Configure execution profile with load balancing policy
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=datacenter)),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=30
        )

        # Connect to cluster
        log_info(f"Connecting to Cassandra at {', '.join(hosts)}...")
        self.cluster = Cluster(
            contact_points=hosts,
            auth_provider=auth_provider,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile}
        )

        try:
            self.session = self.cluster.connect()
            log_success(f"Connected to Cassandra cluster")

            # Set keyspace
            self.session.set_keyspace(keyspace)
            log_success(f"Using keyspace: {keyspace}")

        except Exception as e:
            log_error(f"Failed to connect to Cassandra: {e}")
            raise

    def close(self):
        """Close the connection"""
        if self.cluster:
            self.cluster.shutdown()
            log_info("Connection closed")

    def parse_csv_value(self, value: str, column_type: str) -> Any:
        """
        Parse CSV value based on column type

        Args:
            value: Raw CSV value
            column_type: CQL column type

        Returns:
            Parsed value in appropriate Python type
        """
        # Handle NULL/empty values
        if value is None or value == '' or value.lower() == 'null':
            return None

        # Handle specific types
        if column_type == 'uuid' or column_type == 'timeuuid':
            return UUID(value)
        elif column_type == 'timestamp':
            # ISO 8601 format: 2025-04-28T02:49:41.964Z
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        elif column_type == 'int' or column_type == 'bigint':
            return int(value)
        elif column_type == 'float' or column_type == 'double':
            return float(value)
        elif column_type == 'boolean':
            return value.lower() in ('true', '1', 'yes')
        elif column_type.startswith('set<'):
            # Parse set notation: {"item1","item2"}
            if value.startswith('{') and value.endswith('}'):
                inner = value[1:-1]
                if not inner:
                    return set()
                # Handle escaped quotes
                items = []
                current = ""
                in_quotes = False
                i = 0
                while i < len(inner):
                    if inner[i:i+2] == '""':
                        current += '"'
                        i += 2
                    elif inner[i] == '"':
                        in_quotes = not in_quotes
                        i += 1
                    elif inner[i] == ',' and not in_quotes:
                        if current:
                            items.append(current)
                        current = ""
                        i += 1
                    else:
                        current += inner[i]
                        i += 1
                if current:
                    items.append(current)
                return set(items) if items else None
            return None
        elif column_type.startswith('map<'):
            # Parse map notation: {"key1":value1,"key2":value2}
            if value.startswith('{') and value.endswith('}'):
                inner = value[1:-1]
                if not inner:
                    return {}
                result = {}
                # Simple parser for key:value pairs
                pairs = []
                current = ""
                in_quotes = False
                depth = 0
                for char in inner:
                    if char == '"':
                        in_quotes = not in_quotes
                        current += char
                    elif char == ',' and not in_quotes and depth == 0:
                        if current:
                            pairs.append(current)
                        current = ""
                    else:
                        current += char
                if current:
                    pairs.append(current)

                for pair in pairs:
                    if ':' in pair:
                        key_part, val_part = pair.split(':', 1)
                        key = key_part.strip().strip('"')
                        try:
                            val = float(val_part.strip())
                        except ValueError:
                            val = val_part.strip().strip('"')
                        result[key] = val
                return result if result else None
            return None
        else:
            # Default: return as string
            return value

    def load_table(self, table_name: str, csv_path: Path,
                   column_types: Optional[Dict[str, str]] = None) -> tuple[int, int]:
        """
        Load data from CSV into a table

        Args:
            table_name: Target table name
            csv_path: Path to CSV file
            column_types: Dictionary of column name -> CQL type (for parsing)

        Returns:
            Tuple of (rows_loaded, rows_failed)
        """
        log_info(f"Loading table: {Colors.YELLOW}{table_name}{Colors.NC}")

        if not csv_path.exists():
            log_error(f"CSV file not found: {csv_path}")
            return 0, 0

        rows_loaded = 0
        rows_failed = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                columns = reader.fieldnames

                if not columns:
                    log_error(f"No columns found in CSV: {csv_path}")
                    return 0, 0

                log_info(f"CSV columns: {', '.join(columns)}")

                # Build INSERT statement
                placeholders = ', '.join(['?' for _ in columns])
                column_list = ', '.join(columns)
                insert_query = f"INSERT INTO {self.keyspace}.{table_name} ({column_list}) VALUES ({placeholders})"

                prepared = self.session.prepare(insert_query)

                # Read and insert rows
                batch_size = 100
                batch = []

                for row in reader:
                    try:
                        # Parse values based on column types if provided
                        parsed_values = []
                        for col in columns:
                            raw_value = row[col]
                            if column_types and col in column_types:
                                parsed_value = self.parse_csv_value(raw_value, column_types[col])
                            else:
                                # Auto-detect: try UUID, then timestamp, then string
                                parsed_value = raw_value if raw_value != '' else None
                            parsed_values.append(parsed_value)

                        batch.append(parsed_values)

                        # Execute batch when full
                        if len(batch) >= batch_size:
                            self._execute_batch(prepared, batch)
                            rows_loaded += len(batch)
                            batch = []

                            # Progress indicator
                            if rows_loaded % 500 == 0:
                                print(f"  Loaded {rows_loaded} rows...", end='\r')

                    except Exception as e:
                        log_warning(f"Failed to parse row: {e}")
                        rows_failed += 1

                # Execute remaining batch
                if batch:
                    self._execute_batch(prepared, batch)
                    rows_loaded += len(batch)

                print()  # New line after progress indicator
                log_success(f"{table_name}: loaded {rows_loaded} rows")

                if rows_failed > 0:
                    log_warning(f"{table_name}: {rows_failed} rows failed")

                return rows_loaded, rows_failed

        except Exception as e:
            log_error(f"Failed to load {table_name}: {e}")
            return rows_loaded, rows_failed

    def load_counter_table(self, table_name: str, csv_path: Path,
                          update_query: str) -> tuple[int, int]:
        """
        Load data into a counter table using UPDATE statements

        Args:
            table_name: Target counter table name
            csv_path: Path to CSV file
            update_query: UPDATE CQL query with named parameters

        Returns:
            Tuple of (rows_loaded, rows_failed)
        """
        log_info(f"Loading counter table: {Colors.YELLOW}{table_name}{Colors.NC}")

        if not csv_path.exists():
            log_error(f"CSV file not found: {csv_path}")
            return 0, 0

        rows_loaded = 0
        rows_failed = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                prepared = self.session.prepare(update_query)

                for row in reader:
                    try:
                        # Convert string values to appropriate types
                        typed_row = {}
                        for key, value in row.items():
                            if value == '' or value.lower() == 'null':
                                typed_row[key] = None
                            elif key.endswith('id'):
                                typed_row[key] = UUID(value)
                            else:
                                try:
                                    typed_row[key] = int(value)
                                except ValueError:
                                    typed_row[key] = value

                        self.session.execute(prepared, typed_row)
                        rows_loaded += 1

                        if rows_loaded % 500 == 0:
                            print(f"  Updated {rows_loaded} rows...", end='\r')

                    except Exception as e:
                        log_warning(f"Failed to update counter row: {e}")
                        rows_failed += 1

                print()  # New line after progress indicator
                log_success(f"{table_name}: updated {rows_loaded} rows")

                if rows_failed > 0:
                    log_warning(f"{table_name}: {rows_failed} rows failed")

                return rows_loaded, rows_failed

        except Exception as e:
            log_error(f"Failed to load counter table {table_name}: {e}")
            return rows_loaded, rows_failed

    def _execute_batch(self, prepared_statement, batch):
        """Execute a batch of inserts with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                for values in batch:
                    self.session.execute(prepared_statement, values)
                return
            except (Unavailable, Timeout, ReadTimeout, WriteTimeout) as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    raise

def main():
    parser = argparse.ArgumentParser(
        description='Load KillrVideo CSV data into OSS Cassandra 5.0+',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--host', default='127.0.0.1',
                       help='Cassandra contact point (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=9042,
                       help='Cassandra port (default: 9042)')
    parser.add_argument('--keyspace', default='killrvideo',
                       help='Target keyspace (default: killrvideo)')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--datacenter', default='datacenter1',
                       help='Local datacenter name (default: datacenter1)')
    parser.add_argument('--csv-dir', default='../../data/csv',
                       help='Directory containing CSV files (default: ../../data/csv)')

    args = parser.parse_args()

    log_section("KillrVideo Data Loading Script - Python Implementation")

    csv_dir = Path(args.csv_dir)
    if not csv_dir.exists():
        log_error(f"CSV directory not found: {csv_dir}")
        return 1

    log_info(f"CSV directory: {csv_dir}")
    log_info(f"Target keyspace: {args.keyspace}")

    # Initialize loader
    try:
        loader = CassandraLoader(
            hosts=[args.host],
            keyspace=args.keyspace,
            username=args.username,
            password=args.password,
            datacenter=args.datacenter
        )
    except Exception as e:
        log_error(f"Failed to initialize loader: {e}")
        return 1

    start_time = time.time()
    total_loaded = 0
    total_failed = 0

    try:
        # Level 1: Independent tables
        log_section("LEVEL 1: Independent Tables")

        loaded, failed = loader.load_table('users', csv_dir / 'users.csv')
        total_loaded += loaded
        total_failed += failed

        loaded, failed = loader.load_table('tags', csv_dir / 'tags.csv')
        total_loaded += loaded
        total_failed += failed

        # Level 2: Depends on users
        log_section("LEVEL 2: Tables Depending on Users")

        loaded, failed = loader.load_table('user_credentials', csv_dir / 'user_credentials.csv')
        total_loaded += loaded
        total_failed += failed

        loaded, failed = loader.load_table('user_preferences', csv_dir / 'user_preferences.csv')
        total_loaded += loaded
        total_failed += failed

        loaded, failed = loader.load_table('videos', csv_dir / 'videos.csv')
        total_loaded += loaded
        total_failed += failed

        # Level 3: Depends on videos
        log_section("LEVEL 3: Tables Depending on Videos")

        loaded, failed = loader.load_table('latest_videos', csv_dir / 'latest_videos.csv')
        total_loaded += loaded
        total_failed += failed

        # Counter tables
        loaded, failed = loader.load_counter_table(
            'video_playback_stats',
            csv_dir / 'video_playback_stats.csv',
            f"UPDATE {args.keyspace}.video_playback_stats SET views = views + :views, "
            "total_play_time = total_play_time + :total_play_time, "
            "complete_views = complete_views + :complete_views, "
            "unique_viewers = unique_viewers + :unique_viewers "
            "WHERE videoid = :videoid"
        )
        total_loaded += loaded
        total_failed += failed

        loaded, failed = loader.load_counter_table(
            'video_ratings',
            csv_dir / 'video_ratings.csv',
            f"UPDATE {args.keyspace}.video_ratings SET "
            "rating_counter = rating_counter + :rating_counter, "
            "rating_total = rating_total + :rating_total "
            "WHERE videoid = :videoid"
        )
        total_loaded += loaded
        total_failed += failed

        # Level 4: Depends on users + videos
        log_section("LEVEL 4: Tables Depending on Users + Videos")

        loaded, failed = loader.load_table('comments', csv_dir / 'comments.csv')
        total_loaded += loaded
        total_failed += failed

        loaded, failed = loader.load_table('comments_by_user', csv_dir / 'comments_by_user.csv')
        total_loaded += loaded
        total_failed += failed

        loaded, failed = loader.load_table('video_ratings_by_user', csv_dir / 'video_ratings_by_user.csv')
        total_loaded += loaded
        total_failed += failed

        # Level 5: Depends on tags
        log_section("LEVEL 5: Tables Depending on Tags")

        loaded, failed = loader.load_counter_table(
            'tag_counts',
            csv_dir / 'tag_counts.csv',
            f"UPDATE {args.keyspace}.tag_counts SET count = count + :count WHERE tag = :tag"
        )
        total_loaded += loaded
        total_failed += failed

    finally:
        loader.close()

    # Summary
    log_section("Load Summary")
    duration = time.time() - start_time

    print(f"Total rows loaded: {total_loaded}")
    print(f"Total rows failed: {total_failed}")
    print(f"Duration: {duration:.1f} seconds")
    print()

    if total_failed == 0:
        log_success("ALL DATA LOADED SUCCESSFULLY")
        return 0
    else:
        log_warning(f"Completed with {total_failed} failures")
        return 1

if __name__ == '__main__':
    sys.exit(main())
