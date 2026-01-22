"""CSV writer with DSBulk-optimized formatting"""

import csv
import json
import os
from typing import List, Dict, Any
from datetime import datetime, date
from pathlib import Path


class CSVWriter:
    """Writes data to CSV files with DSBulk-compatible formatting"""

    def __init__(self, output_dir: str, encoding: str = 'utf-8'):
        """
        Initialize CSV writer.

        Args:
            output_dir: Directory for output files
            encoding: CSV file encoding
        """
        self.output_dir = Path(output_dir)
        self.encoding = encoding

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_table(self, table_name: str, data: List[Dict[str, Any]], schema: Dict[str, str]):
        """
        Write data to a CSV file.

        Args:
            table_name: Name of the table (becomes filename)
            data: List of data dictionaries
            schema: Dictionary mapping column names to types
        """
        if not data:
            print(f"⚠️  No data for {table_name}, skipping")
            return

        filepath = self.output_dir / f"{table_name}.csv"

        with open(filepath, 'w', newline='', encoding=self.encoding) as csvfile:
            # Get all columns from schema
            columns = list(schema.keys())

            # Use backslash escaping instead of quote doubling for DSBulk compatibility
            # QUOTE_ALL ensures fields like ["cassandra"] get quoted even without commas
            writer = csv.DictWriter(
                csvfile,
                fieldnames=columns,
                extrasaction='ignore',
                doublequote=False,
                escapechar='\\',
                quoting=csv.QUOTE_ALL
            )
            writer.writeheader()

            for row in data:
                # Format each field according to its type
                formatted_row = {}
                for col, col_type in schema.items():
                    value = row.get(col)
                    formatted_row[col] = self._format_value(value, col_type)

                writer.writerow(formatted_row)

        print(f"✅ Wrote {len(data)} rows to {filepath.name}")

    def _format_value(self, value: Any, col_type: str) -> str:
        """
        Format a value for DSBulk CSV format.

        Args:
            value: The value to format
            col_type: The column type

        Returns:
            Formatted string value
        """
        if value is None:
            return ''

        # Timestamp formatting
        if col_type == 'timestamp':
            if isinstance(value, datetime):
                return value.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            elif isinstance(value, str):
                return value
            return ''

        # Date formatting
        if col_type == 'date':
            if isinstance(value, date):
                return value.strftime('%Y-%m-%d')
            elif isinstance(value, datetime):
                return value.strftime('%Y-%m-%d')
            return str(value)

        # UUID formatting (already strings)
        if col_type in ['uuid', 'timeuuid']:
            return str(value)

        # Boolean formatting
        if col_type == 'boolean':
            return 'true' if value else 'false'

        # Set formatting: ["item1","item2","item3"] (JSON array format for DSBulk)
        if col_type.startswith('set<'):
            if isinstance(value, (set, list)):
                if not value:
                    return '[]'
                # JSON array format - DSBulk expects JSON, not CQL set literals
                return json.dumps(list(value))
            return '[]'

        # Map formatting: {"key1":value1,"key2":value2} (JSON object format for DSBulk)
        if col_type.startswith('map<'):
            if isinstance(value, dict):
                if not value:
                    return '{}'
                # JSON object format - DSBulk expects valid JSON
                return json.dumps(value)
            return '{}'

        # Vector formatting: [0.1, 0.2, 0.3, ...] with spaces (DSBulk requirement)
        if col_type.startswith('vector<'):
            if isinstance(value, (list, tuple)):
                # Format with spaces after commas per DSBulk docs: [8, 2.3, 58]
                return '[' + ', '.join(str(float(v)) for v in value) + ']'
            return ''

        # Counter (just a number)
        if col_type == 'counter':
            return str(value)

        # Float/Double
        if col_type in ['float', 'double']:
            return str(float(value))

        # Int
        if col_type == 'int':
            return str(int(value))

        # Text (default) - normalize for DSBulk compatibility
        text = str(value)
        # Replace newlines and tabs with spaces to avoid CSV parsing issues
        text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        # Remove embedded quotes which cause DSBulk parsing issues
        text = text.replace('"', "'")
        # Collapse multiple spaces
        while '  ' in text:
            text = text.replace('  ', ' ')
        return text.strip()

    def write_all_tables(self, all_data: Dict[str, List[Dict]], schemas: Dict[str, Dict[str, str]]):
        """
        Write all tables to CSV files.

        Args:
            all_data: Dictionary mapping table names to data lists
            schemas: Dictionary mapping table names to column schemas
        """
        print(f"\n💾 Writing CSV files to {self.output_dir}/")

        for table_name, data in all_data.items():
            if table_name in schemas:
                self.write_table(table_name, data, schemas[table_name])
            else:
                print(f"⚠️  No schema defined for {table_name}, skipping")


# Define schemas for all tables
KILLRVIDEO_SCHEMAS = {
    'users': {
        'userid': 'uuid',
        'created_date': 'timestamp',
        'email': 'text',
        'firstname': 'text',
        'lastname': 'text',
        'account_status': 'text',
        'last_login_date': 'timestamp'
    },
    'user_credentials': {
        'email': 'text',
        'password': 'text',
        'userid': 'uuid',
        'account_locked': 'boolean'
    },
    'videos': {
        'videoid': 'uuid',
        'added_date': 'timestamp',
        'description': 'text',
        'location': 'text',
        'location_type': 'int',
        'name': 'text',
        'preview_image_location': 'text',
        'tags': 'set<text>',
        'content_features': 'vector<float,384>',  # IBM Granite embeddings
        'userid': 'uuid',
        'content_rating': 'text',
        'category': 'text',
        'language': 'text'
    },
    'latest_videos': {
        'day': 'date',
        'added_date': 'timestamp',
        'videoid': 'uuid',
        'name': 'text',
        'preview_image_location': 'text',
        'userid': 'uuid',
        'content_rating': 'text',
        'category': 'text'
    },
    'tags': {
        'tag': 'text',
        'tag_vector': 'vector<float,384>',  # IBM Granite embeddings
        'related_tags': 'set<text>',
        'category': 'text'
    },
    'tag_counts': {
        'tag': 'text',
        'count': 'counter'
    },
    'comments': {
        'videoid': 'uuid',
        'commentid': 'timeuuid',
        'comment': 'text',
        'userid': 'uuid',
        'sentiment_score': 'float'
    },
    'comments_by_user': {
        'userid': 'uuid',
        'commentid': 'timeuuid',
        'videoid': 'uuid',
        'comment': 'text',
        'sentiment_score': 'float'
    },
    'video_ratings': {
        'videoid': 'uuid',
        'rating_counter': 'counter',
        'rating_total': 'counter'
    },
    'video_ratings_by_user': {
        'videoid': 'uuid',
        'userid': 'uuid',
        'rating': 'int',
        'rating_date': 'timestamp'
    },
    'video_playback_stats': {
        'videoid': 'uuid',
        'views': 'counter',
        'total_play_time': 'counter',
        'complete_views': 'counter',
        'unique_viewers': 'counter'
    },
    'user_preferences': {
        'userid': 'uuid',
        'preference_vector': 'vector<float,384>',  # IBM Granite embeddings
        'tag_preferences': 'map<text,float>',
        'category_preferences': 'map<text,float>',
        'last_updated': 'timestamp'
    }
}
