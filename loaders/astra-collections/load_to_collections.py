#!/usr/bin/env python3
"""
KillrVideo Data Loader for Astra Collections (Data API)

This script loads CSV data into Astra DB Collections using the Data API.
It demonstrates:
- Transforming relational schema to JSON documents
- Automatic embedding generation with Vectorize
- Nested documents (joins from multiple tables)
- Automatic indexing of all fields

Prerequisites:
    - Astra DB database created
    - astrapy SDK installed (pip install astrapy)
    - Configuration file (config.yaml) with credentials
    - Embedding provider API key (for Vectorize)

Usage:
    python load_to_collections.py --config config.yaml

    # Drop existing collections first
    python load_to_collections.py --config config.yaml --drop-existing
"""

import argparse
import csv
import json
import sys
import time
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from uuid import UUID
from collections import defaultdict
#from sentence_transformers import SentenceTransformer

#try:
from astrapy import AsyncCollection, DataAPIClient
#except ImportError:
#    print("ERROR: astrapy not installed")
#    print("Install with: pip install astrapy")
#    sys.exit(1)

# ANSI colors
class Colors:
    BLUE = '\033[0;34m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    BOLD = '\033[1m'
    NC = '\033[0m'

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

class CollectionLoader:
    """Loader for KillrVideo data into Astra Collections"""

    def __init__(self, config: dict, schema_mapping: dict):
        """
        Initialize Astra Data API connection

        Args:
            config: Configuration from config.yaml
            schema_mapping: Schema mapping from schema_mapping.yaml
        """
        self.config = config
        self.schema_mapping = schema_mapping

        astra_config = config['astra']

        log_info(f"Connecting to Astra Data API...")

        # Initialize AstraDB client
        astraClient = DataAPIClient(token=astra_config['token'])
        self.db = astraClient.get_database(
            api_endpoint=astra_config['api_endpoint'],
            keyspace=astra_config.get('namespace', 'killrvideo')
        )

        log_success("Connected to Astra Data API")
        #log_info(f"Namespace: {astra_config.get('namespace', 'killrvideo')}")
        log_info(f"Namespace: {self.db.keyspace}")
        log_info(f"API endpoint: {self.db.api_endpoint}")

        # Cache for loaded data (for joins)
        self.data_cache = {}

    def load_csv_to_dict(self, csv_path: Path, key_field: str) -> Dict[str, Dict]:
        """
        Load CSV into dictionary keyed by specified field

        Args:
            csv_path: Path to CSV file
            key_field: Field to use as dictionary key

        Returns:
            Dictionary mapping key -> row data
        """
        data = {}

        if not csv_path.exists():
            log_warning(f"CSV file not found: {csv_path}")
            return data

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row.get(key_field)
                if key:
                    # Parse values
                    parsed_row = self.parse_row(row)
                    data[key] = parsed_row

        return data

    def parse_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """
        Parse CSV row, converting types appropriately

        Args:
            row: Dictionary of string values from CSV

        Returns:
            Dictionary with properly typed values
        """
        parsed = {}

        for key, value in row.items():
            # Skip empty values
            if value is None or value == '' or value.lower() == 'null':
                continue

            # Try to parse as different types
            try:
                # Check if it's a UUID
                if len(value) == 36 and value.count('-') == 4:
                    parsed[key] = value  # Keep as string for JSON
                # Check if it's a timestamp
                elif 'T' in value and ('Z' in value or '+' in value):
                    parsed[key] = value  # Keep ISO format string
                # Check if it's an integer
                elif value.isdigit() or (value[0] == '-' and value[1:].isdigit()):
                    parsed[key] = int(value)
                # Check if it's a float
                elif '.' in value:
                    try:
                        parsed[key] = float(value)
                    except ValueError:
                        parsed[key] = value
                # Parse sets: {"item1","item2"}
                elif value.startswith('{') and value.endswith('}') and '"' in value:
                    # Parse as array (JSON doesn't have sets)
                    inner = value[1:-1]
                    if inner:
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
                        parsed[key] = items
                    else:
                        parsed[key] = []
                # Parse maps: {"key1":value1,"key2":value2}
                elif value.startswith('{') and value.endswith('}') and ':' in value:
                    inner = value[1:-1]
                    if inner:
                        result = {}
                        pairs = []
                        current = ""
                        in_quotes = False
                        for char in inner:
                            if char == '"':
                                in_quotes = not in_quotes
                                current += char
                            elif char == ',' and not in_quotes:
                                if current:
                                    pairs.append(current)
                                current = ""
                            else:
                                current += char
                        if current:
                            pairs.append(current)

                        for pair in pairs:
                            if ':' in pair:
                                k, v = pair.split(':', 1)
                                k = k.strip().strip('"')
                                try:
                                    v = float(v.strip())
                                except ValueError:
                                    v = v.strip().strip('"')
                                result[k] = v
                        parsed[key] = result
                    else:
                        parsed[key] = {}
                # Boolean
                elif value.lower() in ('true', 'false'):
                    parsed[key] = value.lower() == 'true'
                else:
                    parsed[key] = value

            except Exception as e:
                # If parsing fails, keep as string
                parsed[key] = value

        return parsed

    def create_collection_with_vectorize(self, collection_name: str,
                                        collection_config: dict) -> AsyncCollection:
        """
        Create collection with optional vectorize configuration

        Args:
            collection_name: Name of collection to create
            collection_config: Collection configuration from schema_mapping

        Returns:
            AsyncCollection instance
        """
        log_info(f"Creating collection: {Colors.YELLOW}{collection_name}{Colors.NC}")
        log_info(f"List of keyspaces: {self.db.get_database_admin().list_keyspaces()}")

        # Check if collection exists
        #existing_collections = self.db.get_collections()['status']['collections']
        existing_collections = self.db.list_collection_names()

        if collection_name in existing_collections:
            if self.config.get('loading', {}).get('drop_existing', False):
                log_warning(f"Dropping existing collection: {collection_name}")
                self.db.drop_collection(collection_name)
            else:
                log_info(f"Collection already exists: {collection_name}")
                return self.db.get_collection(collection_name)

        # Prepare collection options
        options = {}

        # Configure vectorize if enabled
        vectorize_config = collection_config.get('vectorize', {})
        if vectorize_config.get('enabled', False):
            provider_config = self.schema_mapping.get('vectorize_provider', {})
            provider_name = provider_config.get('name')
            model = provider_config.get('model')

            if not provider_name or not model:
                log_error(f"Vectorize enabled but provider not configured")
                raise ValueError("Missing vectorize provider configuration")

            log_info(f"  Vectorize enabled: {provider_name} / {model}")
            log_info(f"  Text field: {vectorize_config.get('text_field')}")

            # Note: Collection-level vectorize configuration is set via Astra Portal
            # or via astrapy's advanced collection creation methods
            # For simplicity, we'll create a standard collection
            # Users should configure vectorize via Astra Portal after collection creation

        # Create collection
        try:
            collection = self.db.create_collection(collection_name, **options)
            log_success(f"Collection created: {collection_name}")

            if vectorize_config.get('enabled', False):
                log_warning(f"Configure Vectorize for '{collection_name}' in Astra Portal:")
                log_info(f"  1. Go to Database → Collections → {collection_name}")
                log_info(f"  2. Click 'Configure Embedding Provider'")
                log_info(f"  3. Select provider: {provider_config.get('name')}")
                log_info(f"  4. Select model: {provider_config.get('model')}")
                log_info(f"  5. Configure to vectorize field: {vectorize_config.get('text_field')}")
                print()

            return collection

        except Exception as e:
            log_error(f"Failed to create collection {collection_name}: {e}")
            raise

    def build_document(self, primary_row: Dict, collection_name: str,
                      collection_config: dict) -> Dict[str, Any]:
        """
        Build JSON document from primary row and joined tables

        Args:
            primary_row: Primary table row data
            collection_name: Collection name
            collection_config: Collection configuration

        Returns:
            JSON document
        """
        document = primary_row.copy()

        # Set document ID
        id_field = collection_config.get('id_field')
        if id_field == '_composite':
            # Generate composite ID
            composite_format = self.schema_mapping['transformations']['composite_ids']['format']
            separator = self.schema_mapping['transformations']['composite_ids']['separator']
            # Simple implementation: userid_videoid
            doc_id = f"{document.get('userid', 'unknown')}{separator}{document.get('videoid', 'unknown')}"
            document['_id'] = doc_id
        elif id_field in document:
            document['_id'] = document[id_field]

        # Process joins
        joins = collection_config.get('joins', [])
        for join in joins:
            join_table = join['table']
            join_key = join['join_on']
            nest_as = join['nest_as']
            include_fields = join.get('include_fields')

            # Get join key value from primary row
            join_value = document.get(join_key)
            if not join_value:
                continue

            # Look up joined data
            cache_key = f"{join_table}_{join_key}"
            if cache_key not in self.data_cache:
                # Load joined table data
                csv_path = Path(self.config['data']['csv_dir']) / f"{join_table}.csv"
                self.data_cache[cache_key] = self.load_csv_to_dict(csv_path, join_key)

            joined_data = self.data_cache[cache_key].get(join_value)
            if joined_data:
                # Filter to include_fields if specified
                if include_fields:
                    nested_doc = {k: v for k, v in joined_data.items() if k in include_fields}
                else:
                    nested_doc = joined_data

                # Remove the join key from nested doc (redundant)
                nested_doc.pop(join_key, None)

                document[nest_as] = nested_doc

        return document

    def load_collection(self, collection_name: str) -> tuple[int, int]:
        """
        Load data into a collection

        Args:
            collection_name: Collection to load

        Returns:
            Tuple of (documents_loaded, documents_failed)
        """
        log_section(f"Loading Collection: {collection_name}")

        collection_config = self.schema_mapping['collections'].get(collection_name)
        if not collection_config:
            log_error(f"No configuration found for collection: {collection_name}")
            return 0, 0

        # Create collection
        collection = self.create_collection_with_vectorize(collection_name, collection_config)

        # embedding model
        #model = SentenceTransformer('ibm-granite/granite-embedding-30m-english')

        # Load primary table data
        primary_table = collection_config['primary_table']
        csv_path = Path(self.config['data']['csv_dir']) / f"{primary_table}.csv"

        if not csv_path.exists() or collection_config.get('skip',False):
            log_error(f"CSV file not found or skip==True: {csv_path}")
            return 0, 0

        log_info(f"Loading from: {csv_path}")

        docs_loaded = 0
        docs_failed = 0

        batch_size = self.config.get('loading', {}).get('batch_size', 20)
        batch = []

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for row in reader:
                    try:
                        # Parse row
                        parsed_row = self.parse_row(row)

                        # Build document with joins
                        document = self.build_document(parsed_row, collection_name, collection_config)

                        # Handle vectorize field
                        #vectorize_config = collection_config.get('vectorize', {})
                        #if vectorize_config.get('enabled', False):
                        if collection_name == "videos":
                        #    text_field = vectorize_config.get('text_field')
                        #    if text_field and text_field in document:
                                # Use $vectorize for automatic embedding
                                # Note: This requires vectorize to be configured on the collection
                                #document['$vectorize'] = document[text_field]
                            vector = json.loads(document['content_features'])
                            if isinstance(vector, list):
                                document['$vector'] = [float(x) for x in vector]
                            document['content_features'] = ""

                        batch.append(document)

                        # Insert batch
                        if len(batch) >= batch_size:
                            self._insert_batch(collection, batch)
                            docs_loaded += len(batch)
                            batch = []

                            if docs_loaded % 100 == 0:
                                print(f"  Loaded {docs_loaded} documents...", end='\r')

                    except Exception as e:
                        log_warning(f"Failed to process row: {e}")
                        docs_failed += 1

                # Insert remaining batch
                if batch:
                    self._insert_batch(collection, batch)
                    docs_loaded += len(batch)

            print()  # New line
            log_success(f"{collection_name}: loaded {docs_loaded} documents")

            if docs_failed > 0:
                log_warning(f"{collection_name}: {docs_failed} documents failed")

            return docs_loaded, docs_failed

        except Exception as e:
            log_error(f"Failed to load collection {collection_name}: {e}")
            return docs_loaded, docs_failed

    def _insert_batch(self, collection: AsyncCollection, batch: List[Dict]):
        """Insert batch of documents with retry logic"""
        max_retries = self.config.get('loading', {}).get('max_retries', 3)
        retry_delay = self.config.get('loading', {}).get('retry_delay', 2)

        for attempt in range(max_retries):
            try:
                # Data API insert_many
                for doc in batch:
                    collection.insert_one(doc)
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    log_warning(f"Batch insert failed (attempt {attempt + 1}), retrying...")
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    log_error(f"Batch insert failed after {max_retries} attempts: {e}")
                    raise

def main():
    parser = argparse.ArgumentParser(
        description='Load KillrVideo data into Astra Collections',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--config', default='config.yaml',
                       help='Path to configuration file (default: config.yaml)')
    parser.add_argument('--schema-mapping', default='schema_mapping.yaml',
                       help='Path to schema mapping file (default: schema_mapping.yaml)')
    parser.add_argument('--drop-existing', action='store_true',
                       help='Drop existing collections before loading')

    args = parser.parse_args()

    config_path = Path(args.config)
    mapping_path = Path(args.schema_mapping)

    # Load configuration
    log_section("Astra Collections Data Loading")

    if not config_path.exists():
        log_error(f"Configuration file not found: {config_path}")
        log_info("Copy config.example.yaml to config.yaml and configure it")
        return 1

    if not mapping_path.exists():
        log_error(f"Schema mapping file not found: {mapping_path}")
        return 1

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        log_success(f"Configuration loaded: {config_path}")

        with open(mapping_path, 'r') as f:
            schema_mapping = yaml.safe_load(f)
        log_success(f"Schema mapping loaded: {mapping_path}")

    except Exception as e:
        log_error(f"Failed to load configuration: {e}")
        return 1

    # Override drop_existing if specified
    if args.drop_existing:
        if 'loading' not in config:
            config['loading'] = {}
        config['loading']['drop_existing'] = True
        log_warning("Will drop existing collections before loading")

    # Initialize loader
    try:
        loader = CollectionLoader(config, schema_mapping)
    except Exception as e:
        log_error(f"Failed to initialize loader: {e}")
        return 1

    start_time = time.time()
    total_loaded = 0
    total_failed = 0

    # Load collections
    collections_to_load = schema_mapping['collections'].keys()

    log_info(f"Collections to load: {', '.join(collections_to_load)}")
    print()

    for collection_name in collections_to_load:
        loaded, failed = loader.load_collection(collection_name)
        total_loaded += loaded
        total_failed += failed

    # Summary
    log_section("Load Summary")
    duration = time.time() - start_time

    print(f"Total documents loaded: {total_loaded}")
    print(f"Total documents failed: {total_failed}")
    print(f"Duration: {duration:.1f} seconds")
    print()

    if total_failed == 0:
        log_success("ALL COLLECTIONS LOADED SUCCESSFULLY")
        print()
        log_info("Next steps:")
        print("  1. Configure Vectorize for collections in Astra Portal")
        print("  2. Test vector search queries")
        print("  3. Explore automatic indexing and flexible querying")
        print()
        return 0
    else:
        log_warning(f"Completed with {total_failed} failures")
        return 1

if __name__ == '__main__':
    sys.exit(main())
    #asyncio.run(main())
