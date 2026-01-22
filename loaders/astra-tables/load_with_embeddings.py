#!/usr/bin/env python3
"""
KillrVideo Data Loader for Astra Tables with IBM Granite Embeddings

This script loads CSV data into Astra DB while generating embeddings using
the IBM Granite 30M English model (384D → 16D/8D with PCA).

Prerequisites:
    - Astra DB database created with killrvideo keyspace
    - Schema loaded from data/schemas/schema-astra.cql
    - Configuration file (config.yaml) configured
    - Run setup_embeddings.py first to verify setup

Usage:
    python load_with_embeddings.py --config config.yaml

    # Skip embedding generation (load NULL vectors)
    python load_with_embeddings.py --config config.yaml --skip-embeddings
"""

import argparse
import csv
from curses import raw
import json
import sys
import time
import yaml
import pickle
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from uuid import UUID
from decimal import Decimal

try:
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
    from cassandra.query import SimpleStatement, ConsistencyLevel
    from cassandra import InvalidRequest, Unavailable, Timeout
except ImportError:
    print("ERROR: cassandra-driver not installed")
    print("Install with: pip install -r ../requirements.txt")
    sys.exit(1)

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    from sklearn.decomposition import PCA
except ImportError:
    print("ERROR: sentence-transformers or scikit-learn not installed")
    print("Install with: pip install -r ../requirements.txt")
    sys.exit(1)

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

class GraniteEmbedder:
    """IBM Granite embedding generator with dimension reduction"""

    def __init__(self, config: dict):
        """
        Initialize Granite model and PCA reducers

        Args:
            config: Configuration dictionary
        """
        self.config = config
        embedding_config = config.get('embedding', {})

        model_name = embedding_config.get('model_name', 'ibm-granite/granite-embedding-30m-english')

        log_info(f"Loading IBM Granite model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.native_dim = self.model.get_sentence_embedding_dimension()

        log_success(f"Model loaded: {self.native_dim}D embeddings")

        # Initialize PCA reducers for each target dimension
        self.pca_reducers = {}
        self.reduction_method = config.get('dimension_reduction', {}).get('method', 'pca')

        if self.reduction_method == 'pca':
            pca_config = config.get('dimension_reduction', {}).get('pca', {})
            self.cache_models = pca_config.get('cache_models', True)
            self.cache_dir = Path(pca_config.get('cache_dir', './.pca_cache'))

            if self.cache_models:
                self.cache_dir.mkdir(exist_ok=True)

    def generate_embedding(self, text: str) -> np.ndarray:
        """
        Generate embedding for text

        Args:
            text: Input text

        Returns:
            384D numpy array
        """
        return self.model.encode(text, convert_to_numpy=True)

    def reduce_dimensions(self, embeddings: np.ndarray, target_dim: int) -> np.ndarray:
        """
        Reduce embedding dimensions

        Args:
            embeddings: Input embeddings (n_samples, native_dim)
            target_dim: Target dimensions

        Returns:
            Reduced embeddings (n_samples, target_dim)
        """
        if embeddings.ndim == 1:
            embeddings = embeddings.reshape(1, -1)

        if embeddings.shape[1] == target_dim:
            return embeddings

        if self.reduction_method == 'pca':
            return self._reduce_pca(embeddings, target_dim)
        else:
            # Truncate method
            return embeddings[:, :target_dim]

    def _reduce_pca(self, embeddings: np.ndarray, target_dim: int) -> np.ndarray:
        """
        Reduce dimensions using PCA

        Args:
            embeddings: Input embeddings
            target_dim: Target dimensions

        Returns:
            Reduced embeddings
        """
        # Check cache
        if self.cache_models and target_dim in self.pca_reducers:
            pca = self.pca_reducers[target_dim]
            return pca.transform(embeddings)

        # Load from disk cache
        if self.cache_models:
            cache_file = self.cache_dir / f"pca_{self.native_dim}to{target_dim}.pkl"
            if cache_file.exists():
                with open(cache_file, 'rb') as f:
                    pca = pickle.load(f)
                    self.pca_reducers[target_dim] = pca
                    return pca.transform(embeddings)

        # Fit new PCA
        pca = PCA(n_components=target_dim)
        reduced = pca.fit_transform(embeddings)

        # Cache in memory and disk
        self.pca_reducers[target_dim] = pca
        if self.cache_models:
            cache_file = self.cache_dir / f"pca_{self.native_dim}to{target_dim}.pkl"
            with open(cache_file, 'wb') as f:
                pickle.dump(pca, f)

        return reduced

class AstraTableLoader:
    """Loader for KillrVideo data into Astra Tables with Granite embeddings"""

    def __init__(self, config: dict, skip_embeddings: bool = False):
        """
        Initialize Astra connection and embedding generator

        Args:
            config: Configuration dictionary
            skip_embeddings: Skip embedding generation
        """
        self.config = config
        self.skip_embeddings = skip_embeddings

        astra_config = config['astra']
        self.keyspace = astra_config['keyspace']

        # Initialize embedding generator
        self.embedder = None
        if not skip_embeddings:
            try:
                self.embedder = GraniteEmbedder(config)
            except Exception as e:
                log_error(f"Failed to initialize embedder: {e}")
                raise

        # Connect to Astra
        log_info(f"Connecting to Astra DB...")

        secure_bundle = astra_config['secure_bundle_path']
        token = astra_config['token']

        if not Path(secure_bundle).exists():
            raise FileNotFoundError(f"Secure Connect Bundle not found: {secure_bundle}")

        auth_provider = PlainTextAuthProvider('token', token)

        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=30
        )

        try:
            self.cluster = Cluster(
                cloud={'secure_connect_bundle': secure_bundle},
                auth_provider=auth_provider,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile}
            )

            self.session = self.cluster.connect()
            self.session.set_keyspace(self.keyspace)

            log_success(f"Connected to Astra DB")
            log_success(f"Using keyspace: {self.keyspace}")

        except Exception as e:
            log_error(f"Failed to connect to Astra: {e}")
            raise

    def close(self):
        """Close connections"""
        if self.cluster:
            self.cluster.shutdown()
            log_info("Connection closed")

    def generate_embedding_for_text(self, text: str, target_dimensions: int) -> Optional[List[float]]:
        """
        Generate and reduce embedding for text

        Args:
            text: Input text
            target_dimensions: Target vector dimensions

        Returns:
            List of floats or None
        """
        if self.skip_embeddings or not text or not self.embedder:
            return None

        try:
            # Generate 384D embedding
            embedding = self.embedder.generate_embedding(text)

            # Reduce to target dimensions
            #reduced = self.embedder.reduce_dimensions(embedding, target_dimensions)

            #return reduced.flatten().tolist()
            return embedding

        except Exception as e:
            log_warning(f"Failed to generate embedding: {e}")
            return None

    def parse_csv_value(self, value: str, column_type: str = 'string') -> Any:
        """Parse CSV value (same as before)"""

        if value is None or value == '' or value.lower() == 'null':
            return None

        if column_type == 'uuid' or column_type == 'timeuuid':
            return UUID(value)
        elif column_type == 'timestamp':
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        elif column_type == 'int' or column_type == 'bigint':
            return int(value)
        elif column_type == 'float' or column_type == 'double':
            return float(value)
        elif column_type == 'boolean':
            return value.lower() in ('true', '1', 'yes')
        elif column_type.startswith('set<'):
            if value.startswith('{') and value.endswith('}'):
                inner = value[1:-1]
                if not inner:
                    return set()
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
            if value.startswith('{') and value.endswith('}'):
                inner = value[1:-1]
                if not inner:
                    return {}
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
            return value

    def load_table_with_embeddings(self, table_name: str, csv_path: Path) -> tuple[int, int]:
        """
        Load table with embedding generation

        Args:
            table_name: Target table name
            csv_path: Path to CSV file

        Returns:
            Tuple of (rows_loaded, rows_failed)
        """
        log_info(f"Loading table: {Colors.YELLOW}{table_name}{Colors.NC}")

        if not csv_path.exists():
            log_error(f"CSV file not found: {csv_path}")
            return 0, 0

        # Check if this table has vector mappings
        vector_mappings = self.config.get('vector_mappings', {})
        has_vectors = table_name in vector_mappings

        if has_vectors and not self.skip_embeddings:
            mapping = vector_mappings[table_name]
            log_info(f"Will generate {mapping['dimensions']}D embeddings from '{mapping['text_column']}'")

        rows_loaded = 0
        rows_failed = 0
        embeddings_generated = 0

        #try:
        with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                columns = list(reader.fieldnames)

                # Add vector column if needed
                if has_vectors:
                    mapping = vector_mappings[table_name]
                    vector_col = mapping['vector_column']
                    if vector_col not in columns:
                        columns.append(vector_col)

                log_info(f"Columns: {', '.join(columns)}")

                # Build INSERT statement
                placeholders = ', '.join(['?' for _ in columns])
                column_list = ', '.join(columns)
                insert_query = f"INSERT INTO {self.keyspace}.{table_name} ({column_list}) VALUES ({placeholders})"

                prepared = self.session.prepare(insert_query)

                batch_size = self.config.get('loading', {}).get('batch_size', 100)
                batch = []

                for row in reader:
                    #try:
                        parsed_values = []
                        text_for_embedding = None

                        for col in columns:
                            # Handle vector column
                            if has_vectors and col == vector_mappings[table_name]['vector_column']:
                                text_col = vector_mappings[table_name]['text_column']

                                # Handle synthesized text columns
                                if text_col == '_synthesized_preferences':
                                    # For user_preferences: concatenate top tags
                                    tag_prefs = row.get('tag_preferences', '')
                                    if tag_prefs and tag_prefs != '':
                                        tags = []
                                        if tag_prefs.startswith('{'):
                                            inner = tag_prefs[1:-1]
                                            for pair in inner.split(','):
                                                if ':' in pair:
                                                    tag = pair.split(':')[0].strip().strip('"')
                                                    tags.append(tag)
                                        text_for_embedding = ' '.join(tags) if tags else None
                                else:
                                    text_for_embedding = row.get(text_col)

                                # Generate embedding
                                if text_for_embedding:
                                    #print("Got here")
                                    embedding = self.generate_embedding_for_text(
                                        text_for_embedding,
                                        vector_mappings[table_name]['dimensions']
                                    )
                                    #print("Got here2")
                                    parsed_values.append(embedding)
                                    if embedding is not None:
                                        embeddings_generated += 1
                                else:
                                    parsed_values.append(None)
                            elif col.endswith('id'):
                                raw_value = UUID(row.get(col, ''))
                                parsed_values.append(raw_value if raw_value != '' else None)
                            elif col.endswith('date'):
                                value = row.get(col, '')
                                raw_value = datetime.fromisoformat(value.replace("Z", "+00:00"))
                                parsed_values.append(raw_value if raw_value != '' else None)
                            elif row.get(col, '').replace('.','').isdigit():
                                # numeric
                                value = row.get(col, '')
                                if '.' in value:
                                    #float
                                    raw_value = float(Decimal(value))
                                    parsed_values.append(raw_value)
                                else:
                                    #integer
                                    raw_value = int(value)
                                    parsed_values.append(raw_value)
                            elif row.get(col, '').startswith("[") and row.get(col, '').endswith("]"):
                                # set
                                raw_value = set(json.loads(row.get(col, '')))
                                parsed_values.append(raw_value if raw_value != '' else None)
                            elif row.get(col, '').startswith("{") and row.get(col, '').endswith("}"):
                                # list
                                items_str = row.get(col, '')[1:-1]
                                raw_value = [
                                    item.strip().strip('"').strip("'")
                                    for item in items_str.split(",")
                                    if item.strip()
                                ]
                                parsed_values.append(raw_value if raw_value != '' else None)
                            else:
                                # Regular text column
                                raw_value = row.get(col, '')
                                parsed_values.append(raw_value if raw_value != '' else None)

                        batch.append(parsed_values)

                        # Execute batch
                        if len(batch) >= batch_size:
                            self._execute_batch(prepared, batch)
                            rows_loaded += len(batch)
                            batch = []

                            if rows_loaded % 500 == 0:
                                status = f"  Loaded {rows_loaded} rows"
                                if embeddings_generated > 0:
                                    status += f" ({embeddings_generated} embeddings)"
                                print(status + "...", end='\r')

                    #except Exception as e:
                    #    log_warning(f"Failed to process row: {e}")
                    #    rows_failed += 1

                # Execute remaining batch
                if batch:
                    self._execute_batch(prepared, batch)
                    rows_loaded += len(batch)

                print()  # New line
                status = f"{table_name}: loaded {rows_loaded} rows"
                if embeddings_generated > 0:
                    status += f" with {embeddings_generated} embeddings"
                log_success(status)

                if rows_failed > 0:
                    log_warning(f"{table_name}: {rows_failed} rows failed")

                return rows_loaded, rows_failed

        #except Exception as e:
        #    log_error(f"Failed to load {table_name}: {e}")
        #    return rows_loaded, rows_failed

    def load_counter_table(self, table_name: str, csv_path: Path, update_query: str) -> tuple[int, int]:
        """Load counter table (same as before)"""

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

                print()
                log_success(f"{table_name}: updated {rows_loaded} rows")

                if rows_failed > 0:
                    log_warning(f"{table_name}: {rows_failed} rows failed")

                return rows_loaded, rows_failed

        except Exception as e:
            log_error(f"Failed to load counter table {table_name}: {e}")
            return rows_loaded, rows_failed

    def _execute_batch(self, prepared_statement, batch):
        """Execute batch with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                for values in batch:
                    self.session.execute(prepared_statement, values)
                return
            except (Unavailable, Timeout) as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise

def main():
    parser = argparse.ArgumentParser(
        description='Load KillrVideo data into Astra Tables with IBM Granite embeddings',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--config', default='config.yaml',
                       help='Path to configuration file (default: config.yaml)')
    parser.add_argument('--skip-embeddings', action='store_true',
                       help='Skip embedding generation, load NULL vectors')
    parser.add_argument('--table',
                       help='Loads only a specific table')

    args = parser.parse_args()

    config_path = Path(args.config)
    table = args.table

    if not config_path.exists():
        log_error(f"Configuration file not found: {config_path}")
        log_info("Copy config.example.yaml to config.yaml and configure it")
        return 1

    # Load configuration
    log_section("Astra Tables Data Loading with IBM Granite")

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        log_success(f"Configuration loaded from: {config_path}")
    except Exception as e:
        log_error(f"Failed to load configuration: {e}")
        return 1

    csv_dir = Path(config.get('loading', {}).get('csv_dir', '../../data/csv'))

    if not csv_dir.exists():
        log_error(f"CSV directory not found: {csv_dir}")
        return 1

    # Initialize loader
    try:
        loader = AstraTableLoader(config, skip_embeddings=args.skip_embeddings)
    except Exception as e:
        log_error(f"Failed to initialize loader: {e}")
        return 1

    keyspace = config['astra']['keyspace']
    start_time = time.time()
    total_loaded = 0
    total_failed = 0

    try:

        if not table:
            # Load data in dependency order
            log_section("LEVEL 1: Independent Tables")

            loaded, failed = loader.load_table_with_embeddings('users', csv_dir / 'users.csv')
            total_loaded += loaded
            total_failed += failed

            loaded, failed = loader.load_table_with_embeddings('tags', csv_dir / 'tags.csv')
            total_loaded += loaded
            total_failed += failed

            log_section("LEVEL 2: Tables Depending on Users")

            loaded, failed = loader.load_table_with_embeddings('user_credentials', csv_dir / 'user_credentials.csv')
            total_loaded += loaded
            total_failed += failed

            loaded, failed = loader.load_table_with_embeddings('user_preferences', csv_dir / 'user_preferences.csv')
            total_loaded += loaded
            total_failed += failed

            loaded, failed = loader.load_table_with_embeddings('videos', csv_dir / 'videos.csv')
            total_loaded += loaded
            total_failed += failed

            log_section("LEVEL 3: Tables Depending on Videos")

            loaded, failed = loader.load_table_with_embeddings('latest_videos', csv_dir / 'latest_videos.csv')
            total_loaded += loaded
            total_failed += failed

            loaded, failed = loader.load_counter_table(
                'video_playback_stats',
                csv_dir / 'video_playback_stats.csv',
                f"UPDATE {keyspace}.video_playback_stats SET views = views + :views, "
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
                f"UPDATE {keyspace}.video_ratings SET "
                "rating_counter = rating_counter + :rating_counter, "
                "rating_total = rating_total + :rating_total "
                "WHERE videoid = :videoid"
            )
            total_loaded += loaded
            total_failed += failed

            log_section("LEVEL 4: Tables Depending on Users + Videos")

            loaded, failed = loader.load_table_with_embeddings('comments', csv_dir / 'comments.csv')
            total_loaded += loaded
            total_failed += failed

            loaded, failed = loader.load_table_with_embeddings('comments_by_user', csv_dir / 'comments_by_user.csv')
            total_loaded += loaded
            total_failed += failed

            loaded, failed = loader.load_table_with_embeddings('video_ratings_by_user', csv_dir / 'video_ratings_by_user.csv')
            total_loaded += loaded
            total_failed += failed

            log_section("LEVEL 5: Tables Depending on Tags")

            loaded, failed = loader.load_counter_table(
                'tag_counts',
                csv_dir / 'tag_counts.csv',
                f"UPDATE {keyspace}.tag_counts SET count = count + :count WHERE tag = :tag"
            )
            total_loaded += loaded
            total_failed += failed
        elif table == "users":
            loaded, failed = loader.load_table_with_embeddings('users', csv_dir / 'users.csv')
            total_loaded += loaded
            total_failed += failed
        elif table == "tags":
            loaded, failed = loader.load_table_with_embeddings('tags', csv_dir / 'tags.csv')
            total_loaded += loaded
            total_failed += failed
        elif table == "user_credentials":
            loaded, failed = loader.load_table_with_embeddings('user_credentials', csv_dir / 'user_credentials.csv')
            total_loaded += loaded
            total_failed += failed
        elif table == "user_preferences":
            loaded, failed = loader.load_table_with_embeddings('user_preferences', csv_dir / 'user_preferences.csv')
            total_loaded += loaded
            total_failed += failed
        elif table == "videos":
            loaded, failed = loader.load_table_with_embeddings('videos', csv_dir / 'videos.csv')
            total_loaded += loaded
            total_failed += failed
        elif table == "latest_videos":
            loaded, failed = loader.load_table_with_embeddings('latest_videos', csv_dir / 'latest_videos.csv')
            total_loaded += loaded
            total_failed += failed
        elif table == "comments":
            loaded, failed = loader.load_table_with_embeddings('comments', csv_dir / 'comments.csv')
            total_loaded += loaded
            total_failed += failed
        elif table == "comments_by_user":
            loaded, failed = loader.load_table_with_embeddings('comments_by_user', csv_dir / 'comments_by_user.csv')
            total_loaded += loaded
            total_failed += failed
        elif table == "video_ratings_by_user":
            loaded, failed = loader.load_table_with_embeddings('video_ratings_by_user', csv_dir / 'video_ratings_by_user.csv')
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
