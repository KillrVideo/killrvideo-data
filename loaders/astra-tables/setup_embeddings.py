#!/usr/bin/env python3
"""
Setup script for testing IBM Granite embedding generation.

This script validates that the IBM Granite embedding model works correctly
and that dimension reduction produces the expected output.

Usage:
    python setup_embeddings.py --config config.yaml
"""

import argparse
import sys
import yaml
from pathlib import Path
from typing import List

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("ERROR: sentence-transformers not installed")
    print("Install with: pip install sentence-transformers")
    sys.exit(1)

try:
    from sklearn.decomposition import PCA
    import numpy as np
except ImportError:
    print("ERROR: scikit-learn not installed")
    print("Install with: pip install scikit-learn")
    sys.exit(1)

# ANSI color codes
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

def reduce_dimensions_pca(embeddings: np.ndarray, target_dim: int) -> np.ndarray:
    """
    Reduce embedding dimensions using PCA

    Args:
        embeddings: 2D array of embeddings (n_samples, n_features)
        target_dim: Target number of dimensions

    Returns:
        Reduced embeddings (n_samples, target_dim)
    """
    if embeddings.shape[1] == target_dim:
        return embeddings

    pca = PCA(n_components=target_dim)
    reduced = pca.fit_transform(embeddings)

    # Show variance explained
    variance_explained = sum(pca.explained_variance_ratio_) * 100
    log_info(f"    PCA variance explained: {variance_explained:.1f}%")

    return reduced

def reduce_dimensions_truncate(embedding: List[float], target_dim: int) -> List[float]:
    """Simply truncate to target dimensions"""
    return embedding[:target_dim]

def main():
    parser = argparse.ArgumentParser(
        description='Setup and test IBM Granite embedding generation'
    )
    parser.add_argument('--config', default='config.yaml',
                       help='Path to configuration file (default: config.yaml)')

    args = parser.parse_args()

    config_path = Path(args.config)

    if not config_path.exists():
        log_error(f"Configuration file not found: {config_path}")
        log_info("Copy config.example.yaml to config.yaml")
        return 1

    # Load configuration
    log_section("IBM Granite Embedding Setup")
    log_info(f"Loading configuration from: {config_path}")

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        log_success("Configuration loaded successfully")
    except Exception as e:
        log_error(f"Failed to load configuration: {e}")
        return 1

    # Load Granite model
    log_section("Loading IBM Granite Model")

    model_name = config.get('embedding', {}).get('model_name', 'ibm-granite/granite-embedding-30m-english')
    log_info(f"Model: {model_name}")
    log_info("Downloading model (first run only, ~120MB)...")

    try:
        model = SentenceTransformer(model_name)
        log_success(f"Model loaded successfully")

        # Get model info
        native_dim = model.get_sentence_embedding_dimension()
        log_info(f"Native embedding dimensions: {native_dim}")

        if native_dim != 384:
            log_warning(f"Expected 384 dimensions, got {native_dim}")

    except Exception as e:
        log_error(f"Failed to load model: {e}")
        return 1

    # Test embedding generation
    log_section("Testing Embedding Generation")

    test_texts = [
        "Apache Cassandra is a highly scalable NoSQL database designed for handling large amounts of data",
        "DataStax Astra DB provides a cloud-native database built on Apache Cassandra",
        "Vector search enables semantic similarity queries for finding related content",
        "Distributed databases use replication to ensure high availability and fault tolerance"
    ]

    log_info(f"Generating embeddings for {len(test_texts)} test texts...")
    print()

    try:
        # Generate all embeddings at once
        embeddings = model.encode(test_texts, convert_to_numpy=True)

        for i, text in enumerate(test_texts):
            embedding = embeddings[i]
            log_success(f"Text {i+1}: \"{text[:60]}...\"")
            log_info(f"  Generated: {len(embedding)} dimensions")
            log_info(f"  Sample: [{embedding[0]:.4f}, {embedding[1]:.4f}, {embedding[2]:.4f}, ...]")
            print()

    except Exception as e:
        log_error(f"Failed to generate embeddings: {e}")
        return 1

    # Test dimension reduction
    log_section("Testing Dimension Reduction")

    mappings = config.get('vector_mappings', {})
    reduction_method = config.get('dimension_reduction', {}).get('method', 'pca')

    log_info(f"Reduction method: {reduction_method}")
    print()

    # Test each target dimension
    target_dimensions = set()
    for table, mapping in mappings.items():
        target_dimensions.add(mapping['dimensions'])

    for target_dim in sorted(target_dimensions):
        log_info(f"Testing reduction to {target_dim} dimensions...")

        try:
            if reduction_method == 'pca':
                # Use PCA (requires multiple samples)
                reduced = reduce_dimensions_pca(embeddings, target_dim)
                sample = reduced[0]
                log_success(f"  PCA reduction: {native_dim}D → {target_dim}D")
                log_info(f"  Sample: [{sample[0]:.4f}, {sample[1]:.4f}, {sample[2]:.4f}, ...]")
            else:
                # Use truncation
                sample = reduce_dimensions_truncate(embeddings[0].tolist(), target_dim)
                log_success(f"  Truncate: {native_dim}D → {target_dim}D")
                log_info(f"  Sample: [{sample[0]:.4f}, {sample[1]:.4f}, {sample[2]:.4f}, ...]")

        except Exception as e:
            log_error(f"Failed to reduce dimensions: {e}")
            return 1


        print()

    # Verify schema compatibility
    log_section("Verifying Schema Compatibility")

    schema_dims = {
        'videos.content_features': 16,
        'tags.tag_vector': 8,
        'user_preferences.preference_vector': 16
    }

    all_ok = True

    for table_col, expected_dim in schema_dims.items():
        table = table_col.split('.')[0]
        if table in mappings:
            configured_dim = mappings[table]['dimensions']
            if configured_dim == expected_dim:
                log_success(f"  {table_col}: {configured_dim}D ✓")
            else:
                log_error(f"  {table_col}: expected {expected_dim}D, configured {configured_dim}D ✗")
                all_ok = False
        else:
            log_warning(f"  {table_col}: no mapping configured")

    print()

    if not all_ok:
        log_error("Schema compatibility check failed!")
        log_info("Fix dimension mismatches in config.yaml")
        return 1

    # Performance info
    log_section("Performance Information")

    log_info("IBM Granite 30M Model Benefits:")
    print("  • Runs locally - no API keys or network calls")
    print("  • Small model size: ~120MB")
    print("  • Fast inference: 2x faster than similar models")
    print("  • Good performance: MTEB Retrieval 49.1")
    print("  • Enterprise-friendly license")
    print()

    # Summary
    log_section("Setup Complete")
    log_success("IBM Granite embeddings are working correctly!")
    print()
    log_info("Next steps:")
    print("  1. Review the configuration in config.yaml")
    print("  2. Run the data loader: python load_with_embeddings.py")
    print("  3. Embeddings will be generated for all vector columns")
    print()

    return 0

if __name__ == '__main__':
    sys.exit(main())
