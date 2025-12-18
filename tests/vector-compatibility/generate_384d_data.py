#!/usr/bin/env python3
"""
Generate 384-dimension test vector data using IBM Granite embedding model.

This script creates test data for the standard_vectors table (vector<float, 384>)
using the IBM Granite 30M English embedding model which produces 384-dimensional vectors.

Output files:
- test-data-384d.csv: CSV format for DSBulk CSV connector
- test-data-384d.cql: CQL INSERT statements for direct cqlsh testing
- test-data-384d.json: JSON lines format for DSBulk JSON connector

Requirements:
- sentence-transformers
- torch (CPU version sufficient)

Install: pip install sentence-transformers torch
"""

import json
import uuid
import sys
from pathlib import Path

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("ERROR: sentence-transformers not installed", file=sys.stderr)
    print("Install with: pip install sentence-transformers torch", file=sys.stderr)
    sys.exit(1)


# Test phrases for embedding generation
TEST_PHRASES = [
    "Introduction to Apache Cassandra database",
    "Machine learning with Python tutorial",
    "Cloud native application development",
    "Data modeling best practices",
    "Vector search and similarity"
]

# Fixed UUIDs for reproducibility (deterministic test data)
FIXED_UUIDS = [
    "a1111111-1111-1111-1111-111111111111",
    "b2222222-2222-2222-2222-222222222222",
    "c3333333-3333-3333-3333-333333333333",
    "d4444444-4444-4444-4444-444444444444",
    "e5555555-5555-5555-5555-555555555555"
]

# Model configuration
MODEL_NAME = "ibm-granite/granite-embedding-30m-english"
EXPECTED_DIM = 384


def load_model():
    """Load the IBM Granite embedding model."""
    print(f"Loading model: {MODEL_NAME}...")
    try:
        model = SentenceTransformer(MODEL_NAME)
        print(f"Model loaded successfully")
        return model
    except Exception as e:
        print(f"ERROR: Failed to load model: {e}", file=sys.stderr)
        sys.exit(1)


def generate_embeddings(model, phrases):
    """Generate embeddings for the given phrases."""
    print(f"\nGenerating embeddings for {len(phrases)} phrases...")
    try:
        embeddings = model.encode(phrases, convert_to_numpy=True)

        # Verify dimensions
        actual_dim = embeddings.shape[1]
        print(f"Embedding dimension: {actual_dim}")

        if actual_dim != EXPECTED_DIM:
            print(f"WARNING: Expected {EXPECTED_DIM} dimensions, got {actual_dim}", file=sys.stderr)

        return embeddings
    except Exception as e:
        print(f"ERROR: Failed to generate embeddings: {e}", file=sys.stderr)
        sys.exit(1)


def write_csv(embeddings, output_path):
    """Write CSV file with test data."""
    print(f"\nWriting CSV file: {output_path}")

    with open(output_path, 'w') as f:
        # Header
        f.write("id,name,embedding\n")

        # Data rows
        for i, (uuid_str, phrase) in enumerate(zip(FIXED_UUIDS, TEST_PHRASES)):
            embedding_list = embeddings[i].tolist()
            # Format as JSON array string
            embedding_json = json.dumps(embedding_list)
            f.write(f'{uuid_str},{phrase},"{embedding_json}"\n')

        # NULL vector test case
        f.write(f'{uuid.uuid4()},null_vector_test,\n')

    print(f"CSV file created with {len(TEST_PHRASES) + 1} rows (including NULL test)")


def write_cql(embeddings, output_path):
    """Write CQL INSERT statements."""
    print(f"\nWriting CQL file: {output_path}")

    with open(output_path, 'w') as f:
        f.write("-- CQL INSERT statements for 384-dimension vector test data\n")
        f.write("-- Generated using IBM Granite embedding model\n")
        f.write(f"-- Model: {MODEL_NAME}\n")
        f.write(f"-- Dimension: {EXPECTED_DIM}\n\n")

        f.write("-- Target table: default_keyspace.standard_vectors\n")
        f.write("-- Schema: CREATE TABLE standard_vectors (id uuid PRIMARY KEY, name text, embedding vector<float, 384>);\n\n")

        for i, (uuid_str, phrase) in enumerate(zip(FIXED_UUIDS, TEST_PHRASES)):
            embedding_list = embeddings[i].tolist()
            # Format as CQL vector literal
            vector_str = "[" + ", ".join(str(v) for v in embedding_list) + "]"

            f.write(f"-- Phrase: {phrase}\n")
            f.write(f"INSERT INTO default_keyspace.standard_vectors (id, name, embedding)\n")
            f.write(f"VALUES ({uuid_str}, '{phrase}', {vector_str});\n\n")

        # NULL vector test case
        null_uuid = str(uuid.uuid4())
        f.write(f"-- NULL vector test case\n")
        f.write(f"INSERT INTO default_keyspace.standard_vectors (id, name, embedding)\n")
        f.write(f"VALUES ({null_uuid}, 'null_vector_test', null);\n")

    print(f"CQL file created with {len(TEST_PHRASES) + 1} INSERT statements")


def write_json(embeddings, output_path):
    """Write JSON lines file for DSBulk."""
    print(f"\nWriting JSON file: {output_path}")

    with open(output_path, 'w') as f:
        for i, (uuid_str, phrase) in enumerate(zip(FIXED_UUIDS, TEST_PHRASES)):
            embedding_list = embeddings[i].tolist()

            record = {
                "id": uuid_str,
                "name": phrase,
                "embedding": embedding_list
            }

            f.write(json.dumps(record) + "\n")

        # NULL vector test case
        null_record = {
            "id": str(uuid.uuid4()),
            "name": "null_vector_test",
            "embedding": None
        }
        f.write(json.dumps(null_record) + "\n")

    print(f"JSON file created with {len(TEST_PHRASES) + 1} records (JSON lines format)")


def print_summary(embeddings):
    """Print summary statistics."""
    print("\n" + "="*60)
    print("GENERATION SUMMARY")
    print("="*60)
    print(f"Model: {MODEL_NAME}")
    print(f"Embedding dimension: {embeddings.shape[1]}")
    print(f"Number of test phrases: {len(TEST_PHRASES)}")
    print(f"Total records (including NULL test): {len(TEST_PHRASES) + 1}")
    print("\nTest phrases:")
    for i, phrase in enumerate(TEST_PHRASES, 1):
        print(f"  {i}. {phrase}")
    print("\nOutput files created:")
    print(f"  - test-data-384d.csv (CSV format)")
    print(f"  - test-data-384d.cql (CQL statements)")
    print(f"  - test-data-384d.json (JSON lines)")
    print("="*60)


def main():
    """Main execution function."""
    # Get output directory (same as script location)
    script_dir = Path(__file__).parent

    # Load model and generate embeddings
    model = load_model()
    embeddings = generate_embeddings(model, TEST_PHRASES)

    # Write output files
    write_csv(embeddings, script_dir / "test-data-384d.csv")
    write_cql(embeddings, script_dir / "test-data-384d.cql")
    write_json(embeddings, script_dir / "test-data-384d.json")

    # Print summary
    print_summary(embeddings)

    print("\nSuccess! Test data generation complete.")


if __name__ == "__main__":
    main()
