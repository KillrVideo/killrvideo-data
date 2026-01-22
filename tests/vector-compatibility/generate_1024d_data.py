#!/usr/bin/env python3
"""
Generate 1024-dimension test vector data using BAAI/bge-large-en-v1.5 model.

This script creates test data in multiple formats (CSV, CQL, JSON) for testing
Cassandra vector search capabilities with 1024-dimensional embeddings.

Requirements:
    pip install sentence-transformers
"""

import json
import csv
import sys
from pathlib import Path
from typing import List, Tuple

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("ERROR: sentence-transformers package not found.", file=sys.stderr)
    print("Install with: pip install sentence-transformers", file=sys.stderr)
    sys.exit(1)


# Test phrases for embedding generation
TEST_PHRASES = [
    "Introduction to Apache Cassandra database",
    "Machine learning with Python tutorial",
    "Cloud native application development",
    "Data modeling best practices",
    "Vector search and similarity"
]

# Fixed UUIDs for reproducibility (using 1024xxxx pattern)
FIXED_UUIDS = [
    "10240000-0000-0000-0000-000000000001",
    "10240000-0000-0000-0000-000000000002",
    "10240000-0000-0000-0000-000000000003",
    "10240000-0000-0000-0000-000000000004",
    "10240000-0000-0000-0000-000000000005"
]

# Output directory
OUTPUT_DIR = Path(__file__).parent
CSV_FILE = OUTPUT_DIR / "test-data-1024d.csv"
CQL_FILE = OUTPUT_DIR / "test-data-1024d.cql"
JSON_FILE = OUTPUT_DIR / "test-data-1024d.json"


def load_model() -> SentenceTransformer:
    """Load the bge-large model."""
    print("Loading BAAI/bge-large-en-v1.5 model...")
    model = SentenceTransformer('BAAI/bge-large-en-v1.5')
    print("Model loaded successfully.")
    return model


def generate_embeddings(model: SentenceTransformer, phrases: List[str]) -> List[List[float]]:
    """Generate embeddings for the given phrases."""
    print(f"\nGenerating embeddings for {len(phrases)} phrases...")
    embeddings = model.encode(phrases, normalize_embeddings=True)

    # Convert to list of lists for easier handling
    embeddings_list = [embedding.tolist() for embedding in embeddings]

    # Verify dimension
    if embeddings_list:
        dimension = len(embeddings_list[0])
        print(f"Embedding dimension: {dimension}")
        if dimension != 1024:
            print(f"WARNING: Expected 1024 dimensions but got {dimension}", file=sys.stderr)

    return embeddings_list


def write_csv(data: List[Tuple[str, str, List[float]]]) -> None:
    """Write data to CSV file."""
    print(f"\nWriting CSV file: {CSV_FILE}")

    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Write header
        writer.writerow(['id', 'name', 'embedding'])

        # Write data rows
        for uuid, name, embedding in data:
            # Convert embedding to JSON array string
            embedding_str = json.dumps(embedding)
            writer.writerow([uuid, name, embedding_str])

        # Add NULL vector test case
        writer.writerow(['10240000-0000-0000-0000-000000000999', 'NULL vector test', ''])

    print(f"CSV file written successfully ({len(data) + 1} rows including NULL test)")


def write_cql(data: List[Tuple[str, str, List[float]]]) -> None:
    """Write CQL INSERT statements."""
    print(f"\nWriting CQL file: {CQL_FILE}")

    with open(CQL_FILE, 'w', encoding='utf-8') as f:
        # Write header comments
        f.write("-- CQL INSERT statements for 1024-dimension vector test data\n")
        f.write("-- Generated using BAAI/bge-large-en-v1.5 model\n")
        f.write("-- \n")
        f.write("-- Usage: cqlsh -f test-data-1024d.cql\n")
        f.write("-- \n")
        f.write("-- Schema:\n")
        f.write("-- CREATE TABLE IF NOT EXISTS default_keyspace.vectors_1024 (\n")
        f.write("--     id uuid PRIMARY KEY,\n")
        f.write("--     name text,\n")
        f.write("--     embedding vector<float, 1024>\n")
        f.write("-- );\n\n")

        # Write INSERT statements
        for uuid, name, embedding in data:
            # Format vector as CQL vector literal
            vector_str = '[' + ', '.join(str(v) for v in embedding) + ']'

            f.write(f"INSERT INTO default_keyspace.vectors_1024 (id, name, embedding)\n")
            f.write(f"VALUES ({uuid}, '{name}', {vector_str});\n\n")

        # Add NULL vector test case
        f.write("-- NULL vector test case\n")
        f.write("INSERT INTO default_keyspace.vectors_1024 (id, name, embedding)\n")
        f.write("VALUES (10240000-0000-0000-0000-000000000999, 'NULL vector test', NULL);\n")

    print(f"CQL file written successfully ({len(data) + 1} INSERT statements)")


def write_json(data: List[Tuple[str, str, List[float]]]) -> None:
    """Write JSON lines format for DSBulk."""
    print(f"\nWriting JSON file: {JSON_FILE}")

    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        # Write data rows (one JSON object per line)
        for uuid, name, embedding in data:
            json_obj = {
                'id': uuid,
                'name': name,
                'embedding': embedding
            }
            f.write(json.dumps(json_obj) + '\n')

        # Add NULL vector test case
        null_obj = {
            'id': '10240000-0000-0000-0000-000000000999',
            'name': 'NULL vector test',
            'embedding': None
        }
        f.write(json.dumps(null_obj) + '\n')

    print(f"JSON file written successfully ({len(data) + 1} records)")


def main():
    """Main execution function."""
    try:
        print("=" * 70)
        print("1024-Dimension Vector Test Data Generator")
        print("Model: BAAI/bge-large-en-v1.5")
        print("=" * 70)

        # Load model
        model = load_model()

        # Generate embeddings
        embeddings = generate_embeddings(model, TEST_PHRASES)

        # Combine data
        data = list(zip(FIXED_UUIDS, TEST_PHRASES, embeddings))

        # Write output files
        write_csv(data)
        write_cql(data)
        write_json(data)

        print("\n" + "=" * 70)
        print("SUCCESS: All files generated successfully")
        print("=" * 70)
        print(f"\nGenerated files:")
        print(f"  - {CSV_FILE}")
        print(f"  - {CQL_FILE}")
        print(f"  - {JSON_FILE}")
        print(f"\nTest data summary:")
        print(f"  - {len(TEST_PHRASES)} test phrases")
        print(f"  - 1 NULL vector test case")
        print(f"  - 1024-dimensional embeddings")
        print(f"\nTo load into Cassandra:")
        print(f"  cqlsh -f {CQL_FILE}")

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
