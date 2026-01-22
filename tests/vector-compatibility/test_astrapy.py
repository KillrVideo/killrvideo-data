#!/usr/bin/env python3
"""
Comprehensive AstraPy Vector Compatibility Test Script

Tests vector operations via astrapy (DataStax Python SDK) against Astra DB.
Tests both Tables API and Collections API with various vector dimensions and edge cases.

Requirements:
    - astrapy>=1.0.0
    - numpy

Environment Variables:
    - ASTRA_DB_API_ENDPOINT: Astra DB API endpoint (required)
    - ASTRA_DB_APPLICATION_TOKEN: Astra DB application token (required)

Usage:
    python test_astrapy.py [--keyspace KEYSPACE] [--skip-tables] [--skip-collections]

    Options:
        --keyspace KEYSPACE     Target keyspace (default: default_keyspace)
        --skip-tables           Skip Tables API tests
        --skip-collections      Skip Collections API tests
        --verbose              Show detailed output for passing tests
"""

import os
import sys
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import traceback

# Check for required dependencies
try:
    from astrapy import DataAPIClient
except ImportError:
    print("ERROR: astrapy not installed", file=sys.stderr)
    print("Install with: pip install astrapy", file=sys.stderr)
    sys.exit(1)

try:
    import numpy as np
except ImportError:
    print("ERROR: numpy not installed", file=sys.stderr)
    print("Install with: pip install numpy", file=sys.stderr)
    sys.exit(1)


# Test configuration
VECTOR_DIMENSIONS = [384, 768, 1024]
TEST_TABLE_PREFIX = "test_vectors_"
TEST_COLLECTION_NAME = "test_vector_collection"


class TestResult:
    """Represents a single test result."""

    def __init__(self, name: str, passed: bool, error: Optional[str] = None,
                 expected_fail: bool = False, details: Optional[str] = None):
        self.name = name
        self.passed = passed
        self.error = error
        self.expected_fail = expected_fail
        self.details = details
        self.timestamp = datetime.now()

    def __repr__(self):
        status = "PASS" if self.passed else "FAIL"
        if self.expected_fail and not self.passed:
            status = "EXPECTED FAIL"
        return f"[{status}] {self.name}"


class TestSuite:
    """Manages test execution and results."""

    def __init__(self, verbose: bool = False):
        self.results: List[TestResult] = []
        self.verbose = verbose

    def add_result(self, result: TestResult):
        """Add a test result and print it."""
        self.results.append(result)
        self._print_result(result)

    def _print_result(self, result: TestResult):
        """Print a test result with appropriate formatting."""
        if result.passed:
            if result.expected_fail:
                print(f"  UNEXPECTED PASS: {result.name}")
                if result.details and self.verbose:
                    print(f"    Details: {result.details}")
            else:
                if self.verbose:
                    print(f"  PASS: {result.name}")
                    if result.details:
                        print(f"    Details: {result.details}")
                else:
                    print(".", end="", flush=True)
        else:
            if result.expected_fail:
                print(f"  EXPECTED FAIL: {result.name}")
                if self.verbose and result.error:
                    print(f"    Error: {result.error}")
            else:
                print(f"\n  FAIL: {result.name}")
                if result.error:
                    print(f"    Error: {result.error}")

    def run_test(self, name: str, test_func, expected_fail: bool = False):
        """Run a single test and record the result."""
        try:
            details = test_func()
            result = TestResult(name, True, details=details, expected_fail=expected_fail)
        except Exception as e:
            error_msg = str(e)
            if self.verbose:
                error_msg = traceback.format_exc()
            result = TestResult(name, False, error_msg, expected_fail=expected_fail)

        self.add_result(result)
        return result.passed

    def print_summary(self):
        """Print a summary of all test results."""
        print("\n\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)

        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed and not r.expected_fail)
        failed = sum(1 for r in self.results if not r.passed and not r.expected_fail)
        expected_fails = sum(1 for r in self.results if r.expected_fail and not r.passed)
        unexpected_passes = sum(1 for r in self.results if r.expected_fail and r.passed)

        print(f"Total tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Expected failures: {expected_fails}")

        if unexpected_passes > 0:
            print(f"Unexpected passes: {unexpected_passes} (investigate!)")

        # List failures
        if failed > 0:
            print("\nFailed tests:")
            for result in self.results:
                if not result.passed and not result.expected_fail:
                    print(f"  - {result.name}")
                    if result.error:
                        print(f"    {result.error[:200]}")

        # List unexpected passes
        if unexpected_passes > 0:
            print("\nUnexpected passes (tests expected to fail but passed):")
            for result in self.results:
                if result.passed and result.expected_fail:
                    print(f"  - {result.name}")

        print("="*70)

        # Return exit code
        return 0 if failed == 0 and unexpected_passes == 0 else 1


def create_test_vector(dimension: int, seed: int = 42) -> List[float]:
    """Create a test vector with the specified dimension."""
    np.random.seed(seed)
    vector = np.random.randn(dimension).astype(np.float32)
    # Normalize to unit length for better COSINE similarity results
    vector = vector / np.linalg.norm(vector)
    return vector.tolist()


def create_edge_case_vectors(dimension: int) -> Dict[str, List[float]]:
    """Create edge case test vectors."""
    vectors = {}

    # All zeros
    vectors['zeros'] = [0.0] * dimension

    # All ones
    vectors['ones'] = [1.0] * dimension

    # High precision values
    vectors['high_precision'] = [1.123456789123456789] * dimension

    # Very small values
    vectors['very_small'] = [1e-38] * dimension

    # Very large values
    vectors['very_large'] = [1e38] * dimension

    # Mixed range
    mixed = []
    for i in range(dimension):
        if i % 4 == 0:
            mixed.append(1e-38)
        elif i % 4 == 1:
            mixed.append(1e38)
        elif i % 4 == 2:
            mixed.append(0.0)
        else:
            mixed.append(1.0)
    vectors['mixed_range'] = mixed

    return vectors


class AstraPyVectorTester:
    """Test harness for astrapy vector operations."""

    def __init__(self, api_endpoint: str, token: str, keyspace: str, verbose: bool = False):
        self.api_endpoint = api_endpoint
        self.token = token
        self.keyspace = keyspace
        self.verbose = verbose
        self.suite = TestSuite(verbose=verbose)

        # Initialize client
        print(f"Connecting to Astra DB...")
        print(f"  Endpoint: {api_endpoint}")
        print(f"  Keyspace: {keyspace}")

        self.client = DataAPIClient(token)
        self.db = self.client.get_database(api_endpoint, keyspace=keyspace)

        print(f"Connected successfully!\n")

    def cleanup_test_tables(self):
        """Drop test tables before starting."""
        print("Cleaning up test tables...")
        for dim in VECTOR_DIMENSIONS:
            table_name = f"{TEST_TABLE_PREFIX}{dim}"
            try:
                self.db.drop_table(table_name)
                print(f"  Dropped table: {table_name}")
            except Exception as e:
                if "does not exist" not in str(e).lower():
                    print(f"  Warning: Could not drop {table_name}: {e}")
        print()

    def cleanup_test_collections(self):
        """Drop test collections before starting."""
        print("Cleaning up test collections...")
        try:
            self.db.drop_collection(TEST_COLLECTION_NAME)
            print(f"  Dropped collection: {TEST_COLLECTION_NAME}")
        except Exception as e:
            if "does not exist" not in str(e).lower():
                print(f"  Warning: Could not drop {TEST_COLLECTION_NAME}: {e}")
        print()

    def create_test_tables(self):
        """Create test tables for vector dimensions."""
        print("Creating test tables...")
        for dim in VECTOR_DIMENSIONS:
            table_name = f"{TEST_TABLE_PREFIX}{dim}"

            def test_func():
                table = self.db.create_table(
                    table_name,
                    definition={
                        "columns": {
                            "id": {"type": "uuid"},
                            "name": {"type": "text"},
                            "embedding": {"type": f"vector", "dimension": dim}
                        },
                        "primaryKey": "id"
                    }
                )
                return f"Created table with vector<float, {dim}>"

            self.suite.run_test(f"Create table {table_name}", test_func)
        print()

    def test_tables_api(self):
        """Run all Tables API tests."""
        print("\n" + "="*70)
        print("TABLES API TESTS")
        print("="*70 + "\n")

        for dim in VECTOR_DIMENSIONS:
            print(f"\nTesting dimension: {dim}")
            print("-" * 40)
            self._test_table_dimension(dim)

    def _test_table_dimension(self, dimension: int):
        """Test a specific vector dimension with Tables API."""
        table_name = f"{TEST_TABLE_PREFIX}{dimension}"
        table = self.db.get_table(table_name)

        # Generate test vectors
        test_vector = create_test_vector(dimension)
        edge_vectors = create_edge_case_vectors(dimension)

        # Test 1: Insert with Python list
        def test_list_insert():
            import uuid
            doc_id = uuid.uuid4()
            table.insert_one({
                "id": doc_id,
                "name": "python_list_test",
                "embedding": test_vector
            })
            # Verify
            result = table.find_one({"id": doc_id})
            assert result is not None, "Document not found"
            assert result["name"] == "python_list_test"
            return f"Inserted and verified with list of {len(test_vector)} floats"

        self.suite.run_test(f"Insert with Python list (dim={dimension})", test_list_insert)

        # Test 2: Insert with numpy array
        def test_numpy_insert():
            import uuid
            doc_id = uuid.uuid4()
            np_vector = np.array(test_vector, dtype=np.float32)
            table.insert_one({
                "id": doc_id,
                "name": "numpy_array_test",
                "embedding": np_vector
            })
            result = table.find_one({"id": doc_id})
            assert result is not None
            return f"Inserted numpy array shape {np_vector.shape}"

        self.suite.run_test(f"Insert with numpy array (dim={dimension})", test_numpy_insert)

        # Test 3: Insert with numpy .tolist()
        def test_numpy_tolist():
            import uuid
            doc_id = uuid.uuid4()
            np_vector = np.array(test_vector, dtype=np.float32)
            table.insert_one({
                "id": doc_id,
                "name": "numpy_tolist_test",
                "embedding": np_vector.tolist()
            })
            result = table.find_one({"id": doc_id})
            assert result is not None
            return "Inserted numpy array converted to list"

        self.suite.run_test(f"Insert with numpy.tolist() (dim={dimension})", test_numpy_tolist)

        # Test 4: Insert with tuple
        def test_tuple_insert():
            import uuid
            doc_id = uuid.uuid4()
            table.insert_one({
                "id": doc_id,
                "name": "tuple_test",
                "embedding": tuple(test_vector)
            })
            result = table.find_one({"id": doc_id})
            assert result is not None
            return f"Inserted tuple of {len(test_vector)} floats"

        self.suite.run_test(f"Insert with tuple (dim={dimension})", test_tuple_insert)

        # Test 5: Insert with JSON string (should fail)
        def test_json_string_insert():
            import uuid
            doc_id = uuid.uuid4()
            json_string = json.dumps(test_vector)
            table.insert_one({
                "id": doc_id,
                "name": "json_string_test",
                "embedding": json_string
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            f"Insert with JSON string (dim={dimension})",
            test_json_string_insert,
            expected_fail=True
        )

        # Test 6: Insert with None
        def test_none_insert():
            import uuid
            doc_id = uuid.uuid4()
            table.insert_one({
                "id": doc_id,
                "name": "none_test",
                "embedding": None
            })
            result = table.find_one({"id": doc_id})
            assert result is not None
            return "Inserted NULL vector successfully"

        self.suite.run_test(f"Insert with None/NULL (dim={dimension})", test_none_insert)

        # Test 7: Insert with empty list (should fail)
        def test_empty_list():
            import uuid
            doc_id = uuid.uuid4()
            table.insert_one({
                "id": doc_id,
                "name": "empty_list_test",
                "embedding": []
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            f"Insert with empty list (dim={dimension})",
            test_empty_list,
            expected_fail=True
        )

        # Test 8: Insert with wrong dimension (should fail)
        def test_wrong_dimension():
            import uuid
            doc_id = uuid.uuid4()
            wrong_vector = [1.0, 2.0, 3.0]  # Only 3 dimensions
            table.insert_one({
                "id": doc_id,
                "name": "wrong_dimension_test",
                "embedding": wrong_vector
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            f"Insert with wrong dimension (dim={dimension})",
            test_wrong_dimension,
            expected_fail=True
        )

        # Test 9: Batch insert with insert_many
        def test_batch_insert():
            import uuid
            docs = []
            for i in range(5):
                docs.append({
                    "id": uuid.uuid4(),
                    "name": f"batch_test_{i}",
                    "embedding": create_test_vector(dimension, seed=i)
                })
            result = table.insert_many(docs)
            return f"Batch inserted {len(docs)} documents"

        self.suite.run_test(f"Batch insert with insert_many (dim={dimension})", test_batch_insert)

        # Test 10: High precision floats
        def test_high_precision():
            import uuid
            doc_id = uuid.uuid4()
            table.insert_one({
                "id": doc_id,
                "name": "high_precision_test",
                "embedding": edge_vectors['high_precision']
            })
            result = table.find_one({"id": doc_id})
            assert result is not None
            return "Inserted high-precision floats"

        self.suite.run_test(f"Insert high precision floats (dim={dimension})", test_high_precision)

        # Test 11: Very small values
        def test_very_small():
            import uuid
            doc_id = uuid.uuid4()
            table.insert_one({
                "id": doc_id,
                "name": "very_small_test",
                "embedding": edge_vectors['very_small']
            })
            result = table.find_one({"id": doc_id})
            assert result is not None
            return "Inserted very small values (1e-38)"

        self.suite.run_test(f"Insert very small values (dim={dimension})", test_very_small)

        # Test 12: Very large values
        def test_very_large():
            import uuid
            doc_id = uuid.uuid4()
            table.insert_one({
                "id": doc_id,
                "name": "very_large_test",
                "embedding": edge_vectors['very_large']
            })
            result = table.find_one({"id": doc_id})
            assert result is not None
            return "Inserted very large values (1e38)"

        self.suite.run_test(f"Insert very large values (dim={dimension})", test_very_large)

        # Test 13: NaN handling (should fail)
        def test_nan():
            import uuid
            doc_id = uuid.uuid4()
            nan_vector = [float('nan')] * dimension
            table.insert_one({
                "id": doc_id,
                "name": "nan_test",
                "embedding": nan_vector
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            f"Insert NaN values (dim={dimension})",
            test_nan,
            expected_fail=True
        )

        # Test 14: Infinity handling (should fail)
        def test_infinity():
            import uuid
            doc_id = uuid.uuid4()
            inf_vector = [float('inf')] * dimension
            table.insert_one({
                "id": doc_id,
                "name": "infinity_test",
                "embedding": inf_vector
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            f"Insert Infinity values (dim={dimension})",
            test_infinity,
            expected_fail=True
        )

        # Test 15: Vector similarity search
        def test_similarity_search():
            import uuid
            # Insert a reference vector
            ref_id = uuid.uuid4()
            ref_vector = create_test_vector(dimension, seed=99)
            table.insert_one({
                "id": ref_id,
                "name": "similarity_reference",
                "embedding": ref_vector
            })

            # Insert similar vectors
            for i in range(3):
                # Add small noise to create similar vectors
                similar = [v + np.random.normal(0, 0.01) for v in ref_vector]
                table.insert_one({
                    "id": uuid.uuid4(),
                    "name": f"similar_{i}",
                    "embedding": similar
                })

            # Perform ANN search
            results = table.find(
                sort={"embedding": ref_vector},
                limit=5
            )
            results_list = list(results)

            assert len(results_list) > 0, "No results from similarity search"
            return f"Found {len(results_list)} similar vectors"

        self.suite.run_test(f"Vector similarity search (dim={dimension})", test_similarity_search)

    def test_collections_api(self):
        """Run all Collections API tests."""
        print("\n" + "="*70)
        print("COLLECTIONS API TESTS")
        print("="*70 + "\n")

        # Create test collection with vector support
        print("Creating test collection with vector support...")

        def create_collection():
            collection = self.db.create_collection(
                TEST_COLLECTION_NAME,
                dimension=384,  # Standard dimension for collections
                metric="cosine"
            )
            return f"Created collection with 384-dimensional vectors"

        self.suite.run_test("Create vector collection", create_collection)

        collection = self.db.get_collection(TEST_COLLECTION_NAME)

        print("\nTesting Collections API operations")
        print("-" * 40)

        # Test 1: Insert with $vector as list
        def test_vector_list():
            from astrapy.ids import ObjectId
            vector = create_test_vector(384)
            result = collection.insert_one({
                "_id": str(ObjectId()),
                "name": "collection_list_test",
                "$vector": vector,
                "metadata": {"type": "test"}
            })
            assert result.inserted_id is not None
            return f"Inserted document with $vector list"

        self.suite.run_test("Collections: Insert with $vector as list", test_vector_list)

        # Test 2: Insert with $vector as numpy array
        def test_vector_numpy():
            from astrapy.ids import ObjectId
            vector = np.array(create_test_vector(384), dtype=np.float32)
            result = collection.insert_one({
                "_id": str(ObjectId()),
                "name": "collection_numpy_test",
                "$vector": vector,
                "metadata": {"type": "test"}
            })
            assert result.inserted_id is not None
            return f"Inserted document with numpy array $vector"

        self.suite.run_test("Collections: Insert with numpy array", test_vector_numpy)

        # Test 3: Insert with $vector as string (should fail)
        def test_vector_string():
            from astrapy.ids import ObjectId
            vector = create_test_vector(384)
            vector_str = json.dumps(vector)
            result = collection.insert_one({
                "_id": str(ObjectId()),
                "name": "collection_string_test",
                "$vector": vector_str,
                "metadata": {"type": "test"}
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            "Collections: Insert with $vector as string",
            test_vector_string,
            expected_fail=True
        )

        # Test 4: Insert without $vector field
        def test_no_vector():
            from astrapy.ids import ObjectId
            result = collection.insert_one({
                "_id": str(ObjectId()),
                "name": "collection_no_vector_test",
                "metadata": {"type": "test"}
            })
            assert result.inserted_id is not None
            return "Inserted document without $vector field"

        self.suite.run_test("Collections: Insert without $vector", test_no_vector)

        # Test 5: Vector search with sort parameter
        def test_vector_search():
            from astrapy.ids import ObjectId
            # Insert reference documents
            ref_vector = create_test_vector(384, seed=42)
            ref_id = str(ObjectId())
            collection.insert_one({
                "_id": ref_id,
                "name": "search_reference",
                "$vector": ref_vector,
                "metadata": {"type": "reference"}
            })

            # Insert similar documents
            for i in range(5):
                similar = [v + np.random.normal(0, 0.01) for v in ref_vector]
                collection.insert_one({
                    "_id": str(ObjectId()),
                    "name": f"search_similar_{i}",
                    "$vector": similar,
                    "metadata": {"type": "similar"}
                })

            # Perform vector search
            results = collection.find(
                sort={"$vector": ref_vector},
                limit=10
            )
            results_list = list(results)

            assert len(results_list) > 0, "No results from vector search"
            return f"Found {len(results_list)} results from vector search"

        self.suite.run_test("Collections: Vector similarity search", test_vector_search)

        # Test 6: Insert with wrong dimension (should fail)
        def test_wrong_dim():
            from astrapy.ids import ObjectId
            wrong_vector = [1.0, 2.0, 3.0]  # Only 3 dimensions
            result = collection.insert_one({
                "_id": str(ObjectId()),
                "name": "wrong_dim_test",
                "$vector": wrong_vector
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            "Collections: Insert with wrong dimension",
            test_wrong_dim,
            expected_fail=True
        )

        # Test 7: Batch insert with insert_many
        def test_batch():
            from astrapy.ids import ObjectId
            docs = []
            for i in range(10):
                docs.append({
                    "_id": str(ObjectId()),
                    "name": f"batch_doc_{i}",
                    "$vector": create_test_vector(384, seed=i+100),
                    "metadata": {"batch": True, "index": i}
                })
            result = collection.insert_many(docs)
            assert len(result.inserted_ids) == 10
            return f"Batch inserted {len(result.inserted_ids)} documents"

        self.suite.run_test("Collections: Batch insert", test_batch)

        # Test 8: NaN in vector (should fail)
        def test_nan_collection():
            from astrapy.ids import ObjectId
            nan_vector = [float('nan')] * 384
            result = collection.insert_one({
                "_id": str(ObjectId()),
                "name": "nan_test",
                "$vector": nan_vector
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            "Collections: Insert NaN values",
            test_nan_collection,
            expected_fail=True
        )

        # Test 9: Infinity in vector (should fail)
        def test_inf_collection():
            from astrapy.ids import ObjectId
            inf_vector = [float('inf')] * 384
            result = collection.insert_one({
                "_id": str(ObjectId()),
                "name": "inf_test",
                "$vector": inf_vector
            })
            return "Should have failed but succeeded"

        self.suite.run_test(
            "Collections: Insert Infinity values",
            test_inf_collection,
            expected_fail=True
        )


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Test astrapy vector compatibility with Astra DB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--keyspace",
        default="default_keyspace",
        help="Target keyspace (default: default_keyspace)"
    )
    parser.add_argument(
        "--skip-tables",
        action="store_true",
        help="Skip Tables API tests"
    )
    parser.add_argument(
        "--skip-collections",
        action="store_true",
        help="Skip Collections API tests"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output for passing tests"
    )

    args = parser.parse_args()

    # Check environment variables
    api_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")
    token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")

    if not api_endpoint:
        print("ERROR: ASTRA_DB_API_ENDPOINT environment variable not set", file=sys.stderr)
        sys.exit(1)

    if not token:
        print("ERROR: ASTRA_DB_APPLICATION_TOKEN environment variable not set", file=sys.stderr)
        sys.exit(1)

    # Print test configuration
    print("="*70)
    print("ASTRAPY VECTOR COMPATIBILITY TEST")
    print("="*70)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Keyspace: {args.keyspace}")
    print(f"Vector dimensions to test: {VECTOR_DIMENSIONS}")
    print(f"Skip Tables API: {args.skip_tables}")
    print(f"Skip Collections API: {args.skip_collections}")
    print(f"Verbose: {args.verbose}")
    print("="*70 + "\n")

    # Initialize tester
    tester = AstraPyVectorTester(api_endpoint, token, args.keyspace, verbose=args.verbose)

    # Run Tests API tests
    if not args.skip_tables:
        tester.cleanup_test_tables()
        tester.create_test_tables()
        tester.test_tables_api()

    # Run Collections API tests
    if not args.skip_collections:
        tester.cleanup_test_collections()
        tester.test_collections_api()

    # Print summary and exit
    exit_code = tester.suite.print_summary()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
