# Sample Test Output

This document shows example output from running `test_astrapy.py`.

## Normal Mode (Default)

```bash
$ python test_astrapy.py
==================================================================
ASTRAPY VECTOR COMPATIBILITY TEST
==================================================================
Timestamp: 2025-12-02T15:30:45.123456
Keyspace: default_keyspace
Vector dimensions to test: [384, 768, 1024]
Skip Tables API: False
Skip Collections API: False
Verbose: False
==================================================================

Connecting to Astra DB...
  Endpoint: https://12345678-1234-1234-1234-123456789012-us-east1.apps.astra.datastax.com
  Keyspace: default_keyspace
Connected successfully!

Cleaning up test tables...
  Dropped table: test_vectors_384
  Dropped table: test_vectors_768
  Dropped table: test_vectors_1024

Creating test tables...
  PASS: Create table test_vectors_384
  PASS: Create table test_vectors_768
  PASS: Create table test_vectors_1024


==================================================================
TABLES API TESTS
==================================================================

Testing dimension: 384
----------------------------------------
..........
  EXPECTED FAIL: Insert with JSON string (dim=384)
.
  EXPECTED FAIL: Insert with empty list (dim=384)
  EXPECTED FAIL: Insert with wrong dimension (dim=384)
....
  EXPECTED FAIL: Insert NaN values (dim=384)
  EXPECTED FAIL: Insert Infinity values (dim=384)
.

Testing dimension: 768
----------------------------------------
..........
  EXPECTED FAIL: Insert with JSON string (dim=768)
.
  EXPECTED FAIL: Insert with empty list (dim=768)
  EXPECTED FAIL: Insert with wrong dimension (dim=768)
....
  EXPECTED FAIL: Insert NaN values (dim=768)
  EXPECTED FAIL: Insert Infinity values (dim=768)
.

Testing dimension: 1024
----------------------------------------
..........
  EXPECTED FAIL: Insert with JSON string (dim=1024)
.
  EXPECTED FAIL: Insert with empty list (dim=1024)
  EXPECTED FAIL: Insert with wrong dimension (dim=1024)
....
  EXPECTED FAIL: Insert NaN values (dim=1024)
  EXPECTED FAIL: Insert Infinity values (dim=1024)
.

==================================================================
COLLECTIONS API TESTS
==================================================================

Creating test collection with vector support...
  PASS: Create vector collection

Testing Collections API operations
----------------------------------------
..
  EXPECTED FAIL: Collections: Insert with $vector as string
.
  EXPECTED FAIL: Collections: Insert with wrong dimension
.
  EXPECTED FAIL: Collections: Insert NaN values
  EXPECTED FAIL: Collections: Insert Infinity values


==================================================================
TEST SUMMARY
==================================================================
Total tests: 58
Passed: 40
Failed: 0
Expected failures: 18
Unexpected passes: 0
==================================================================
```

## Verbose Mode

```bash
$ python test_astrapy.py --verbose
==================================================================
ASTRAPY VECTOR COMPATIBILITY TEST
==================================================================
Timestamp: 2025-12-02T15:30:45.123456
Keyspace: default_keyspace
Vector dimensions to test: [384, 768, 1024]
Skip Tables API: False
Skip Collections API: False
Verbose: True
==================================================================

Connecting to Astra DB...
  Endpoint: https://12345678-1234-1234-1234-123456789012-us-east1.apps.astra.datastax.com
  Keyspace: default_keyspace
Connected successfully!

Cleaning up test tables...
  Dropped table: test_vectors_384
  Dropped table: test_vectors_768
  Dropped table: test_vectors_1024

Creating test tables...
  PASS: Create table test_vectors_384
    Details: Created table with vector<float, 384>
  PASS: Create table test_vectors_768
    Details: Created table with vector<float, 768>
  PASS: Create table test_vectors_1024
    Details: Created table with vector<float, 1024>


==================================================================
TABLES API TESTS
==================================================================

Testing dimension: 384
----------------------------------------
  PASS: Insert with Python list (dim=384)
    Details: Inserted and verified with list of 384 floats
  PASS: Insert with numpy array (dim=384)
    Details: Inserted numpy array shape (384,)
  PASS: Insert with numpy.tolist() (dim=384)
    Details: Inserted numpy array converted to list
  PASS: Insert with tuple (dim=384)
    Details: Inserted tuple of 384 floats
  EXPECTED FAIL: Insert with JSON string (dim=384)
    Error: TypeError: Vector field requires list or array, got str
  PASS: Insert with None/NULL (dim=384)
    Details: Inserted NULL vector successfully
  EXPECTED FAIL: Insert with empty list (dim=384)
    Error: ValueError: Vector dimension mismatch: expected 384, got 0
  EXPECTED FAIL: Insert with wrong dimension (dim=384)
    Error: ValueError: Vector dimension mismatch: expected 384, got 3
  PASS: Batch insert with insert_many (dim=384)
    Details: Batch inserted 5 documents
  PASS: Insert high precision floats (dim=384)
    Details: Inserted high-precision floats
  PASS: Insert very small values (dim=384)
    Details: Inserted very small values (1e-38)
  PASS: Insert very large values (dim=384)
    Details: Inserted very large values (1e38)
  EXPECTED FAIL: Insert NaN values (dim=384)
    Error: ValueError: Vector contains NaN or Inf values
  EXPECTED FAIL: Insert Infinity values (dim=384)
    Error: ValueError: Vector contains NaN or Inf values
  PASS: Vector similarity search (dim=384)
    Details: Found 5 similar vectors

[... similar output for 768d and 1024d ...]

==================================================================
COLLECTIONS API TESTS
==================================================================

Creating test collection with vector support...
  PASS: Create vector collection
    Details: Created collection with 384-dimensional vectors

Testing Collections API operations
----------------------------------------
  PASS: Collections: Insert with $vector as list
    Details: Inserted document with $vector list
  PASS: Collections: Insert with numpy array
    Details: Inserted document with numpy array $vector
  EXPECTED FAIL: Collections: Insert with $vector as string
    Error: TypeError: $vector field requires list or array, got str
  PASS: Collections: Insert without $vector
    Details: Inserted document without $vector field
  PASS: Collections: Vector similarity search
    Details: Found 10 results from vector search
  EXPECTED FAIL: Collections: Insert with wrong dimension
    Error: ValueError: Vector dimension mismatch: expected 384, got 3
  PASS: Collections: Batch insert
    Details: Batch inserted 10 documents
  EXPECTED FAIL: Collections: Insert NaN values
    Error: ValueError: Vector contains NaN or Inf values
  EXPECTED FAIL: Collections: Insert Infinity values
    Error: ValueError: Vector contains NaN or Inf values


==================================================================
TEST SUMMARY
==================================================================
Total tests: 58
Passed: 40
Failed: 0
Expected failures: 18
Unexpected passes: 0
==================================================================
```

## Skip Tables API

```bash
$ python test_astrapy.py --skip-tables
==================================================================
ASTRAPY VECTOR COMPATIBILITY TEST
==================================================================
Timestamp: 2025-12-02T15:30:45.123456
Keyspace: default_keyspace
Vector dimensions to test: [384, 768, 1024]
Skip Tables API: True
Skip Collections API: False
Verbose: False
==================================================================

Connecting to Astra DB...
  Endpoint: https://...
  Keyspace: default_keyspace
Connected successfully!

Cleaning up test collections...
  Dropped collection: test_vector_collection

==================================================================
COLLECTIONS API TESTS
==================================================================

Creating test collection with vector support...
  PASS: Create vector collection

Testing Collections API operations
----------------------------------------
..
  EXPECTED FAIL: Collections: Insert with $vector as string
.
  EXPECTED FAIL: Collections: Insert with wrong dimension
.
  EXPECTED FAIL: Collections: Insert NaN values
  EXPECTED FAIL: Collections: Insert Infinity values


==================================================================
TEST SUMMARY
==================================================================
Total tests: 10
Passed: 7
Failed: 0
Expected failures: 3
Unexpected passes: 0
==================================================================
```

## Test Failure Example

```bash
$ python test_astrapy.py
[... normal output ...]

Testing dimension: 384
----------------------------------------
..........
  FAIL: Batch insert with insert_many (dim=384)
    Error: HTTPError: 503 Service Unavailable

[... rest of output ...]

==================================================================
TEST SUMMARY
==================================================================
Total tests: 58
Passed: 39
Failed: 1
Expected failures: 18
Unexpected passes: 0

Failed tests:
  - Batch insert with insert_many (dim=384)
    HTTPError: 503 Service Unavailable
==================================================================

$ echo $?
1
```

## Missing Environment Variable

```bash
$ python test_astrapy.py
ERROR: ASTRA_DB_API_ENDPOINT environment variable not set

$ echo $?
1
```

## Help Output

```bash
$ python test_astrapy.py --help
usage: test_astrapy.py [-h] [--keyspace KEYSPACE] [--skip-tables]
                       [--skip-collections] [--verbose]

Test astrapy vector compatibility with Astra DB

options:
  -h, --help           show this help message and exit
  --keyspace KEYSPACE  Target keyspace (default: default_keyspace)
  --skip-tables        Skip Tables API tests
  --skip-collections   Skip Collections API tests
  --verbose            Show detailed output for passing tests

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
```

## Unexpected Pass Example

If a test expected to fail actually passes (e.g., if API behavior changes):

```bash
$ python test_astrapy.py
[... normal output ...]

  UNEXPECTED PASS: Insert with JSON string (dim=384)
    Details: Inserted successfully (but was expected to fail)

[... rest of output ...]

==================================================================
TEST SUMMARY
==================================================================
Total tests: 58
Passed: 40
Failed: 0
Expected failures: 17
Unexpected passes: 1

Unexpected passes (tests expected to fail but passed):
  - Insert with JSON string (dim=384)
==================================================================

$ echo $?
1
```

Note: The script returns exit code 1 for unexpected passes, indicating that test expectations need to be updated.

## Real-World Timing

Actual execution times on different network conditions:

### Low Latency (same region as Astra DB)
```
Total runtime: 2m 45s
- Infrastructure: 8s
- Tables API (384d): 35s
- Tables API (768d): 35s
- Tables API (1024d): 35s
- Collections API: 22s
```

### High Latency (different continent)
```
Total runtime: 4m 30s
- Infrastructure: 15s
- Tables API (384d): 60s
- Tables API (768d): 60s
- Tables API (1024d): 60s
- Collections API: 35s
```

### With Verbose Output
```
Total runtime: +10% (due to additional logging)
```

## Exit Codes

| Exit Code | Meaning |
|-----------|---------|
| 0 | All tests passed (or only expected failures) |
| 1 | One or more tests failed unexpectedly OR unexpected passes occurred |
| 1 | Missing environment variables |
| 1 | Missing dependencies (astrapy, numpy) |
| 1 | Connection/authentication errors |
