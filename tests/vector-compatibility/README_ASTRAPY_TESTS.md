# AstraPy Vector Compatibility Tests

Comprehensive test suite for validating vector operations using the astrapy (DataStax Python SDK) against Astra DB.

## Overview

The `test_astrapy.py` script tests both the **Tables API** and **Collections API** with various vector dimensions, data types, and edge cases to ensure robust vector support in production applications.

## Features

### Tables API Tests

For each vector dimension (384, 768, 1024):
- Insert with Python list
- Insert with numpy array
- Insert with numpy `.tolist()`
- Insert with tuple
- Insert with JSON string (expected to fail)
- Insert with None/NULL
- Insert with empty list (expected to fail)
- Insert with wrong dimension (expected to fail)
- Batch insert with `insert_many()`
- High precision float values
- Very small values (1e-38)
- Very large values (1e38)
- NaN handling (expected to fail)
- Infinity handling (expected to fail)
- Vector similarity search with ANN

### Collections API Tests

- Insert with `$vector` as list
- Insert with `$vector` as numpy array
- Insert with `$vector` as string (expected to fail)
- Insert without `$vector` field
- Vector search with sort parameter
- Wrong dimension handling (expected to fail)
- Batch insert with `insert_many()`
- NaN handling (expected to fail)
- Infinity handling (expected to fail)

## Prerequisites

### Python Dependencies

```bash
pip install astrapy numpy
```

### Environment Variables

Set these environment variables before running the tests:

```bash
export ASTRA_DB_API_ENDPOINT="https://your-database-id-region.apps.astra.datastax.com"
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:xxxxx..."
```

To get these values:
1. Log into [Astra DB](https://astra.datastax.com)
2. Select your database
3. Go to "Connect" tab
4. Copy the API Endpoint
5. Generate an Application Token (or use existing one)

### Database Setup

The test script will automatically create test tables and collections in the specified keyspace. By default, it uses `default_keyspace`.

If you want to use a different keyspace:
1. Create the keyspace in Astra DB UI
2. Pass it via the `--keyspace` argument

## Usage

### Basic Usage

Run all tests with default settings:

```bash
python test_astrapy.py
```

### Custom Keyspace

```bash
python test_astrapy.py --keyspace my_custom_keyspace
```

### Skip Tables or Collections Tests

```bash
# Skip Tables API tests
python test_astrapy.py --skip-tables

# Skip Collections API tests
python test_astrapy.py --skip-collections

# Run only Collections API tests
python test_astrapy.py --skip-tables
```

### Verbose Output

Show detailed output for all tests (including passing tests):

```bash
python test_astrapy.py --verbose
```

### Complete Example

```bash
export ASTRA_DB_API_ENDPOINT="https://12345678-1234-1234-1234-123456789012-us-east1.apps.astra.datastax.com"
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:AbCdEfGh..."

python test_astrapy.py --keyspace default_keyspace --verbose
```

## Output

### Normal Mode

In normal mode, the script shows:
- Test progress dots (`.`) for passing tests
- Detailed output for failing tests
- Summary at the end

Example:
```
==================================================================
ASTRAPY VECTOR COMPATIBILITY TEST
==================================================================
Timestamp: 2025-12-02T15:30:45.123456
Keyspace: default_keyspace
Vector dimensions to test: [384, 768, 1024]
...

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
..........

[... more tests ...]

==================================================================
TEST SUMMARY
==================================================================
Total tests: 68
Passed: 52
Failed: 0
Expected failures: 16
Unexpected passes: 0
==================================================================
```

### Verbose Mode

Shows all test details including passing tests:

```
  PASS: Insert with Python list (dim=384)
    Details: Inserted and verified with list of 384 floats
  PASS: Insert with numpy array (dim=384)
    Details: Inserted numpy array shape (384,)
  EXPECTED FAIL: Insert with JSON string (dim=384)
    Error: Invalid type for vector field...
```

## Test Results

### Exit Codes

- `0`: All tests passed (failures only in expected-to-fail tests)
- `1`: One or more tests failed unexpectedly

### Expected Failures

Some tests are designed to fail and validate error handling:
- JSON string as vector (should be list/array)
- Empty list
- Wrong dimension
- NaN values
- Infinity values

These show as `EXPECTED FAIL` in the output and don't cause the script to exit with error code 1.

### Unexpected Passes

If a test expected to fail actually passes, it shows as `UNEXPECTED PASS`. This may indicate:
- API behavior changed
- Bug was fixed
- Test needs to be updated

Investigate these cases and update the test expectations if needed.

## Test Tables Created

The script creates temporary tables for testing:

- `test_vectors_384` - 384-dimensional vectors
- `test_vectors_768` - 768-dimensional vectors
- `test_vectors_1024` - 1024-dimensional vectors
- `test_vector_collection` - Collection with 384-dimensional vectors

These are cleaned up and recreated on each run.

## Troubleshooting

### Connection Errors

```
ERROR: ASTRA_DB_API_ENDPOINT environment variable not set
```
**Solution**: Set the required environment variables (see Prerequisites).

### Authentication Errors

```
Error: Unauthorized
```
**Solution**: Verify your application token is valid and has permissions for the keyspace.

### Keyspace Not Found

```
Error: Keyspace 'xyz' does not exist
```
**Solution**: Create the keyspace in Astra DB UI or use `--keyspace default_keyspace`.

### Dimension Mismatch

```
Error: Vector dimension mismatch
```
**Solution**: This is an expected failure for wrong dimension tests. If it happens elsewhere, check your vector dimensions match the table/collection definition.

### Import Errors

```
ERROR: astrapy not installed
```
**Solution**: Install dependencies: `pip install astrapy numpy`

## Integration with CI/CD

### GitHub Actions Example

```yaml
name: AstraPy Vector Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install astrapy numpy

      - name: Run vector tests
        env:
          ASTRA_DB_API_ENDPOINT: ${{ secrets.ASTRA_DB_API_ENDPOINT }}
          ASTRA_DB_APPLICATION_TOKEN: ${{ secrets.ASTRA_DB_APPLICATION_TOKEN }}
        run: |
          cd tests/vector-compatibility
          python test_astrapy.py --keyspace default_keyspace
```

### Makefile Integration

Add to your `Makefile`:

```makefile
.PHONY: test-vectors
test-vectors:
	@echo "Running AstraPy vector compatibility tests..."
	cd tests/vector-compatibility && python test_astrapy.py

.PHONY: test-vectors-verbose
test-vectors-verbose:
	cd tests/vector-compatibility && python test_astrapy.py --verbose
```

## Performance Considerations

### Test Duration

- Tables API tests (3 dimensions × ~15 tests): ~2-3 minutes
- Collections API tests (~9 tests): ~30 seconds
- Total runtime: ~3-4 minutes

### Network Latency

Tests run against Astra DB cloud, so network latency affects duration. For faster testing:
- Run from a region close to your Astra DB
- Use `--skip-tables` or `--skip-collections` to run subset

### Data Cleanup

Test data is automatically cleaned up before each run. Old test tables/collections are dropped.

## Development

### Adding New Tests

1. Add test function in the appropriate section:
   - `_test_table_dimension()` for Tables API
   - `test_collections_api()` for Collections API

2. Use the test runner pattern:
```python
def test_my_feature():
    # Test implementation
    # Return success message string
    return "Feature worked correctly"

self.suite.run_test(
    "My feature test",
    test_my_feature,
    expected_fail=False  # Set to True if should fail
)
```

3. Test failures should raise exceptions (AssertionError or any Exception)

### Test Organization

Tests are organized by:
1. **API Type**: Tables vs Collections
2. **Dimension**: 384, 768, 1024 (Tables only)
3. **Test Type**: Basic inserts, edge cases, similarity search

## Related Files

- `schema.cql` - Schema definitions for vector tables
- `generate_384d_data.py` - Generate test data with real embeddings
- `generate_768d_data.py` - Generate 768d test data
- `generate_1024d_data.py` - Generate 1024d test data
- `test-data-*.csv` - Sample data files (if generated)

## References

- [AstraPy Documentation](https://docs.datastax.com/en/astra-serverless/docs/develop/dev-with-python.html)
- [Astra DB Vector Search](https://docs.datastax.com/en/astra-serverless/docs/vector-search/overview.html)
- [KillrVideo Schema Documentation](../../schema-astra.cql)

## License

Part of the KillrVideo project. See main repository for license details.
