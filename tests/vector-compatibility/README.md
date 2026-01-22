# Vector Compatibility Testing

This directory contains comprehensive test suites for validating vector data loading and querying across different tools and dimensions in Apache Cassandra 5.0+ and DataStax Astra DB.

## Overview

The test suite validates vector operations across multiple dimensions (8D, 384D, 768D, 1024D) and different data loading tools:
- **cqlsh**: CQL shell for direct INSERT statements
- **DSBulk**: DataStax Bulk Loader for CSV/JSON file imports

## Prerequisites

### Software Requirements

1. **DSBulk 1.11.0+** (for vector support)
   - Download from: https://downloads.datastax.com/#bulk-loader
   - Extract and add to PATH
   - Verify: `dsbulk --version`

2. **cqlsh** (any version supporting Cassandra 5.0+)
   - Included with Cassandra installations
   - Astra CLI includes cqlsh support

3. **Python 3.8+** (for test data generation)
   - Required packages: `sentence-transformers`, `torch`
   - Install: `pip install sentence-transformers torch`

### Astra DB Requirements

- Active Astra DB database
- Application token with read/write permissions
- Secure connect bundle (.zip file)
- Database must have `default_keyspace` created

## Directory Structure

```
vector-compatibility/
├── README.md                    # This file
├── schema.cql                   # Table definitions for all test tables
├── test_dsbulk.sh              # DSBulk test suite (CSV/JSON formats)
├── test-data-8d.csv            # Sample 8D vector data
├── generate_384d_data.py       # Generate 384D test data (IBM Granite)
├── generate_768d_data.py       # Generate 768D test data (all-mpnet-base-v2)
└── generate_1024d_data.py      # Generate 1024D test data (BGE Large)
```

## Schema Setup

### 1. Load Test Schema

```bash
# For Astra DB (requires cqlsh with -b flag)
cqlsh -b /path/to/secure-connect-bundle.zip \
      -u token \
      -p "AstraCS:xxxxx" \
      -f schema.cql

# For local Cassandra 5.0+
cqlsh -f schema.cql
```

### 2. Verify Tables Created

```bash
cqlsh> DESCRIBE KEYSPACE default_keyspace;
```

Expected tables:
- `test_vectors_8d` - 8-dimensional vectors (quick testing)
- `vectors_384` - 384-dimensional vectors (IBM Granite 30M)
- `vectors_768` - 768-dimensional vectors (all-mpnet-base-v2)
- `vectors_1024` - 1024-dimensional vectors (BGE Large EN v1.5)

## Test Data Generation

### Generate Embedding-Based Test Data

Each generator creates three output formats:
1. **CSV** - For DSBulk CSV connector
2. **CQL** - For direct cqlsh execution
3. **JSON** - For DSBulk JSON connector

```bash
# Generate 384D vectors using IBM Granite 30M
./generate_384d_data.py
# Output: test-data-384d.{csv,cql,json}

# Generate 768D vectors using all-mpnet-base-v2
./generate_768d_data.py
# Output: test-data-768d.{csv,cql,json}

# Generate 1024D vectors using BGE Large EN v1.5
./generate_1024d_data.py
# Output: test-data-1024d.{csv,cql,json}
```

### Load Sample Data

```bash
# Load via cqlsh (CQL INSERT statements)
cqlsh -f test-data-384d.cql

# Load via DSBulk (CSV format)
dsbulk load \
  -url test-data-384d.csv \
  -k default_keyspace \
  -t vectors_384 \
  -b /path/to/secure-connect-bundle.zip \
  -u token \
  -p "$ASTRA_TOKEN" \
  -header true
```

## Running Tests

### DSBulk Test Suite

The DSBulk test suite (`test_dsbulk.sh`) validates various data format variations:

**CSV Format Tests:**
- JSON arrays with/without quotes
- Scientific notation in vectors
- Integer values in vectors
- NULL vectors (empty string vs literal "null")
- Spaces in array formatting
- Negative values

**JSON Format Tests:**
- Native JSON arrays
- Arrays as strings (expected to fail)
- NULL values

**Usage:**

```bash
# Basic usage
./test_dsbulk.sh /path/to/secure-connect-bundle.zip "AstraCS:xxxxx"

# Using environment variable for token
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:xxxxx"
./test_dsbulk.sh /path/to/secure-connect-bundle.zip

# View help
./test_dsbulk.sh
```

**Test Coverage:**

The script runs **24 tests** across all dimensions:
- 8 tests for 8D vectors (all format variations)
- 3 tests each for 384D, 768D, 1024D vectors
- 4 JSON format tests

**Exit Codes:**
- `0` - All tests passed
- `1` - One or more tests failed
- `2` - Missing requirements or invalid arguments

**Output:**

The script provides color-coded output:
- **GREEN [PASS]** - Test succeeded
- **RED [FAIL]** - Test failed
- **YELLOW [SKIP]** - Test skipped
- **BLUE [INFO]** - Informational message

Example output:
```
================================================
DSBulk Vector Compatibility Test Suite
================================================
Target: Astra DB
Bundle: /path/to/secure-connect-bundle.zip

================================================
Checking Prerequisites
================================================
[INFO] DSBulk version: 1.11.0
[PASS] DSBulk version check (1.11.0 >= 1.11.0)
[PASS] Secure connect bundle: /path/to/secure-connect-bundle.zip
[PASS] Token configured

================================================
CSV Format Tests - 8 Dimensions
================================================

>>> Test 1: 8D: JSON array with quotes
[PASS] 8D: JSON array with quotes

>>> Test 2: 8D: JSON array without quotes
[PASS] 8D: JSON array without quotes

...

================================================
Test Results Summary
================================================

Total Tests:   24
Passed:        23
Failed:        1
Skipped:       0

All tests passed!

Logs saved to: /tmp/tmp.xxxxx
```

## Test Tables Schema Reference

### test_vectors_8d
- **Dimension:** 8
- **Use Case:** Quick format testing and rapid iteration
- **Column:** `vec vector<float, 8>`

### vectors_384
- **Dimension:** 384
- **Model:** IBM Granite 30M (granite-embedding-30m-english)
- **Use Case:** Lightweight embeddings for resource-constrained environments
- **Column:** `embedding vector<float, 384>`
- **Note:** Current production dimension for KillrVideo

### vectors_768
- **Dimension:** 768
- **Model:** sentence-transformers/all-mpnet-base-v2
- **Use Case:** High-quality sentence embeddings for semantic search
- **Column:** `embedding vector<float, 768>`

### vectors_1024
- **Dimension:** 1024
- **Model:** BAAI/bge-large-en-v1.5
- **Use Case:** State-of-the-art semantic search and retrieval
- **Column:** `embedding vector<float, 1024>`

## Troubleshooting

### DSBulk Version Check Fails

**Error:** `DSBulk 1.11.0+ required for vector support`

**Solution:**
- Download DSBulk 1.11.0 or higher from DataStax
- Update PATH to point to new version
- Run `dsbulk --version` to verify

### Authentication Errors

**Error:** `Authentication failed`

**Solution:**
- Verify token is valid and not expired
- Ensure token has read/write permissions
- Check token format: `AstraCS:...`
- Verify secure connect bundle path is correct

### Table Not Found

**Error:** `table ... does not exist`

**Solution:**
- Run `schema.cql` to create tables
- Verify keyspace name is `default_keyspace`
- Check table names match schema (e.g., `test_vectors_8d`, not `small_vectors`)

### Vector Dimension Mismatch

**Error:** `Vector dimension mismatch`

**Solution:**
- Verify CSV/JSON data matches table dimension
- Check vector array has correct number of elements
- Ensure no trailing commas in vector arrays

### JSON Format Fails

**Error:** `Failed to parse JSON`

**Solution:**
- Use native JSON arrays: `[1.0, 2.0, 3.0]`
- Do NOT use quoted strings: `"[1.0, 2.0, 3.0]"` (for JSON format)
- For CSV format, quoted strings are required

## Vector Search Query Examples

### Basic Similarity Search (384D example)

```sql
-- Find top 10 most similar vectors to query vector
SELECT id, name, similarity_cosine(embedding, [0.1, 0.2, ...]) AS score
FROM default_keyspace.vectors_384
ORDER BY embedding ANN OF [0.1, 0.2, ...]
LIMIT 10;
```

### Filter with Similarity Search

```sql
-- Combine filtering with vector search
SELECT id, name, similarity_cosine(embedding, [0.1, 0.2, ...]) AS score
FROM default_keyspace.vectors_384
WHERE name = 'test_vector'
ORDER BY embedding ANN OF [0.1, 0.2, ...]
LIMIT 10;
```

### Important Query Considerations

1. **Dimension Matching:** Query vectors must have EXACTLY the same dimensions as the table column
2. **SAI Indexes:** Ensure SAI indexes are built (check with `DESCRIBE INDEX`)
3. **COSINE Similarity:** Returns values in range [-1, 1] where 1 is most similar
4. **Performance:** First few queries may be slower as indexes warm up

## Extending Tests

### Adding New Test Cases

1. Edit `test_dsbulk.sh`
2. Add new test function following existing patterns:
   ```bash
   test_new_format() {
       print_header "New Format Tests"

       local csv="$TEST_DIR/test_new_format.csv"
       cat > "$csv" <<EOF
   id,name,vec
   test-uuid,test_name,[1.0, 2.0, ...]
   EOF
       run_test "New format test" "$csv" "test_vectors_8d" true
   }
   ```
3. Add function call in `main()`

### Testing Additional Dimensions

1. Update `schema.cql` with new table definition
2. Add SAI index for new table
3. Add test function in `test_dsbulk.sh`
4. Create corresponding data generator script

## Performance Notes

### DSBulk Loading Performance

- **8D vectors:** ~1000-5000 rows/sec
- **384D vectors:** ~500-2000 rows/sec
- **768D vectors:** ~300-1000 rows/sec
- **1024D vectors:** ~200-800 rows/sec

Performance varies based on:
- Network latency to Astra DB
- Local CPU/memory resources
- DSBulk parallelism settings
- Vector dimension size

### Optimization Tips

1. **Increase Parallelism:**
   ```bash
   dsbulk load ... --executor.maxPerSecond 1000
   ```

2. **Batch Size Tuning:**
   ```bash
   dsbulk load ... --batch.maxBatchSize 32
   ```

3. **Connection Pool:**
   ```bash
   dsbulk load ... --driver.pooling.local.connections 8
   ```

## Resources

- **DSBulk Documentation:** https://docs.datastax.com/en/dsbulk/docs/
- **Cassandra Vector Type:** https://cassandra.apache.org/doc/latest/cassandra/developing/cql/types.html#vectors
- **Astra DB Documentation:** https://docs.datastax.com/en/astra/
- **IBM Granite Model:** https://huggingface.co/ibm-granite/granite-embedding-30m-english
- **Sentence Transformers:** https://www.sbert.net/

## Contributing

When adding new tests or data generators:
1. Follow existing naming conventions
2. Document test purpose and expected behavior
3. Include error handling and cleanup
4. Update this README with new test descriptions
5. Ensure tests work with both Astra DB and local Cassandra 5.0+
