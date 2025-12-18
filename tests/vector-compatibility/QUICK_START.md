# Quick Start Guide - AstraPy Vector Tests

## 1. Install Dependencies

```bash
pip install astrapy numpy
```

## 2. Set Environment Variables

Get your credentials from [Astra DB Console](https://astra.datastax.com):

```bash
export ASTRA_DB_API_ENDPOINT="https://YOUR-DATABASE-ID-REGION.apps.astra.datastax.com"
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:YOUR_TOKEN_HERE"
```

## 3. Run Tests

```bash
cd tests/vector-compatibility
python test_astrapy.py
```

## Common Commands

### Run all tests
```bash
python test_astrapy.py
```

### Run with verbose output
```bash
python test_astrapy.py --verbose
```

### Test only Tables API
```bash
python test_astrapy.py --skip-collections
```

### Test only Collections API
```bash
python test_astrapy.py --skip-tables
```

### Use custom keyspace
```bash
python test_astrapy.py --keyspace my_keyspace
```

## Expected Output

```
==================================================================
ASTRAPY VECTOR COMPATIBILITY TEST
==================================================================
Timestamp: 2025-12-02T15:30:45.123456
Keyspace: default_keyspace
Vector dimensions to test: [384, 768, 1024]
...

Connecting to Astra DB...
  Endpoint: https://...
  Keyspace: default_keyspace
Connected successfully!

Creating test tables...
  PASS: Create table test_vectors_384
  PASS: Create table test_vectors_768
  PASS: Create table test_vectors_1024

==================================================================
TABLES API TESTS
==================================================================

Testing dimension: 384
----------------------------------------
...........
  EXPECTED FAIL: Insert with JSON string (dim=384)
...

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

## Troubleshooting

### Missing environment variables
```
ERROR: ASTRA_DB_API_ENDPOINT environment variable not set
```
**Fix**: Set both `ASTRA_DB_API_ENDPOINT` and `ASTRA_DB_APPLICATION_TOKEN`

### Missing dependencies
```
ERROR: astrapy not installed
```
**Fix**: `pip install astrapy numpy`

### Keyspace doesn't exist
```
Error: Keyspace 'xyz' does not exist
```
**Fix**: Create keyspace in Astra UI or use `--keyspace default_keyspace`

## What Gets Tested

### Tables API (per dimension: 384, 768, 1024)
- Python list insertion
- Numpy array insertion
- Tuple insertion
- NULL handling
- Edge cases (NaN, Inf, high precision, very small/large values)
- Vector similarity search

### Collections API
- `$vector` field operations
- Vector search with sort
- Batch operations
- Error handling

## Next Steps

- Review `README_ASTRAPY_TESTS.md` for detailed documentation
- Examine `test_astrapy.py` to understand test implementation
- Add custom tests for your specific use cases
- Integrate into CI/CD pipeline

## Quick Test of Specific Feature

To test just one thing, you can modify the script or use Python interactively:

```python
from astrapy import DataAPIClient
import numpy as np

# Connect
client = DataAPIClient(token)
db = client.get_database(api_endpoint, keyspace="default_keyspace")
table = db.get_table("test_vectors_384")

# Test numpy array insertion
vector = np.random.randn(384).astype(np.float32)
table.insert_one({
    "id": "a1111111-1111-1111-1111-111111111111",
    "name": "test",
    "embedding": vector
})

# Verify
result = table.find_one({"id": "a1111111-1111-1111-1111-111111111111"})
print(result)
```
