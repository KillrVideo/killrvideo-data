# AstraPy Vector Test Coverage

## Test Matrix

### Tables API Coverage

| Test Case | 384d | 768d | 1024d | Expected Result |
|-----------|------|------|-------|-----------------|
| **Data Type Tests** |
| Python list | ✓ | ✓ | ✓ | PASS |
| Numpy array | ✓ | ✓ | ✓ | PASS |
| Numpy .tolist() | ✓ | ✓ | ✓ | PASS |
| Tuple | ✓ | ✓ | ✓ | PASS |
| JSON string | ✓ | ✓ | ✓ | FAIL (expected) |
| None/NULL | ✓ | ✓ | ✓ | PASS |
| Empty list | ✓ | ✓ | ✓ | FAIL (expected) |
| Wrong dimension | ✓ | ✓ | ✓ | FAIL (expected) |
| **Batch Operations** |
| insert_many() | ✓ | ✓ | ✓ | PASS |
| **Edge Cases** |
| High precision floats | ✓ | ✓ | ✓ | PASS |
| Very small (1e-38) | ✓ | ✓ | ✓ | PASS |
| Very large (1e38) | ✓ | ✓ | ✓ | PASS |
| NaN values | ✓ | ✓ | ✓ | FAIL (expected) |
| Infinity values | ✓ | ✓ | ✓ | FAIL (expected) |
| **Search Operations** |
| Vector similarity search | ✓ | ✓ | ✓ | PASS |
| **Total per dimension** | 15 | 15 | 15 | 45 tests |

### Collections API Coverage

| Test Case | 384d Vector | Expected Result |
|-----------|-------------|-----------------|
| **Data Type Tests** |
| $vector as list | ✓ | PASS |
| $vector as numpy array | ✓ | PASS |
| $vector as string | ✓ | FAIL (expected) |
| No $vector field | ✓ | PASS |
| Wrong dimension | ✓ | FAIL (expected) |
| **Batch Operations** |
| insert_many() with vectors | ✓ | PASS |
| **Edge Cases** |
| NaN values in $vector | ✓ | FAIL (expected) |
| Infinity in $vector | ✓ | FAIL (expected) |
| **Search Operations** |
| Vector search with sort | ✓ | PASS |
| **Total** | 9 | 9 tests |

### Infrastructure Tests

| Test Case | Result |
|-----------|--------|
| Create table with vector column (384d) | PASS |
| Create table with vector column (768d) | PASS |
| Create table with vector column (1024d) | PASS |
| Create collection with vector support | PASS |
| **Total** | 4 tests |

## Total Test Count

- **Tables API**: 45 tests (15 per dimension × 3 dimensions)
- **Collections API**: 9 tests
- **Infrastructure**: 4 tests
- **Grand Total**: 58 tests

## Test Categories

### 1. Data Type Compatibility (22 tests)
Validates that astrapy correctly handles different Python data types when inserting vectors.

**Covered types:**
- Python list (native)
- Numpy array (ndarray)
- Numpy .tolist() conversion
- Tuple
- JSON string (should reject)
- None/NULL
- Empty collections

### 2. Dimension Validation (8 tests)
Ensures proper validation of vector dimensions.

**Scenarios:**
- Correct dimension insertion
- Wrong dimension rejection
- Empty vector rejection

### 3. Edge Case Handling (24 tests)
Tests boundary conditions and unusual values.

**Cases:**
- High precision floats (many decimal places)
- Very small values (near zero: 1e-38)
- Very large values (1e38)
- NaN (Not a Number)
- Infinity (positive/negative)

### 4. Batch Operations (4 tests)
Validates bulk insert capabilities.

**Operations:**
- Tables API: insert_many()
- Collections API: insert_many()

### 5. Search Operations (4 tests)
Tests vector similarity search functionality.

**Features:**
- ANN (Approximate Nearest Neighbor) search
- COSINE similarity
- Sort by vector similarity
- Limit and pagination

## Expected Failures

The following tests are designed to fail and validate error handling:

### Tables API (18 expected failures)
- JSON string as vector (3 tests - one per dimension)
- Empty list (3 tests)
- Wrong dimension (3 tests)
- NaN values (3 tests)
- Infinity values (3 tests)
- JSON string as $vector (3 tests)

### Collections API (4 expected failures)
- JSON string as $vector (1 test)
- Wrong dimension (1 test)
- NaN values (1 test)
- Infinity values (1 test)

**Total Expected Failures: 22**

## Test Execution Time

Approximate execution times:

| Test Suite | Time | Tests |
|------------|------|-------|
| Infrastructure setup | 10s | 4 |
| Tables API (384d) | 45s | 15 |
| Tables API (768d) | 45s | 15 |
| Tables API (1024d) | 45s | 15 |
| Collections API | 30s | 9 |
| **Total** | **~3 minutes** | **58** |

Note: Times vary based on network latency to Astra DB.

## Coverage Gaps

The following scenarios are NOT currently tested but may be worth adding:

### Missing Test Cases

1. **Update operations**
   - Update vector field on existing row
   - Update with different data types

2. **Delete operations**
   - Delete rows with vectors
   - Set vector to NULL via update

3. **Concurrent operations**
   - Multiple simultaneous inserts
   - Race conditions

4. **Large batch operations**
   - insert_many() with 100+ documents
   - Performance under load

5. **Query variations**
   - Complex filters with vector search
   - Pagination with vector results
   - Hybrid search (vector + filters)

6. **Error recovery**
   - Connection failures mid-operation
   - Retry behavior
   - Transaction rollback

7. **Vector operations**
   - Different similarity functions (EUCLIDEAN, DOT_PRODUCT)
   - Vector normalization
   - Distance thresholds

8. **Data validation**
   - Vectors with all zeros
   - Negative values only
   - Sparse vectors

## Adding New Tests

To expand coverage, add tests to `test_astrapy.py`:

```python
# In _test_table_dimension() method
def test_new_feature():
    # Implementation
    return "Success message"

self.suite.run_test(
    f"New feature test (dim={dimension})",
    test_new_feature,
    expected_fail=False
)
```

## Test Reliability

### High Confidence Tests (Pass Rate: ~100%)
- Python list insertion
- Numpy array insertion
- NULL handling
- Batch operations

### Medium Confidence Tests (Pass Rate: ~95%)
- Edge case handling (very small/large values)
- High precision floats
- Vector similarity search

### Validation Tests (Should Always Fail)
- Invalid data types (JSON string)
- Invalid values (NaN, Infinity)
- Wrong dimensions

## Continuous Integration

Recommended CI configuration:

```yaml
- name: Run vector tests
  env:
    ASTRA_DB_API_ENDPOINT: ${{ secrets.ASTRA_DB_API_ENDPOINT }}
    ASTRA_DB_APPLICATION_TOKEN: ${{ secrets.ASTRA_DB_APPLICATION_TOKEN }}
  run: |
    python test_astrapy.py
```

## Reporting Issues

If tests fail unexpectedly:

1. Check environment variables are set correctly
2. Verify network connectivity to Astra DB
3. Confirm keyspace exists
4. Review error messages in test output
5. Run with `--verbose` for detailed logs
6. Check Astra DB status page

## Version Compatibility

This test suite is compatible with:

- **astrapy**: >= 1.0.0
- **Python**: >= 3.8
- **Numpy**: >= 1.19.0
- **Astra DB**: All current versions with vector support

## Maintenance Schedule

Recommended test updates:

- **Weekly**: Run full test suite against production Astra DB
- **Monthly**: Review and update edge cases
- **Quarterly**: Add tests for new astrapy features
- **Per Release**: Validate against new Astra DB versions
