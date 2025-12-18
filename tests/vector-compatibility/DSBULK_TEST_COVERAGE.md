# DSBulk Vector Compatibility Test Coverage

## Test Matrix

This document provides a comprehensive overview of all tests included in `test_dsbulk.sh`.

### Summary Statistics
- **Total Tests:** 21
- **CSV Format Tests:** 17
- **JSON Format Tests:** 4
- **Dimensions Tested:** 8D, 384D, 768D, 1024D

---

## CSV Format Tests

### 8-Dimensional Vectors (8 tests)
Testing table: `test_vectors_8d` (column: `vec`)

| Test # | Test Name | Description | Expected Result |
|--------|-----------|-------------|-----------------|
| 1 | JSON array with quotes | Vector in quoted JSON array: `"[1.0, 0.5, ...]"` | PASS |
| 2 | JSON array without quotes | Vector in unquoted JSON array: `[1.0, 0.5, ...]` | PASS |
| 3 | Scientific notation | Vector with scientific notation: `[1.0e-1, 2.0e-1, ...]` | PASS |
| 4 | Integer values | Vector with integers: `[1, 2, 3, ...]` | PASS |
| 5 | Empty string for NULL | Empty cell for NULL vector | PASS |
| 6 | Literal 'null' for NULL | Cell contains literal `null` | PASS |
| 7 | Spaces in array | Vector with spaces: `[ 1.0 , 0.5 , ... ]` | PASS |
| 8 | Negative values | Vector with negative numbers: `[-0.5, -0.25, ...]` | PASS |

### 384-Dimensional Vectors (3 tests)
Testing table: `vectors_384` (column: `embedding`)

| Test # | Test Name | Description | Expected Result |
|--------|-----------|-------------|-----------------|
| 9 | JSON array with quotes | 384D vector in quoted JSON array | PASS |
| 10 | JSON array without quotes | 384D vector in unquoted JSON array | PASS |
| 11 | Empty string for NULL | Empty cell for NULL 384D vector | PASS |

### 768-Dimensional Vectors (2 tests)
Testing table: `vectors_768` (column: `embedding`)

| Test # | Test Name | Description | Expected Result |
|--------|-----------|-------------|-----------------|
| 12 | JSON array with quotes | 768D vector in quoted JSON array | PASS |
| 13 | JSON array without quotes | 768D vector in unquoted JSON array | PASS |

### 1024-Dimensional Vectors (2 tests)
Testing table: `vectors_1024` (column: `embedding`)

| Test # | Test Name | Description | Expected Result |
|--------|-----------|-------------|-----------------|
| 14 | JSON array with quotes | 1024D vector in quoted JSON array | PASS |
| 15 | JSON array without quotes | 1024D vector in unquoted JSON array | PASS |

---

## JSON Format Tests

### 8-Dimensional JSON Tests (3 tests)
Testing table: `test_vectors_8d` (column: `vec`)

| Test # | Test Name | Description | Expected Result |
|--------|-----------|-------------|-----------------|
| 16 | Native JSON array | Vector as native JSON array: `{"vec": [1.0, ...]}` | PASS |
| 17 | Array as string | Vector as JSON string: `{"vec": "[1.0, ...]"}` | FAIL (expected) |
| 18 | NULL value | NULL vector: `{"vec": null}` | PASS |

### 384-Dimensional JSON Tests (1 test)
Testing table: `vectors_384` (column: `embedding`)

| Test # | Test Name | Description | Expected Result |
|--------|-----------|-------------|-----------------|
| 19 | Native JSON array | 384D vector as native JSON array | PASS |

---

## Test Data Format Examples

### CSV Format Examples

#### Quoted JSON Array (Standard Format)
```csv
id,name,vec
11111111-1111-1111-1111-111111111111,test_name,"[1.0, 0.5, 0.25, 0.125]"
```

#### Unquoted JSON Array
```csv
id,name,vec
11111111-1111-1111-1111-111111111111,test_name,[1.0, 0.5, 0.25, 0.125]
```

#### NULL Vector (Empty)
```csv
id,name,vec
11111111-1111-1111-1111-111111111111,test_name,
```

#### NULL Vector (Explicit)
```csv
id,name,vec
11111111-1111-1111-1111-111111111111,test_name,null
```

### JSON Format Examples

#### Native JSON Array (Correct)
```json
{
  "id": "11111111-1111-1111-1111-111111111111",
  "name": "test_name",
  "vec": [1.0, 0.5, 0.25, 0.125]
}
```

#### String Array (Incorrect - Expected to Fail)
```json
{
  "id": "11111111-1111-1111-1111-111111111111",
  "name": "test_name",
  "vec": "[1.0, 0.5, 0.25, 0.125]"
}
```

---

## Test Execution Flow

```
1. Prerequisites Check
   ├── Verify DSBulk installation
   ├── Check DSBulk version (>= 1.11.0)
   ├── Validate bundle path
   └── Confirm token configured

2. CSV Format Tests - 8D (8 tests)
   ├── Quoted arrays
   ├── Unquoted arrays
   ├── Scientific notation
   ├── Integers
   ├── NULL variations
   ├── Spaces
   └── Negative values

3. CSV Format Tests - 384D (3 tests)
   ├── Quoted arrays
   ├── Unquoted arrays
   └── NULL vectors

4. CSV Format Tests - 768D (2 tests)
   ├── Quoted arrays
   └── Unquoted arrays

5. CSV Format Tests - 1024D (2 tests)
   ├── Quoted arrays
   └── Unquoted arrays

6. JSON Format Tests (4 tests)
   ├── 8D native arrays
   ├── 8D string arrays (fail test)
   ├── 8D NULL values
   └── 384D native arrays

7. Results Summary
   ├── Total/Passed/Failed counts
   ├── Detailed failure list
   └── Log directory location
```

---

## Expected Test Results

### Success Criteria
- **20 tests should PASS**
- **1 test should FAIL** (JSON string array - test #17)
- **0 tests should be SKIPPED**

---

## Related Documentation

- [README.md](README.md) - Main testing documentation
- [schema.cql](schema.cql) - Table definitions
- [test_dsbulk.sh](test_dsbulk.sh) - Test script source

---

**Last Updated:** 2025-12-02
**Script Version:** 1.0
**Total Test Count:** 21
