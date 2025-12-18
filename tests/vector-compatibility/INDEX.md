# AstraPy Vector Compatibility Tests - Index

Complete documentation for the AstraPy vector compatibility test suite.

## Quick Links

- **[Quick Start](QUICK_START.md)** - Get up and running in 5 minutes
- **[Full Documentation](README_ASTRAPY_TESTS.md)** - Comprehensive guide
- **[Test Coverage](TEST_COVERAGE.md)** - What's tested and what's not
- **[Sample Output](SAMPLE_OUTPUT.md)** - Example test runs

## Files in This Directory

### Test Scripts

| File | Size | Description |
|------|------|-------------|
| `test_astrapy.py` | 28K | Main test script (824 lines, 58 tests) |

### Documentation

| File | Size | Description |
|------|------|-------------|
| `README_ASTRAPY_TESTS.md` | 9.2K | Comprehensive documentation |
| `QUICK_START.md` | 3.5K | Quick start guide |
| `TEST_COVERAGE.md` | 6.7K | Test coverage matrix |
| `SAMPLE_OUTPUT.md` | 12K | Example outputs |
| `INDEX.md` | - | This file |

### Supporting Files

| File | Description |
|------|-------------|
| `schema.cql` | Test table schemas |
| `generate_384d_data.py` | Generate 384d test data |
| `generate_768d_data.py` | Generate 768d test data |
| `generate_1024d_data.py` | Generate 1024d test data |

## Test Suite Overview

### What It Tests

- **Tables API**: Vector operations in Astra DB tables
- **Collections API**: Vector operations in Astra DB collections
- **Dimensions**: 384, 768, 1024 (Tables API), 384 (Collections API)
- **Data Types**: Lists, numpy arrays, tuples, NULL
- **Edge Cases**: NaN, Infinity, precision, dimension validation
- **Operations**: Insert, batch insert, similarity search

### Test Statistics

- **Total Tests**: 58
- **Tables API**: 45 tests (15 per dimension × 3 dimensions)
- **Collections API**: 9 tests
- **Infrastructure**: 4 tests
- **Expected Failures**: 18 tests (validation tests)
- **Execution Time**: ~3 minutes

### Test Classes

```python
TestResult       # Represents a single test result
TestSuite        # Manages test execution and results
AstraPyVectorTester  # Main test harness
```

## Getting Started

### Prerequisites

```bash
pip install astrapy numpy
```

### Environment Setup

```bash
export ASTRA_DB_API_ENDPOINT="https://your-db.apps.astra.datastax.com"
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:..."
```

### Run Tests

```bash
python test_astrapy.py
```

See [QUICK_START.md](QUICK_START.md) for detailed instructions.

## Test Categories

### 1. Data Type Tests (22 tests)
- Python list, numpy array, tuple
- JSON string (should fail)
- None/NULL
- Empty collections (should fail)

### 2. Dimension Validation (8 tests)
- Correct dimensions (384, 768, 1024)
- Wrong dimensions (should fail)
- Empty vectors (should fail)

### 3. Edge Cases (24 tests)
- High precision floats
- Very small values (1e-38)
- Very large values (1e38)
- NaN (should fail)
- Infinity (should fail)

### 4. Batch Operations (4 tests)
- insert_many() for tables
- insert_many() for collections

### 5. Search Operations (4 tests)
- Vector similarity search (ANN)
- COSINE similarity
- Sort by vector

## Command Reference

### Basic Commands

```bash
# Run all tests
python test_astrapy.py

# Verbose output
python test_astrapy.py --verbose

# Custom keyspace
python test_astrapy.py --keyspace my_keyspace
```

### Selective Testing

```bash
# Tables API only
python test_astrapy.py --skip-collections

# Collections API only
python test_astrapy.py --skip-tables
```

### Getting Help

```bash
python test_astrapy.py --help
```

## Understanding Results

### Exit Codes

- `0`: Success (all tests passed or only expected failures)
- `1`: Failure (unexpected test failures or missing config)

### Test Status Indicators

- `PASS`: Test passed as expected
- `FAIL`: Test failed unexpectedly
- `EXPECTED FAIL`: Test failed as designed (validation test)
- `UNEXPECTED PASS`: Test passed but was expected to fail (needs review)

### Output Modes

**Normal Mode** (default):
- Dots (`.`) for passing tests
- Full output for failures
- Summary at end

**Verbose Mode** (`--verbose`):
- Detailed output for all tests
- Success messages
- Error traces
- Full summary

## Documentation Guide

### For First-Time Users

1. Start with [QUICK_START.md](QUICK_START.md)
2. Run the basic test command
3. Review [SAMPLE_OUTPUT.md](SAMPLE_OUTPUT.md) to understand results

### For In-Depth Understanding

1. Read [README_ASTRAPY_TESTS.md](README_ASTRAPY_TESTS.md) for complete documentation
2. Review [TEST_COVERAGE.md](TEST_COVERAGE.md) for test matrix
3. Examine `test_astrapy.py` source code

### For Troubleshooting

1. Check [README_ASTRAPY_TESTS.md - Troubleshooting](README_ASTRAPY_TESTS.md#troubleshooting)
2. Compare your output with [SAMPLE_OUTPUT.md](SAMPLE_OUTPUT.md)
3. Run with `--verbose` for detailed error messages

### For Extension

1. Review [TEST_COVERAGE.md - Coverage Gaps](TEST_COVERAGE.md#coverage-gaps)
2. Study existing test patterns in `test_astrapy.py`
3. Follow development guidelines in [README_ASTRAPY_TESTS.md](README_ASTRAPY_TESTS.md#development)

## Use Cases

### Development Testing

Test vector operations during development:
```bash
python test_astrapy.py --verbose
```

### CI/CD Integration

Automated testing in pipelines:
```bash
python test_astrapy.py
exit_code=$?
if [ $exit_code -ne 0 ]; then
  echo "Vector tests failed"
  exit 1
fi
```

### Regression Testing

Validate after Astra DB or astrapy updates:
```bash
python test_astrapy.py --verbose > test_results_$(date +%Y%m%d).log
```

### Performance Baseline

Measure execution time:
```bash
time python test_astrapy.py
```

## Key Features

### Comprehensive Coverage

- Tests all major vector dimensions (384, 768, 1024)
- Covers both Tables API and Collections API
- Validates data types, edge cases, and operations

### Production-Ready

- Environment variable configuration
- Proper error handling
- Clear exit codes
- Detailed logging

### Developer-Friendly

- Verbose mode for debugging
- Selective test execution
- Clear result reporting
- Extensible architecture

### Well-Documented

- 5 documentation files
- Code comments
- Usage examples
- Sample outputs

## Support

### Issues and Questions

- Check documentation files first
- Run with `--verbose` for detailed logs
- Review [SAMPLE_OUTPUT.md](SAMPLE_OUTPUT.md) for expected behavior
- Consult [TEST_COVERAGE.md](TEST_COVERAGE.md) for test details

### Contributing

To add new tests:
1. Study existing test patterns
2. Add test function to appropriate section
3. Use `suite.run_test()` method
4. Update documentation
5. Verify with verbose output

### Related Resources

- [AstraPy Documentation](https://docs.datastax.com/en/astra-serverless/docs/develop/dev-with-python.html)
- [Astra DB Vector Search](https://docs.datastax.com/en/astra-serverless/docs/vector-search/overview.html)
- [KillrVideo Schema](../../schema-astra.cql)

## Version History

### v1.0 (Current)
- Initial release
- 58 tests across Tables and Collections APIs
- Support for 384, 768, 1024 dimensions
- Comprehensive documentation

## File Structure

```
tests/vector-compatibility/
├── test_astrapy.py              # Main test script (824 lines)
├── README_ASTRAPY_TESTS.md      # Comprehensive documentation
├── QUICK_START.md               # Quick start guide
├── TEST_COVERAGE.md             # Test coverage matrix
├── SAMPLE_OUTPUT.md             # Example outputs
├── INDEX.md                     # This file
├── schema.cql                   # Test table schemas
├── generate_384d_data.py        # 384d data generator
├── generate_768d_data.py        # 768d data generator
└── generate_1024d_data.py       # 1024d data generator
```

## Quick Reference Card

| Task | Command |
|------|---------|
| Run all tests | `python test_astrapy.py` |
| Verbose output | `python test_astrapy.py --verbose` |
| Tables only | `python test_astrapy.py --skip-collections` |
| Collections only | `python test_astrapy.py --skip-tables` |
| Custom keyspace | `python test_astrapy.py --keyspace NAME` |
| Help | `python test_astrapy.py --help` |
| Check syntax | `python3 -m py_compile test_astrapy.py` |

## Next Steps

- [ ] Set environment variables
- [ ] Install dependencies (`pip install astrapy numpy`)
- [ ] Run basic test (`python test_astrapy.py`)
- [ ] Review results
- [ ] Integrate into your workflow

For detailed instructions, see [QUICK_START.md](QUICK_START.md).
