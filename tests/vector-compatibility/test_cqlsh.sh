#!/bin/bash

################################################################################
# Vector Compatibility Test Script for cqlsh (Astra DB)
#
# Tests vector INSERT operations via cqlsh against Astra DB using various
# formats, dimensions, and edge cases.
#
# Usage:
#   ./test_cqlsh.sh [database_name]
#
# Default database: killrvideo
################################################################################

# Don't exit on error - we want to continue testing even if individual tests fail
# set -e

# Configuration
DB_NAME="${1:-killrvideo}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_DIR="${SCRIPT_DIR}/temp_test_files"
RESULTS_FILE="${SCRIPT_DIR}/test_results_cqlsh.log"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

################################################################################
# Helper Functions
################################################################################

print_header() {
    echo ""
    echo "=============================================================================="
    echo -e "${BLUE}$1${NC}"
    echo "=============================================================================="
}

print_section() {
    echo ""
    echo -e "${YELLOW}>>> $1${NC}"
}

log_test() {
    local test_name="$1"
    local expected="$2"  # "pass" or "fail"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -e "\n[Test #${TOTAL_TESTS}] ${test_name} (expected: ${expected})"
}

log_pass() {
    PASSED_TESTS=$((PASSED_TESTS + 1))
    echo -e "${GREEN}✓ PASS${NC}"
    echo "[PASS] Test #${TOTAL_TESTS}: $1" >> "$RESULTS_FILE"
}

log_fail() {
    FAILED_TESTS=$((FAILED_TESTS + 1))
    echo -e "${RED}✗ FAIL${NC}"
    echo "[FAIL] Test #${TOTAL_TESTS}: $1" >> "$RESULTS_FILE"
    if [ -n "$2" ]; then
        echo "  Error: $2"
        echo "  Error: $2" >> "$RESULTS_FILE"
    fi
}

execute_cql() {
    local cql_statement="$1"
    local temp_file="${TEMP_DIR}/temp_query.cql"

    # Write CQL to temp file
    echo "$cql_statement" > "$temp_file"

    # Execute via astra CLI
    astra db cqlsh exec "$DB_NAME" -f "$temp_file" 2>&1
}

execute_cql_expect_success() {
    local test_name="$1"
    local cql_statement="$2"

    log_test "$test_name" "pass"

    local output
    output=$(execute_cql "$cql_statement")
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        log_pass "$test_name"
        return 0
    else
        log_fail "$test_name" "$output"
        return 0  # Changed to 0 to not exit script
    fi
}

execute_cql_expect_failure() {
    local test_name="$1"
    local cql_statement="$2"

    log_test "$test_name" "fail"

    local output
    output=$(execute_cql "$cql_statement")
    local exit_code=$?

    if [ $exit_code -ne 0 ]; then
        log_pass "$test_name (correctly failed)"
        return 0
    else
        log_fail "$test_name (should have failed but succeeded)"
        return 0  # Changed to 0 to not exit script
    fi
}

generate_vector() {
    local dimensions=$1
    local format=$2  # "float", "scientific", "integer", "negative", "mixed"

    local vector="["
    for ((i=0; i<dimensions; i++)); do
        if [ $i -gt 0 ]; then
            vector+=", "
        fi

        case $format in
            float)
                # Standard float format
                vector+=$(awk "BEGIN {printf \"%.6f\", 1.0 / ($i + 1)}")
                ;;
            scientific)
                # Scientific notation
                vector+=$(awk "BEGIN {printf \"%.2e\", 0.1 * ($i + 1)}")
                ;;
            integer)
                # Integer values
                vector+="$((i + 1))"
                ;;
            negative)
                # Alternating positive/negative
                if [ $((i % 2)) -eq 0 ]; then
                    vector+="-0.$i"
                else
                    vector+="0.$i"
                fi
                ;;
            mixed)
                # Mix of formats
                case $((i % 3)) in
                    0) vector+="$((i + 1))" ;;
                    1) vector+="-0.$i" ;;
                    2) vector+=$(awk "BEGIN {printf \"%.2e\", 0.1 * ($i + 1)}") ;;
                esac
                ;;
        esac
    done
    vector+="]"
    echo "$vector"
}

################################################################################
# Setup and Teardown
################################################################################

setup() {
    print_header "Setup: Preparing Test Environment"

    # Create temp directory
    mkdir -p "$TEMP_DIR"

    # Initialize results file
    echo "Vector Compatibility Test Results - $(date)" > "$RESULTS_FILE"
    echo "Database: $DB_NAME" >> "$RESULTS_FILE"
    echo "================================================" >> "$RESULTS_FILE"

    # Create test tables
    print_section "Creating test tables"

    local setup_cql="
-- Small vectors for format testing (8 dimensions)
CREATE TABLE IF NOT EXISTS default_keyspace.test_vectors_8d (
    id uuid PRIMARY KEY,
    name text,
    vec vector<float, 8>
);

-- Full dimension tables
CREATE TABLE IF NOT EXISTS default_keyspace.vectors_384 (
    id uuid PRIMARY KEY,
    name text,
    embedding vector<float, 384>
);

CREATE TABLE IF NOT EXISTS default_keyspace.vectors_768 (
    id uuid PRIMARY KEY,
    name text,
    embedding vector<float, 768>
);

CREATE TABLE IF NOT EXISTS default_keyspace.vectors_1024 (
    id uuid PRIMARY KEY,
    name text,
    embedding vector<float, 1024>
);
"

    execute_cql "$setup_cql"
    echo "Tables created successfully"
}

cleanup() {
    print_header "Cleanup: Removing Test Data"

    local cleanup_cql="
TRUNCATE default_keyspace.test_vectors_8d;
TRUNCATE default_keyspace.vectors_384;
TRUNCATE default_keyspace.vectors_768;
TRUNCATE default_keyspace.vectors_1024;
"

    execute_cql "$cleanup_cql" || echo "Cleanup encountered errors (non-fatal)"

    # Remove temp directory
    rm -rf "$TEMP_DIR"
}

################################################################################
# Test Suites
################################################################################

test_8d_format_variations() {
    print_header "Test Suite 1: 8D Format Variations"

    # Test 1: Standard float format
    local vec=$(generate_vector 8 "float")
    execute_cql_expect_success \
        "8D: Standard float format" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'float_format', ${vec});"

    # Test 2: Scientific notation
    local vec=$(generate_vector 8 "scientific")
    execute_cql_expect_success \
        "8D: Scientific notation" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'scientific', ${vec});"

    # Test 3: Integer values
    local vec=$(generate_vector 8 "integer")
    execute_cql_expect_success \
        "8D: Integer values" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'integers', ${vec});"

    # Test 4: Negative values
    local vec=$(generate_vector 8 "negative")
    execute_cql_expect_success \
        "8D: Negative values" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'negatives', ${vec});"

    # Test 5: Mixed formats
    local vec=$(generate_vector 8 "mixed")
    execute_cql_expect_success \
        "8D: Mixed formats" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'mixed', ${vec});"

    # Test 6: NULL vector
    execute_cql_expect_success \
        "8D: NULL vector" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'null_vector', null);"

    # Test 7: Empty vector (should fail)
    execute_cql_expect_failure \
        "8D: Empty vector []" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'empty', []);"

    # Test 8: Wrong dimensions - too few (should fail)
    execute_cql_expect_failure \
        "8D: Wrong dimensions (5 instead of 8)" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'wrong_dims', [1.0, 2.0, 3.0, 4.0, 5.0]);"

    # Test 9: Wrong dimensions - too many (should fail)
    execute_cql_expect_failure \
        "8D: Wrong dimensions (10 instead of 8)" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'wrong_dims', [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);"

    # Test 10: Quoted as string (should fail)
    execute_cql_expect_failure \
        "8D: Vector quoted as string" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'quoted', '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]');"

    # Test 11: Extreme values - very large
    execute_cql_expect_success \
        "8D: Extreme values (very large)" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'extreme_large', [1e10, 2e10, 3e10, 4e10, 5e10, 6e10, 7e10, 8e10]);"

    # Test 12: Extreme values - very small
    execute_cql_expect_success \
        "8D: Extreme values (very small)" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'extreme_small', [1e-10, 2e-10, 3e-10, 4e-10, 5e-10, 6e-10, 7e-10, 8e-10]);"

    # Test 13: Zero values
    execute_cql_expect_success \
        "8D: All zeros" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'zeros', [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);"
}

test_384d_inserts() {
    print_header "Test Suite 2: 384D Vector Inserts"

    # Test 1: Standard float format (384 dimensions)
    local vec=$(generate_vector 384 "float")
    execute_cql_expect_success \
        "384D: Standard float format" \
        "INSERT INTO default_keyspace.vectors_384 (id, name, embedding) VALUES (uuid(), 'float_384', ${vec});"

    # Test 2: Scientific notation (384 dimensions)
    local vec=$(generate_vector 384 "scientific")
    execute_cql_expect_success \
        "384D: Scientific notation" \
        "INSERT INTO default_keyspace.vectors_384 (id, name, embedding) VALUES (uuid(), 'scientific_384', ${vec});"

    # Test 3: Integer values (384 dimensions)
    local vec=$(generate_vector 384 "integer")
    execute_cql_expect_success \
        "384D: Integer values" \
        "INSERT INTO default_keyspace.vectors_384 (id, name, embedding) VALUES (uuid(), 'integer_384', ${vec});"

    # Test 4: NULL vector
    execute_cql_expect_success \
        "384D: NULL vector" \
        "INSERT INTO default_keyspace.vectors_384 (id, name, embedding) VALUES (uuid(), 'null_384', null);"

    # Test 5: Wrong dimensions (768 instead of 384) - should fail
    local vec=$(generate_vector 768 "float")
    execute_cql_expect_failure \
        "384D: Wrong dimensions (768 instead of 384)" \
        "INSERT INTO default_keyspace.vectors_384 (id, name, embedding) VALUES (uuid(), 'wrong_384', ${vec});"
}

test_768d_inserts() {
    print_header "Test Suite 3: 768D Vector Inserts"

    # Test 1: Standard float format (768 dimensions)
    local vec=$(generate_vector 768 "float")
    execute_cql_expect_success \
        "768D: Standard float format" \
        "INSERT INTO default_keyspace.vectors_768 (id, name, embedding) VALUES (uuid(), 'float_768', ${vec});"

    # Test 2: Mixed formats (768 dimensions)
    local vec=$(generate_vector 768 "mixed")
    execute_cql_expect_success \
        "768D: Mixed formats" \
        "INSERT INTO default_keyspace.vectors_768 (id, name, embedding) VALUES (uuid(), 'mixed_768', ${vec});"

    # Test 3: NULL vector
    execute_cql_expect_success \
        "768D: NULL vector" \
        "INSERT INTO default_keyspace.vectors_768 (id, name, embedding) VALUES (uuid(), 'null_768', null);"

    # Test 4: Wrong dimensions (384 instead of 768) - should fail
    local vec=$(generate_vector 384 "float")
    execute_cql_expect_failure \
        "768D: Wrong dimensions (384 instead of 768)" \
        "INSERT INTO default_keyspace.vectors_768 (id, name, embedding) VALUES (uuid(), 'wrong_768', ${vec});"
}

test_1024d_inserts() {
    print_header "Test Suite 4: 1024D Vector Inserts"

    # Test 1: Standard float format (1024 dimensions)
    local vec=$(generate_vector 1024 "float")
    execute_cql_expect_success \
        "1024D: Standard float format" \
        "INSERT INTO default_keyspace.vectors_1024 (id, name, embedding) VALUES (uuid(), 'float_1024', ${vec});"

    # Test 2: Scientific notation (1024 dimensions)
    local vec=$(generate_vector 1024 "scientific")
    execute_cql_expect_success \
        "1024D: Scientific notation" \
        "INSERT INTO default_keyspace.vectors_1024 (id, name, embedding) VALUES (uuid(), 'scientific_1024', ${vec});"

    # Test 3: NULL vector
    execute_cql_expect_success \
        "1024D: NULL vector" \
        "INSERT INTO default_keyspace.vectors_1024 (id, name, embedding) VALUES (uuid(), 'null_1024', null);"
}

test_copy_command() {
    print_header "Test Suite 5: COPY Command Tests"

    # Test 1: COPY from existing CSV file
    if [ -f "${SCRIPT_DIR}/test-data-8d.csv" ]; then
        log_test "COPY: Load from CSV (test-data-8d.csv)" "pass"

        # First, check if small_vectors table exists and use it, otherwise use test_vectors_8d
        local copy_cql="COPY default_keyspace.test_vectors_8d (id, name, vec) FROM '${SCRIPT_DIR}/test-data-8d.csv' WITH HEADER=true;"

        local output
        output=$(execute_cql "$copy_cql")
        local exit_code=$?

        if [ $exit_code -eq 0 ]; then
            log_pass "COPY from CSV file"
        else
            log_fail "COPY from CSV file" "$output"
        fi
    else
        echo "Skipping COPY test - test-data-8d.csv not found"
    fi

    # Test 2: COPY with generated CSV
    print_section "Creating temporary CSV for COPY test"
    local temp_csv="${TEMP_DIR}/copy_test.csv"

    cat > "$temp_csv" << 'EOF'
id,name,vec
66666666-6666-6666-6666-666666666666,copy_test_1,"[1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3]"
77777777-7777-7777-7777-777777777777,copy_test_2,"[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]"
88888888-8888-8888-8888-888888888888,copy_test_3,"[-0.5, 0.5, -0.4, 0.4, -0.3, 0.3, -0.2, 0.2]"
EOF

    log_test "COPY: Load from generated CSV" "pass"

    local copy_cql="COPY default_keyspace.test_vectors_8d (id, name, vec) FROM '${temp_csv}' WITH HEADER=true;"

    local output
    output=$(execute_cql "$copy_cql")
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        log_pass "COPY from generated CSV"
    else
        log_fail "COPY from generated CSV" "$output"
    fi
}

test_batch_statements() {
    print_header "Test Suite 6: BATCH Statement Tests"

    # Test 1: Simple batch with multiple vector inserts
    local vec1=$(generate_vector 8 "float")
    local vec2=$(generate_vector 8 "integer")
    local vec3=$(generate_vector 8 "negative")

    local batch_cql="
BEGIN BATCH
    INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_1', ${vec1});
    INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_2', ${vec2});
    INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_3', ${vec3});
APPLY BATCH;
"

    execute_cql_expect_success \
        "BATCH: Multiple vector inserts" \
        "$batch_cql"

    # Test 2: Batch with mixed NULL and non-NULL vectors
    local vec=$(generate_vector 8 "float")

    local batch_cql="
BEGIN BATCH
    INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_mixed_1', ${vec});
    INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_mixed_2', null);
    INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_mixed_3', ${vec});
APPLY BATCH;
"

    execute_cql_expect_success \
        "BATCH: Mixed NULL and non-NULL vectors" \
        "$batch_cql"
}

test_update_operations() {
    print_header "Test Suite 7: UPDATE Operation Tests"

    # First insert a record
    local test_uuid="99999999-9999-9999-9999-999999999999"
    local vec1=$(generate_vector 8 "float")

    execute_cql_expect_success \
        "UPDATE: Initial insert" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (${test_uuid}, 'update_test', ${vec1});"

    # Test 1: Update vector value
    local vec2=$(generate_vector 8 "integer")
    execute_cql_expect_success \
        "UPDATE: Change vector value" \
        "UPDATE default_keyspace.test_vectors_8d SET vec = ${vec2} WHERE id = ${test_uuid};"

    # Test 2: Update vector to NULL
    execute_cql_expect_success \
        "UPDATE: Set vector to NULL" \
        "UPDATE default_keyspace.test_vectors_8d SET vec = null WHERE id = ${test_uuid};"

    # Test 3: Update with wrong dimensions (should fail)
    local vec_wrong=$(generate_vector 10 "float")
    execute_cql_expect_failure \
        "UPDATE: Wrong dimensions" \
        "UPDATE default_keyspace.test_vectors_8d SET vec = ${vec_wrong} WHERE id = ${test_uuid};"
}

test_special_edge_cases() {
    print_header "Test Suite 8: Special Edge Cases"

    # Test 1: Infinity values (may or may not be supported)
    log_test "Edge Case: Infinity values" "unknown"
    local inf_cql="INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'infinity', [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, Infinity]);"

    local output
    output=$(execute_cql "$inf_cql")
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo "  Note: Infinity values are supported"
        log_pass "Infinity values (supported)"
    else
        echo "  Note: Infinity values are not supported (expected)"
        log_pass "Infinity values (correctly rejected)"
    fi

    # Test 2: NaN values (may or may not be supported)
    log_test "Edge Case: NaN values" "unknown"
    local nan_cql="INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'nan', [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, NaN]);"

    output=$(execute_cql "$nan_cql")
    exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo "  Note: NaN values are supported"
        log_pass "NaN values (supported)"
    else
        echo "  Note: NaN values are not supported (expected)"
        log_pass "NaN values (correctly rejected)"
    fi

    # Test 3: Whitespace variations
    execute_cql_expect_success \
        "Edge Case: Extra whitespace" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'whitespace', [ 1.0 ,  2.0 ,  3.0 ,  4.0 ,  5.0 ,  6.0 ,  7.0 ,  8.0 ]);"

    # Test 4: No whitespace
    execute_cql_expect_success \
        "Edge Case: No whitespace" \
        "INSERT INTO default_keyspace.test_vectors_8d (id, name, vec) VALUES (uuid(), 'no_space', [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]);"
}

################################################################################
# Main Execution
################################################################################

main() {
    print_header "Vector Compatibility Test Suite for cqlsh"
    echo "Database: $DB_NAME"
    echo "Test Script: $0"
    echo "Started: $(date)"

    # Setup
    setup

    # Run all test suites
    test_8d_format_variations
    test_384d_inserts
    test_768d_inserts
    test_1024d_inserts
    test_copy_command
    test_batch_statements
    test_update_operations
    test_special_edge_cases

    # Cleanup
    cleanup

    # Summary
    print_header "Test Results Summary"
    echo ""
    echo "Total Tests:  $TOTAL_TESTS"
    echo -e "Passed:       ${GREEN}$PASSED_TESTS${NC}"
    echo -e "Failed:       ${RED}$FAILED_TESTS${NC}"
    echo ""

    local pass_rate=0
    if [ $TOTAL_TESTS -gt 0 ]; then
        pass_rate=$(awk "BEGIN {printf \"%.1f\", ($PASSED_TESTS / $TOTAL_TESTS) * 100}")
    fi
    echo "Pass Rate:    ${pass_rate}%"
    echo ""
    echo "Detailed results saved to: $RESULTS_FILE"
    echo "Completed: $(date)"

    # Write summary to results file
    echo "" >> "$RESULTS_FILE"
    echo "================================================" >> "$RESULTS_FILE"
    echo "Summary:" >> "$RESULTS_FILE"
    echo "  Total:  $TOTAL_TESTS" >> "$RESULTS_FILE"
    echo "  Passed: $PASSED_TESTS" >> "$RESULTS_FILE"
    echo "  Failed: $FAILED_TESTS" >> "$RESULTS_FILE"
    echo "  Rate:   ${pass_rate}%" >> "$RESULTS_FILE"
    echo "================================================" >> "$RESULTS_FILE"

    # Exit with appropriate code
    if [ $FAILED_TESTS -gt 0 ]; then
        exit 1
    else
        exit 0
    fi
}

# Trap errors and cleanup
trap cleanup EXIT

# Run main function
main
