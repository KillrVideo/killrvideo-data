#!/bin/bash

################################################################################
# Astra CLI Vector Compatibility Test Script
################################################################################
# Tests vector operations using the `astra` CLI tool against Astra DB
#
# Usage: ./test_astra_cli.sh [database_name] [keyspace_name]
#
# Requirements:
#   - astra CLI installed and authenticated
#   - Target database must exist and be active
#   - Test keyspace must exist
################################################################################

set -o pipefail

# Configuration
DB_NAME="${1:-killrvideo}"
KEYSPACE="${2:-killrvideo}"
TEMP_DIR="/tmp/astra_vector_tests_$$"
RESULTS_FILE="$TEMP_DIR/results.txt"
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

################################################################################
# Helper Functions
################################################################################

print_header() {
    echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
}

print_test() {
    echo -e "\n${YELLOW}TEST:${NC} $1"
}

log_pass() {
    echo -e "${GREEN}✓ PASS${NC}: $1"
    echo "PASS: $1" >> "$RESULTS_FILE"
    ((PASSED_TESTS++))
    ((TOTAL_TESTS++))
}

log_fail() {
    echo -e "${RED}✗ FAIL${NC}: $1"
    echo "FAIL: $1" >> "$RESULTS_FILE"
    if [ -n "$2" ]; then
        echo -e "${RED}  Error: $2${NC}"
        echo "  Error: $2" >> "$RESULTS_FILE"
    fi
    ((FAILED_TESTS++))
    ((TOTAL_TESTS++))
}

log_skip() {
    echo -e "${YELLOW}⊘ SKIP${NC}: $1"
    echo "SKIP: $1" >> "$RESULTS_FILE"
}

cleanup() {
    if [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}

trap cleanup EXIT

################################################################################
# Setup
################################################################################

setup() {
    print_header "Astra CLI Vector Compatibility Test Suite"
    echo "Database: $DB_NAME"
    echo "Keyspace: $KEYSPACE"
    echo "Temp Directory: $TEMP_DIR"
    echo ""

    # Create temp directory
    mkdir -p "$TEMP_DIR"

    # Check if astra CLI is installed
    if ! command -v astra &> /dev/null; then
        echo -e "${RED}ERROR: astra CLI not found. Please install it first.${NC}"
        echo "Installation: npm install -g @datastax/astra-cli"
        exit 1
    fi

    # Check if authenticated
    if ! astra org &> /dev/null; then
        echo -e "${RED}ERROR: Not authenticated with Astra. Run 'astra setup' first.${NC}"
        exit 1
    fi

    # Check if database exists
    if ! astra db get "$DB_NAME" &> /dev/null; then
        echo -e "${RED}ERROR: Database '$DB_NAME' not found.${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Prerequisites validated${NC}"
    echo ""
}

################################################################################
# Test Schema Setup
################################################################################

create_test_tables() {
    print_header "Setting Up Test Tables"

    cat > "$TEMP_DIR/setup_tables.cql" <<EOF
-- Drop existing test tables if they exist
DROP TABLE IF EXISTS $KEYSPACE.test_vectors_8d;
DROP TABLE IF EXISTS $KEYSPACE.test_vectors_16d;
DROP TABLE IF EXISTS $KEYSPACE.test_vectors_384d;
DROP TABLE IF EXISTS $KEYSPACE.test_vectors_768d;
DROP TABLE IF EXISTS $KEYSPACE.test_vectors_1024d;

-- Create test tables with various vector dimensions
CREATE TABLE $KEYSPACE.test_vectors_8d (
    id UUID PRIMARY KEY,
    name TEXT,
    vec VECTOR<FLOAT, 8>
);

CREATE TABLE $KEYSPACE.test_vectors_16d (
    id UUID PRIMARY KEY,
    name TEXT,
    vec VECTOR<FLOAT, 16>
);

CREATE TABLE $KEYSPACE.test_vectors_384d (
    id UUID PRIMARY KEY,
    name TEXT,
    vec VECTOR<FLOAT, 384>
);

CREATE TABLE $KEYSPACE.test_vectors_768d (
    id UUID PRIMARY KEY,
    name TEXT,
    vec VECTOR<FLOAT, 768>
);

CREATE TABLE $KEYSPACE.test_vectors_1024d (
    id UUID PRIMARY KEY,
    name TEXT,
    vec VECTOR<FLOAT, 1024>
);
EOF

    if astra db cqlsh exec "$DB_NAME" -f "$TEMP_DIR/setup_tables.cql" 2>&1 | tee "$TEMP_DIR/setup_output.txt"; then
        echo -e "${GREEN}✓ Test tables created successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Failed to create test tables${NC}"
        cat "$TEMP_DIR/setup_output.txt"
        exit 1
    fi
}

################################################################################
# Test 1: Inline CQL Execution - Simple Vector Insert
################################################################################

test_inline_simple_vector() {
    print_test "Inline CQL: Simple 8D vector insert"

    local cql="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'simple_test', [1.0, 0.5, 0.25, 0.125, 0.0625, 0.03125, 0.015625, 0.0078125]);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test1_output.txt" 2>&1; then
        log_pass "Simple 8D vector insert via inline CQL"
    else
        log_fail "Simple 8D vector insert via inline CQL" "$(cat "$TEMP_DIR/test1_output.txt")"
    fi
}

################################################################################
# Test 2: Inline CQL - Vector with Special Characters (Brackets)
################################################################################

test_inline_special_chars() {
    print_test "Inline CQL: Vector with special characters"

    # Test with brackets in the data (should be properly escaped)
    local cql="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'special[chars]', [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test2_output.txt" 2>&1; then
        log_pass "Vector insert with special characters in name"
    else
        log_fail "Vector insert with special characters in name" "$(cat "$TEMP_DIR/test2_output.txt")"
    fi
}

################################################################################
# Test 3: Inline CQL - Shell Escaping Issues
################################################################################

test_inline_shell_escaping() {
    print_test "Inline CQL: Shell escaping validation"

    # Test with single quotes in name
    local cql="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'test''s data', [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test3_output.txt" 2>&1; then
        log_pass "Vector insert with single quotes in name"
    else
        log_fail "Vector insert with single quotes in name" "$(cat "$TEMP_DIR/test3_output.txt")"
    fi
}

################################################################################
# Test 4: File-Based CQL - Vector Inserts
################################################################################

test_file_simple_inserts() {
    print_test "File-based CQL: Multiple vector inserts"

    cat > "$TEMP_DIR/test4.cql" <<EOF
INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'file_test_1', [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]);
INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'file_test_2', [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]);
INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'file_test_3', [-1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]);
EOF

    if astra db cqlsh exec "$DB_NAME" -f "$TEMP_DIR/test4.cql" > "$TEMP_DIR/test4_output.txt" 2>&1; then
        log_pass "Multiple vector inserts from CQL file"
    else
        log_fail "Multiple vector inserts from CQL file" "$(cat "$TEMP_DIR/test4_output.txt")"
    fi
}

################################################################################
# Test 5: File-Based CQL - Batch Statements
################################################################################

test_file_batch_statements() {
    print_test "File-based CQL: Batch statements with vectors"

    cat > "$TEMP_DIR/test5.cql" <<EOF
BEGIN BATCH
INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_test_1', [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]);
INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_test_2', [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0]);
INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'batch_test_3', [3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0]);
APPLY BATCH;
EOF

    if astra db cqlsh exec "$DB_NAME" -f "$TEMP_DIR/test5.cql" > "$TEMP_DIR/test5_output.txt" 2>&1; then
        log_pass "Batch statements with vectors from CQL file"
    else
        log_fail "Batch statements with vectors from CQL file" "$(cat "$TEMP_DIR/test5_output.txt")"
    fi
}

################################################################################
# Test 6: Large Vector - 384 Dimensions (Inline)
################################################################################

test_large_vector_384_inline() {
    print_test "Inline CQL: 384-dimension vector"

    # Generate a 384-dimension vector
    local vec="["
    for i in $(seq 1 384); do
        if [ $i -eq 384 ]; then
            vec+="$(awk -v seed=$i 'BEGIN{srand(seed); print rand()}')"
        else
            vec+="$(awk -v seed=$i 'BEGIN{srand(seed); print rand()}'), "
        fi
    done
    vec+="]"

    local cql="INSERT INTO $KEYSPACE.test_vectors_384d (id, name, vec) VALUES (uuid(), 'inline_384d', $vec);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test6_output.txt" 2>&1; then
        log_pass "384-dimension vector insert via inline CQL"
    else
        # This might fail due to command line length limits
        log_fail "384-dimension vector insert via inline CQL (may hit CLI length limits)" "$(cat "$TEMP_DIR/test6_output.txt")"
    fi
}

################################################################################
# Test 7: Large Vector - 384 Dimensions (File)
################################################################################

test_large_vector_384_file() {
    print_test "File-based CQL: 384-dimension vector"

    # Generate a 384-dimension vector
    local vec="["
    for i in $(seq 1 384); do
        if [ $i -eq 384 ]; then
            vec+="$(awk -v seed=$i 'BEGIN{srand(seed); print rand()}')"
        else
            vec+="$(awk -v seed=$i 'BEGIN{srand(seed); print rand()}'), "
        fi
    done
    vec+="]"

    cat > "$TEMP_DIR/test7.cql" <<EOF
INSERT INTO $KEYSPACE.test_vectors_384d (id, name, vec) VALUES (uuid(), 'file_384d', $vec);
EOF

    if astra db cqlsh exec "$DB_NAME" -f "$TEMP_DIR/test7.cql" > "$TEMP_DIR/test7_output.txt" 2>&1; then
        log_pass "384-dimension vector insert from CQL file"
    else
        log_fail "384-dimension vector insert from CQL file" "$(cat "$TEMP_DIR/test7_output.txt")"
    fi
}

################################################################################
# Test 8: Large Vector - 768 Dimensions (File)
################################################################################

test_large_vector_768_file() {
    print_test "File-based CQL: 768-dimension vector"

    # Generate a 768-dimension vector
    local vec="["
    for i in $(seq 1 768); do
        if [ $i -eq 768 ]; then
            vec+="$(awk -v seed=$i 'BEGIN{srand(seed); print rand()}')"
        else
            vec+="$(awk -v seed=$i 'BEGIN{srand(seed); print rand()}'), "
        fi
    done
    vec+="]"

    cat > "$TEMP_DIR/test8.cql" <<EOF
INSERT INTO $KEYSPACE.test_vectors_768d (id, name, vec) VALUES (uuid(), 'file_768d', $vec);
EOF

    if astra db cqlsh exec "$DB_NAME" -f "$TEMP_DIR/test8.cql" > "$TEMP_DIR/test8_output.txt" 2>&1; then
        log_pass "768-dimension vector insert from CQL file"
    else
        log_fail "768-dimension vector insert from CQL file" "$(cat "$TEMP_DIR/test8_output.txt")"
    fi
}

################################################################################
# Test 9: Large Vector - 1024 Dimensions (File)
################################################################################

test_large_vector_1024_file() {
    print_test "File-based CQL: 1024-dimension vector"

    # Generate a 1024-dimension vector
    local vec="["
    for i in $(seq 1 1024); do
        if [ $i -eq 1024 ]; then
            vec+="$(awk -v seed=$i 'BEGIN{srand(seed); print rand()}')"
        else
            vec+="$(awk -v seed=$i 'BEGIN{srand(seed); print rand()}'), "
        fi
    done
    vec+="]"

    cat > "$TEMP_DIR/test9.cql" <<EOF
INSERT INTO $KEYSPACE.test_vectors_1024d (id, name, vec) VALUES (uuid(), 'file_1024d', $vec);
EOF

    if astra db cqlsh exec "$DB_NAME" -f "$TEMP_DIR/test9.cql" > "$TEMP_DIR/test9_output.txt" 2>&1; then
        log_pass "1024-dimension vector insert from CQL file"
    else
        log_fail "1024-dimension vector insert from CQL file" "$(cat "$TEMP_DIR/test9_output.txt")"
    fi
}

################################################################################
# Test 10: Shell Escaping - Negative Numbers
################################################################################

test_escaping_negative_numbers() {
    print_test "Shell escaping: Vectors with negative numbers"

    local cql="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'negative_test', [-1.5, -0.75, -0.25, 0.0, 0.25, 0.75, 1.5, 2.0]);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test10_output.txt" 2>&1; then
        log_pass "Vector insert with negative numbers via inline CQL"
    else
        log_fail "Vector insert with negative numbers via inline CQL" "$(cat "$TEMP_DIR/test10_output.txt")"
    fi
}

################################################################################
# Test 11: Shell Escaping - Scientific Notation
################################################################################

test_escaping_scientific_notation() {
    print_test "Shell escaping: Vectors with scientific notation"

    local cql="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'scientific_test', [1.5e-3, 2.5e-2, 3.5e-1, 4.5e0, 5.5e1, 6.5e2, 7.5e3, 8.5e4]);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test11_output.txt" 2>&1; then
        log_pass "Vector insert with scientific notation via inline CQL"
    else
        log_fail "Vector insert with scientific notation via inline CQL" "$(cat "$TEMP_DIR/test11_output.txt")"
    fi
}

################################################################################
# Test 12: Shell Escaping - Quoted Strings
################################################################################

test_escaping_quoted_strings() {
    print_test "Shell escaping: Quoted strings around vectors"

    cat > "$TEMP_DIR/test12.cql" <<EOF
INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'quoted_test', [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]);
EOF

    if astra db cqlsh exec "$DB_NAME" -f "$TEMP_DIR/test12.cql" > "$TEMP_DIR/test12_output.txt" 2>&1; then
        log_pass "Vector insert with proper CQL formatting from file"
    else
        log_fail "Vector insert with proper CQL formatting from file" "$(cat "$TEMP_DIR/test12_output.txt")"
    fi
}

################################################################################
# Test 13: Comparison - Inline vs File for Large Vectors
################################################################################

test_inline_vs_file_comparison() {
    print_test "Comparison: Inline vs File execution for 16D vector"

    # Test inline
    local vec_16d="[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6]"
    local cql_inline="INSERT INTO $KEYSPACE.test_vectors_16d (id, name, vec) VALUES (uuid(), 'inline_16d', $vec_16d);"

    local inline_start=$(date +%s%N)
    if astra db cqlsh exec "$DB_NAME" "$cql_inline" > "$TEMP_DIR/test13a_output.txt" 2>&1; then
        local inline_end=$(date +%s%N)
        local inline_duration=$((($inline_end - $inline_start) / 1000000))
        log_pass "Inline execution for 16D vector (${inline_duration}ms)"
    else
        log_fail "Inline execution for 16D vector" "$(cat "$TEMP_DIR/test13a_output.txt")"
        return
    fi

    # Test file-based
    cat > "$TEMP_DIR/test13b.cql" <<EOF
INSERT INTO $KEYSPACE.test_vectors_16d (id, name, vec) VALUES (uuid(), 'file_16d', $vec_16d);
EOF

    local file_start=$(date +%s%N)
    if astra db cqlsh exec "$DB_NAME" -f "$TEMP_DIR/test13b.cql" > "$TEMP_DIR/test13b_output.txt" 2>&1; then
        local file_end=$(date +%s%N)
        local file_duration=$((($file_end - $file_start) / 1000000))
        log_pass "File-based execution for 16D vector (${file_duration}ms)"
    else
        log_fail "File-based execution for 16D vector" "$(cat "$TEMP_DIR/test13b_output.txt")"
    fi
}

################################################################################
# Test 14: Query Operations - Vector Retrieval
################################################################################

test_vector_retrieval() {
    print_test "Query: Vector retrieval and validation"

    # Insert a known vector
    local cql_insert="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (550e8400-e29b-41d4-a716-446655440000, 'retrieval_test', [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]);"

    if astra db cqlsh exec "$DB_NAME" "$cql_insert" > "$TEMP_DIR/test14a_output.txt" 2>&1; then
        # Query it back
        local cql_select="SELECT vec FROM $KEYSPACE.test_vectors_8d WHERE id=550e8400-e29b-41d4-a716-446655440000;"

        if astra db cqlsh exec "$DB_NAME" "$cql_select" > "$TEMP_DIR/test14b_output.txt" 2>&1; then
            # Check if the output contains vector data
            if grep -q "\[1, 2, 3, 4, 5, 6, 7, 8\]" "$TEMP_DIR/test14b_output.txt" || grep -q "1.0, 2.0, 3.0" "$TEMP_DIR/test14b_output.txt"; then
                log_pass "Vector retrieval and validation"
            else
                log_fail "Vector retrieval and validation" "Vector data not found in query result"
            fi
        else
            log_fail "Vector retrieval and validation" "Query failed: $(cat "$TEMP_DIR/test14b_output.txt")"
        fi
    else
        log_fail "Vector retrieval and validation" "Insert failed: $(cat "$TEMP_DIR/test14a_output.txt")"
    fi
}

################################################################################
# Test 15: Edge Case - Zero Vector
################################################################################

test_edge_case_zero_vector() {
    print_test "Edge case: Zero vector"

    local cql="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'zero_vector', [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test15_output.txt" 2>&1; then
        log_pass "Zero vector insert"
    else
        log_fail "Zero vector insert" "$(cat "$TEMP_DIR/test15_output.txt")"
    fi
}

################################################################################
# Test 16: Edge Case - Very Small Values
################################################################################

test_edge_case_tiny_values() {
    print_test "Edge case: Very small floating point values"

    local cql="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'tiny_values', [1.0e-10, 1.0e-20, 1.0e-30, 1.0e-38, 1.0e-38, 1.0e-38, 1.0e-38, 1.0e-38]);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test16_output.txt" 2>&1; then
        log_pass "Very small floating point values insert"
    else
        log_fail "Very small floating point values insert" "$(cat "$TEMP_DIR/test16_output.txt")"
    fi
}

################################################################################
# Test 17: Edge Case - Very Large Values
################################################################################

test_edge_case_large_values() {
    print_test "Edge case: Very large floating point values"

    local cql="INSERT INTO $KEYSPACE.test_vectors_8d (id, name, vec) VALUES (uuid(), 'large_values', [1.0e10, 1.0e20, 1.0e30, 1.0e38, 1.0e38, 1.0e38, 1.0e38, 1.0e38]);"

    if astra db cqlsh exec "$DB_NAME" "$cql" > "$TEMP_DIR/test17_output.txt" 2>&1; then
        log_pass "Very large floating point values insert"
    else
        log_fail "Very large floating point values insert" "$(cat "$TEMP_DIR/test17_output.txt")"
    fi
}

################################################################################
# Results Summary
################################################################################

print_results() {
    print_header "Test Results Summary"

    echo ""
    echo "Total Tests: $TOTAL_TESTS"
    echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
    echo -e "${RED}Failed: $FAILED_TESTS${NC}"
    echo ""

    if [ $FAILED_TESTS -eq 0 ]; then
        echo -e "${GREEN}✓ All tests passed!${NC}"
        echo ""
        return 0
    else
        echo -e "${RED}✗ Some tests failed. See details above.${NC}"
        echo ""
        echo "Failed tests:"
        grep "^FAIL:" "$RESULTS_FILE" | sed 's/^FAIL: /  - /'
        echo ""
        return 1
    fi
}

################################################################################
# Main Execution
################################################################################

main() {
    setup
    create_test_tables

    print_header "Running Tests"

    # Section 1: Inline CQL Execution
    echo ""
    echo -e "${BLUE}═══ Section 1: Inline CQL Execution ═══${NC}"
    test_inline_simple_vector
    test_inline_special_chars
    test_inline_shell_escaping

    # Section 2: File-Based CQL Execution
    echo ""
    echo -e "${BLUE}═══ Section 2: File-Based CQL Execution ═══${NC}"
    test_file_simple_inserts
    test_file_batch_statements

    # Section 3: Large Vector Handling
    echo ""
    echo -e "${BLUE}═══ Section 3: Large Vector Handling ═══${NC}"
    test_large_vector_384_inline
    test_large_vector_384_file
    test_large_vector_768_file
    test_large_vector_1024_file

    # Section 4: Shell Escaping Edge Cases
    echo ""
    echo -e "${BLUE}═══ Section 4: Shell Escaping Edge Cases ═══${NC}"
    test_escaping_negative_numbers
    test_escaping_scientific_notation
    test_escaping_quoted_strings

    # Section 5: Comparison Tests
    echo ""
    echo -e "${BLUE}═══ Section 5: Performance Comparison ═══${NC}"
    test_inline_vs_file_comparison

    # Section 6: Query Operations
    echo ""
    echo -e "${BLUE}═══ Section 6: Query Operations ═══${NC}"
    test_vector_retrieval

    # Section 7: Edge Cases
    echo ""
    echo -e "${BLUE}═══ Section 7: Edge Cases ═══${NC}"
    test_edge_case_zero_vector
    test_edge_case_tiny_values
    test_edge_case_large_values

    # Print results
    echo ""
    print_results
}

# Run the test suite
main

exit $?
