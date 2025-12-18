#!/usr/bin/env bash
#
# DSBulk Vector Compatibility Test Suite
#
# Tests vector data loading capabilities for Astra DB using DSBulk 1.11.0+
# Validates various CSV and JSON formats for vector columns across multiple dimensions
#
# Usage:
#   ./test_dsbulk.sh <secure-connect-bundle.zip> [astra-token]
#
# Arguments:
#   secure-connect-bundle.zip : Path to Astra DB secure connect bundle
#   astra-token              : Astra DB application token (optional, uses ASTRA_DB_APPLICATION_TOKEN env var if not provided)
#
# Requirements:
#   - DSBulk 1.11.0 or higher (vector support required)
#   - Astra DB database with default_keyspace
#   - Tables created from schema.cql
#
# Exit codes:
#   0 : All tests passed
#   1 : One or more tests failed
#   2 : Missing requirements or arguments

set -euo pipefail

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
SKIPPED_TESTS=0

# Test results array
declare -a TEST_RESULTS

# Temp directory for test files
TEST_DIR=$(mktemp -d)
trap 'rm -rf "$TEST_DIR"' EXIT

#
# Utility Functions
#

print_header() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================${NC}"
}

print_section() {
    echo ""
    echo -e "${YELLOW}>>> $1${NC}"
}

print_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

print_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
}

print_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

#
# Argument Parsing
#

if [ $# -lt 1 ]; then
    echo "Usage: $0 <secure-connect-bundle.zip> [astra-token]"
    echo ""
    echo "Arguments:"
    echo "  secure-connect-bundle.zip : Path to Astra DB secure connect bundle"
    echo "  astra-token              : Astra DB application token (optional)"
    echo ""
    echo "Environment Variables:"
    echo "  ASTRA_DB_APPLICATION_TOKEN : Astra DB application token (used if not provided as argument)"
    exit 2
fi

BUNDLE_PATH="$1"
ASTRA_TOKEN="${2:-${ASTRA_DB_APPLICATION_TOKEN:-}}"

if [ -z "$ASTRA_TOKEN" ]; then
    echo -e "${RED}ERROR: Astra DB token not provided${NC}"
    echo "Provide token as second argument or set ASTRA_DB_APPLICATION_TOKEN environment variable"
    exit 2
fi

if [ ! -f "$BUNDLE_PATH" ]; then
    echo -e "${RED}ERROR: Secure connect bundle not found: $BUNDLE_PATH${NC}"
    exit 2
fi

#
# Prerequisites Check
#

check_prerequisites() {
    print_header "Checking Prerequisites"

    # Check if dsbulk is installed
    if ! command -v dsbulk &> /dev/null; then
        echo -e "${RED}ERROR: dsbulk command not found${NC}"
        echo "Please install DSBulk from: https://downloads.datastax.com/#bulk-loader"
        exit 2
    fi

    # Check DSBulk version
    DSBULK_VERSION=$(dsbulk --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
    print_info "DSBulk version: $DSBULK_VERSION"

    # Parse version components
    VERSION_MAJOR=$(echo "$DSBULK_VERSION" | cut -d. -f1)
    VERSION_MINOR=$(echo "$DSBULK_VERSION" | cut -d. -f2)

    # Check if version is 1.11.0 or higher
    if [ "$VERSION_MAJOR" -lt 1 ] || ([ "$VERSION_MAJOR" -eq 1 ] && [ "$VERSION_MINOR" -lt 11 ]); then
        echo -e "${RED}ERROR: DSBulk 1.11.0+ required for vector support (found $DSBULK_VERSION)${NC}"
        exit 2
    fi

    print_pass "DSBulk version check ($DSBULK_VERSION >= 1.11.0)"
    print_pass "Secure connect bundle: $BUNDLE_PATH"
    print_pass "Token configured"
}

#
# Test Execution
#

run_test() {
    local test_name="$1"
    local csv_file="$2"
    local table_name="$3"
    local should_pass="${4:-true}"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    print_section "Test $TOTAL_TESTS: $test_name"

    # Create unique error log for this test
    local error_log="$TEST_DIR/error_${TOTAL_TESTS}.log"

    # Run DSBulk load command
    if dsbulk load \
        -url "$csv_file" \
        -k default_keyspace \
        -t "$table_name" \
        -b "$BUNDLE_PATH" \
        -u token \
        -p "$ASTRA_TOKEN" \
        -header true \
        --log.directory "$TEST_DIR/logs_${TOTAL_TESTS}" \
        > "$error_log" 2>&1; then

        if [ "$should_pass" = "true" ]; then
            print_pass "$test_name"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            TEST_RESULTS+=("PASS: $test_name")
        else
            print_fail "$test_name (expected to fail but passed)"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            TEST_RESULTS+=("FAIL: $test_name (expected to fail)")
        fi
    else
        if [ "$should_pass" = "false" ]; then
            print_pass "$test_name (correctly failed as expected)"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            TEST_RESULTS+=("PASS: $test_name (expected failure)")
        else
            print_fail "$test_name"
            echo "Error output:"
            cat "$error_log" | head -20
            FAILED_TESTS=$((FAILED_TESTS + 1))
            TEST_RESULTS+=("FAIL: $test_name")
        fi
    fi
}

run_json_test() {
    local test_name="$1"
    local json_file="$2"
    local table_name="$3"
    local should_pass="${4:-true}"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    print_section "Test $TOTAL_TESTS: $test_name"

    # Create unique error log for this test
    local error_log="$TEST_DIR/error_${TOTAL_TESTS}.log"

    # Run DSBulk load command with JSON connector
    if dsbulk load \
        -url "$json_file" \
        -k default_keyspace \
        -t "$table_name" \
        -b "$BUNDLE_PATH" \
        -u token \
        -p "$ASTRA_TOKEN" \
        --connector.json.mode MULTI_DOCUMENT \
        --log.directory "$TEST_DIR/logs_${TOTAL_TESTS}" \
        > "$error_log" 2>&1; then

        if [ "$should_pass" = "true" ]; then
            print_pass "$test_name"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            TEST_RESULTS+=("PASS: $test_name")
        else
            print_fail "$test_name (expected to fail but passed)"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            TEST_RESULTS+=("FAIL: $test_name (expected to fail)")
        fi
    else
        if [ "$should_pass" = "false" ]; then
            print_pass "$test_name (correctly failed as expected)"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            TEST_RESULTS+=("PASS: $test_name (expected failure)")
        else
            print_fail "$test_name"
            echo "Error output:"
            cat "$error_log" | head -20
            FAILED_TESTS=$((FAILED_TESTS + 1))
            TEST_RESULTS+=("FAIL: $test_name")
        fi
    fi
}

#
# Test Data Generators
#

generate_8d_vector() {
    echo "[1.0, 0.5, 0.25, 0.125, 0.0625, 0.03125, 0.015625, 0.0078125]"
}

generate_384d_vector() {
    # Generate a simple 384-dimensional vector with pattern
    local values=""
    for i in $(seq 1 384); do
        if [ $i -eq 384 ]; then
            values="${values}$(awk "BEGIN {printf \"%.6f\", $i/384.0}")"
        else
            values="${values}$(awk "BEGIN {printf \"%.6f\", $i/384.0}"),"
        fi
    done
    echo "[$values]"
}

generate_768d_vector() {
    # Generate a simple 768-dimensional vector with pattern
    local values=""
    for i in $(seq 1 768); do
        if [ $i -eq 768 ]; then
            values="${values}$(awk "BEGIN {printf \"%.6f\", $i/768.0}")"
        else
            values="${values}$(awk "BEGIN {printf \"%.6f\", $i/768.0}"),"
        fi
    done
    echo "[$values]"
}

generate_1024d_vector() {
    # Generate a simple 1024-dimensional vector with pattern
    local values=""
    for i in $(seq 1 1024); do
        if [ $i -eq 1024 ]; then
            values="${values}$(awk "BEGIN {printf \"%.6f\", $i/1024.0}")"
        else
            values="${values}$(awk "BEGIN {printf \"%.6f\", $i/1024.0}"),"
        fi
    done
    echo "[$values]"
}

#
# CSV Format Tests
#

test_csv_formats() {
    print_header "CSV Format Tests - 8 Dimensions"

    # Test 1: JSON array in quotes
    local csv1="$TEST_DIR/test_quoted_array_8d.csv"
    cat > "$csv1" <<EOF
id,name,vec
11111111-1111-1111-1111-111111111111,quoted_array,"$(generate_8d_vector)"
EOF
    run_test "8D: JSON array with quotes" "$csv1" "test_vectors_8d" true

    # Test 2: JSON array without quotes
    local csv2="$TEST_DIR/test_unquoted_array_8d.csv"
    cat > "$csv2" <<EOF
id,name,vec
22222222-2222-2222-2222-222222222222,unquoted_array,$(generate_8d_vector)
EOF
    run_test "8D: JSON array without quotes" "$csv2" "test_vectors_8d" true

    # Test 3: Scientific notation
    local csv3="$TEST_DIR/test_scientific_8d.csv"
    cat > "$csv3" <<EOF
id,name,vec
33333333-3333-3333-3333-333333333333,scientific_notation,"[1.0e-1, 2.0e-1, 3.0e-1, 4.0e-1, 5.0e-1, 6.0e-1, 7.0e-1, 8.0e-1]"
EOF
    run_test "8D: Scientific notation in vector" "$csv3" "test_vectors_8d" true

    # Test 4: Integer values
    local csv4="$TEST_DIR/test_integers_8d.csv"
    cat > "$csv4" <<EOF
id,name,vec
44444444-4444-4444-4444-444444444444,integers,"[1, 2, 3, 4, 5, 6, 7, 8]"
EOF
    run_test "8D: Integer values in vector" "$csv4" "test_vectors_8d" true

    # Test 5: Empty string for NULL
    local csv5="$TEST_DIR/test_empty_null_8d.csv"
    cat > "$csv5" <<EOF
id,name,vec
55555555-5555-5555-5555-555555555555,empty_null,
EOF
    run_test "8D: Empty string for NULL vector" "$csv5" "test_vectors_8d" true

    # Test 6: Literal "null" for NULL
    local csv6="$TEST_DIR/test_literal_null_8d.csv"
    cat > "$csv6" <<EOF
id,name,vec
66666666-6666-6666-6666-666666666666,literal_null,null
EOF
    run_test "8D: Literal 'null' for NULL vector" "$csv6" "test_vectors_8d" true

    # Test 7: Spaces in array
    local csv7="$TEST_DIR/test_spaces_8d.csv"
    cat > "$csv7" <<EOF
id,name,vec
77777777-7777-7777-7777-777777777777,spaces_in_array,"[ 1.0 , 0.5 , 0.25 , 0.125 , 0.0625 , 0.03125 , 0.015625 , 0.0078125 ]"
EOF
    run_test "8D: Spaces in array elements" "$csv7" "test_vectors_8d" true

    # Test 8: Negative values
    local csv8="$TEST_DIR/test_negative_8d.csv"
    cat > "$csv8" <<EOF
id,name,vec
88888888-8888-8888-8888-888888888888,negative_values,"[-0.5, -0.25, 0.0, 0.25, 0.5, -0.125, 0.125, 0.0]"
EOF
    run_test "8D: Negative values in vector" "$csv8" "test_vectors_8d" true
}

test_csv_384d() {
    print_header "CSV Format Tests - 384 Dimensions"

    # Test 1: Standard 384D vector with quotes
    local csv1="$TEST_DIR/test_quoted_384d.csv"
    cat > "$csv1" <<EOF
id,name,embedding
a1111111-1111-1111-1111-111111111111,quoted_384d,"$(generate_384d_vector)"
EOF
    run_test "384D: JSON array with quotes" "$csv1" "vectors_384" true

    # Test 2: 384D without quotes
    local csv2="$TEST_DIR/test_unquoted_384d.csv"
    cat > "$csv2" <<EOF
id,name,embedding
a2222222-2222-2222-2222-222222222222,unquoted_384d,$(generate_384d_vector)
EOF
    run_test "384D: JSON array without quotes" "$csv2" "vectors_384" true

    # Test 3: NULL vector
    local csv3="$TEST_DIR/test_null_384d.csv"
    cat > "$csv3" <<EOF
id,name,embedding
a3333333-3333-3333-3333-333333333333,null_384d,
EOF
    run_test "384D: Empty string for NULL vector" "$csv3" "vectors_384" true
}

test_csv_768d() {
    print_header "CSV Format Tests - 768 Dimensions"

    # Test 1: Standard 768D vector with quotes
    local csv1="$TEST_DIR/test_quoted_768d.csv"
    cat > "$csv1" <<EOF
id,name,embedding
b1111111-1111-1111-1111-111111111111,quoted_768d,"$(generate_768d_vector)"
EOF
    run_test "768D: JSON array with quotes" "$csv1" "vectors_768" true

    # Test 2: 768D without quotes
    local csv2="$TEST_DIR/test_unquoted_768d.csv"
    cat > "$csv2" <<EOF
id,name,embedding
b2222222-2222-2222-2222-222222222222,unquoted_768d,$(generate_768d_vector)
EOF
    run_test "768D: JSON array without quotes" "$csv2" "vectors_768" true
}

test_csv_1024d() {
    print_header "CSV Format Tests - 1024 Dimensions"

    # Test 1: Standard 1024D vector with quotes
    local csv1="$TEST_DIR/test_quoted_1024d.csv"
    cat > "$csv1" <<EOF
id,name,embedding
c1111111-1111-1111-1111-111111111111,quoted_1024d,"$(generate_1024d_vector)"
EOF
    run_test "1024D: JSON array with quotes" "$csv1" "vectors_1024" true

    # Test 2: 1024D without quotes
    local csv2="$TEST_DIR/test_unquoted_1024d.csv"
    cat > "$csv2" <<EOF
id,name,embedding
c2222222-2222-2222-2222-222222222222,unquoted_1024d,$(generate_1024d_vector)
EOF
    run_test "1024D: JSON array without quotes" "$csv2" "vectors_1024" true
}

#
# JSON Format Tests
#

test_json_formats() {
    print_header "JSON Format Tests"

    # Test 1: Array as native JSON array
    local json1="$TEST_DIR/test_native_array_8d.json"
    cat > "$json1" <<EOF
{"id": "d1111111-1111-1111-1111-111111111111", "name": "native_json_array", "vec": [1.0, 0.5, 0.25, 0.125, 0.0625, 0.03125, 0.015625, 0.0078125]}
EOF
    run_json_test "JSON: Native JSON array for vector" "$json1" "test_vectors_8d" true

    # Test 2: Array as string (should likely fail or need codec)
    local json2="$TEST_DIR/test_string_array_8d.json"
    cat > "$json2" <<EOF
{"id": "d2222222-2222-2222-2222-222222222222", "name": "string_json_array", "vec": "[1.0, 0.5, 0.25, 0.125, 0.0625, 0.03125, 0.015625, 0.0078125]"}
EOF
    run_json_test "JSON: Array as string (expected to fail)" "$json2" "test_vectors_8d" false

    # Test 3: NULL value in JSON
    local json3="$TEST_DIR/test_null_json_8d.json"
    cat > "$json3" <<EOF
{"id": "d3333333-3333-3333-3333-333333333333", "name": "null_vector", "vec": null}
EOF
    run_json_test "JSON: NULL value for vector" "$json3" "test_vectors_8d" true

    # Test 4: 384D native JSON array
    local json4="$TEST_DIR/test_native_384d.json"
    local vector_384=$(generate_384d_vector)
    cat > "$json4" <<EOF
{"id": "d4444444-4444-4444-4444-444444444444", "name": "native_json_384d", "embedding": $vector_384}
EOF
    run_json_test "JSON: Native JSON array for 384D vector" "$json4" "vectors_384" true
}

#
# Summary Report
#

print_summary() {
    print_header "Test Results Summary"

    echo ""
    echo "Total Tests:   $TOTAL_TESTS"
    echo -e "Passed:        ${GREEN}$PASSED_TESTS${NC}"
    echo -e "Failed:        ${RED}$FAILED_TESTS${NC}"
    echo -e "Skipped:       ${YELLOW}$SKIPPED_TESTS${NC}"
    echo ""

    if [ $FAILED_TESTS -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
    else
        echo -e "${RED}Some tests failed. Details:${NC}"
        echo ""
        for result in "${TEST_RESULTS[@]}"; do
            if [[ $result == FAIL* ]]; then
                echo -e "${RED}  - $result${NC}"
            fi
        done
    fi

    echo ""
    echo -e "${BLUE}Logs saved to: $TEST_DIR${NC}"
    echo ""
}

#
# Main Execution
#

main() {
    print_header "DSBulk Vector Compatibility Test Suite"
    echo "Target: Astra DB"
    echo "Bundle: $BUNDLE_PATH"
    echo ""

    check_prerequisites

    # Run all test suites
    test_csv_formats
    test_csv_384d
    test_csv_768d
    test_csv_1024d
    test_json_formats

    # Print summary
    print_summary

    # Exit with appropriate code
    if [ $FAILED_TESTS -eq 0 ]; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main
