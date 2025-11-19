#!/bin/bash

###############################################################################
# KillrVideo Data Loading Script - OSS Cassandra 5.0+
###############################################################################
# Loads CSV data into OSS Cassandra using DSBulk in dependency order
#
# Prerequisites:
#   - Cassandra 5.0+ cluster running
#   - Schema loaded from data/schemas/schema-v5.cql
#   - DSBulk 1.11.0+ installed (required for vector support)
#   - Cassandra accessible from this machine
#
# Usage:
#   ./load_with_dsbulk.sh
#
# Configuration:
#   Set environment variables to override defaults:
#   - CASSANDRA_HOST: Contact point (default: 127.0.0.1)
#   - CASSANDRA_PORT: Native transport port (default: 9042)
#   - CASSANDRA_DC: Local datacenter name (default: datacenter1)
#   - CASSANDRA_USER: Username (optional, for auth)
#   - CASSANDRA_PASSWORD: Password (optional, for auth)
#   - KEYSPACE: Keyspace name (default: killrvideo)
#   - CSV_DIR: Path to CSV files (default: ../../data/csv)
#   - DSBULK_BIN: Path to DSBulk binary (default: dsbulk)
###############################################################################

set -e  # Exit on error
set -o pipefail  # Catch errors in pipelines

# ============================================================================
# Configuration
# ============================================================================

CASSANDRA_HOST="${CASSANDRA_HOST:-127.0.0.1}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
CASSANDRA_DC="${CASSANDRA_DC:-datacenter1}"
CASSANDRA_USER="${CASSANDRA_USER:-}"
CASSANDRA_PASSWORD="${CASSANDRA_PASSWORD:-}"
KEYSPACE="${KEYSPACE:-killrvideo}"
CSV_DIR="${CSV_DIR:-../../data/csv}"
DSBULK_BIN="${DSBULK_BIN:-dsbulk}"
LOG_DIR="./load-logs"

# DSBulk Performance Settings
CONNECTION_POOL_SIZE=16
MAX_PER_SECOND=50000  # Higher for local OSS (no cloud throttling)
MAX_ERRORS=100
MAX_RETRIES=3

# ============================================================================
# Color Output
# ============================================================================

if [[ -t 1 ]]; then
  GREEN='\033[0;32m'
  RED='\033[0;31m'
  YELLOW='\033[1;33m'
  BLUE='\033[0;34m'
  BOLD='\033[1m'
  NC='\033[0m'
else
  GREEN=''
  RED=''
  YELLOW=''
  BLUE=''
  BOLD=''
  NC=''
fi

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
  echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warning() {
  echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_section() {
  echo ""
  echo -e "${BOLD}====================================================================${NC}"
  echo -e "${BOLD}  $1${NC}"
  echo -e "${BOLD}====================================================================${NC}"
  echo ""
}

# Build authentication arguments
build_auth_args() {
  local auth_args=""

  if [ -n "$CASSANDRA_USER" ] && [ -n "$CASSANDRA_PASSWORD" ]; then
    auth_args="-u \"$CASSANDRA_USER\" -p \"$CASSANDRA_PASSWORD\""
    log_info "Using authentication (username: ${CASSANDRA_USER})"
  else
    log_info "No authentication configured (using default)"
  fi

  echo "$auth_args"
}

# Function to load a regular table
load_table() {
  local table=$1
  local csv_file="${CSV_DIR}/${table}.csv"

  log_info "Loading table: ${YELLOW}${table}${NC}"

  if [ ! -f "$csv_file" ]; then
    log_error "CSV file not found: ${csv_file}"
    return 1
  fi

  # Count lines for progress reporting
  local line_count=$(wc -l < "$csv_file")
  log_info "CSV file has ${line_count} lines (including header)"

  # Get auth args
  local auth_args=$(build_auth_args)

  # Build base command
  local cmd="$DSBULK_BIN load \
    -url \"$csv_file\" \
    -k \"$KEYSPACE\" \
    -t \"$table\" \
    -h \"$CASSANDRA_HOST\" \
    --driver.advanced.connection.pool.local.size $CONNECTION_POOL_SIZE \
    --dsbulk.executor.maxPerSecond $MAX_PER_SECOND \
    --dsbulk.log.maxErrors $MAX_ERRORS \
    --driver.advanced.retry-policy.max-retries $MAX_RETRIES \
    -header true \
    -logDir \"${LOG_DIR}/${table}\" \
    --report-rate 10"

  # Add authentication if configured
  if [ -n "$auth_args" ]; then
    cmd="$cmd $auth_args"
  fi

  # Add special parameters for tables with complex data types
  case "$table" in
    tags|tags_cleaned|user_preferences|user_preferences_cleaned)
      # These tables have set/map columns with nested quotes
      cmd="$cmd --connector.csv.quote='\"' --connector.csv.escape='\"'"
      ;;
    videos|videos_cleaned)
      # Videos table may have missing optional fields
      cmd="$cmd --connector.csv.quote='\"' --connector.csv.escape='\"' --schema.allowMissingFields=true"
      ;;
  esac

  eval "$cmd" 2>&1 | tee "${LOG_DIR}/${table}_load.log"

  local exit_code=${PIPESTATUS[0]}

  if [ $exit_code -eq 0 ]; then
    log_success "${table} loaded successfully"
    echo ""
    return 0
  else
    log_error "${table} failed with exit code ${exit_code}"
    log_error "Check logs at: ${LOG_DIR}/${table}/"
    return 1
  fi
}

# Function to load counter table (requires UPDATE query)
load_counter_table() {
  local table=$1
  local csv_file="${CSV_DIR}/${table}.csv"
  local query=""

  log_info "Loading counter table: ${YELLOW}${table}${NC}"

  if [ ! -f "$csv_file" ]; then
    log_error "CSV file not found: ${csv_file}"
    return 1
  fi

  # Count lines for progress reporting
  local line_count=$(wc -l < "$csv_file")
  log_info "CSV file has ${line_count} lines (including header)"

  # Get auth args
  local auth_args=$(build_auth_args)

  # Define UPDATE queries for each counter table
  case "$table" in
    video_playback_stats)
      query="UPDATE ${KEYSPACE}.${table} SET views = views + :views, total_play_time = total_play_time + :total_play_time, complete_views = complete_views + :complete_views, unique_viewers = unique_viewers + :unique_viewers WHERE videoid = :videoid"
      ;;
    video_ratings)
      query="UPDATE ${KEYSPACE}.${table} SET rating_counter = rating_counter + :rating_counter, rating_total = rating_total + :rating_total WHERE videoid = :videoid"
      ;;
    tag_counts)
      query="UPDATE ${KEYSPACE}.${table} SET count = count + :count WHERE tag = :tag"
      ;;
    *)
      log_error "Unknown counter table: ${table}"
      return 1
      ;;
  esac

  log_info "Using UPDATE query for counter increments"

  local cmd="$DSBULK_BIN load \
    -url \"$csv_file\" \
    -h \"$CASSANDRA_HOST\" \
    -query \"$query\" \
    --driver.advanced.connection.pool.local.size \"$CONNECTION_POOL_SIZE\" \
    --dsbulk.executor.maxPerSecond \"$MAX_PER_SECOND\" \
    --dsbulk.log.maxErrors \"$MAX_ERRORS\" \
    --driver.advanced.retry-policy.max-retries \"$MAX_RETRIES\" \
    -header true \
    -logDir \"${LOG_DIR}/${table}\" \
    --report-rate 10"

  # Add authentication if configured
  if [ -n "$auth_args" ]; then
    cmd="$cmd $auth_args"
  fi

  eval "$cmd" 2>&1 | tee "${LOG_DIR}/${table}_load.log"

  local exit_code=${PIPESTATUS[0]}

  if [ $exit_code -eq 0 ]; then
    log_success "${table} loaded successfully"
    echo ""
    return 0
  else
    log_error "${table} failed with exit code ${exit_code}"
    log_error "Check logs at: ${LOG_DIR}/${table}/"
    return 1
  fi
}

# ============================================================================
# Prerequisite Checks
# ============================================================================

check_prerequisites() {
  log_section "Prerequisite Checks"

  local all_ok=true

  # Check CSV directory
  if [ ! -d "$CSV_DIR" ]; then
    log_error "CSV directory not found at: ${CSV_DIR}"
    all_ok=false
  else
    local csv_count=$(find "$CSV_DIR" -name "*.csv" -type f | wc -l)
    log_success "CSV directory found with ${csv_count} files"
  fi

  # Check DSBulk binary
  if ! command -v "$DSBULK_BIN" &> /dev/null; then
    log_error "DSBulk binary not found: ${DSBULK_BIN}"
    log_info "Install from: https://downloads.datastax.com/dsbulk/"
    log_info "Required version: 1.11.0+ (for vector support)"
    all_ok=false
  else
    log_success "DSBulk binary found: ${DSBULK_BIN}"

    # Try to get version
    if $DSBULK_BIN --version &>/dev/null; then
      local version=$($DSBULK_BIN --version 2>&1 | head -n1)
      log_info "DSBulk version: ${version}"
    fi
  fi

  # Test Cassandra connectivity
  log_info "Testing connection to ${CASSANDRA_HOST}:${CASSANDRA_PORT}..."

  if command -v cqlsh &> /dev/null; then
    local cqlsh_cmd="cqlsh $CASSANDRA_HOST $CASSANDRA_PORT"

    if [ -n "$CASSANDRA_USER" ] && [ -n "$CASSANDRA_PASSWORD" ]; then
      cqlsh_cmd="$cqlsh_cmd -u $CASSANDRA_USER -p $CASSANDRA_PASSWORD"
    fi

    if timeout 5 $cqlsh_cmd -e "DESCRIBE KEYSPACE $KEYSPACE" &>/dev/null; then
      log_success "Successfully connected to Cassandra"
      log_success "Keyspace '${KEYSPACE}' found"
    else
      log_error "Cannot connect to Cassandra at ${CASSANDRA_HOST}:${CASSANDRA_PORT}"
      log_error "Or keyspace '${KEYSPACE}' does not exist"
      log_info "Create keyspace from: data/schemas/schema-v5.cql"
      all_ok=false
    fi
  else
    log_warning "cqlsh not found - skipping connectivity test"
    log_info "Ensure Cassandra is running at ${CASSANDRA_HOST}:${CASSANDRA_PORT}"
  fi

  # Create log directory
  if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
    log_success "Created log directory: ${LOG_DIR}"
  else
    log_success "Log directory exists: ${LOG_DIR}"
  fi

  echo ""

  if [ "$all_ok" = false ]; then
    log_error "Prerequisite checks failed. Please resolve issues above."
    exit 1
  fi

  log_success "All prerequisites verified"
}

# ============================================================================
# Main Loading Sequence
# ============================================================================

load_all_data() {
  log_section "Starting Data Load Sequence"

  log_info "Cassandra Host: ${CASSANDRA_HOST}:${CASSANDRA_PORT}"
  log_info "Local Datacenter: ${CASSANDRA_DC}"
  log_info "Keyspace: ${KEYSPACE}"
  log_info "CSV Source: ${CSV_DIR}"
  log_info "Connection Pool Size: ${CONNECTION_POOL_SIZE}"
  log_info "Max Operations/Second: ${MAX_PER_SECOND}"
  log_info "Max Errors Allowed: ${MAX_ERRORS}"
  log_info "Max Retries: ${MAX_RETRIES}"

  echo ""

  # Track overall success
  local tables_loaded=0
  local tables_failed=0

  # -------------------------------------------------------------------------
  # LEVEL 1: Independent Tables
  # -------------------------------------------------------------------------
  log_section "LEVEL 1: Independent Tables"
  log_info "These tables have no foreign key dependencies"
  echo ""

  if load_table "users"; then ((tables_loaded++)); else ((tables_failed++)); fi
  if load_table "tags"; then ((tables_loaded++)); else ((tables_failed++)); fi

  # -------------------------------------------------------------------------
  # LEVEL 2: Tables Depending on Users
  # -------------------------------------------------------------------------
  log_section "LEVEL 2: Tables Depending on Users"
  log_info "These tables reference users.userid"
  echo ""

  if load_table "user_credentials"; then ((tables_loaded++)); else ((tables_failed++)); fi
  if load_table "user_preferences"; then ((tables_loaded++)); else ((tables_failed++)); fi
  if load_table "videos"; then ((tables_loaded++)); else ((tables_failed++)); fi

  # -------------------------------------------------------------------------
  # LEVEL 3: Tables Depending on Videos
  # -------------------------------------------------------------------------
  log_section "LEVEL 3: Tables Depending on Videos"
  log_info "These tables reference videos.videoid"
  echo ""

  if load_table "latest_videos"; then ((tables_loaded++)); else ((tables_failed++)); fi
  if load_counter_table "video_playback_stats"; then ((tables_loaded++)); else ((tables_failed++)); fi
  if load_counter_table "video_ratings"; then ((tables_loaded++)); else ((tables_failed++)); fi

  # -------------------------------------------------------------------------
  # LEVEL 4: Tables Depending on Users + Videos
  # -------------------------------------------------------------------------
  log_section "LEVEL 4: Tables Depending on Users + Videos"
  log_info "These tables reference both users.userid and videos.videoid"
  echo ""

  if load_table "comments"; then ((tables_loaded++)); else ((tables_failed++)); fi
  if load_table "comments_by_user"; then ((tables_loaded++)); else ((tables_failed++)); fi
  if load_table "video_ratings_by_user"; then ((tables_loaded++)); else ((tables_failed++)); fi

  # -------------------------------------------------------------------------
  # LEVEL 5: Tables Depending on Tags
  # -------------------------------------------------------------------------
  log_section "LEVEL 5: Tables Depending on Tags"
  log_info "These tables reference tags.tag"
  echo ""

  if load_counter_table "tag_counts"; then ((tables_loaded++)); else ((tables_failed++)); fi

  # -------------------------------------------------------------------------
  # Summary
  # -------------------------------------------------------------------------
  log_section "Load Summary"

  echo "Tables loaded successfully: ${tables_loaded}"
  echo "Tables failed: ${tables_failed}"
  echo ""

  if [ $tables_failed -eq 0 ]; then
    log_success "ALL TABLES LOADED SUCCESSFULLY"
    echo ""
    log_info "Next steps:"
    echo "  1. Verify data with: cqlsh ${CASSANDRA_HOST}"
    echo "  2. Check logs in: ${LOG_DIR}"
    echo "  3. Run example queries from: ../../examples/schema-v5-query-examples.cql"
    echo ""
    log_warning "Note: Vector columns are NULL in CSV data"
    log_info "To populate vectors, use a separate embedding generation step"
    echo ""
    return 0
  else
    log_error "SOME TABLES FAILED TO LOAD"
    echo ""
    log_info "Troubleshooting:"
    echo "  1. Check error logs in: ${LOG_DIR}"
    echo "  2. Review DSBulk operation logs for details"
    echo "  3. Verify CSV file formats match schema"
    echo "  4. Check Cassandra cluster health"
    echo ""
    return 1
  fi
}

# ============================================================================
# Main Execution
# ============================================================================

main() {
  log_section "KillrVideo Data Loading Script - OSS Cassandra 5.0+"

  echo "This script will load CSV data into your Cassandra cluster"
  echo "in the correct dependency order."
  echo ""

  # Run prerequisite checks
  check_prerequisites

  # Confirm before proceeding
  echo ""
  log_warning "This will load data into keyspace: ${KEYSPACE} on ${CASSANDRA_HOST}"
  read -p "Continue? (y/N): " -n 1 -r
  echo ""

  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_info "Load cancelled by user"
    exit 0
  fi

  # Record start time
  local start_time=$(date +%s)

  # Run the data loading
  if load_all_data; then
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    log_success "Data loading completed in ${duration} seconds"
    exit 0
  else
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    log_error "Data loading failed after ${duration} seconds"
    exit 1
  fi
}

# Run main function
main "$@"
