#!/bin/bash
#
# Load KillrVideo data into DataStax Astra DB using DSBulk
#
# Prerequisites:
# - DSBulk installed: https://docs.datastax.com/en/dsbulk/docs/install/install.html
# - Astra DB secure connect bundle downloaded
# - .env.local file configured (copy from .env.example)
#
# Usage:
#   ./scripts/load_to_astra.sh [--truncate]
#
# Options:
#   --truncate    Truncate all tables before loading data

set -e

# Parse command line arguments
TRUNCATE_TABLES=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --truncate)
            TRUNCATE_TABLES=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--truncate]"
            echo ""
            echo "Options:"
            echo "  --truncate    Truncate all tables before loading data"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load environment variables from .env.local
ENV_FILE="$PROJECT_DIR/.env.local"
if [ -f "$ENV_FILE" ]; then
    echo "📋 Loading configuration from .env.local"
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "❌ Configuration file not found: $ENV_FILE"
    echo "   Copy .env.example to .env.local and configure your Astra credentials"
    exit 1
fi

# Configuration from environment variables (with defaults)
KEYSPACE="${ASTRA_KEYSPACE:-killrvideo}"
SCB_PATH="${ASTRA_SCB_PATH:-$HOME/.astra/scb/secure-connect-bundle.zip}"
CLIENT_ID="${ASTRA_CLIENT_ID:-}"
CLIENT_SECRET="${ASTRA_CLIENT_SECRET:-}"
OUTPUT_DIR="${OUTPUT_DIR:-$PROJECT_DIR/output}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "=================================================================="
echo "🚀 Loading KillrVideo Data into Astra DB"
if [ "$TRUNCATE_TABLES" = true ]; then
    echo "   (with --truncate: tables will be cleared first)"
fi
echo "=================================================================="

# Validate configuration
if [ ! -f "$SCB_PATH" ]; then
    echo -e "${RED}❌ Secure connect bundle not found: $SCB_PATH${NC}"
    echo "   Download it from the Astra DB dashboard"
    exit 1
fi

if [ -z "$CLIENT_ID" ] || [ -z "$CLIENT_SECRET" ]; then
    echo -e "${RED}❌ Client ID and Secret not configured${NC}"
    echo "   Set ASTRA_CLIENT_ID and ASTRA_CLIENT_SECRET in .env.local"
    exit 1
fi

if [ ! -d "$OUTPUT_DIR" ]; then
    echo -e "${RED}❌ Output directory not found: $OUTPUT_DIR${NC}"
    echo "   Run generate.py first to create the CSV files"
    exit 1
fi

# DSBulk common options
# Note: maxCharsPerColumn increased to 16384 to handle 384-dim vector embeddings (~8000 chars)
DSBULK_OPTS="-b $SCB_PATH -u $CLIENT_ID -p $CLIENT_SECRET --driver.advanced.connection.pool.local.size 16 --dsbulk.log.maxErrors 100 --connector.csv.maxCharsPerColumn 16384"

# List of all tables to load (in order)
TABLES=(
    "users"
    "user_credentials"
    "videos"
    "latest_videos"
    "tags"
    "comments"
    "comments_by_user"
    "video_ratings_by_user"
    "user_preferences"
    "tag_counts"
    "video_ratings"
    "video_playback_stats"
)

# Function to truncate a table
truncate_table() {
    local table=$1
    echo -e "${BLUE}  Truncating $table...${NC}"
    astra db cqlsh "$ASTRA_DB_NAME" -k "$KEYSPACE" -e "TRUNCATE $table;" 2>/dev/null || true
}

# Truncate tables if requested
if [ "$TRUNCATE_TABLES" = true ]; then
    echo -e "\n${BLUE}🗑️  Truncating tables...${NC}\n"

    if [ -z "$ASTRA_DB_NAME" ]; then
        echo -e "${RED}❌ ASTRA_DB_NAME not set in .env.local${NC}"
        echo "   Required for truncate operation"
        exit 1
    fi

    for table in "${TABLES[@]}"; do
        truncate_table "$table"
    done
    echo -e "${GREEN}✅ All tables truncated${NC}\n"
fi

echo -e "\n${BLUE}📦 Loading tables...${NC}\n"

# Load regular tables
echo -e "${GREEN}➜ Loading users...${NC}"
dsbulk load -url "$OUTPUT_DIR/users.csv" -k $KEYSPACE -t users $DSBULK_OPTS

echo -e "${GREEN}➜ Loading user_credentials...${NC}"
dsbulk load -url "$OUTPUT_DIR/user_credentials.csv" -k $KEYSPACE -t user_credentials $DSBULK_OPTS

echo -e "${GREEN}➜ Loading videos...${NC}"
dsbulk load -url "$OUTPUT_DIR/videos.csv" -k $KEYSPACE -t videos $DSBULK_OPTS \
  --schema.allowMissingFields true

echo -e "${GREEN}➜ Loading latest_videos...${NC}"
dsbulk load -url "$OUTPUT_DIR/latest_videos.csv" -k $KEYSPACE -t latest_videos $DSBULK_OPTS

echo -e "${GREEN}➜ Loading tags...${NC}"
dsbulk load -url "$OUTPUT_DIR/tags.csv" -k $KEYSPACE -t tags $DSBULK_OPTS

echo -e "${GREEN}➜ Loading comments...${NC}"
dsbulk load -url "$OUTPUT_DIR/comments.csv" -k $KEYSPACE -t comments $DSBULK_OPTS

echo -e "${GREEN}➜ Loading comments_by_user...${NC}"
dsbulk load -url "$OUTPUT_DIR/comments_by_user.csv" -k $KEYSPACE -t comments_by_user $DSBULK_OPTS

echo -e "${GREEN}➜ Loading video_ratings_by_user...${NC}"
dsbulk load -url "$OUTPUT_DIR/video_ratings_by_user.csv" -k $KEYSPACE -t video_ratings_by_user $DSBULK_OPTS

echo -e "${GREEN}➜ Loading user_preferences...${NC}"
dsbulk load -url "$OUTPUT_DIR/user_preferences.csv" -k $KEYSPACE -t user_preferences $DSBULK_OPTS

# Load counter tables (requires special handling)
echo -e "\n${BLUE}📊 Loading counter tables...${NC}\n"

echo -e "${GREEN}➜ Loading tag_counts...${NC}"
# Note: Remove -k parameter when using fully-qualified table name in query
dsbulk load -url "$OUTPUT_DIR/tag_counts.csv" $DSBULK_OPTS \
  --dsbulk.schema.query "UPDATE $KEYSPACE.tag_counts SET count = count + :count WHERE tag = :tag"

echo -e "${GREEN}➜ Loading video_ratings...${NC}"
# Note: Remove -k parameter when using fully-qualified table name in query
dsbulk load -url "$OUTPUT_DIR/video_ratings.csv" $DSBULK_OPTS \
  --dsbulk.schema.query "UPDATE $KEYSPACE.video_ratings SET rating_counter = rating_counter + :rating_counter, rating_total = rating_total + :rating_total WHERE videoid = :videoid"

echo -e "${GREEN}➜ Loading video_playback_stats...${NC}"
# Note: Remove -k parameter when using fully-qualified table name in query
dsbulk load -url "$OUTPUT_DIR/video_playback_stats.csv" $DSBULK_OPTS \
  --dsbulk.schema.query "UPDATE $KEYSPACE.video_playback_stats SET views = views + :views, total_play_time = total_play_time + :total_play_time, complete_views = complete_views + :complete_views, unique_viewers = unique_viewers + :unique_viewers WHERE videoid = :videoid"

echo -e "\n=================================================================="
echo -e "✅ ${GREEN}Data loading complete!${NC}"
echo "=================================================================="
echo ""
echo "Next steps:"
echo "  1. Verify data in Astra DB dashboard or cqlsh"
echo "  2. Test queries from examples/ directory"
echo ""
