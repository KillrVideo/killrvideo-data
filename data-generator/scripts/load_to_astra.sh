#!/bin/bash
#
# Load KillrVideo data into DataStax Astra DB using DSBulk
#
# Prerequisites:
# - DSBulk installed: https://docs.datastax.com/en/dsbulk/docs/install/install.html
# - Astra DB secure connect bundle downloaded
# - Astra DB client ID and secret created
#
# Usage:
#   ./scripts/load_to_astra.sh

set -e

# Configuration - UPDATE THESE VALUES
KEYSPACE="killrvideo"
SCB_PATH="$HOME/secure-connect-bundle.zip"  # Path to your secure connect bundle
CLIENT_ID="your_client_id_here"             # Your Astra DB client ID
CLIENT_SECRET="your_client_secret_here"     # Your Astra DB client secret
OUTPUT_DIR="./output"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "=================================================================="
echo "🚀 Loading KillrVideo Data into Astra DB"
echo "=================================================================="

# Validate configuration
if [ ! -f "$SCB_PATH" ]; then
    echo -e "${RED}❌ Secure connect bundle not found: $SCB_PATH${NC}"
    echo "   Download it from the Astra DB dashboard"
    exit 1
fi

if [ "$CLIENT_ID" == "your_client_id_here" ] || [ "$CLIENT_SECRET" == "your_client_secret_here" ]; then
    echo -e "${RED}❌ Client ID and Secret not configured${NC}"
    echo "   Edit this script and add your Astra DB credentials"
    exit 1
fi

if [ ! -d "$OUTPUT_DIR" ]; then
    echo -e "${RED}❌ Output directory not found: $OUTPUT_DIR${NC}"
    echo "   Run generate.py first to create the CSV files"
    exit 1
fi

# DSBulk common options
DSBULK_OPTS="-b $SCB_PATH -u $CLIENT_ID -p $CLIENT_SECRET --driver.advanced.connection.pool.local.size 16 --dsbulk.log.maxErrors 100"

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
dsbulk load -url "$OUTPUT_DIR/tags.csv" -k $KEYSPACE -t tags $DSBULK_OPTS \
  --connector.csv.quote '"' \
  --connector.csv.escape '"'

echo -e "${GREEN}➜ Loading comments...${NC}"
dsbulk load -url "$OUTPUT_DIR/comments.csv" -k $KEYSPACE -t comments $DSBULK_OPTS

echo -e "${GREEN}➜ Loading comments_by_user...${NC}"
dsbulk load -url "$OUTPUT_DIR/comments_by_user.csv" -k $KEYSPACE -t comments_by_user $DSBULK_OPTS

echo -e "${GREEN}➜ Loading video_ratings_by_user...${NC}"
dsbulk load -url "$OUTPUT_DIR/video_ratings_by_user.csv" -k $KEYSPACE -t video_ratings_by_user $DSBULK_OPTS

echo -e "${GREEN}➜ Loading user_preferences...${NC}"
dsbulk load -url "$OUTPUT_DIR/user_preferences.csv" -k $KEYSPACE -t user_preferences $DSBULK_OPTS \
  --connector.csv.quote '"' \
  --connector.csv.escape '"'

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
