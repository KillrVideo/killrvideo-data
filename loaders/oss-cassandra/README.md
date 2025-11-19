# OSS Cassandra 5.0+ Data Loader

This directory contains tools for loading KillrVideo data into **Apache Cassandra 5.0+** or compatible distributions (OSS only, not Astra).

## Prerequisites

### 1. Cassandra 5.0+ Cluster

You need a running Cassandra 5.0+ cluster. The schema uses features specific to Cassandra 5.0:
- **Storage-Attached Indexes (SAI)** for flexible querying
- **Vector types** (`vector<float, N>`) for similarity search
- **Data masking functions** for PII protection

To set up Cassandra locally:

```bash
# Using Docker (easiest)
docker run --name cassandra -p 9042:9042 -d cassandra:5.0

# Wait for Cassandra to start (30-60 seconds)
docker logs -f cassandra
# Look for "state jump to NORMAL"

# Verify connectivity
docker exec -it cassandra cqlsh
```

### 2. Load the Schema

Before loading data, create the keyspace and tables:

```bash
# Using cqlsh
cqlsh -f ../../data/schemas/schema-v5.cql

# Or with Docker
docker cp ../../data/schemas/schema-v5.cql cassandra:/tmp/
docker exec -it cassandra cqlsh -f /tmp/schema-v5.cql
```

### 3. Install DSBulk

DSBulk is DataStax's high-performance bulk loader for Cassandra.

**Download:**
```bash
cd /tmp
curl -OL https://downloads.datastax.com/dsbulk/dsbulk-1.11.0.tar.gz
tar -xzf dsbulk-1.11.0.tar.gz
sudo mv dsbulk-1.11.0 /opt/
sudo ln -s /opt/dsbulk-1.11.0/bin/dsbulk /usr/local/bin/dsbulk
```

**Verify installation:**
```bash
dsbulk --version
# Should show: DataStax Bulk Loader v1.11.0 or higher
```

**Why DSBulk 1.11.0+?**
- Vector type support (required for `vector<float, N>` columns)
- Improved error handling
- Better performance tuning options

## Quick Start

### Option 1: DSBulk Script (Recommended for bulk loading)

The fastest way to load all data:

```bash
cd loaders/oss-cassandra/

# Basic usage (localhost, no auth)
./load_with_dsbulk.sh

# With authentication
export CASSANDRA_USER="myuser"
export CASSANDRA_PASSWORD="mypassword"
./load_with_dsbulk.sh

# Remote cluster
export CASSANDRA_HOST="10.0.0.5"
export CASSANDRA_PORT="9042"
./load_with_dsbulk.sh

# Custom keyspace
export KEYSPACE="my_killrvideo"
./load_with_dsbulk.sh
```

**Load time:** ~1-2 minutes for full dataset (1,800+ rows across 13 tables)

### Option 2: Python Script (Coming soon)

Python alternative with more flexibility for transformations and error handling.

```bash
# Install dependencies
pip install -r ../requirements.txt

# Load data
python load_with_python.py --host 127.0.0.1 --keyspace killrvideo
```

## Configuration

### Environment Variables

Configure the DSBulk loader with these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CASSANDRA_HOST` | `127.0.0.1` | Contact point IP/hostname |
| `CASSANDRA_PORT` | `9042` | Native transport port |
| `CASSANDRA_DC` | `datacenter1` | Local datacenter name |
| `CASSANDRA_USER` | _(none)_ | Username (if auth enabled) |
| `CASSANDRA_PASSWORD` | _(none)_ | Password (if auth enabled) |
| `KEYSPACE` | `killrvideo` | Target keyspace |
| `CSV_DIR` | `../../data/csv` | Path to CSV files |
| `DSBULK_BIN` | `dsbulk` | DSBulk binary location |

### Performance Tuning

Edit values in `load_with_dsbulk.sh`:

```bash
CONNECTION_POOL_SIZE=16          # Connections per host
MAX_PER_SECOND=50000             # Max operations/sec (OSS can handle high throughput)
MAX_ERRORS=100                   # Stop after N errors
MAX_RETRIES=3                    # Retry failed operations
```

**Recommendations:**
- **Small clusters (1-3 nodes):** Keep defaults
- **Large clusters (4+ nodes):** Increase `CONNECTION_POOL_SIZE` to 32-64
- **High-latency networks:** Increase `MAX_RETRIES` to 5-10

## Data Loading Order

The script loads tables in **dependency order** to respect foreign key relationships:

### Level 1: Independent Tables
- `users` - User profiles
- `tags` - Tag definitions

### Level 2: Depends on Users
- `user_credentials` - Authentication data
- `user_preferences` - User preferences with tag vectors
- `videos` - Video metadata with content vectors

### Level 3: Depends on Videos
- `latest_videos` - Recent videos (denormalized)
- `video_playback_stats` (**counter table**)
- `video_ratings` (**counter table**)

### Level 4: Depends on Users + Videos
- `comments` - Video comments
- `comments_by_user` - Comments by user (denormalized)
- `video_ratings_by_user` - Individual ratings

### Level 5: Depends on Tags
- `tag_counts` (**counter table**)

**Total:** 13 tables

## Important Notes

### 1. Vector Columns are NULL

The CSV files have **empty vector columns** by design:
- `videos.content_features` (vector<float, 16>)
- `tags.tag_vector` (vector<float, 8>)
- `user_preferences.preference_vector` (vector<float, 16>)

**Why?** Vectors require embedding models which are environment-specific.

**To populate vectors:**

Option A: Use a local embedding model (sentence-transformers, etc.)
```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')  # 384-dim
# You'll need to reduce dimensions to match schema (16-dim, 8-dim)

# Generate embeddings for each text field
# Update rows with CQL UPDATE statements
```

Option B: Use the Astra loader with Vectorize (automatic)
- See `loaders/astra-tables/` for automatic embedding generation

### 2. Counter Tables

Three tables use Cassandra counters:
- `video_playback_stats`
- `video_ratings`
- `tag_counts`

**Key differences:**
- Use `UPDATE` syntax, not `INSERT`
- Cannot mix counters with regular columns
- Cannot use `TRUNCATE` (use `DELETE` instead)

The script handles this automatically with the `load_counter_table()` function.

### 3. Collection Types

Some tables use CQL collections:
- **Sets:** `tags.related_tags` → `set<text>`
- **Maps:** `user_preferences.tag_preferences` → `map<text, float>`

**CSV Format:**
```csv
# Set example
"{""cassandra"",""database"",""nosql""}"

# Map example
"{""cassandra"":0.95,""astra"":0.87}"
```

DSBulk automatically parses these with the proper escape settings.

## Verification

After loading, verify the data:

### Row Counts

```cql
SELECT COUNT(*) FROM killrvideo.users;           -- Should be ~150
SELECT COUNT(*) FROM killrvideo.videos;          -- Should be ~300-500
SELECT COUNT(*) FROM killrvideo.comments;        -- Should be ~500-1500
SELECT COUNT(*) FROM killrvideo.tags;            -- Should be ~30-50
```

### Sample Queries

```cql
-- Get a user
SELECT * FROM killrvideo.users LIMIT 1;

-- Get videos with their tags
SELECT videoid, name, description, tags FROM killrvideo.videos LIMIT 5;

-- Check counter values
SELECT * FROM killrvideo.video_playback_stats LIMIT 5;

-- User preferences with tag preferences
SELECT userid, tag_preferences FROM killrvideo.user_preferences LIMIT 5;
```

### Check Vector Columns (should be NULL)

```cql
SELECT videoid, name, content_features FROM killrvideo.videos LIMIT 5;
-- content_features should be NULL for all rows
```

## Troubleshooting

### "Connection refused" error

**Problem:** Cannot connect to Cassandra

**Solutions:**
1. Verify Cassandra is running: `docker ps` or `nodetool status`
2. Check port: `netstat -an | grep 9042`
3. Verify host/port in environment variables
4. Check firewall rules

### "Keyspace does not exist" error

**Problem:** Schema not loaded

**Solution:**
```bash
cqlsh -f ../../data/schemas/schema-v5.cql
```

### "Authentication failed" error

**Problem:** Wrong credentials or auth not configured

**Solutions:**
1. If auth is enabled, set `CASSANDRA_USER` and `CASSANDRA_PASSWORD`
2. If auth is disabled, unset those variables:
   ```bash
   unset CASSANDRA_USER CASSANDRA_PASSWORD
   ```

### "Invalid vector dimension" error

**Problem:** CSV has vector data with wrong dimensions

**Solution:** This shouldn't happen with the provided CSVs (vectors are NULL). If you populated vectors, ensure dimensions match schema:
- `content_features`: 16 dimensions
- `tag_vector`: 8 dimensions
- `preference_vector`: 16 dimensions

### "Too many errors" message

**Problem:** CSV format doesn't match schema

**Solutions:**
1. Check DSBulk logs in `./load-logs/[table-name]/`
2. Look for parsing errors in operation logs
3. Verify CSV file encoding (should be UTF-8)
4. Check for trailing commas or malformed collection values

### Slow load performance

**Problem:** Taking longer than expected

**Solutions:**
1. Increase `CONNECTION_POOL_SIZE` in script
2. Increase `MAX_PER_SECOND` (default is conservative)
3. Check Cassandra cluster health: `nodetool status`
4. Monitor CPU/disk on Cassandra nodes
5. For Docker: allocate more resources to container

## Performance Benchmarks

Typical load times on different setups:

| Setup | Tables | Rows | Time | Notes |
|-------|--------|------|------|-------|
| Docker (local, 1 node) | 13 | 1,800 | ~2 min | Default settings |
| 3-node cluster (local) | 13 | 1,800 | ~1 min | RF=3 |
| Remote cluster (WAN) | 13 | 1,800 | ~5 min | High latency |

**Variables:**
- CPU/disk performance
- Network latency
- Replication factor
- Concurrent loaders

## Next Steps

1. **Verify data:** Run example queries from `../../examples/schema-v5-query-examples.cql`

2. **Populate vectors:** Choose an embedding model and populate the NULL vector columns

3. **Test SAI indexes:** The schema includes SAI indexes on multiple columns for flexible querying

4. **Explore data masking:** Cassandra 5.0 supports data masking for PII fields

## Related Documentation

- [Main README](../../README.md) - Repository overview
- [Schema v5](../../data/schemas/schema-v5.cql) - Full schema with comments
- [Query Examples](../../examples/schema-v5-query-examples.cql) - Sample queries
- [Astra Tables Loader](../astra-tables/) - Load with automatic vectorization
- [Astra Collections Loader](../astra-collections/) - Load as JSON documents

## Support

For issues with this loader:
1. Check logs in `./load-logs/`
2. Review error messages in terminal output
3. Consult [DSBulk documentation](https://docs.datastax.com/en/dsbulk/)
4. Open an issue on the KillrVideo GitHub repository
