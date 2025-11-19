# Astra Collections Data Loader with Data API

This directory contains tools for loading KillrVideo data into **DataStax Astra DB Collections** using the Data API with automatic embedding generation via Astra Vectorize.

## Overview

This loader demonstrates:
- **Collections approach**: Schema-less JSON documents with flexible structure
- **Data API**: HTTP REST interface for CRUD operations
- **Automatic vectorization**: Astra generates embeddings server-side (no client code needed)
- **Automatic indexing**: All fields indexed automatically - no index management
- **Nested documents**: Join related tables into single documents

## Key Differences from Tables

| Feature | Collections (This Loader) | Tables |
|---------|--------------------------|--------|
| Schema | Flexible JSON documents | Fixed, typed columns |
| API | Data API (HTTP/REST) | CQL + Table API |
| Indexing | **Automatic on all fields** | Manual (SAI) |
| Vectorize | **Server-side automatic** | Client-side generation |
| Query | JSON-based filters | CQL queries |
| Joins | **Nested documents** | Denormalized tables |
| Best For | Evolving schemas, rapid development | Structured, stable schemas |

## Prerequisites

### 1. Astra Database

Create an Astra DB database:
1. Go to [astra.datastax.com](https://astra.datastax.com)
2. Create a new Serverless database
3. Note your Database ID and Region
4. **No schema loading needed** - Collections are schema-less!

### 2. Generate Astra Token

Generate an application token with **Database Administrator** permissions:
1. Astra Portal → Settings → Tokens
2. Create token with Database Administrator role
3. Save the token (starts with `AstraCS:`)

### 3. Get Data API Endpoint

From Astra Portal:
1. Database → Connect → Data API
2. Copy the API Endpoint URL
3. Format: `https://<database-id>-<region>.apps.astra.datastax.com`

### 4. Configure Embedding Provider (for Vectorize)

Since Collections use Astra's automatic vectorization, configure an embedding provider in Astra Portal:

#### Option 1: OpenAI (Recommended)

1. Get API key from [platform.openai.com](https://platform.openai.com)
2. Astra Portal → Database → Embedding Providers
3. Add OpenAI integration
4. Enter API key and select model (e.g., `text-embedding-3-small`)

#### Option 2: Hugging Face

1. Get API token from [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)
2. Astra Portal → Database → Embedding Providers
3. Add Hugging Face integration
4. Choose Serverless or Dedicated endpoint

#### Option 3: NVIDIA (Astra-hosted, no API key needed)

1. Astra Portal → Database → Embedding Providers
2. Select NVIDIA
3. Choose model (e.g., `nvidia/embed-qa-4`)
4. No API key required - hosted by DataStax

### 5. Install Python Dependencies

```bash
# Install all dependencies
pip install -r ../requirements.txt

# Or install specific package
pip install astrapy pyyaml
```

## Quick Start

### Step 1: Configure

Copy the example configuration:

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml`:

```yaml
astra:
  api_endpoint: "https://your-db-id-your-region.apps.astra.datastax.com"
  token: "AstraCS:your-token-here"
  namespace: "killrvideo"  # Astra calls keyspaces "namespaces" in Data API

embedding_provider:
  name: "openai"
  openai:
    api_key: "sk-your-api-key-here"
    model: "text-embedding-3-small"
```

### Step 2: Review Schema Mapping

The `schema_mapping.yaml` file defines how relational tables transform to JSON documents.

**Example:** Videos collection merges 3 tables:

```
videos (primary)
  + video_playback_stats (nested as "stats")
  + video_ratings (nested as "ratings")
  = Single JSON document
```

Resulting document:
```json
{
  "_id": "uuid-here",
  "name": "Introduction to Cassandra",
  "description": "Learn about distributed databases...",
  "$vectorize": "Learn about distributed databases...",
  "tags": ["cassandra", "database", "nosql"],
  "stats": {
    "views": 1250,
    "unique_viewers": 892
  },
  "ratings": {
    "rating_counter": 45,
    "rating_total": 185
  }
}
```

**Customize** `schema_mapping.yaml` to:
- Add/remove collections
- Change which tables to join
- Configure vectorize fields
- Adjust indexed fields (though all fields are auto-indexed)

### Step 3: Load Data

Run the loader:

```bash
# Load all collections
python load_to_collections.py --config config.yaml

# Drop existing collections first (CAUTION!)
python load_to_collections.py --config config.yaml --drop-existing
```

**Load time:** ~3-5 minutes for full dataset

### Step 4: Configure Vectorize (Important!)

After collections are created, configure Vectorize in Astra Portal:

1. Database → Collections → Select collection (e.g., `videos`)
2. Click **"Configure Embedding Provider"**
3. Select your provider (e.g., OpenAI)
4. Select model (e.g., `text-embedding-3-small`)
5. Configure to vectorize field: `description` (for videos) or `tag` (for tags)

**Note:** This step is required for vector search to work. The loader uses `$vectorize` field but Astra needs the provider configuration.

## Understanding Schema Mapping

### Collection Definitions

Each collection is defined in `schema_mapping.yaml`:

```yaml
collections:
  videos:
    primary_table: "videos"
    joins:
      - table: "video_playback_stats"
        join_on: "videoid"
        nest_as: "stats"
    vectorize:
      enabled: true
      text_field: "description"
```

**Key concepts:**
- `primary_table`: Main CSV source
- `joins`: Additional tables to merge
- `nest_as`: Name for nested object
- `vectorize`: Enable automatic embedding generation

### Automatic Indexing

Collections automatically index **all fields** at all levels:

- Top-level fields: `name`, `description`, `added_date`
- Nested fields: `stats.views`, `ratings.rating_total`
- Array elements: `tags[0]`, `tags[1]`

**No CREATE INDEX statements needed!**

### Benefits of Nested Documents

**Problem with tables:** Query "videos with high views and good ratings" requires:
```sql
-- Tables: Need 3 tables and multiple queries
SELECT v.* FROM videos v
  JOIN video_playback_stats s ON v.videoid = s.videoid
  JOIN video_ratings r ON v.videoid = r.videoid
WHERE s.views > 1000 AND r.rating_total / r.rating_counter > 4.0;
```

**Solution with collections:** Single document, single query:
```json
{
  "stats.views": {"$gt": 1000},
  "ratings": {
    "$expr": {"$gt": [{"$divide": ["$ratings.rating_total", "$ratings.rating_counter"]}, 4.0]}
  }
}
```

## Querying Collections

### Basic Queries

```python
from astrapy.db import AstraDB

db = AstraDB(token="...", api_endpoint="...")
videos = db.collection("videos")

# Find by field
results = videos.find({"name": "Introduction to Cassandra"})

# Find with nested field
results = videos.find({"stats.views": {"$gt": 1000}})

# Find in array
results = videos.find({"tags": "cassandra"})
```

### Vector Search

```python
# Find similar videos (Astra generates query embedding automatically)
results = videos.find(
    sort={"$vectorize": "distributed database tutorial"},
    limit=10
)

# Or use pre-computed vector
query_vector = [0.1, 0.2, ..., 0.9]
results = videos.find(
    sort={"$vector": query_vector},
    limit=10
)
```

### Hybrid Search (Vector + Filters)

```python
# Find similar videos with at least 1000 views
results = videos.find(
    filter={"stats.views": {"$gte": 1000}},
    sort={"$vectorize": "database performance optimization"},
    limit=10
)
```

## Configuration Details

### Schema Mapping Customization

#### Adding a New Collection

```yaml
collections:
  my_new_collection:
    primary_table: "my_table"
    id_field: "my_id_column"
    vectorize:
      enabled: true
      text_field: "my_text_column"
    joins:
      - table: "related_table"
        join_on: "shared_key"
        nest_as: "related_data"
```

#### Composite IDs

For tables with composite keys:

```yaml
collections:
  video_ratings:
    id_field: "_composite"  # Special marker
    # Generates: f"{userid}_{videoid}"
```

### Vectorize Provider Configuration

**Important:** Provider choice is **permanent** for a collection!

Once configured, you cannot change the provider without:
1. Exporting all data
2. Dropping the collection
3. Recreating with new provider
4. Re-importing data

Choose carefully based on:
- **Quality:** OpenAI > Hugging Face > others (generally)
- **Cost:** NVIDIA (free) > Hugging Face > OpenAI
- **Dimensions:** Varies by model (384-3072)
- **Latency:** NVIDIA (hosted) fastest, external APIs slower

## Verification

### Check Collections

```python
from astrapy.db import AstraDB

db = AstraDB(token="...", api_endpoint="...")

# List collections
collections = db.get_collections()
print(collections)
```

### Document Counts

```python
videos = db.collection("videos")
count = videos.count_documents({})
print(f"Videos: {count}")  # Should be ~300-500
```

### Sample Documents

```python
# Get a sample document
doc = videos.find_one({})
print(doc)

# Check if vector was generated
if "$vector" in doc:
    print(f"Vector dimensions: {len(doc['$vector'])}")
```

### Test Vector Search

```python
# Find similar content
similar = videos.find(
    sort={"$vectorize": "distributed database fundamentals"},
    limit=5
)

for doc in similar:
    print(f"{doc['name']}: {doc.get('description', '')[:100]}...")
```

## Troubleshooting

### "Collection creation failed"

**Problem:** Cannot create collection

**Solutions:**
- Check token has Database Administrator role
- Verify API endpoint is correct
- Check database is in Serverless mode (not Dedicated)

### "Vectorize not working"

**Problem:** Vector search returns no results or errors

**Causes:**
1. Vectorize not configured in Astra Portal
2. Provider credentials invalid
3. Text field is empty/null

**Solutions:**
1. Astra Portal → Collections → Configure Embedding Provider
2. Verify provider API key is valid
3. Check documents have text in vectorize field

### "Rate limiting errors"

**Problem:** `429 Too Many Requests` errors

**Solution:** Reduce batch size and concurrency:

```yaml
loading:
  batch_size: 10  # Reduce from 20
  max_concurrency: 3  # Reduce from 5
```

### "Document too large"

**Problem:** Document exceeds size limit

**Cause:** Data API has document size limits (~1MB)

**Solution:**
- Remove large fields from nested documents
- Store large content separately (e.g., video files in object storage)
- Reference by ID instead of embedding

### "Joins not working"

**Problem:** Nested documents are empty

**Causes:**
1. Join key mismatch (e.g., UUID vs string)
2. Related CSV file missing
3. No matching rows

**Debug:**
```python
# Check cache loading
print(loader.data_cache.keys())

# Verify join keys exist
primary_keys = set(primary_data.keys())
related_keys = set(related_data.keys())
print(f"Missing keys: {primary_keys - related_keys}")
```

## Performance Characteristics

### Load Performance

| Factor | Impact |
|--------|--------|
| Batch size | Higher = faster (up to rate limits) |
| Concurrency | Higher = faster (up to rate limits) |
| Vectorize | Slows loading (embedding generation) |
| Network latency | Affects all operations |

**Typical rates:**
- Without vectorize: ~100-200 docs/sec
- With vectorize: ~10-50 docs/sec (depends on provider)

### Query Performance

| Query Type | Performance |
|------------|-------------|
| Exact match | Very fast (~10ms) |
| Range query | Fast (~20-50ms) |
| Vector search | Moderate (~50-200ms) |
| Hybrid search | Moderate to slow (~100-500ms) |

**Tips:**
- Use filters to reduce result set before vector search
- Limit results (e.g., `limit=10`) for faster responses
- Consider caching frequent queries

## Comparison: When to Use Each Approach

### Use Collections When:

✅ **Schema is evolving** - Easy to add/remove fields
✅ **Rapid prototyping** - No schema management overhead
✅ **Document model fits** - Naturally nested data (e.g., user profile with nested preferences)
✅ **Automatic indexing desired** - Query any field without index management
✅ **Automatic vectorization desired** - Let Astra handle embeddings
✅ **JSON-native queries** - Application uses JSON APIs already

### Use Tables When:

✅ **Schema is stable** - Well-defined, unlikely to change
✅ **CQL compatibility needed** - Existing CQL applications
✅ **Control over indexing** - Specific performance tuning
✅ **Control over embeddings** - Custom embedding logic
✅ **Complex queries** - Need full CQL features
✅ **Migrating from OSS Cassandra** - Direct path from on-premises

## Cost Implications

### Storage

Collections may use more storage due to:
- Automatic indexing of all fields
- Vector storage
- Document structure overhead

**Estimate:** ~1.5-2x table storage

### Operations

- **Writes:** Same cost as tables
- **Reads:** Same cost as tables
- **Vector search:** May use more compute
- **Embeddings:** External provider costs (OpenAI, etc.)

**For KillrVideo dataset:** Easily within Astra free tier

## Next Steps

1. **Explore queries**: Test vector and hybrid search
2. **Experiment with schema**: Modify `schema_mapping.yaml` and reload
3. **Compare with Tables**: Load same data to `../astra-tables/` and compare
4. **Build application**: Use Collections for rapid app development

## Related Documentation

- [Main README](../../README.md) - Repository overview
- [Schema Mapping](./schema_mapping.yaml) - Collection definitions
- [Astra Tables Loader](../astra-tables/) - Structured schema alternative
- [Astra Data API Docs](https://docs.datastax.com/en/astra-db-serverless/api-reference/collections.html) - Official API reference
- [astrapy SDK](https://github.com/datastax/astrapy) - Python SDK documentation

## Support

For issues:
1. Verify configuration in `config.yaml` and `schema_mapping.yaml`
2. Check Astra Portal for database and provider status
3. Review error messages in terminal
4. Test with simple collection first (e.g., `users`)
5. Open an issue on the KillrVideo GitHub repository

## Examples

### Example 1: Load Only Videos Collection

Edit `schema_mapping.yaml` to comment out other collections, then run:

```bash
python load_to_collections.py
```

### Example 2: Custom Nested Structure

Modify `schema_mapping.yaml`:

```yaml
collections:
  users:
    joins:
      - table: "user_credentials"
        nest_as: "auth"
      - table: "user_preferences"
        nest_as: "prefs"
      - table: "videos"  # User's videos!
        join_on: "userid"
        nest_as: "uploaded_videos"
```

### Example 3: Query from Application

```python
# app.py
from astrapy.db import AstraDB

db = AstraDB(
    token=os.getenv("ASTRA_TOKEN"),
    api_endpoint=os.getenv("ASTRA_API_ENDPOINT")
)

videos = db.collection("videos")

# Search for videos
results = videos.find(
    filter={"tags": "cassandra"},
    sort={"added_date": -1},
    limit=10
)

for video in results:
    print(f"{video['name']} - {video['stats']['views']} views")
```

This demonstrates the power and flexibility of the Collections approach!
