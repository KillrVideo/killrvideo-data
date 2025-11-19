# Astra Tables Data Loader with Embedding Generation

This directory contains tools for loading KillrVideo data into **DataStax Astra DB** using Tables (structured schema) with client-side embedding generation for vector search.

## Overview

This loader demonstrates:
- **Tables approach**: Structured, typed schema with CQL compatibility
- **Client-side embedding generation**: Pre-compute vectors using OpenAI, Hugging Face, or other providers
- **Dimension reduction**: Adapt high-dimensional embeddings (384-1536D) to schema dimensions (8-16D)
- **Python SDK**: Full control over data loading and transformations

## Prerequisites

### 1. Astra Database

Create an Astra DB database:
1. Go to [astra.datastax.com](https://astra.datastax.com)
2. Create a new database
3. Create a keyspace named `killrvideo`
4. Download the Secure Connect Bundle

### 2. Load the Schema

Load the Astra-compatible schema:

```bash
# Using Astra CLI (recommended)
astra db cqlsh <database-name> -f ../../data/schemas/schema-astra.cql

# Or download and use cqlsh Docker
docker run --rm -it -v $(pwd):/workspace datastax/cqlsh:latest \
  -b /workspace/secure-connect-killrvideo.zip \
  -u token -p "$ASTRA_TOKEN" \
  -f /workspace/../../data/schemas/schema-astra.cql
```

### 3. Generate Astra Token

Generate an application token with **Database Administrator** permissions:
1. Astra Portal → Settings → Tokens
2. Create new token with Database Administrator role
3. Save the token (starts with `AstraCS:`)

### 4. Install Python Dependencies

```bash
# Install all dependencies
pip install -r ../requirements.txt

# Or install specific packages
pip install cassandra-driver pyyaml

# For OpenAI embeddings
pip install openai

# For Hugging Face embeddings
pip install sentence-transformers
```

### 5. Download Secure Connect Bundle

Download from Astra Portal:
1. Database → Connect → CQL Drivers
2. Download Secure Connect Bundle
3. Save as `secure-connect-killrvideo.zip` in this directory

## Quick Start

### Step 1: Configure

Copy the example configuration:

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` and fill in:

```yaml
astra:
  database_id: "your-database-id"
  region: "us-east-1"
  keyspace: "killrvideo"
  token: "AstraCS:your-token-here"
  secure_bundle_path: "./secure-connect-killrvideo.zip"

embedding_provider:
  name: "openai"  # or "huggingface"
  openai:
    api_key: "sk-your-api-key-here"
    model: "text-embedding-3-small"
```

### Step 2: Test Embedding Configuration

Verify your embedding provider is configured correctly:

```bash
python setup_embeddings.py --config config.yaml
```

This will:
- Test connection to your embedding provider
- Generate sample embeddings
- Verify dimensions match your schema
- Test dimension reduction

Expected output:
```
[SUCCESS] Provider initialized: openai
[INFO] Native dimensions: 1536
[SUCCESS] Generated embedding: 1536 dimensions
[INFO] Reduced to 16D for videos: [0.1234, -0.5678, ...]
[SUCCESS] All embedding tests passed!
```

### Step 3: Load Data

Run the loader:

```bash
# Load data with embedding generation
python load_with_embeddings.py --config config.yaml

# Skip embeddings (load NULL vectors)
python load_with_embeddings.py --config config.yaml --skip-embeddings
```

**Load time:** ~5-10 minutes for full dataset with embeddings (depends on API rate limits)

## Configuration Details

### Embedding Providers

#### OpenAI (Recommended)

```yaml
embedding_provider:
  name: "openai"
  openai:
    api_key: "sk-..."
    model: "text-embedding-3-small"  # 1536D default
    dimensions: 16  # Optional: native dimension reduction
```

**Pros:**
- High quality embeddings
- Native dimension reduction support
- Fast API

**Cons:**
- Requires paid API key
- Rate limits apply

#### Hugging Face (Open Source)

```yaml
embedding_provider:
  name: "huggingface"
  huggingface:
    model: "sentence-transformers/all-MiniLM-L12-v2"  # 384D
```

**Pros:**
- Free and open source
- Runs locally (no API calls)
- Many models available

**Cons:**
- Slower than API-based providers
- Requires model download (~120-500MB)
- Higher memory usage

### Vector Column Mappings

The loader generates embeddings for these vector columns:

| Table | Text Column | Vector Column | Dimensions | Purpose |
|-------|------------|---------------|------------|---------|
| videos | description | content_features | 16 | Content similarity search |
| tags | tag | tag_vector | 8 | Semantic tag search |
| user_preferences | _synthesized_preferences | preference_vector | 16 | User preference matching |

### Dimension Reduction

Since most embedding models produce 384-1536 dimensions but the schema uses 8-16 dimensions, we apply dimension reduction:

#### Truncate Method (Default)

```yaml
dimension_reduction:
  method: "truncate"
```

Simply uses the first N dimensions. Fast but loses information.

#### PCA Method (Better Quality)

```yaml
dimension_reduction:
  method: "pca"
```

Uses Principal Component Analysis to preserve more information. Requires `scikit-learn`:

```bash
pip install scikit-learn
```

#### Provider Method (OpenAI Only)

```yaml
embedding_provider:
  name: "openai"
  openai:
    dimensions: 16  # OpenAI does dimension reduction server-side
dimension_reduction:
  method: "provider"
```

Uses OpenAI's native dimension reduction (highest quality for OpenAI models).

## Understanding the Schema

The Astra schema uses:

### Storage-Attached Indexes (SAI)

Replaces traditional secondary indexes with more flexible indexing:

```sql
CREATE CUSTOM INDEX videos_tags_idx ON killrvideo.videos(tags)
  USING 'StorageAttachedIndex';
```

**Benefits:**
- Query any indexed column
- Better performance than secondary indexes
- No denormalized tables needed

### Vector Types

For similarity search:

```sql
CREATE TABLE videos (
  videoid uuid PRIMARY KEY,
  content_features vector<float, 16>  -- 16-dimensional vector
);
```

**Query with ANN search:**
```sql
SELECT * FROM videos
ORDER BY content_features ANN OF [0.1, 0.2, ..., 0.9]
LIMIT 10;
```

## Verification

After loading, verify the data:

### Row Counts

```sql
SELECT COUNT(*) FROM killrvideo.users;        -- ~150
SELECT COUNT(*) FROM killrvideo.videos;       -- ~300-500
SELECT COUNT(*) FROM killrvideo.comments;     -- ~500-1500
```

### Check Vectors

```sql
-- Verify vectors were generated
SELECT videoid, name, content_features
FROM killrvideo.videos
WHERE content_features IS NOT NULL
LIMIT 5;
```

### Test Vector Search

```sql
-- Find similar videos (using a video's own vector)
SELECT videoid, name
FROM killrvideo.videos
ORDER BY content_features ANN OF (
  SELECT content_features FROM killrvideo.videos WHERE videoid = <some-uuid>
)
LIMIT 10;
```

## Troubleshooting

### "Secure Connect Bundle not found"

**Problem:** Cannot find the bundle file

**Solution:**
```bash
# Download from Astra Portal or use Astra CLI
astra db download-scb <database-name> -f ./secure-connect-killrvideo.zip
```

### "Authentication failed"

**Problem:** Invalid or expired token

**Solution:**
1. Generate a new token from Astra Portal
2. Ensure it has Database Administrator role
3. Update `config.yaml` with new token

### "Embedding generation failed"

**Problem:** API key invalid or rate limited

**Solutions:**
- **OpenAI:** Verify API key at platform.openai.com
- **Rate limits:** Reduce batch size in config
- **Hugging Face:** Check model name is correct

### "Dimension mismatch"

**Problem:** Generated embeddings don't match schema dimensions

**Solution:**
Run `setup_embeddings.py` to verify configuration, or alter schema:

```sql
-- Change vector dimensions to match your model
ALTER TABLE killrvideo.videos ALTER content_features TYPE vector<float, 384>;
```

### Slow loading

**Problem:** Taking longer than expected

**Causes:**
1. **API rate limits:** Embedding generation is rate-limited by provider
2. **Network latency:** Astra cloud database may be in different region
3. **Batch size too small:** Increase in config.yaml

**Solutions:**
```yaml
loading:
  batch_size: 200  # Increase from default 100
  max_concurrency: 32  # Increase from 16
```

## Performance Tips

### Optimize Batch Size

```yaml
loading:
  batch_size: 100  # Start here
  # Increase if:
  # - Network latency is high
  # - No embedding generation (skip-embeddings)
  # - Astra throttling not occurring
```

### Parallel Loading

The loader uses connection pooling automatically. Tune in config:

```yaml
# In load_with_embeddings.py, edit:
CONNECTION_POOL_SIZE = 32  # Default: 16
```

### Skip Embeddings for Testing

Load data quickly without embeddings:

```bash
python load_with_embeddings.py --skip-embeddings
```

Populate vectors later using a separate script.

## Cost Considerations

### Embedding Generation Costs

**OpenAI text-embedding-3-small:**
- $0.02 per 1M tokens
- ~500 videos × 50 tokens/description = 25,000 tokens
- **Cost: ~$0.001** (negligible)

**Hugging Face:**
- Free (runs locally)
- Compute time only

### Astra Costs

Astra pricing is based on:
- Storage: $0.25/GB/month
- Reads/writes: Included in monthly quota
- KillrVideo dataset: ~2MB compressed, well within free tier

## Comparison: Tables vs Collections

| Aspect | Tables (This Loader) | Collections (Data API) |
|--------|---------------------|------------------------|
| Schema | Fixed, typed | Schema-less, flexible |
| Access | CQL + Table API | Data API only |
| Indexing | Manual (SAI) | Automatic (all fields) |
| Vectorize | Client-side | Server-side (automatic) |
| Joins | Denormalized tables | Nested documents |
| Query Language | CQL | JSON-based API |
| Use Case | Structured, predictable | Unstructured, evolving |

**When to use Tables:**
- You need CQL compatibility
- Schema is well-defined and stable
- You want control over embedding generation
- You're migrating from OSS Cassandra

**When to use Collections:**
- Schema is evolving
- You want automatic vectorization
- Document model fits your domain
- Rapid prototyping

## Next Steps

1. **Test vector search**: Run queries from `../../examples/schema-v5-query-examples.cql`
2. **Explore SAI indexes**: Query on any indexed column
3. **Try Collections**: Compare with `../astra-collections/` loader
4. **Production deployment**: Scale up Astra database as needed

## Related Documentation

- [Main README](../../README.md) - Repository overview
- [Astra Schema](../../data/schemas/schema-astra.cql) - Full schema
- [OSS Cassandra Loader](../oss-cassandra/) - On-premises alternative
- [Astra Collections Loader](../astra-collections/) - Document model approach
- [Astra Documentation](https://docs.datastax.com/en/astra-db-serverless/) - Official docs

## Support

For issues:
1. Check configuration in `config.yaml`
2. Run `setup_embeddings.py` to verify setup
3. Check Astra Portal for database health
4. Review error messages in terminal output
5. Open an issue on the KillrVideo GitHub repository
