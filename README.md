# KillrVideo Database Schemas & Data Loaders

Cassandra database schemas, sample data, and multi-platform data loaders for the KillrVideo reference application.

![](https://img.shields.io/badge/Cassandra-3.x_to_5.x-5C4D7D?logo=apache-cassandra)
![](https://img.shields.io/badge/DataStax-Astra_DB-29A8E0)
![](https://img.shields.io/badge/License-Apache_2.0-2599AF)

## 🎯 What's New

This repository now includes **comprehensive data loading tools** for multiple platforms:

- **OSS Cassandra 5.0+**: DSBulk and Python loaders for on-premises deployments
- **Astra Tables**: CQL-compatible tables with client-side embedding generation
- **Astra Collections**: Schema-less JSON documents with automatic vectorization

Each approach demonstrates different Cassandra capabilities and deployment patterns. Choose based on your use case!

## 🗂️ Repository Structure

```
killrvideo-data/
├── data/                       # Shared data source
│   ├── csv/                    # CSV files (~1.8MB, 1800+ rows)
│   └── schemas/                # Versioned CQL schemas
├── loaders/                    # Platform-specific data loaders
│   ├── oss-cassandra/          # OSS Cassandra 5.0+ loaders
│   ├── astra-tables/           # Astra with Tables (structured)
│   ├── astra-collections/      # Astra with Collections (JSON)
│   └── requirements.txt        # Python dependencies
├── examples/                   # Sample data and queries
├── graph/                      # Graph database schema (DSE only)
├── migrating/                  # Version migration guides
└── search/                     # Search integration (DSE only)
```

## 📊 Dataset Overview

The KillrVideo dataset includes:

- **150+ users** with authentication and preferences
- **300-500 videos** from real YouTube content (DataStax channel)
- **500-1,500 comments** with tech-themed content
- **30-50 tags** with relationships
- **Counters** for views, ratings, and tag usage
- **Vector columns** ready for embedding population (8-16 dimensions)

**Total size:** ~1.8MB compressed, perfect for learning and demos.

## 🚀 Quick Start Guide

### Step 1: Choose Your Platform

| Platform | Use Case | Setup Time | Best For |
|----------|----------|------------|----------|
| **[OSS Cassandra](#oss-cassandra-50)** | On-premises, full control | 10 min | Production deployments, learning Cassandra internals |
| **[Astra Tables](#astra-tables)** | Cloud, structured data | 5 min | CQL applications, migrating from OSS |
| **[Astra Collections](#astra-collections)** | Cloud, flexible schema | 5 min | Rapid prototyping, evolving schemas |

### Step 2: Follow Platform-Specific Instructions

Each platform has a dedicated loader with comprehensive documentation:

- 📘 [OSS Cassandra 5.0+ Loader](loaders/oss-cassandra/README.md)
- 📗 [Astra Tables Loader](loaders/astra-tables/README.md)
- 📙 [Astra Collections Loader](loaders/astra-collections/README.md)

## 🎯 Platform Selection Guide

### OSS Cassandra 5.0+

**✅ Choose this if:**
- Running on-premises or private cloud
- Need full control over infrastructure
- Want to learn Cassandra internals
- Have existing Cassandra clusters
- Require features not in Astra (UDFs, custom compaction, etc.)

**Features:**
- Vector types (`vector<float, N>`)
- Storage-Attached Indexes (SAI)
- Data masking for PII
- Full CQL feature set

**Loading approach:**
- DSBulk (recommended for bulk loading)
- Python SDK (more flexibility)

**Vector embeddings:**
- Pre-compute client-side or load NULL vectors

**Get started:** → [OSS Cassandra Loader](loaders/oss-cassandra/README.md)

---

### Astra Tables

**✅ Choose this if:**
- Want cloud-native managed Cassandra
- Need CQL compatibility for existing apps
- Prefer structured, typed schema
- Want control over embedding generation
- Migrating from OSS Cassandra

**Features:**
- Fully managed, serverless
- CQL binary protocol + Table API
- Storage-Attached Indexes (SAI)
- Manual index management
- Client-side embedding generation

**Loading approach:**
- Python SDK with cassandra-driver
- Configure embedding provider (OpenAI, Hugging Face, etc.)
- Pre-compute vectors during loading

**Vector embeddings:**
- Generated client-side
- Supports dimension reduction (e.g., 1536D → 16D)
- Multiple provider options

**Get started:** → [Astra Tables Loader](loaders/astra-tables/README.md)

---

### Astra Collections

**✅ Choose this if:**
- Building new applications rapidly
- Schema is evolving or unknown
- Want automatic vector generation
- Prefer JSON/document model
- Don't need CQL compatibility

**Features:**
- Schema-less JSON documents
- **Automatic indexing** of all fields
- **Automatic vectorization** (Astra Vectorize)
- Data API (HTTP REST)
- Nested documents (no denormalization needed)

**Loading approach:**
- Python SDK with astrapy
- Transform relational data to JSON documents
- Let Astra generate embeddings automatically

**Vector embeddings:**
- **Automatic server-side generation**
- Configure provider once in Astra Portal
- No client-side embedding code needed

**Get started:** → [Astra Collections Loader](loaders/astra-collections/README.md)

---

## 📋 Detailed Comparison

| Feature | OSS Cassandra | Astra Tables | Astra Collections |
|---------|---------------|--------------|-------------------|
| **Deployment** | Self-managed | Managed (cloud) | Managed (cloud) |
| **Schema** | Fixed CQL types | Fixed CQL types | Flexible JSON |
| **Query Language** | CQL | CQL + Table API | Data API (JSON) |
| **Indexing** | Manual (SAI) | Manual (SAI) | **Automatic** |
| **Vectorization** | Manual | Client-side | **Server-side** |
| **Denormalization** | Required | Required | **Not needed** |
| **Setup Complexity** | High | Medium | Low |
| **Learning Curve** | Steep | Moderate | Gentle |
| **Cost** | Infrastructure | Pay-as-you-go | Pay-as-you-go |
| **CQL Compatibility** | Full | Full | None |
| **Best For** | Production, control | CQL apps, migration | Rapid dev, flexibility |

## 📚 Schema Versions

The repository includes multiple schema versions demonstrating Cassandra evolution:

### `schema-v3.cql` - Cassandra 3.x
- Basic schema with denormalized tables
- UDTs and collections
- Secondary indexes
- **Use for:** Legacy systems, learning basics

### `schema-v4.cql` - Cassandra 4.0
- Virtual tables
- Arithmetic operators
- Current time functions
- Improved UDF support
- **Use for:** Cassandra 4.0 clusters

### `schema-v5.cql` - Cassandra 5.0 (Latest)
- **Vector types** for AI/ML features
- **Storage-Attached Indexes (SAI)** replace many denormalized tables
- **Data masking** for PII protection
- Enhanced functions (currentTimestamp, etc.)
- **Use for:** Modern deployments, OSS Cassandra 5.0+

### `schema-astra.cql` - Astra DB
- Adapted from v5 for Astra compatibility
- SAI with Astra-specific syntax
- Data masking removed (not yet supported)
- Optimized for serverless
- **Use for:** Astra Tables approach

See [CLAUDE.md](CLAUDE.md) for detailed schema architecture and design decisions.

## 🛠️ Installation & Setup

### Prerequisites

**All platforms:**
- Python 3.8+

**OSS Cassandra:**
- Cassandra 5.0+ cluster
- DSBulk 1.11.0+ (for vector support)

**Astra (both Tables and Collections):**
- Astra DB database
- Astra token with Database Administrator role
- Embedding provider API key (OpenAI, Hugging Face, etc.)

### Install Python Dependencies

```bash
cd loaders
pip install -r requirements.txt
```

This installs:
- `cassandra-driver` - for OSS Cassandra and Astra Tables
- `astrapy` - for Astra Collections
- `pyyaml` - for configuration
- `python-dotenv` - for environment variables

## 🔢 Loading Data

### OSS Cassandra 5.0+

```bash
cd loaders/oss-cassandra

# Using DSBulk (fastest)
./load_with_dsbulk.sh

# Using Python (more control)
python load_with_python.py --host 127.0.0.1 --keyspace killrvideo
```

**Time:** ~1-2 minutes

### Astra Tables

```bash
cd loaders/astra-tables

# Configure embedding provider
cp config.example.yaml config.yaml
# Edit config.yaml with your credentials

# Test configuration
python setup_embeddings.py --config config.yaml

# Load data with embedding generation
python load_with_embeddings.py --config config.yaml
```

**Time:** ~5-10 minutes (embedding generation)

### Astra Collections

```bash
cd loaders/astra-collections

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml

# Load data
python load_to_collections.py --config config.yaml

# Configure Vectorize in Astra Portal after loading
```

**Time:** ~3-5 minutes

## 🧪 Sample Data and Queries

After loading data, try the example queries:

```bash
# For v4
cqlsh -f examples/schema-v4-query-examples.cql

# For v5
cqlsh -f examples/schema-v5-query-examples.cql
```

Examples include:
- Basic CRUD operations
- SAI index queries
- Counter table operations
- Collection type usage
- Vector similarity search (once vectors are populated)

## 🔍 Key Features by Version

### Vector Search (v5, Astra)

```sql
-- Find similar videos (requires populated vectors)
SELECT videoid, name FROM videos
ORDER BY content_features ANN OF [0.1, 0.2, ..., 0.9]
LIMIT 10;
```

### Storage-Attached Indexes (v5, Astra)

```sql
-- Query on any SAI-indexed column
SELECT * FROM videos WHERE tags CONTAINS 'cassandra';
SELECT * FROM users WHERE account_status = 'active';
```

### Data Masking (v5 only, not Astra)

```sql
-- PII automatically masked for non-admin users
SELECT email FROM users LIMIT 5;
-- Result: m****@example.com (masked)
```

### Counter Tables (all versions)

```sql
-- Update video view counts
UPDATE video_playback_stats
SET views = views + 1, unique_viewers = unique_viewers + 1
WHERE videoid = ?;
```

## 📖 Additional Resources

### Documentation

- **[CLAUDE.md](CLAUDE.md)** - Comprehensive schema architecture and design guide
- **[CQL Version Chart](CQL%20version%20to%20Cassandra%20version%20chart.md)** - Feature availability by version
- **[Migration Guides](migrating/)** - Upgrading between versions

### Platform-Specific Guides

- **[OSS Cassandra Loader](loaders/oss-cassandra/README.md)** - Full OSS documentation
- **[Astra Tables Loader](loaders/astra-tables/README.md)** - Tables + embedding generation
- **[Astra Collections Loader](loaders/astra-collections/README.md)** - Collections + auto vectorization

### Example Code

- **[v4 Data Examples](examples/schema-v4-data-examples.cql)** - Sample inserts for v4
- **[v5 Data Examples](examples/schema-v5-data-examples.cql)** - Sample inserts for v5
- **[v4 Query Examples](examples/schema-v4-query-examples.cql)** - Query patterns for v4
- **[v5 Query Examples](examples/schema-v5-query-examples.cql)** - Query patterns for v5

## 🤔 Common Questions

### Which platform should I use?

**Learning Cassandra?** → Start with OSS Cassandra to understand internals
**Building production app?** → Use Astra (Tables or Collections based on use case)
**Rapid prototyping?** → Use Astra Collections for fastest development
**Existing CQL app?** → Use Astra Tables for easy migration

### Can I use the same data for all platforms?

**Yes!** The CSV files in `data/csv/` work with all loaders. Each loader transforms the data appropriately for its platform.

### What about vector embeddings?

- **OSS Cassandra**: Pre-compute or load NULL, populate later
- **Astra Tables**: Generated client-side during loading
- **Astra Collections**: Generated automatically by Astra (server-side)

### Can I try multiple approaches?

**Absolutely!** Load the same data into all three platforms and compare:
- Query performance
- Development experience
- Feature set
- Cost

This is a great way to understand trade-offs!

## 🐛 Troubleshooting

### "No CSV files found"

**Solution:** CSVs are in `data/csv/`. Make sure you're running from the correct directory.

### "Schema not loaded"

**Solution:** Load the appropriate schema for your platform from `data/schemas/`

### "Embedding generation failed"

**Solution:** Check your embedding provider API key and rate limits

### "Collection creation failed"

**Solution:** Verify Astra token has Database Administrator permissions

For detailed troubleshooting, see the README for your specific loader.

## 🎓 Learning Path

**Beginner:**
1. Start with Astra Collections (easiest)
2. Load data and run example queries
3. Explore automatic indexing and vectorization

**Intermediate:**
1. Try Astra Tables
2. Compare with Collections approach
3. Understand schema design trade-offs

**Advanced:**
1. Set up OSS Cassandra
2. Load data and configure replication
3. Tune performance and explore advanced features

## 🤝 Contributing

Contributions welcome! Areas of interest:

- Additional loaders (e.g., DSE, Scylla DB)
- Pre-computed embedding datasets
- More comprehensive query examples
- Performance benchmarks
- Additional data generators

## 📄 License

[Apache License 2.0](LICENSE)

## 🔗 Related Projects

- **[KillrVideo Application](https://github.com/killrvideo)** - Reference application
- **[DataStax Documentation](https://docs.datastax.com)** - Official docs
- **[Astra Portal](https://astra.datastax.com)** - Create Astra databases

---

**Questions?** Open an issue or check the [loader documentation](loaders/).

**Need help choosing?** See the [platform comparison table](#detailed-comparison) above.
