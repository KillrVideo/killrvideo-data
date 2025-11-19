# KillrVideo Data Generator

Generate realistic CSV datasets for the KillrVideo Cassandra database using YouTube Data API v3 and synthetic data.

## Features

- 📹 **Real YouTube Videos**: Fetches actual video metadata from DataStax Developers channel and keyword searches
- 👥 **Synthetic Users**: Generates realistic users, credentials, and preferences using Faker
- 💬 **Realistic Comments**: Tech-themed comments with sentiment scores
- ⭐ **Ratings & Stats**: User ratings, video views, and engagement metrics
- 🏷️ **Tag System**: Extracted tags with relationships and counters
- ✅ **Referential Integrity**: Validates all foreign key relationships
- 📦 **DSBulk Ready**: CSV files optimized for DataStax Bulk Loader

## Quick Start

### 1. Install uv

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Get YouTube API Key

1. Go to [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
2. Create a new project (or select existing)
3. Enable **YouTube Data API v3**
4. Create credentials → API Key
5. Copy the API key

### 3. Configure

```bash
# Copy example config
cp config.example.yaml config.yaml

# Edit config.yaml and add your API key
nano config.yaml  # or your favorite editor
```

### 4. Generate Data

```bash
# One command - installs dependencies and runs generator
uv run generate.py
```

That's it! CSV files will be in `./output/`

## Configuration

Edit `config.yaml` to customize generation:

```yaml
youtube:
  api_key: "YOUR_API_KEY"              # Required
  channel_id: "UCAIQY251avaMv7bBv5PCo-A"  # DataStax Developers
  search_queries:                      # Additional videos
    - "Apache Cassandra tutorial"
    - "Astra DB introduction"
  max_videos: 500                      # Total videos to collect
  max_search_results: 100              # Per search query

dataset:
  num_users: 150                       # Number of users
  num_comments_min: 500                # Min comments
  num_comments_max: 1500               # Max comments
  rating_probability: 0.35             # 35% of user-video pairs rated
  popular_video_threshold: 0.2         # Top 20% get more engagement
  random_seed: 42                      # For reproducibility (null = random)

output:
  directory: "./output"                # Output directory
  csv_encoding: "utf-8"                # CSV encoding
  date_format: "iso8601"               # Timestamp format
```

## Generated Tables

The generator creates 12 CSV files:

### User Management
- **users.csv** - User profiles (150 users)
- **user_credentials.csv** - Authentication (150 records)

### Video Management
- **videos.csv** - Video metadata (300-500 videos)
- **latest_videos.csv** - Recent videos denormalized (100 records)

### Tags & Discovery
- **tags.csv** - Tag metadata with relationships
- **tag_counts.csv** - Tag usage counters

### Engagement
- **comments.csv** - Comments by video (500-1500 comments)
- **comments_by_user.csv** - Comments by user (denormalized)

### Ratings
- **video_ratings.csv** - Aggregate ratings (counters)
- **video_ratings_by_user.csv** - Individual ratings

### Statistics
- **video_playback_stats.csv** - Views and engagement (counters)
- **user_preferences.csv** - User preference profiles

## Loading Data

### Load into Astra DB

```bash
# Edit scripts/load_to_astra.sh and configure:
# - SCB_PATH: Path to secure connect bundle
# - CLIENT_ID: Astra DB client ID
# - CLIENT_SECRET: Astra DB client secret

./scripts/load_to_astra.sh
```

### Load into Cassandra

```bash
# Prerequisites:
# 1. Cassandra 4.0+ or 5.0 running
# 2. Load schema: cqlsh -f ../schema-v5.cql (or schema-astra.cql)

# Edit scripts/load_to_cassandra.sh if needed (host, port, auth)
./scripts/load_to_cassandra.sh
```

## Validation

Validate referential integrity before loading:

```bash
./scripts/validate_data.py

# Or specify output directory:
./scripts/validate_data.py ./output
```

## Project Structure

```
data-generator/
├── generate.py           # Main CLI entry point
├── config.yaml           # Your configuration (gitignored)
├── config.example.yaml   # Configuration template
├── pyproject.toml        # uv project configuration
├── src/
│   ├── youtube_collector.py   # YouTube Data API v3 client
│   ├── data_generator.py      # Faker-based synthetic data
│   ├── csv_writer.py           # DSBulk-optimized CSV export
│   └── relationships.py        # FK relationship tracker
├── scripts/
│   ├── load_to_astra.sh       # Astra DB loading script
│   ├── load_to_cassandra.sh   # Cassandra loading script
│   └── validate_data.py       # Data validation
└── output/                # Generated CSV files (gitignored)
```

## API Quota Usage

The generator is optimized for minimal API quota usage:

### Channel Scraping (Efficient)
- Get channel uploads playlist: **1 unit**
- Fetch 50 videos: **1 unit** per request
- ~350 videos from DataStax channel: **~7 units**

### Keyword Search (Expensive)
- Each search request: **100 units** per query
- 2 search queries @ 100 videos each: **~200 units**

**Total**: ~207 units (well under the 10,000 daily free quota)

## Troubleshooting

### "Config file not found"
```bash
cp config.example.yaml config.yaml
# Then edit config.yaml with your API key
```

### "YouTube API key not configured"
Make sure `config.yaml` has your actual API key, not `YOUR_YOUTUBE_API_KEY_HERE`

### "API quota exceeded"
- Daily limit is 10,000 units
- Reduce `max_videos` and `max_search_results` in config
- Wait 24 hours for quota reset

### "Module not found" errors
```bash
uv sync  # Install/update dependencies
```

### DSBulk loading fails
- Verify keyspace exists in Cassandra/Astra
- Verify schema is loaded: `cqlsh -f ../schema-v5.cql`
- Check DSBulk is installed: `dsbulk --version`
- For Astra: verify secure connect bundle path and credentials

## Development

### Install dependencies manually
```bash
uv sync
```

### Run with specific Python version
```bash
uv run --python 3.11 generate.py
```

### Run tests (when implemented)
```bash
uv run pytest
```

## Schema Compatibility

This generator targets:
- **Cassandra 5.0** (schema-v5.cql) - Full feature support
- **Astra DB** (schema-astra.cql) - Compatible subset
- **Cassandra 4.0** (schema-v4.cql) - Partial support

**Note**: Vector fields (`content_features`, `tag_vector`, `preference_vector`) are set to NULL. To populate these, you'll need a separate vector generation step using a model like sentence-transformers.

## Data Characteristics

### Users
- Realistic names and emails (Faker)
- 90% active, 8% inactive, 2% suspended
- 5% accounts locked
- Created over last 2 years

### Videos
- Real YouTube metadata from DataStax channel
- Assigned to synthetic users (weighted)
- Categories: Tutorial, Demo, Conference, Education
- Tags extracted from titles/descriptions
- All content rated 'G'

### Comments
- Tech-themed with realistic sentiment (skewed positive)
- Popular videos get more comments (top 20%)
- Generated using templates with tech topics

### Ratings
- 1-5 stars, weighted toward 4-5 (positive bias)
- 35% of user-video combinations have ratings
- Each user rates 5-30 videos

### Engagement
- Views: Power law distribution (10-10,000)
- Watch time: 2-10 minutes per view
- Completion rate: 30-80%
- Unique viewers: 60-95% of views

## License

Apache License 2.0 (same as KillrVideo project)

## Links

- [KillrVideo Project](https://killrvideo.github.io/)
- [YouTube Data API Documentation](https://developers.google.com/youtube/v3)
- [DSBulk Documentation](https://docs.datastax.com/en/dsbulk/docs/)
- [Faker Documentation](https://faker.readthedocs.io/)
- [uv Documentation](https://docs.astral.sh/uv/)
