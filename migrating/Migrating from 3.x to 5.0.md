# KillrVideo: Migrating from Cassandra 3.x to 5.0

This guide demonstrates how to migrate the KillrVideo application schema from Cassandra 3.x to 5.0, leveraging new features to improve query performance, maintainability, and functionality. We'll walk through each aspect of the application, showing how modern Cassandra features can enhance your data model.

## Table of Contents

1. [Introduction](#introduction)
2. [User Management](#user-management)
3. [Video Management](#video-management)
4. [Tags and Discovery](#tags-and-discovery)
5. [Comments System](#comments-system)
6. [Ratings and Recommendations](#ratings-and-recommendations)
7. [New Feature Examples](#new-feature-examples)
8. [Complete Schema](#complete-schema)
9. [Migration Strategy](#migration-strategy)

## Introduction

KillrVideo is a sample video-sharing application that demonstrates Cassandra data modeling best practices. The original schema was created for Cassandra 2.1, with updates for 3.x. This guide shows how to leverage features in Cassandra 4.0 and 5.0 to improve the application.

Each section follows this pattern:
- Original Schema: The current 3.x implementation
- Query Patterns: What functionality this powers
- New Features: Relevant Cassandra 4.0/5.0 features 
- Improved Schema: Updated schema leveraging new features
- Migration Path: How to safely transition to the new model

## User Management

### Original Schema

```sql
CREATE TABLE killrvideo.users (
    userid uuid PRIMARY KEY,
    created_date timestamp,
    email text,
    firstname text,
    lastname text);

CREATE TABLE killrvideo.user_credentials (
    email text PRIMARY KEY,
    password text,
    userid uuid);
```

### Query Patterns
- Retrieve user profile by ID
- Authenticate users by email/password
- Email lookup for user information

### New Features to Apply
- **Data Masking Functions (5.0)**: Protect PII in user records
- **Storage-Attached Indexes (5.0)**: Replace custom email lookup tables
- **Role-Based Authentication (4.0)**: Enhanced security model
- **Current Time Functions (4.0)**: Automatic timestamps for user creation
- **Audit Logging (4.0)**: Track authentication attempts

### Improved Schema

```sql
CREATE TABLE killrvideo.users (
    userid uuid PRIMARY KEY,
    created_date timestamp DEFAULT currentTimestamp(),
    email text MASKED WITH mask_inner('*', 1, 1),
    firstname text,
    lastname text);

-- Create an SAI index on email for efficient lookups
CREATE INDEX users_email_idx ON killrvideo.users(email) USING 'StorageAttachedIndex';

CREATE TABLE killrvideo.user_credentials (
    email text PRIMARY KEY,
    password text MASKED WITH mask_hash('SHA-256'),
    userid uuid);
```

### Migration Path

1. **Add the SAI index first**:
   ```sql
   CREATE INDEX users_email_idx ON killrvideo.users(email) USING 'StorageAttachedIndex';
   ```

2. **Apply data masking to existing tables**:
   ```sql
   ALTER TABLE killrvideo.users ADD MASKED WITH mask_inner('*', 1, 1) ON email;
   ALTER TABLE killrvideo.user_credentials ADD MASKED WITH mask_hash('SHA-256') ON password;
   ```

3. **Add default timestamp function**:
   ```sql
   ALTER TABLE killrvideo.users ALTER created_date SET DEFAULT currentTimestamp();
   ```

4. **Enable audit logging** in cassandra.yaml for authentication events.

5. **Create roles with datacenter restrictions**:
   ```sql
   CREATE ROLE app_user WITH PASSWORD = '...' AND LOGIN = true AND ACCESS TO DATACENTERS {'dc1'};
   ```

### Benefits

- **Data Protection**: Email and password now have built-in protection
- **Simplified Queries**: SAI index allows email lookups directly against the users table
- **Improved Security**: Role-based authentication with datacenter restrictions 
- **Automatic Timestamps**: No need to generate timestamps client-side
- **Compliance**: Audit logging provides security trail

## Video Management

### Original Schema

```sql
CREATE TABLE killrvideo.videos (
    videoid uuid PRIMARY KEY,
    added_date timestamp,
    description text,
    location text,
    location_type int,
    name text,
    preview_image_location text,
    solr_query text,
    tags set<text>,
    userid uuid);

CREATE TABLE killrvideo.user_videos (
    userid uuid,
    added_date timestamp,
    videoid uuid,
    name text,
    preview_image_location text,
    PRIMARY KEY (userid, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

CREATE TABLE killrvideo.latest_videos (
    yyyymmdd text,
    added_date timestamp,
    videoid uuid,
    name text,
    preview_image_location text,
    userid uuid,
    PRIMARY KEY (yyyymmdd, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

CREATE TABLE killrvideo.video_playback_stats (
    videoid uuid PRIMARY KEY,
    views counter);
```

### Query Patterns
- Fetch video details by ID
- List videos by user
- Get recently added videos
- Track video view counts

### New Features to Apply
- **Storage-Attached Indexes (5.0)**: Replace denormalized tables
- **Vector Type (5.0)**: Add content feature vectors for similarity search
- **Extended TTL (5.0)**: Manage content lifecycle
- **Enhanced Time Functions (4.0)**: Simplify time-series table management
- **Collection Access (4.0)**: More powerful tag queries
- **Current Time Functions (4.0)**: Automatic timestamps

### Improved Schema

```sql
-- Enhanced videos table
CREATE TABLE killrvideo.videos (
    videoid uuid PRIMARY KEY,
    added_date timestamp DEFAULT currentTimestamp(),
    description text,
    location text,
    location_type int,
    name text,
    preview_image_location text,
    tags set<text>,
    content_features vector<float, 512>, -- Feature vector for content similarity
    userid uuid)
    WITH default_time_to_live = 15552000; -- 180 days TTL by default

-- SAI indexes for efficient queries
CREATE INDEX videos_name_idx ON killrvideo.videos(name) USING 'StorageAttachedIndex';
CREATE INDEX videos_tags_idx ON killrvideo.videos(tags) USING 'StorageAttachedIndex';
CREATE INDEX videos_userid_idx ON killrvideo.videos(userid) USING 'StorageAttachedIndex';
CREATE INDEX videos_added_date_idx ON killrvideo.videos(added_date) USING 'StorageAttachedIndex';

-- Materialized view for latest videos
CREATE MATERIALIZED VIEW killrvideo.latest_videos_mv AS
    SELECT videoid, added_date, name, preview_image_location, userid
    FROM killrvideo.videos
    WHERE videoid IS NOT NULL AND added_date IS NOT NULL
    PRIMARY KEY (added_date, videoid)
WITH CLUSTERING ORDER BY (added_date DESC);

-- Video playback stats remains unchanged
CREATE TABLE killrvideo.video_playback_stats (
    videoid uuid PRIMARY KEY,
    views counter);
```

### Migration Path

1. **Enhance the videos table**:
   ```sql
   ALTER TABLE killrvideo.videos 
   ADD content_features vector<float, 512>
   WITH default_time_to_live = 15552000;
   ```

2. **Create SAI indexes**:
   ```sql
   CREATE INDEX videos_name_idx ON killrvideo.videos(name) USING 'StorageAttachedIndex';
   CREATE INDEX videos_tags_idx ON killrvideo.videos(tags) USING 'StorageAttachedIndex';
   CREATE INDEX videos_userid_idx ON killrvideo.videos(userid) USING 'StorageAttachedIndex';
   CREATE INDEX videos_added_date_idx ON killrvideo.videos(added_date) USING 'StorageAttachedIndex';
   ```

3. **Create materialized view (optional)**:
   ```sql
   CREATE MATERIALIZED VIEW killrvideo.latest_videos_mv AS
       SELECT videoid, added_date, name, preview_image_location, userid
       FROM killrvideo.videos
       WHERE videoid IS NOT NULL AND added_date IS NOT NULL
       PRIMARY KEY (added_date, videoid)
   WITH CLUSTERING ORDER BY (added_date DESC);
   ```

4. **Update application to populate content_features vector**.

5. **Migrate data from user_videos and latest_videos**:
   - Run a process to ensure all videos are properly indexed
   - Validate query patterns with new SAI indexes
   - Gradually transition from old tables to new tables

### Benefits

- **Simplified Schema**: Less denormalization needed with SAI indexes
- **Content Similarity**: Vector type enables AI-powered recommendations
- **Content Lifecycle**: TTL automatically manages storage
- **Automatic Timestamps**: Consistent date/time handling
- **Reduced Maintenance**: Fewer tables to keep in sync

## Tags and Discovery

### Original Schema

```sql
CREATE TABLE killrvideo.tags_by_letter (
    first_letter text,
    tag text,
    PRIMARY KEY (first_letter, tag)
) WITH CLUSTERING ORDER BY (tag ASC);

CREATE TABLE killrvideo.videos_by_tag (
    tag text,
    videoid uuid,
    added_date timestamp,
    name text,
    preview_image_location text,
    tagged_date timestamp,
    userid uuid,
    PRIMARY KEY (tag, videoid)
) WITH CLUSTERING ORDER BY (videoid ASC);
```

### Query Patterns
- Find tags beginning with a specific letter
- Browse videos with a given tag
- Tag suggestion/autocomplete

### New Features to Apply
- **Storage-Attached Indexes (5.0)**: Replace denormalized tag tables
- **Collection Functions (5.0)**: Operate on tag collections
- **ANN Vector Search (5.0)**: Related tag search

### Improved Schema

```sql
-- Enhanced videos table with SAI on tags
-- (Already added in previous section)

-- New table for tag metadata with vector representation
CREATE TABLE killrvideo.tags (
    tag text PRIMARY KEY,
    tag_vector vector<float, 64>,  -- Vector representation of tag for similarity
    tag_count counter,             -- Number of videos with this tag
    related_tags set<text>         -- Manually curated related tags
);

-- SAI index for prefix search on tags
CREATE INDEX tags_prefix_idx ON killrvideo.tags(tag) USING 'StorageAttachedIndex'
WITH OPTIONS = {'mode': 'PREFIX'};
```

### Migration Path

1. **Create tag metadata table**:
   ```sql
   CREATE TABLE killrvideo.tags (
       tag text PRIMARY KEY,
       tag_vector vector<float, 64>,
       tag_count counter,
       related_tags set<text>
   );
   ```

2. **Create prefix index**:
   ```sql
   CREATE INDEX tags_prefix_idx ON killrvideo.tags(tag) USING 'StorageAttachedIndex'
   WITH OPTIONS = {'mode': 'PREFIX'};
   ```

3. **Populate tag counts**:
   ```sql
   -- For each tag found in videos_by_tag
   UPDATE killrvideo.tags SET tag_count = tag_count + 1 WHERE tag = ?;
   ```

4. **Compute tag vectors** (application code).

5. **Verify query patterns**:
   - Prefix search: `SELECT tag FROM killrvideo.tags WHERE tag LIKE 'c%';`
   - Tag similarity: `SELECT tag FROM killrvideo.tags ORDER BY ANN OF tag_vector [?] LIMIT 10;`

### Benefits

- **Simplified Queries**: Prefix search with SAI removes need for first_letter table
- **Tagged Videos**: Direct SAI query on videos.tags instead of videos_by_tag table
- **Tag Similarity**: Vector representation enables "related tags" feature
- **Reduced Maintenance**: Fewer tables to keep in sync

## Comments System

### Original Schema

```sql
CREATE TABLE killrvideo.comments_by_user (
    userid uuid,
    commentid timeuuid,
    comment text,
    videoid uuid,
    PRIMARY KEY (userid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

CREATE TABLE killrvideo.comments_by_video (
    videoid uuid,
    commentid timeuuid,
    comment text,
    userid uuid,
    PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);
```

### Query Patterns
- Retrieve a user's comments
- Get comments for a video

### New Features to Apply
- **Current Time Functions (4.0)**: Use for comment timestamps
- **Materialized Views (4.0)**: Replace one of the denormalized tables
- **Vector Type (5.0)**: Add sentiment analysis
- **Mathematical Functions (5.0)**: Calculate average sentiment

### Improved Schema

```sql
-- Primary comments table
CREATE TABLE killrvideo.comments (
    videoid uuid,
    commentid timeuuid DEFAULT currentTimeUUID(),
    comment text,
    userid uuid,
    sentiment_score float,  -- Sentiment analysis score
    PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

-- Materialized view for comments by user
CREATE MATERIALIZED VIEW killrvideo.comments_by_user_mv AS
    SELECT videoid, commentid, comment, userid, sentiment_score
    FROM killrvideo.comments
    WHERE videoid IS NOT NULL AND commentid IS NOT NULL AND userid IS NOT NULL
    PRIMARY KEY (userid, commentid)
WITH CLUSTERING ORDER BY (commentid DESC);
```

### Migration Path

1. **Create enhanced comments table**:
   ```sql
   CREATE TABLE killrvideo.comments (
       videoid uuid,
       commentid timeuuid DEFAULT currentTimeUUID(),
       comment text,
       userid uuid,
       sentiment_score float,
       PRIMARY KEY (videoid, commentid)
   ) WITH CLUSTERING ORDER BY (commentid DESC);
   ```

2. **Create materialized view**:
   ```sql
   CREATE MATERIALIZED VIEW killrvideo.comments_by_user_mv AS
       SELECT videoid, commentid, comment, userid, sentiment_score
       FROM killrvideo.comments
       WHERE videoid IS NOT NULL AND commentid IS NOT NULL AND userid IS NOT NULL
       PRIMARY KEY (userid, commentid)
   WITH CLUSTERING ORDER BY (commentid DESC);
   ```

3. **Migrate existing comments**:
   ```sql
   -- For each comment in comments_by_video
   INSERT INTO killrvideo.comments (videoid, commentid, comment, userid)
   VALUES (?, ?, ?, ?);
   ```

4. **Update application** to calculate and store sentiment scores.

### Benefits

- **Enhanced Date Functions**: Better time-based partitioning
- **Automatic Timestamps**: Default currentTimeUUID() provides correct ordering
- **Sentiment Analysis**: New sentiment_score enables content moderation
- **Reduced Maintenance**: Only one table to write to

## Ratings and Recommendations

### Original Schema

```sql
CREATE TABLE killrvideo.video_ratings (
    videoid uuid PRIMARY KEY,
    rating_counter counter,
    rating_total counter);

CREATE TABLE killrvideo.video_ratings_by_user (
    videoid uuid,
    userid uuid,
    rating int,
    PRIMARY KEY (videoid, userid)
) WITH CLUSTERING ORDER BY (userid ASC);

CREATE TABLE killrvideo.video_recommendations (
    userid uuid,
    added_date timestamp,
    videoid uuid,
    authorid uuid,
    name text,
    preview_image_location text,
    rating float,
    PRIMARY KEY (userid, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

CREATE TABLE killrvideo.video_recommendations_by_video (
    videoid uuid,
    userid uuid,
    added_date timestamp static,
    authorid uuid static,
    name text static,
    preview_image_location text static,
    rating float,
    PRIMARY KEY (videoid, userid)
) WITH CLUSTERING ORDER BY (userid ASC);
```

### Query Patterns
- Get a video's average rating
- Check if a user has rated a video
- Get personalized video recommendations for a user
- Get similar videos to a specific video

### New Features to Apply
- **Vector Search (5.0)**: Content-based recommendations
- **Mathematical Functions (5.0)**: Calculate average ratings
- **Collection Operations (5.0)**: Aggregate ratings
- **User-Defined Functions (3.0+)**: Replace with built-in functions

### Improved Schema

```sql
-- Keep rating tables mostly unchanged
CREATE TABLE killrvideo.video_ratings (
    videoid uuid PRIMARY KEY,
    rating_counter counter,
    rating_total counter);

CREATE TABLE killrvideo.video_ratings_by_user (
    videoid uuid,
    userid uuid,
    rating int,
    PRIMARY KEY (videoid, userid)
) WITH CLUSTERING ORDER BY (userid ASC);

-- Replace recommendation tables with vector search
-- Note: We already added content_features to videos table

-- Add user preferences vector
CREATE TABLE killrvideo.user_preferences (
    userid uuid PRIMARY KEY,
    preference_vector vector<float, 512>,  -- User preference vector
    tag_preferences map<text, float>,      -- Weighted tag preferences
    last_updated timestamp DEFAULT currentTimestamp()
);
```

### Migration Path

1. **Add user preferences table**:
   ```sql
   CREATE TABLE killrvideo.user_preferences (
       userid uuid PRIMARY KEY,
       preference_vector vector<float, 512>,
       tag_preferences map<text, float>,
       last_updated timestamp DEFAULT currentTimestamp()
   );
   ```

2. **Compute user preference vectors** based on:
   - Existing ratings
   - Watch history
   - Explicit preferences

3. **Update application** to use vector search for recommendations:
   ```sql
   -- Get user preferences
   SELECT preference_vector FROM killrvideo.user_preferences WHERE userid = ?;
   
   -- Find similar videos using ANN search
   SELECT videoid, name, preview_image_location 
   FROM killrvideo.videos 
   ORDER BY ANN OF content_features [?] 
   LIMIT 10;
   ```

4. **Calculate average rating** using built-in functions:
   ```sql
   SELECT videoid, round(rating_total / rating_counter, 1) as avg_rating 
   FROM killrvideo.video_ratings 
   WHERE videoid = ?;
   ```

### Benefits

- **Modern Recommendations**: Vector search enables ML-based recommendations
- **Simplified Calculations**: Built-in mathematical functions reduce complexity
- **Personalization**: User preference vectors enable better content discovery
- **Reduced Maintenance**: Fewer denormalized tables

## New Feature Examples

### Content Moderation with Data Masking

```sql
-- Add moderation status table
CREATE TABLE killrvideo.content_moderation (
    contentid uuid PRIMARY KEY,  -- Can be videoid or commentid
    content_type text,           -- 'video' or 'comment'
    status text,                 -- 'approved', 'pending', 'rejected'
    flagged_reason text MASKED WITH mask_inner('*', 0, 0),  -- Reason content was flagged
    reviewer uuid,
    review_date timestamp DEFAULT currentTimestamp()
);

-- Add permissions
CREATE ROLE content_reviewer WITH UNMASK ON content_moderation;
```

### Video Analytics using Vector Time Series

```sql
-- User engagement time series
CREATE TABLE killrvideo.video_engagement (
    videoid uuid,
    day date DEFAULT currentDate(),
    hour int,
    engagement_metrics vector<float, 8>,  -- [views, avg_watch_time, likes, etc]
    PRIMARY KEY ((videoid, day), hour)
) WITH CLUSTERING ORDER BY (hour ASC);
```

### Tag Similarity Search

```sql
-- Find similar tags
SELECT tag, similarity_cosine(tag_vector, ?) as score
FROM killrvideo.tags
WHERE score > 0.7
ORDER BY score DESC
LIMIT 10;
```

### Content Expiration Policies

```sql
-- Set different TTLs based on content type
ALTER TABLE killrvideo.videos
ADD ttl_policy text;

-- Application sets TTL based on policy
-- For premium content: Long TTL
UPDATE killrvideo.videos USING TTL 31536000 -- 1 year
SET ttl_policy = 'premium' 
WHERE videoid = ?;

-- For test content: Short TTL
UPDATE killrvideo.videos USING TTL 604800 -- 1 week
SET ttl_policy = 'test' 
WHERE videoid = ?;
```

## Complete Schema

[See final schema section for complete CQL]

## Migration Strategy

### Phase 1: Schema Enhancement
1. Add new fields and data types to existing tables
2. Create SAI indexes alongside existing tables
3. Create new tables for new features

### Phase 2: Data Migration
1. Populate vectors and additional fields
2. Verify data consistency between old and new structures
3. Test queries against both old and new patterns

### Phase 3: Application Updates
1. Update application to use new query patterns
2. Implement vector computations for recommendations
3. Add data masking support for sensitive operations

### Phase 4: Cleanup
1. Monitor usage of old tables
2. Gradually deprecate redundant tables
3. Remove unused tables after validation period

### Performance Considerations
1. Benchmark queries before and after migration
2. Monitor SAI index sizes and performance
3. Tune vector dimensions based on performance needs
4. Apply appropriate guardrails on production systems