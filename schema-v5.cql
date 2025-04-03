-- KillrVideo Final Schema for Cassandra 5.0
-- Showcasing all modern data modeling features
--
-- This schema demonstrates the progression from Cassandra 3.x to 5.0,
-- highlighting new features and how they can be used to simplify and enhance
-- your data models. Tables are grouped by functional area with detailed
-- comments explaining the purpose and features used.

-- Note on Materialized Views:
-- Although Cassandra 4.0 and 5.0 include materialized views,
-- they are generally not recommended for production use due to
-- performance and reliability concerns. This schema uses traditional
-- denormalized tables instead of materialized views.

CREATE KEYSPACE IF NOT EXISTS killrvideo 
WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}
AND durable_writes = true;

------------------------------------------------------------------
-- User Management Tables
------------------------------------------------------------------
-- This section contains tables related to user profiles, authentication,
-- and account management. The tables showcase several new Cassandra 5.0
-- features for security and performance.

-- User profile information
-- Supports queries: Get user by ID, Find user by email, List users by status
--
-- New in Cassandra 5:
-- * Data masking for PII protection (email)
-- * Storage-Attached Index (SAI) for efficient filtering and lookups
-- * Default timestamp function from Cassandra 4.0
CREATE TABLE IF NOT EXISTS killrvideo.users (
    userid uuid PRIMARY KEY,
    created_date timestamp DEFAULT currentTimestamp(), -- Auto timestamp from Cassandra 4.0
    email text MASKED WITH mask_inner('*', 1, 1),     -- Data masking from Cassandra 5.0
    firstname text,
    lastname text,
    account_status text,
    last_login_date timestamp
);

-- SAI index for email lookups
-- Replaces the need for a separate email lookup table
-- New in Cassandra 5: Storage-Attached Index with advanced indexing capabilities
CREATE INDEX IF NOT EXISTS users_email_idx ON killrvideo.users(email) 
USING 'StorageAttachedIndex';

-- SAI index for filtering users by account status
-- Enables efficient filtering queries without separate tables
-- New in Cassandra 5: Storage-Attached Index
CREATE INDEX IF NOT EXISTS users_account_status_idx ON killrvideo.users(account_status) 
USING 'StorageAttachedIndex';

-- User authentication credentials
-- Supports queries: Authenticate user, Check account status
--
-- New in Cassandra 5:
-- * Data masking with hash function for password security
-- * Hash algorithm specification for strong security
CREATE TABLE IF NOT EXISTS killrvideo.user_credentials (
    email text PRIMARY KEY,
    password text MASKED WITH mask_hash('SHA-256'), -- Password hash masking from Cassandra 5.0
    userid uuid,
    account_locked boolean,
    failed_login_attempts counter                   -- Counter for tracking login attempts
);

------------------------------------------------------------------
-- Video Management Tables
------------------------------------------------------------------
-- This section contains tables related to video storage, retrieval,
-- and metadata. These tables showcase vector types, SAI indexes,
-- and other advanced features introduced in Cassandra 5.0.

-- Primary video metadata table
-- Supports queries: Get video by ID, Find videos by tag/category/language,
-- Content-based similarity search, Video recommendation
--
-- New in Cassandra 5:
-- * Vector type for content feature vectors (enables AI/ML integration)
-- * Extended TTL capabilities for content lifecycle management
-- * Storage-Attached Indexes for flexible querying (see indexes below)
CREATE TABLE IF NOT EXISTS killrvideo.videos (
    videoid uuid PRIMARY KEY,
    added_date timestamp DEFAULT currentTimestamp(),    -- Auto timestamp from Cassandra 4.0
    description text,
    location text,
    location_type int,
    name text,
    preview_image_location text,
    tags set<text>,                                     -- Collection for efficient tag storage
    content_features vector<float, 512>,                -- Vector type from Cassandra 5.0 for ML features
    userid uuid,
    content_rating text,                                -- 'G', 'PG', 'PG-13', 'R', etc.
    category text,
    language text
) WITH default_time_to_live = 15552000;                 -- Extended TTL (180 days) from Cassandra 5.0

-- SAI indexes for the videos table
-- These indexes replace multiple denormalized tables from Cassandra 3.x
-- New in Cassandra 5: Storage-Attached Indexes with superior performance

-- Name search index
-- Enables text search on video titles
CREATE INDEX IF NOT EXISTS videos_name_idx ON killrvideo.videos(name) 
USING 'StorageAttachedIndex';

-- Tags collection index
-- Enables queries like "Find all videos with tag X"
-- Replaces the videos_by_tag table from earlier versions
CREATE INDEX IF NOT EXISTS videos_tags_idx ON killrvideo.videos(tags) 
USING 'StorageAttachedIndex';

-- User videos index
-- Enables "Find all videos by user X" queries
-- Replaces the user_videos table from earlier versions
CREATE INDEX IF NOT EXISTS videos_userid_idx ON killrvideo.videos(userid) 
USING 'StorageAttachedIndex';

-- Date index for chronological sorting
-- Enables "Latest videos" queries
-- Replaces the latest_videos table from earlier versions
CREATE INDEX IF NOT EXISTS videos_added_date_idx ON killrvideo.videos(added_date) 
USING 'StorageAttachedIndex';

-- Content filtering indexes
-- Enable content discovery and filtering
CREATE INDEX IF NOT EXISTS videos_content_rating_idx ON killrvideo.videos(content_rating) 
USING 'StorageAttachedIndex';

CREATE INDEX IF NOT EXISTS videos_category_idx ON killrvideo.videos(category) 
USING 'StorageAttachedIndex';

CREATE INDEX IF NOT EXISTS videos_language_idx ON killrvideo.videos(language) 
USING 'StorageAttachedIndex';

-- Denormalized table for latest videos
-- Supports queries: Get latest videos in chronological order
-- 
-- New in Cassandra 5:
-- * Enhanced time functions for automatic date partitioning
-- * Improved performance for time-series data
CREATE TABLE IF NOT EXISTS killrvideo.latest_videos (
    day date DEFAULT currentDate(),
    added_date timestamp,
    videoid uuid,
    name text,
    preview_image_location text,
    userid uuid,
    content_rating text,
    category text,
    PRIMARY KEY (day, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

-- Video playback statistics table
-- Supports queries: Get view counts, Track engagement metrics
--
-- New in Cassandra 5:
-- * Enhanced counter column performance
-- * More reliable counter updates
CREATE TABLE IF NOT EXISTS killrvideo.video_playback_stats (
    videoid uuid PRIMARY KEY,
    views counter,                -- Basic view counter
    total_play_time counter,      -- Total seconds watched
    complete_views counter,       -- Number of complete views
    unique_viewers counter        -- Approximate unique viewers
);

------------------------------------------------------------------
-- Tags and Discovery Tables
------------------------------------------------------------------
-- This section contains tables for content discovery, tagging,
-- and search functionality. These tables showcase the prefix search
-- capabilities and vector similarity features in Cassandra 5.0.

-- Tag metadata and similarity table
-- Supports queries: Tag search/autocomplete, Find similar tags,
-- Get tag usage statistics
--
-- New in Cassandra 5:
-- * Vector type for tag embeddings
-- * Counter performance improvements
-- * SAI with PREFIX mode for autocomplete
CREATE TABLE IF NOT EXISTS killrvideo.tags (
    tag text PRIMARY KEY,
    tag_vector vector<float, 64>,  -- Vector type from Cassandra 5.0 for tag embeddings
    tag_count counter,             -- Counter for tag popularity
    related_tags set<text>,        -- Collection of related tags
    category text                  -- Category classification
);

-- Prefix index for tag autocomplete
-- Enables efficient prefix search for typeahead/autocomplete
-- New in Cassandra 5: StorageAttachedIndex with PREFIX mode
CREATE INDEX IF NOT EXISTS tags_prefix_idx ON killrvideo.tags(tag) 
USING 'StorageAttachedIndex'
WITH OPTIONS = {'mode': 'PREFIX'};

-- Category index for tag filtering
-- Enables "Find all tags in category X" queries
CREATE INDEX IF NOT EXISTS tags_category_idx ON killrvideo.tags(category) 
USING 'StorageAttachedIndex';

------------------------------------------------------------------
-- Comments System Tables
------------------------------------------------------------------
-- This section contains tables for the video comment system. 
-- These tables showcase materialized views, sentiment analysis with
-- vectors, and automatic timestamp generation.

-- Video comments table
-- Supports queries: Get comments for a video, Sort comments chronologically,
-- Identify toxic/positive comments with sentiment score
--
-- New in Cassandra 5:
-- * Sentiment analysis support via float score
-- * Automatic TimeUUID generation with currentTimeUUID()
CREATE TABLE IF NOT EXISTS killrvideo.comments (
    videoid uuid,
    commentid timeuuid DEFAULT currentTimeUUID(),   -- Auto timestamp from Cassandra 4.0
    comment text,
    userid uuid,
    sentiment_score float,                          -- ML-derived sentiment score
    PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

-- Denormalized table for user comments
-- Supports queries: "Find all comments by user X across all videos"
-- 
-- New in Cassandra 5:
-- * Sentiment score for content analysis
-- * Default TimeUUID for consistent ordering
CREATE TABLE IF NOT EXISTS killrvideo.comments_by_user (
    userid uuid,
    commentid timeuuid,
    comment text,
    videoid uuid,
    sentiment_score float,
    PRIMARY KEY (userid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

------------------------------------------------------------------
-- Ratings and Recommendations Tables
------------------------------------------------------------------
-- This section contains tables related to video ratings and personalized
-- recommendations. These tables showcase vector search, user preferences,
-- and mathematical functions introduced in Cassandra 5.0.

-- Video ratings aggregation table
-- Supports queries: Get average rating for a video
--
-- New in Cassandra 5:
-- * Mathematical functions for calculating averages
-- * Enhanced counter reliability
CREATE TABLE IF NOT EXISTS killrvideo.video_ratings (
    videoid uuid PRIMARY KEY,
    rating_counter counter,       -- Count of ratings
    rating_total counter          -- Sum of all ratings
);

-- Individual user ratings table
-- Supports queries: Check if user rated a video, Get user's rating
--
-- New in Cassandra 4/5:
-- * Automatic timestamp with currentTimestamp()
-- * Better performance for counter operations
CREATE TABLE IF NOT EXISTS killrvideo.video_ratings_by_user (
    videoid uuid,
    userid uuid,
    rating int,
    rating_date timestamp DEFAULT currentTimestamp(),   -- Auto timestamp from Cassandra 4.0
    PRIMARY KEY (videoid, userid)
) WITH CLUSTERING ORDER BY (userid ASC);

-- User preference model for recommendations
-- Supports queries: Get personalized recommendations, Find preference patterns
--
-- New in Cassandra 5:
-- * Vector type for user preference embeddings
-- * Map collections with float values for weighted preferences
-- * Vector similarity search capabilities
CREATE TABLE IF NOT EXISTS killrvideo.user_preferences (
    userid uuid PRIMARY KEY,
    preference_vector vector<float, 512>,      -- Vector type from Cassandra 5.0
    tag_preferences map<text, float>,          -- Weighted map of tag preferences
    category_preferences map<text, float>,     -- Weighted map of category preferences
    last_updated timestamp DEFAULT currentTimestamp()
);

------------------------------------------------------------------
-- Content Moderation Tables
------------------------------------------------------------------
-- This section contains tables for content moderation and safety.
-- These tables showcase data masking for sensitive information and
-- automatic timestamps for audit trails.

-- Content moderation tracking table
-- Supports queries: Find content pending review, Track moderation history,
-- Audit content decisions with protected reasons
--
-- New in Cassandra 5:
-- * Data masking for sensitive moderation reasons
-- * Role-based access to masked data
-- * Automatic timestamps for audit trail
CREATE TABLE IF NOT EXISTS killrvideo.content_moderation (
    contentid uuid PRIMARY KEY,                           -- Can be videoid or commentid
    content_type text,                                    -- 'video' or 'comment'
    status text,                                          -- 'approved', 'pending', 'rejected'
    flagged_reason text MASKED WITH mask_inner('*', 0, 0), -- Data masking from Cassandra 5.0
    reviewer uuid,
    review_date timestamp DEFAULT currentTimestamp()      -- Auto timestamp from Cassandra 4.0
);

------------------------------------------------------------------
-- Analytics Tables
------------------------------------------------------------------
-- This section contains tables for analytics and user activity tracking.
-- These tables showcase vector types for metrics, date functions,
-- and efficient time-series data modeling.

-- Video engagement metrics time series
-- Supports queries: Track hourly engagement, Analyze performance,
-- Aggregate metrics across time periods
--
-- New in Cassandra 5:
-- * Vector type for compact storage of multiple metrics
-- * currentDate() function for automatic partitioning
-- * Enhanced time-series performance
CREATE TABLE IF NOT EXISTS killrvideo.video_engagement (
    videoid uuid,
    day date DEFAULT currentDate(),               -- Auto date function from Cassandra 4.0
    hour int,
    engagement_metrics vector<float, 8>,          -- Vector type from Cassandra 5.0 for metrics
                                                  -- [views, avg_watch_time, likes, etc]
    PRIMARY KEY ((videoid, day), hour)
) WITH CLUSTERING ORDER BY (hour ASC);

-- User activity time series
-- Supports queries: Track user behavior, Analyze engagement patterns,
-- Build user activity timeline
--
-- New in Cassandra 4/5:
-- * Auto date functions for partitioning
-- * Timestamp functions for ordering
-- * Efficient multi-level clustering
CREATE TABLE IF NOT EXISTS killrvideo.user_activity (
    userid uuid,
    day date DEFAULT currentDate(),               -- Auto date function from Cassandra 4.0
    activity_type text,                           -- 'view', 'comment', 'rate', etc.
    activity_id timeuuid,                         -- UUID of the activity
    activity_timestamp timestamp DEFAULT currentTimestamp(),
    PRIMARY KEY ((userid, day), activity_type, activity_timestamp, activity_id)
) WITH CLUSTERING ORDER BY (activity_type ASC, activity_timestamp DESC, activity_id ASC);

------------------------------------------------------------------
-- External Integration Tables
------------------------------------------------------------------
-- This section contains tables for integration with external systems.
-- These tables showcase vector types for imported content and
-- efficient data organization.

-- External video content import table
-- Supports queries: Import videos from YouTube, Track usage status,
-- Find similar external videos
--
-- New in Cassandra 5:
-- * Vector type for imported content features
-- * Enhanced clustering performance
-- * Support for external content similarity search
CREATE TABLE IF NOT EXISTS killrvideo.youtube_videos (
    sourceid text,
    published_at timestamp,
    youtube_video_id text,
    description text,
    name text,
    used boolean,
    content_features vector<float, 512>,        -- Vector type from Cassandra 5.0
    PRIMARY KEY (sourceid, published_at, youtube_video_id)
) WITH CLUSTERING ORDER BY (published_at DESC, youtube_video_id ASC);

------------------------------------------------------------------
-- User-Defined Functions
------------------------------------------------------------------
-- This section contains user-defined functions that extend CQL capabilities.
-- Cassandra 5.0 includes enhanced mathematical functions that can replace
-- many custom UDFs from previous versions.

-- Average rating calculation function
-- This function can now be replaced with built-in mathematical functions in Cassandra 5.0,
-- but is kept for compatibility with existing code.
--
-- New in Cassandra 5:
-- * Enhanced mathematical functions (round, division)
-- * Could be replaced with: SELECT round(rating_total/rating_counter, 1) FROM ...
CREATE OR REPLACE FUNCTION killrvideo.avg_rating(rating_counter counter, rating_total counter)
RETURNS FLOAT
LANGUAGE java
AS 'return rating_counter.doubleValue() > 0 ? Math.round((rating_total.doubleValue() / rating_counter.doubleValue()) * 10) / 10.0 : 0.0;';

------------------------------------------------------------------
-- Role-Based Access Control
------------------------------------------------------------------
-- This section defines roles with specific permissions.
-- Cassandra 4.0 and 5.0 include enhanced role-based access control,
-- data center restrictions, and masking permissions.

-- Basic application user role
-- New in Cassandra 4.0: Enhanced role-based access control
CREATE ROLE IF NOT EXISTS app_user WITH PASSWORD = 'password' AND LOGIN = true;

-- Content moderation role with unmasking privileges
-- New in Cassandra 5.0: UNMASK permission for accessing masked data
CREATE ROLE IF NOT EXISTS content_manager WITH PASSWORD = 'password' AND LOGIN = true AND UNMASK ON killrvideo.content_moderation;

------------------------------------------------------------------
-- Data Guardrails
------------------------------------------------------------------
-- This section defines guardrails to protect against common issues.
-- Cassandra 5.0 introduces configurable guardrails to prevent performance
-- problems from oversized partitions and other anti-patterns.

-- Partition size guardrails
-- New in Cassandra 5.0: Configurable guardrails to prevent performance issues
-- These settings cap partition sizes to ensure consistent performance

-- Limit video partition sizes to prevent hotspots
ALTER TABLE killrvideo.videos WITH max_partition_size = '100MB';

-- Limit comment partition sizes for busy videos
ALTER TABLE killrvideo.comments WITH max_partition_size = '50MB';