-- KillrVideo Final Schema for Astra as of June 2025
-- Showcasing all modern data modeling features

-- Note on Materialized Views:
-- Although Cassandra 4.0 and 5.0 include materialized views,
-- they are generally not recommended for production use due to
-- performance and reliability concerns. This schema uses traditional
-- denormalized tables instead of materialized views.

-- CREATE KEYSPACE IF NOT EXISTS killrvideo 
-- WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

-- For Astra, add the Keyspace via the UI 
------------------------------------------------------------------
-- User Management Tables
------------------------------------------------------------------
-- This section contains tables related to user profiles, authentication,
-- and account management. The tables showcase several new Cassandra 5.0
-- features for security and performance.

-- User profile information
-- Supports queries: Get user by ID, Find user by email, List users by status
--
-- * Storage-Attached Index (SAI) for efficient filtering and lookups
CREATE TABLE IF NOT EXISTS killrvideo.users (
    userid uuid PRIMARY KEY,
    created_date timestamp,
    email text,
    firstname text,
    lastname text,
    account_status text,
    last_login_date timestamp
);

-- SAI index for email lookups
-- Replaces the need for a separate email lookup table
-- New in Cassandra 5: Storage-Attached Index with advanced indexing capabilities
CREATE CUSTOM INDEX IF NOT EXISTS users_email_idx ON killrvideo.users(email) 
USING 'StorageAttachedIndex';

-- SAI index for filtering users by account status
-- Enables efficient filtering queries without separate tables
-- New in Cassandra 5: Storage-Attached Index
CREATE CUSTOM INDEX IF NOT EXISTS users_account_status_idx ON killrvideo.users(account_status) 
USING 'StorageAttachedIndex';

-- User authentication credentials
-- Supports queries: Authenticate user, Check account status
--
-- New in Cassandra 5:
-- * Data masking can be applied to password in queries using mask_hash('SHA-256')
CREATE TABLE IF NOT EXISTS killrvideo.user_credentials (
    email text PRIMARY KEY,
    password text,                               -- Store password (hashed by application)
    userid uuid,
    account_locked boolean
);

-- Separate counter table for login attempts
-- Counters must be in their own table
CREATE TABLE IF NOT EXISTS killrvideo.login_attempts (
    email text PRIMARY KEY,
    failed_attempts counter                      -- Counter for tracking login attempts
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
-- * Storage-Attached Indexes for flexible querying (see indexes below)
CREATE TABLE IF NOT EXISTS killrvideo.videos (
    videoid uuid PRIMARY KEY,
    added_date timestamp,
    description text,
    location text,
    location_type int,
    name text,
    preview_image_location text,
    tags set<text>,                              -- Collection for efficient tag storage
    content_features vector<float, 384>,        -- Vector type from Cassandra 5.0 for ML features (IBM Granite 30M)
    userid uuid,
    content_rating text,                         -- 'G', 'PG', 'PG-13', 'R', etc.
    category text,
    language text,
    views int,
    youtube_id text
);

-- SAI indexes for the videos table
-- These indexes replace multiple denormalized tables from Cassandra 3.x
-- New in Cassandra 5: Storage-Attached Indexes with superior performance

-- Name search index
-- Enables text search on video titles
CREATE CUSTOM INDEX IF NOT EXISTS videos_name_idx ON killrvideo.videos(name) 
USING 'StorageAttachedIndex';

-- Tags collection index
-- Enables queries like "Find all videos with tag X"
-- Replaces the videos_by_tag table from earlier versions
CREATE CUSTOM INDEX IF NOT EXISTS videos_tags_idx ON killrvideo.videos(tags) 
USING 'StorageAttachedIndex';

-- User videos index
-- Enables "Find all videos by user X" queries
-- Replaces the user_videos table from earlier versions
CREATE CUSTOM INDEX IF NOT EXISTS videos_userid_idx ON killrvideo.videos(userid) 
USING 'StorageAttachedIndex';

-- Date index for chronological sorting
-- Enables "Latest videos" queries
-- Replaces the latest_videos table from earlier versions
CREATE CUSTOM INDEX IF NOT EXISTS videos_added_date_idx ON killrvideo.videos(added_date) 
USING 'StorageAttachedIndex';

-- Content filtering indexes
-- Enable content discovery and filtering
CREATE CUSTOM INDEX IF NOT EXISTS videos_content_rating_idx ON killrvideo.videos(content_rating) 
USING 'StorageAttachedIndex';

CREATE CUSTOM INDEX IF NOT EXISTS videos_category_idx ON killrvideo.videos(category) 
USING 'StorageAttachedIndex';

CREATE CUSTOM INDEX IF NOT EXISTS videos_language_idx ON killrvideo.videos(language) 
USING 'StorageAttachedIndex';

-- Vector search index with COSINE similarity function
-- Enables ANN searches for content-based recommendations
CREATE CUSTOM INDEX IF NOT EXISTS videos_content_features_idx 
ON killrvideo.videos(content_features) 
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

-- Denormalized table for latest videos
-- Supports queries: Get latest videos in chronological order
-- Note: We keep this table alongside SAI indexes for high-performance time-series access
CREATE TABLE IF NOT EXISTS killrvideo.latest_videos (
    day date,
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
-- Supports queries: Tag search/autocomplete, Find similar tags
--
-- New in Cassandra 5:
-- * Vector type for tag embeddings
-- * SAI with PREFIX mode for autocomplete
CREATE TABLE IF NOT EXISTS killrvideo.tags (
    tag text PRIMARY KEY,
    tag_vector vector<float, 384>,   -- Vector type from Cassandra 5.0 for tag embeddings (IBM Granite 30M)
    related_tags set<text>,           -- Collection of related tags
    category text                     -- Category classification
);

-- Tag usage statistics table
-- Separate counter table for tag popularity
CREATE TABLE IF NOT EXISTS killrvideo.tag_counts (
    tag text PRIMARY KEY,
    count counter                     -- Counter for tag popularity
);

-- Category index for tag filtering
-- Enables "Find all tags in category X" queries
CREATE CUSTOM INDEX IF NOT EXISTS tags_category_idx ON killrvideo.tags(category) 
USING 'StorageAttachedIndex';

-- Vector search index for finding similar tags
CREATE CUSTOM INDEX IF NOT EXISTS tags_vector_idx 
ON killrvideo.tags(tag_vector) 
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

------------------------------------------------------------------
-- Comments System Tables
------------------------------------------------------------------
-- This section contains tables for the video comment system. 
-- These tables showcase sentiment analysis with vectors.

-- Video comments table
-- Supports queries: Get comments for a video, Sort comments chronologically,
-- Identify toxic/positive comments with sentiment score
--
-- New in Cassandra 5:
-- * Sentiment analysis support via float score
CREATE TABLE IF NOT EXISTS killrvideo.comments (
    videoid uuid,
    commentid timeuuid,
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
-- * Mathematical functions for calculating averages with round()
-- * Enhanced counter reliability
CREATE TABLE IF NOT EXISTS killrvideo.video_ratings (
    videoid uuid PRIMARY KEY,
    rating_counter counter,       -- Count of ratings
    rating_total counter          -- Sum of all ratings
);

-- Example query using built-in math functions for average calculation:
-- SELECT videoid, round(rating_total / rating_counter, 1) as avg_rating 
-- FROM killrvideo.video_ratings
-- WHERE videoid = ?;

-- Individual user ratings table
-- Supports queries: Check if user rated a video, Get user's rating
CREATE TABLE IF NOT EXISTS killrvideo.video_ratings_by_user (
    videoid uuid,
    userid uuid,
    rating int,
    rating_date timestamp,
    PRIMARY KEY (videoid, userid)
) WITH CLUSTERING ORDER BY (userid ASC);

-- User preference model for recommendations
-- Supports queries: Get personalized recommendations, Find preference patterns
--
-- New in Cassandra 5:
-- * Vector type for user preference embeddings
-- * Map collections with float values for weighted preferences
-- * Vector similarity search capabilities using similarity_cosine, similarity_euclidean, or similarity_dot_product
CREATE TABLE IF NOT EXISTS killrvideo.user_preferences (
    userid uuid PRIMARY KEY,
    preference_vector vector<float, 384>,     -- Vector type from Cassandra 5.0 (IBM Granite 30M)
    tag_preferences map<text, float>,          -- Weighted map of tag preferences
    category_preferences map<text, float>,     -- Weighted map of category preferences
    last_updated timestamp
);

-- Create SAI index on preference vector for similarity searches
CREATE CUSTOM INDEX IF NOT EXISTS user_preferences_vector_idx 
ON killrvideo.user_preferences(preference_vector) 
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

------------------------------------------------------------------
-- Content Moderation Tables
------------------------------------------------------------------
-- This section contains tables for content moderation and safety.
-- These tables showcase data masking for sensitive information.

-- Content moderation tracking table
-- Supports queries: Find content pending review, Track moderation history,
-- Audit content decisions with protected reasons
--
-- New in Cassandra 5:
-- * Data masking for sensitive moderation reasons using mask_inner
-- * Role-based access to masked data with UNMASK permission
CREATE TABLE IF NOT EXISTS killrvideo.content_moderation (
    contentid uuid,                     -- Can be videoid or commentid
    flagid timeuuid,
    content_type text,                              -- 'video' or 'comment'
    status text,                                    -- 'approved', 'pending', 'rejected'
    flagged_reason text,
    reviewer uuid,
    review_date timestamp,
    PRIMARY KEY (contentid, flagid)
);

CREATE TABLE killrvideo.moderation_audit (
    videoid uuid,
    flagid timeuuid,
    action text,          -- flagged, unmask, approve, reject
    actor uuid,
    ts timestamp,
    details text,
    PRIMARY KEY ((videoid), ts, flagid)
) WITH CLUSTERING ORDER BY (ts DESC);

------------------------------------------------------------------
-- Analytics Tables
------------------------------------------------------------------
-- This section contains tables for analytics and user activity tracking.
-- These tables showcase vector types for metrics and efficient time-series data modeling.

-- Video engagement metrics time series
-- Supports queries: Track hourly engagement, Analyze performance,
-- Aggregate metrics across time periods
--
-- New in Cassandra 5:
-- * Vector type for compact storage of multiple metrics
-- * Enhanced time-series performance
CREATE TABLE IF NOT EXISTS killrvideo.video_engagement (
    videoid uuid,
    day date,
    hour int,
    engagement_metrics vector<float, 8>,          -- Vector type from Cassandra 5.0 for metrics
                                                  -- [views, avg_watch_time, likes, etc]
    PRIMARY KEY ((videoid, day), hour)
) WITH CLUSTERING ORDER BY (hour ASC);

-- User activity time series
-- Supports queries: Track user behavior, Analyze engagement patterns,
-- Build user activity timeline
CREATE TABLE IF NOT EXISTS killrvideo.user_activity (
    userid uuid,
    day date,
    activity_type text,                           -- 'view', 'comment', 'rate', etc.
    activity_id timeuuid,                         -- UUID of the activity
    activity_timestamp timestamp,
    PRIMARY KEY ((userid, day), activity_type, activity_timestamp, activity_id)
) WITH CLUSTERING ORDER BY (activity_type ASC, activity_timestamp DESC, activity_id ASC);

-- Total video activity time series
-- Collects all video views as a time series by day
CREATE TABLE IF NOT EXISTS killrvideo.video_activity (
    videoid uuid,
    day date,
    watch_time timeUUID,
    PRIMARY KEY (day, watch_time)
) WITH CLUSTERING ORDER BY (watch_time DESC);

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
    content_features vector<float, 384>,       -- Vector type from Cassandra 5.0 (IBM Granite 30M)
    PRIMARY KEY (sourceid, published_at, youtube_video_id)
) WITH CLUSTERING ORDER BY (published_at DESC, youtube_video_id ASC);

-- Create SAI index on YouTube video content features
CREATE CUSTOM INDEX IF NOT EXISTS youtube_videos_features_idx 
ON killrvideo.youtube_videos(content_features) 
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

------------------------------------------------------------------
-- User-Defined Functions
------------------------------------------------------------------
-- User Defined functions are not allowed in Astra per gaurdrails. 
