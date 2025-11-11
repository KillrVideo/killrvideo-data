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
WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

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
CREATE TABLE IF NOT EXISTS killrvideo.users (
    userid uuid PRIMARY KEY,
    created_date timestamp,
    email text MASKED WITH mask_inner(1, 1),     -- Data masking from Cassandra 5.0
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

-- User payment information with custom masking
-- Supports queries: Get payment methods for a user
--
-- New in Cassandra 5:
-- * Multiple masked columns with different masking functions
CREATE TABLE IF NOT EXISTS killrvideo.payment_info (
    userid uuid,
    payment_id uuid,
    card_number text MASKED WITH mask_replace('**** **** **** 1234'),
    card_expiry date,
    billing_address text,
    PRIMARY KEY (userid, payment_id)
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
    language text
);

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

-- Vector search index with COSINE similarity function
-- Enables ANN searches for content-based recommendations
CREATE INDEX IF NOT EXISTS videos_content_features_idx 
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
CREATE INDEX IF NOT EXISTS tags_category_idx ON killrvideo.tags(category) 
USING 'StorageAttachedIndex';

-- Vector search index for finding similar tags
CREATE INDEX IF NOT EXISTS tags_vector_idx 
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
CREATE INDEX IF NOT EXISTS user_preferences_vector_idx 
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
    flagged_reason text MASKED WITH mask_inner(0, 0), -- Data masking from Cassandra 5.0
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
CREATE INDEX IF NOT EXISTS youtube_videos_features_idx 
ON killrvideo.youtube_videos(content_features) 
USING 'StorageAttachedIndex'
WITH OPTIONS = { 'similarity_function': 'COSINE' };

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
CALLED ON NULL INPUT
RETURNS double
LANGUAGE java AS '
  if (rating_counter > 0) {
    double avg = rating_total.doubleValue() / rating_counter.doubleValue();
    return Math.round(avg * 10.0) / 10.0;
  } else {
    return 0.0;
  }
';

-- Example of a UDF that calculates a video popularity score based on views and ratings
CREATE OR REPLACE FUNCTION killrvideo.popularity_score(views counter, rating_counter counter, rating_total counter)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE java AS '
  if (views == 0 || rating_counter == 0) {
    return 0.0;
  }
  double avgRating = rating_total.doubleValue() / rating_counter.doubleValue();
  return Math.log10(views.doubleValue()) * avgRating;
';

-- Custom email domain redaction function for data masking
CREATE OR REPLACE FUNCTION killrvideo.redact_email_domain(email text)
CALLED ON NULL INPUT
RETURNS text
LANGUAGE java AS '
    if (email == null) return null;
    int atIndex = email.indexOf("@");
    if (atIndex > 0) {
        return email.substring(0, atIndex + 1) + "redacted.com";
    }
    return email;
';

------------------------------------------------------------------
-- Aggregate Functions
------------------------------------------------------------------
-- This section contains user-defined aggregate functions for custom analytics.
-- Cassandra 5.0 enhances UDAs with improved performance and reliability.

-- State function for weighted rating aggregate
CREATE OR REPLACE FUNCTION killrvideo.weighted_rating_state(state tuple<int, double>, video_id uuid, rating int, user_reputation int)
CALLED ON NULL INPUT
RETURNS tuple<int, double>
LANGUAGE java AS '
  if (rating == null || user_reputation == null) {
    return state;
  }
  
  // Weight rating by user reputation (1-10)
  double weightedRating = rating * (user_reputation / 10.0);
  
  // Update state: count and weighted sum
  state.setInt(0, state.getInt(0) + 1);
  state.setDouble(1, state.getDouble(1) + weightedRating);
  
  return state;
';

-- Final function for weighted rating aggregate
CREATE OR REPLACE FUNCTION killrvideo.weighted_rating_final(state tuple<int, double>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE java AS '
  if (state.getInt(0) == 0) {
    return 0.0;
  }
  double result = state.getDouble(1) / state.getInt(0);
  return Math.round(result * 10.0) / 10.0;
';

-- Weighted rating aggregate function
-- This aggregate calculates ratings weighted by user reputation
CREATE OR REPLACE AGGREGATE killrvideo.weighted_rating(uuid, int, int)
SFUNC weighted_rating_state
STYPE tuple<int, double>
FINALFUNC weighted_rating_final
INITCOND (0, 0.0);

------------------------------------------------------------------
-- Role-Based Access Control Notes
------------------------------------------------------------------
-- In a production environment, you would typically set up roles with appropriate
-- permissions for data masking. Below are examples of what these might look like:

-- For regular users:
-- CREATE ROLE app_user WITH LOGIN = true;
-- GRANT SELECT ON KEYSPACE killrvideo TO app_user;

-- For applications that need to query on masked columns:
-- GRANT SELECT, SELECT_MASKED ON killrvideo.users TO app_service;
-- GRANT SELECT, SELECT_MASKED ON killrvideo.content_moderation TO app_service;

-- For content moderators who need to see unmasked sensitive data:
-- GRANT SELECT ON killrvideo.content_moderation TO content_manager;
-- GRANT UNMASK ON killrvideo.content_moderation TO content_manager;

-- For administrators who need full access:
-- GRANT ALL PERMISSIONS ON KEYSPACE killrvideo TO killrvideo_admin;
-- GRANT UNMASK ON ALL TABLES IN KEYSPACE killrvideo TO killrvideo_admin;

------------------------------------------------------------------
-- Data Management Practices for Cassandra 5.0
------------------------------------------------------------------
-- This section outlines best practices for managing data in Cassandra 5.0.
-- While these aren't schema elements, they're important guidelines for users.

-- 1. TTL Management
-- While we don't set default TTLs at the table level, you can apply TTLs selectively:
-- 
-- For temporary content:
-- INSERT INTO killrvideo.videos (videoid, name, ...) VALUES (uuid(), 'Test Video', ...) USING TTL 604800; -- 7 days
--
-- For premium content:
-- INSERT INTO killrvideo.videos (videoid, name, ...) VALUES (uuid(), 'Premium Video', ...) USING TTL 31536000; -- 1 year
--
-- Check TTL of content:
-- SELECT videoid, name, TTL(description) FROM killrvideo.videos WHERE videoid = ?;

-- 2. Partition Size Management
-- Instead of table-level settings, use these practices:
-- 
-- - Choose appropriate partition keys that distribute data evenly
-- - Limit time-series data per partition (e.g., day-level partitioning)
-- - Monitor partition sizes through system tables
-- - Use wide partitions carefully, especially with SAI indexes

-- 3. Collection Best Practices
-- New collection functions in 5.0 enable better collection handling:
--
-- - Working with maps: map_keys(), map_values()
-- - Collection statistics: collection_count(), collection_min/max/sum/avg()
-- - Example: SELECT videoid, collection_count(tags) FROM videos WHERE videoid = ?;

-- 4. Vector Search Implementation
-- Vector search is implemented using:
--
-- - SAI index on vector columns with appropriate similarity function
-- - ANN query syntax: ORDER BY ANN OF column [vector]
-- - Similarity functions: similarity_cosine, similarity_euclidean, similarity_dot_product
-- - Example: SELECT * FROM videos ORDER BY ANN OF content_features [?] LIMIT 10;

-- 5. Data Masking Best Practices
-- Data masking should follow these guidelines:
--
-- - Focus on PII and sensitive data: emails, passwords, payment info
-- - Grant UNMASK only to roles that need it
-- - Use SELECT_MASKED for applications that need to query masked columns
-- - Consider custom masking functions for domain-specific needs
-- - Enable authentication to make data masking effective
