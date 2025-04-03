-- KillrVideo Schema for Cassandra 4.0
-- Showcasing data modeling features introduced in Cassandra 4.0
--
-- This schema demonstrates the progression from Cassandra 3.x to 4.0,
-- highlighting new features and how they can be used to enhance
-- your data models. Tables are grouped by functional area with detailed
-- comments explaining the purpose and features used.

CREATE KEYSPACE IF NOT EXISTS killrvideo 
WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}
AND durable_writes = true;

------------------------------------------------------------------
-- User Management Tables
------------------------------------------------------------------
-- This section contains tables related to user profiles, authentication,
-- and account management. Cassandra 4.0 adds timestamp functions and
-- enhanced security features.

-- User profile information
-- Supports queries: Get user by ID, Find user by email, List users by status
--
-- New in Cassandra 4.0:
-- * Default timestamp function for automatic creation dates
-- * Secondary indexes improve but still have limitations
CREATE TABLE IF NOT EXISTS killrvideo.users (
    userid uuid PRIMARY KEY,
    created_date timestamp DEFAULT currentTimestamp(), -- Auto timestamp from Cassandra 4.0
    email text,
    firstname text,
    lastname text,
    account_status text,
    last_login_date timestamp
);

-- Secondary index on email for lookups
-- Still better to use a separate table for high-volume lookups
CREATE INDEX IF NOT EXISTS users_email_idx ON killrvideo.users(email);

-- Secondary index for filtering users by account status
CREATE INDEX IF NOT EXISTS users_account_status_idx ON killrvideo.users(account_status);

-- User authentication credentials
-- Supports queries: Authenticate user, Check account status
--
-- New in Cassandra 4.0:
-- * Enhanced role-based authentication for security
CREATE TABLE IF NOT EXISTS killrvideo.user_credentials (
    email text PRIMARY KEY,
    password text,
    userid uuid,
    account_locked boolean,
    failed_login_attempts counter
);

------------------------------------------------------------------
-- Video Management Tables
------------------------------------------------------------------
-- This section contains tables related to video storage, retrieval,
-- and metadata. Cassandra 4.0 adds timestamp functions and 
-- materialized view improvements.

-- Primary video metadata table
-- Supports queries: Get video by ID
--
-- New in Cassandra 4.0:
-- * Auto timestamp with currentTimestamp() function
-- * Collection elements can be accessed in queries
CREATE TABLE IF NOT EXISTS killrvideo.videos (
    videoid uuid PRIMARY KEY,
    added_date timestamp DEFAULT currentTimestamp(), -- Auto timestamp from Cassandra 4.0
    description text,
    location text,
    location_type int,
    name text,
    preview_image_location text,
    tags set<text>,                                  -- Can now access elements in queries
    userid uuid,
    content_rating text,                             -- 'G', 'PG', 'PG-13', 'R', etc.
    category text,
    language text
);

-- Secondary index for video name search
-- Note: Limited utility for high-volume or high-cardinality searches
CREATE INDEX IF NOT EXISTS videos_name_idx ON killrvideo.videos(name);

-- User's videos lookup table
-- Cassandra 4.0 materialized views could be an alternative approach,
-- but for performance, denormalized tables are still preferred
CREATE TABLE IF NOT EXISTS killrvideo.user_videos (
    userid uuid,
    added_date timestamp,
    videoid uuid,
    name text,
    preview_image_location text,
    PRIMARY KEY (userid, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

-- Latest videos lookup
-- Shows use of date function from Cassandra 4.0
CREATE TABLE IF NOT EXISTS killrvideo.latest_videos (
    day date DEFAULT currentDate(),   -- New in Cassandra 4.0: currentDate() function
    added_date timestamp,
    videoid uuid,
    name text,
    preview_image_location text,
    userid uuid,
    PRIMARY KEY (day, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

-- Note: Although Cassandra 4.0 includes improvements to materialized views, 
-- they are still considered experimental and not recommended for production use.
-- Continue using denormalized tables for production workloads.

-- Video playback statistics table
-- Supports queries: Get view counts, Track engagement metrics
CREATE TABLE IF NOT EXISTS killrvideo.video_playback_stats (
    videoid uuid PRIMARY KEY,
    views counter,
    total_play_time counter,      -- Total seconds watched
    complete_views counter        -- Number of complete views
);

------------------------------------------------------------------
-- Tags and Discovery Tables
------------------------------------------------------------------
-- This section contains tables for content discovery and tagging.
-- Cassandra 4.0 timestamp functions help with consistent time tracking.

-- Tags by first letter (for autocomplete/browse)
-- No major changes in Cassandra 4.0
CREATE TABLE IF NOT EXISTS killrvideo.tags_by_letter (
    first_letter text,
    tag text,
    PRIMARY KEY (first_letter, tag)
) WITH CLUSTERING ORDER BY (tag ASC);

-- Videos by tag lookup table
-- New in Cassandra 4.0: CurrentTimestamp for consistent tagging dates
CREATE TABLE IF NOT EXISTS killrvideo.videos_by_tag (
    tag text,
    videoid uuid,
    added_date timestamp,
    name text,
    preview_image_location text,
    tagged_date timestamp DEFAULT currentTimestamp(), -- Auto timestamp from Cassandra 4.0
    userid uuid,
    PRIMARY KEY (tag, videoid)
) WITH CLUSTERING ORDER BY (videoid ASC);

------------------------------------------------------------------
-- Comments System Tables
------------------------------------------------------------------
-- This section contains tables for the video comment system.
-- Cassandra 4.0 adds TimeUUID generation and materialized views.

-- Video comments table
-- Supports queries: Get comments for a video, Sort comments chronologically
--
-- New in Cassandra 4.0:
-- * Automatic TimeUUID generation with currentTimeUUID()
CREATE TABLE IF NOT EXISTS killrvideo.comments (
    videoid uuid,
    commentid timeuuid DEFAULT currentTimeUUID(), -- Auto TimeUUID from Cassandra 4.0
    comment text,
    userid uuid,
    PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

-- Comments by user lookup table
-- Denormalized table pattern is still preferred over materialized views
-- Supports queries: "Find all comments by user X across all videos"
CREATE TABLE IF NOT EXISTS killrvideo.comments_by_user (
    userid uuid,
    commentid timeuuid,
    comment text,
    videoid uuid,
    PRIMARY KEY (userid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

------------------------------------------------------------------
-- Ratings and Recommendations Tables
------------------------------------------------------------------
-- This section contains tables related to video ratings and personalized
-- recommendations.

-- Video ratings aggregation table
-- Supports queries: Get average rating for a video
CREATE TABLE IF NOT EXISTS killrvideo.video_ratings (
    videoid uuid PRIMARY KEY,
    rating_counter counter,       -- Count of ratings
    rating_total counter          -- Sum of all ratings
);

-- Individual user ratings table
-- Supports queries: Check if user rated a video, Get user's rating
--
-- New in Cassandra 4.0:
-- * Automatic timestamp with currentTimestamp()
CREATE TABLE IF NOT EXISTS killrvideo.video_ratings_by_user (
    videoid uuid,
    userid uuid,
    rating int,
    rating_date timestamp DEFAULT currentTimestamp(), -- Auto timestamp from Cassandra 4.0
    PRIMARY KEY (videoid, userid)
) WITH CLUSTERING ORDER BY (userid ASC);

-- Video recommendations by user
-- Supports queries: Get personalized video recommendations for a user
CREATE TABLE IF NOT EXISTS killrvideo.video_recommendations (
    userid uuid,
    added_date timestamp,
    videoid uuid,
    authorid uuid,
    name text,
    preview_image_location text,
    rating float,
    PRIMARY KEY (userid, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

-- Video recommendations by video
-- Supports queries: Get related videos for a specific video
CREATE TABLE IF NOT EXISTS killrvideo.video_recommendations_by_video (
    videoid uuid,
    userid uuid,
    added_date timestamp static,
    authorid uuid static,
    name text static,
    preview_image_location text static,
    rating float,
    PRIMARY KEY (videoid, userid)
) WITH CLUSTERING ORDER BY (userid ASC);

------------------------------------------------------------------
-- User Activity Tables
------------------------------------------------------------------
-- This section contains tables for user activity tracking.
-- Cassandra 4.0 timestamp functions improve time-series data.

-- User activity time series
-- Supports queries: Track user behavior, Analyze engagement patterns
--
-- New in Cassandra 4.0:
-- * Auto date functions for partitioning
-- * Timestamp functions for ordering
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

-- External video content import table
-- Supports queries: Import videos from YouTube, Track usage status
CREATE TABLE IF NOT EXISTS killrvideo.youtube_videos (
    sourceid text,
    published_at timestamp,
    youtube_video_id text,
    description text,
    name text,
    used boolean,
    PRIMARY KEY (sourceid, published_at, youtube_video_id)
) WITH CLUSTERING ORDER BY (published_at DESC, youtube_video_id ASC);

------------------------------------------------------------------
-- User-Defined Functions
------------------------------------------------------------------
-- This section contains user-defined functions that extend CQL capabilities.

-- Average rating calculation function
-- Cassandra 4.0 has improvements to UDF sandboxing for security
CREATE OR REPLACE FUNCTION killrvideo.avg_rating(rating_counter counter, rating_total counter)
RETURNS FLOAT
LANGUAGE java
AS 'return rating_counter.doubleValue() > 0 ? Math.round((rating_total.doubleValue() / rating_counter.doubleValue()) * 10) / 10.0 : 0.0;';

------------------------------------------------------------------
-- Role-Based Access Control
------------------------------------------------------------------
-- This section defines roles with specific permissions.
-- Cassandra 4.0 enhances role-based access control and datacenter restrictions.

-- Basic application user role
-- New in Cassandra 4.0: Enhanced role-based access control
CREATE ROLE IF NOT EXISTS app_user WITH PASSWORD = 'password' AND LOGIN = true;

-- Admin role with datacenter restrictions
-- New in Cassandra 4.0: Datacenter restrictions for roles
CREATE ROLE IF NOT EXISTS db_admin 
WITH PASSWORD = 'password' 
AND LOGIN = true 
AND ACCESS TO DATACENTERS {'dc1', 'dc2'};

-- Enable audit logging in cassandra.yaml for security tracking
-- New in Cassandra 4.0: Audit logging capabilities