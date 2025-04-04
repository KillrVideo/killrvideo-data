-- KillrVideo Sample Queries for Cassandra 5.0
-- This script demonstrates the advanced querying capabilities
-- introduced in Cassandra 5.0 using the KillrVideo schema.

-- Note: These queries assume that the vector dimensions in the schema
-- have been adjusted to match our sample data (vector<float, 16> instead of 512).
-- Make sure your schema is updated accordingly before running these queries.

------------------------------------------------------------------
-- Storage-Attached Indexes (SAI) Queries
------------------------------------------------------------------

-- 1. Find videos by tag using SAI
-- Before 5.0: Required a denormalized table (videos_by_tag)
-- Now: Direct query on videos table with SAI
SELECT videoid, name, added_date, preview_image_location 
FROM killrvideo.videos 
WHERE tags CONTAINS 'cassandra';

-- 2. Find videos by category using SAI
SELECT videoid, name, preview_image_location 
FROM killrvideo.videos 
WHERE category = 'Education';

-- 3. Find videos by language using SAI
SELECT videoid, name 
FROM killrvideo.videos 
WHERE language = 'English';

-- 4. Find videos with multiple SAI conditions
SELECT videoid, name, category
FROM killrvideo.videos 
WHERE tags CONTAINS 'tutorial' AND category = 'Education';

-- 5. Find videos by user (SAI replaces the user_videos table)
SELECT videoid, name, added_date 
FROM killrvideo.videos 
WHERE userid = 11111111-1111-1111-1111-111111111111;

-- 6. Complex filtering with multiple indexes
SELECT videoid, name, added_date
FROM killrvideo.videos
WHERE category = 'Education'
  AND content_rating = 'G'
  AND tags CONTAINS 'programming';

------------------------------------------------------------------
-- Vector Search (ANN) Queries
------------------------------------------------------------------

-- 1. Find similar videos using content feature vectors
-- This enables ML-based content recommendations
SELECT videoid, name,
       similarity_cosine(content_features, [0.2, 0.3, 0.1, 0.4, 0.6, 0.2, 0.1, 0.5, 0.3, 0.7, 0.8, 0.1, 0.2, 0.3, 0.4, 0.5]) as similarity
FROM killrvideo.videos 
LIMIT 5;
-- Note: Sort by similarity in the application

-- 2. Find similar videos to a specific video
-- Note: This would typically use a multi-query approach in Cassandra
-- First query to get the reference vector:
SELECT content_features
FROM killrvideo.videos 
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;

-- Then use the retrieved vector in similarity calculation:
-- Replace [content_features_value] with the actual vector retrieved above
SELECT videoid, name,
       similarity_cosine(content_features, [0.2, 0.3, 0.1, 0.4, 0.6, 0.2, 0.1, 0.5, 0.3, 0.7, 0.8, 0.1, 0.2, 0.3, 0.4, 0.5]) as similarity
FROM killrvideo.videos 
LIMIT 5;
-- Note: Sort by similarity in the application

-- 3. Find similar tags using vector similarity
SELECT tag, 
       similarity_cosine(tag_vector, [0.8, 0.1, 0.3, 0.2, 0.7, 0.1, 0.5, 0.3]) as similarity
FROM killrvideo.tags
LIMIT 5;
-- Note: Sort by similarity in the application

-- 4. Personalized recommendations using user preference vector
-- First, query to get user preference vector:
SELECT preference_vector
FROM killrvideo.user_preferences
WHERE userid = 11111111-1111-1111-1111-111111111111;

-- Then use that vector in similarity calculation (separate query):
SELECT videoid, name, category,
       similarity_cosine(content_features, [0.2, 0.4, 0.3, 0.5, 0.7, 0.2, 0.3, 0.1, 0.6, 0.4, 0.2, 0.8, 0.3, 0.5, 0.1, 0.7]) as similarity
FROM killrvideo.videos
LIMIT 5;
-- Note: Sort by similarity in the application

-- 5. Different similarity metrics comparison
-- a. Cosine similarity (angle between vectors)
SELECT videoid, name, 
       similarity_cosine(content_features, [0.2, 0.3, 0.1, 0.4, 0.6, 0.2, 0.1, 0.5, 0.3, 0.7, 0.8, 0.1, 0.2, 0.3, 0.4, 0.5]) as cosine_sim
FROM killrvideo.videos 
LIMIT 3;

-- b. Euclidean similarity (distance between vectors)
SELECT videoid, name, 
       similarity_euclidean(content_features, [0.2, 0.3, 0.1, 0.4, 0.6, 0.2, 0.1, 0.5, 0.3, 0.7, 0.8, 0.1, 0.2, 0.3, 0.4, 0.5]) as euclidean_sim
FROM killrvideo.videos 
LIMIT 3;

-- c. Dot product similarity (raw vector multiplication)
SELECT videoid, name, 
       similarity_dot_product(content_features, [0.2, 0.3, 0.1, 0.4, 0.6, 0.2, 0.1, 0.5, 0.3, 0.7, 0.8, 0.1, 0.2, 0.3, 0.4, 0.5]) as dot_product_sim
FROM killrvideo.videos 
LIMIT 3;

------------------------------------------------------------------
-- Data Masking Queries
------------------------------------------------------------------

-- 1. Query user data with masked email (for users without UNMASK permission)
-- The email column will be automatically masked based on schema definition
SELECT userid, firstname, lastname, email 
FROM killrvideo.users 
WHERE account_status = 'active';

-- 2. Query payment information with masked card numbers
-- The card_number column will be automatically masked
SELECT userid, payment_id, card_number, card_expiry 
FROM killrvideo.payment_info 
WHERE userid = 11111111-1111-1111-1111-111111111111;

-- 3. Query moderation data with masked reasons
-- The flagged_reason column will be automatically masked
-- Note: Filtering on status requires ALLOW FILTERING or an index
SELECT contentid, content_type, status, flagged_reason 
FROM killrvideo.content_moderation 
ALLOW FILTERING;

-- A better approach is to create an index first:
-- CREATE INDEX moderation_status_idx ON killrvideo.content_moderation(status) USING 'StorageAttachedIndex';

------------------------------------------------------------------
-- Mathematical Operations
------------------------------------------------------------------

-- 1. Calculate average rating
-- Query counters directly
SELECT videoid, 
       CAST(rating_total AS FLOAT) / CAST(rating_counter AS FLOAT) as avg_rating 
FROM killrvideo.video_ratings 
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;

-- 2. Calculate video popularity score 
-- Simple counter-based calculation
SELECT videoid, views
FROM killrvideo.video_playback_stats
WHERE videoid = dddddddd-dddd-dddd-dddd-dddddddddddd;

-- 3. Find videos with above average watch time
SELECT videoid, views, total_play_time
FROM killrvideo.video_playback_stats
LIMIT 5;
-- Note: Filter for views > 0 in application code

-- 4. Calculate completion rate percentage
-- Note: Calculate in application code:
-- completion_rate = (complete_views * 100.0) / views
SELECT videoid, complete_views, views
FROM killrvideo.video_playback_stats
LIMIT 5;
-- Note: Filter for views > 0 in application code

------------------------------------------------------------------
-- Collection Operation Queries
------------------------------------------------------------------

-- 1. Count tags per video
-- Simple collection query to get tags
SELECT videoid, name, tags 
FROM killrvideo.videos 
LIMIT 5;
-- Note: Count tags in application code

-- 2. Access preference data 
SELECT userid, tag_preferences
FROM killrvideo.user_preferences
WHERE userid = 11111111-1111-1111-1111-111111111111;

-- 3. Access category preferences
SELECT userid, category_preferences
FROM killrvideo.user_preferences
WHERE userid = 22222222-2222-2222-2222-222222222222;


------------------------------------------------------------------
-- Time-Based Queries
------------------------------------------------------------------

-- 1. Find videos that will expire soon (TTL example)
-- Cassandra 5.0 extends the max TTL significantly
SELECT videoid, name, 
       TTL(description) as seconds_until_expiry
FROM killrvideo.videos
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;

-- 2. Videos added in a specific date range
SELECT videoid, name, added_date
FROM killrvideo.videos
WHERE added_date >= '2023-06-01' AND added_date <= '2023-06-10';

-- 3. Get video engagement metrics for specific hours
SELECT hour, engagement_metrics
FROM killrvideo.video_engagement
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
  AND day = '2023-06-10'
ORDER BY hour;

-- Note: In application code, extract metrics from vector by index

------------------------------------------------------------------
-- Sentiment Analysis Queries
------------------------------------------------------------------

-- 1. Get comments with sentiment scores
-- Note: Can't order without partition key restriction
SELECT videoid, comment, sentiment_score
FROM killrvideo.comments
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
LIMIT 5;

-- 2. Find negative comments requiring moderation
SELECT videoid, comment, sentiment_score
FROM killrvideo.comments
WHERE videoid = eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee
LIMIT 5;

------------------------------------------------------------------
-- Migration Path Queries
------------------------------------------------------------------

-- 1. Finding videos by tag
-- Old pattern (Cassandra 3.x - if using a denormalized table)
/* 
SELECT videoid, name, added_date, preview_image_location 
FROM killrvideo.videos_by_tag 
WHERE tag = 'cassandra';
*/

-- New pattern (Cassandra 5.0)
SELECT videoid, name, added_date, preview_image_location 
FROM killrvideo.videos 
WHERE tags CONTAINS 'cassandra';

-- 2. Finding videos by user
-- Old pattern (Cassandra 3.x - if using a denormalized table)
/*
SELECT videoid, name, added_date, preview_image_location 
FROM killrvideo.user_videos 
WHERE userid = 11111111-1111-1111-1111-111111111111;
*/

-- New pattern (Cassandra 5.0)
SELECT videoid, name, added_date, preview_image_location 
FROM killrvideo.videos 
WHERE userid = 11111111-1111-1111-1111-111111111111;

-- 3. Getting latest videos
-- Old pattern (Cassandra 3.x)
SELECT videoid, name, added_date, preview_image_location 
FROM killrvideo.latest_videos 
WHERE day = '2023-06-10';

-- New pattern (Cassandra 5.0)
SELECT videoid, name, added_date, preview_image_location 
FROM killrvideo.videos 
WHERE added_date >= '2023-06-10' AND added_date < '2023-06-11';

-- 4. Calculating average rating
-- Old pattern (Cassandra 3.x - if using a UDF)
/*
SELECT videoid, killrvideo.avg_rating(rating_counter, rating_total) 
FROM killrvideo.video_ratings
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;
*/

-- New pattern (Cassandra 5.0)
SELECT videoid, CAST(rating_total AS FLOAT) / CAST(rating_counter AS FLOAT) 
FROM killrvideo.video_ratings
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;