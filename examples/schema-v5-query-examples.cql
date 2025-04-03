-- KillrVideo Sample Queries for Cassandra 5.0
-- Using the sample data to demonstrate key Cassandra 5.0 features

-- 1. Storage-Attached Indexes (SAI)
-- New in Cassandra 5.0: Efficient secondary indexing

-- Find videos by tag using SAI instead of denormalized table
SELECT videoid, name, preview_image_location, added_date 
FROM killrvideo.videos 
WHERE 'tutorial' IN tags;

-- Find videos by category using SAI
SELECT videoid, name, preview_image_location 
FROM killrvideo.videos 
WHERE category = 'Education';

-- Find videos by language using SAI
SELECT videoid, name 
FROM killrvideo.videos 
WHERE language = 'English';

-- Find videos with multiple tag criteria using SAI
SELECT videoid, name 
FROM killrvideo.videos 
WHERE 'programming' IN tags AND 'tutorial' IN tags;

-- Find videos by user using SAI instead of denormalized table
SELECT videoid, name, added_date 
FROM killrvideo.videos 
WHERE userid = killrvideo.sample_uuid('user1')
ORDER BY added_date DESC;

-- 2. Vector Search (ANN)
-- New in Cassandra 5.0: Approximate Nearest Neighbor search

-- Find similar videos using content feature vectors
SELECT videoid, name, 
       similarity_cosine(content_features, [0.30, 0.10, 0.05, 0.35, 0.15, 0.00, 0.05, 0.10, 0.15, 0.20, 0.35, 0.40, 0.30, 0.10]) as similarity
FROM killrvideo.videos 
ORDER BY ANN OF content_features [0.30, 0.10, 0.05, 0.35, 0.15, 0.00, 0.05, 0.10, 0.15, 0.20, 0.35, 0.40, 0.30, 0.10] LIMIT 3;

-- Find similar videos to a specific video
SELECT v2.videoid, v2.name, similarity_cosine(v1.content_features, v2.content_features) as similarity
FROM killrvideo.videos v1, killrvideo.videos v2
WHERE v1.videoid = killrvideo.sample_uuid('video1') AND v2.videoid != killrvideo.sample_uuid('video1')
ORDER BY ANN OF v2.content_features [v1.content_features] LIMIT 3;

-- Find similar tags using vector similarity
SELECT tag, similarity_cosine(tag_vector, [0.40, 0.35, 0.25, 0.20, 0.15, 0.10, 0.05, 0.00]) as similarity
FROM killrvideo.tags
ORDER BY ANN OF tag_vector [0.40, 0.35, 0.25, 0.20, 0.15, 0.10, 0.05, 0.00] LIMIT 5;

-- Personalized recommendations using user preference vector
SELECT v.videoid, v.name, similarity_cosine(up.preference_vector, v.content_features) as match_score
FROM killrvideo.videos v, killrvideo.user_preferences up
WHERE up.userid = killrvideo.sample_uuid('user1')
ORDER BY ANN OF v.content_features [up.preference_vector] LIMIT 5;

-- 3. Data Masking
-- New in Cassandra 5.0: Protected sensitive data

-- Select user data with masked email (users without UNMASK permission see masked data)
SELECT userid, firstname, lastname, email 
FROM killrvideo.users 
WHERE account_status = 'active';

-- Select password data with masked passwords (users without UNMASK permission see hashed data)
SELECT email, password, account_locked 
FROM killrvideo.user_credentials 
WHERE email = 'john.doe@example.com';

-- Select content moderation data with masked reasons (users without UNMASK permission see masked data)
SELECT contentid, content_type, status, flagged_reason 
FROM killrvideo.content_moderation 
WHERE status = 'pending';

-- 4. Mathematical Functions
-- New in Cassandra 5.0: Enhanced mathematical operations

-- Calculate average rating using built-in functions
SELECT videoid, 
       round(rating_total / rating_counter, 1) as avg_rating 
FROM killrvideo.video_ratings 
WHERE videoid = killrvideo.sample_uuid('video1');

-- Calculate video popularity score
SELECT videoid, 
       exp(log(views) * 0.5 + log(rating_total / rating_counter) * 0.5) as popularity_score
FROM killrvideo.video_playback_stats vs
JOIN killrvideo.video_ratings vr USING (videoid)
WHERE videoid = killrvideo.sample_uuid('video4');

-- 5. Collection Operations
-- New in Cassandra 5.0: Enhanced collection functions

-- Count tags per video
SELECT videoid, name, collection_count(tags) as tag_count 
FROM killrvideo.videos 
WHERE collection_count(tags) >= 3;

-- Get maximum preference value from user
SELECT userid, collection_max(map_values(tag_preferences)) as max_preference
FROM killrvideo.user_preferences
WHERE userid = killrvideo.sample_uuid('user2');

-- Get weighted tag preferences for recommendation
SELECT v.videoid, v.name, 
       collection_sum(map_values(map_filter(
         map_entries(up.tag_preferences),
         (k, v) -> k.key IN v.tags
       ))) as preference_score
FROM killrvideo.videos v, killrvideo.user_preferences up
WHERE up.userid = killrvideo.sample_uuid('user2')
ORDER BY preference_score DESC
LIMIT 3;

-- 6. Vector Type for Analytics
-- New in Cassandra 5.0: Vector types for metrics

-- Get engagement metrics for a video by hour
SELECT hour, engagement_metrics 
FROM killrvideo.video_engagement 
WHERE videoid = killrvideo.sample_uuid('video1') 
AND day = toDate('2023-06-10');

-- Extract specific engagement metrics
SELECT hour, 
       engagement_metrics[0] as views,
       engagement_metrics[1] as watch_time,
       engagement_metrics[2] as likes,
       engagement_metrics[3] as completion_rate
FROM killrvideo.video_engagement 
WHERE videoid = killrvideo.sample_uuid('video1') 
AND day = toDate('2023-06-10');

-- Calculate average engagement across hours
SELECT videoid,
       avg(engagement_metrics[0]) as avg_views,
       avg(engagement_metrics[1]) as avg_watch_time,
       avg(engagement_metrics[2]) as avg_likes
FROM killrvideo.video_engagement 
WHERE videoid = killrvideo.sample_uuid('video1') 
AND day = toDate('2023-06-10')
GROUP BY videoid;

-- 7. Prefix Search with SAI
-- New in Cassandra 5.0: Efficient prefix search

-- Find tags starting with 'c' (prefix search with SAI)
SELECT tag 
FROM killrvideo.tags 
WHERE tag LIKE 'c%';

-- Find all programming-related tags
SELECT tag 
FROM killrvideo.tags 
WHERE tag LIKE 'java%';

-- 8. Enhanced Time Functions and Extended TTL
-- New in Cassandra 5.0: Extended TTL capabilities

-- Set different TTLs based on content category
UPDATE killrvideo.videos USING TTL 15552000 -- 180 days
SET ttl_policy = 'standard' 
WHERE videoid = killrvideo.sample_uuid('video1');

UPDATE killrvideo.videos USING TTL 31536000 -- 365 days
SET ttl_policy = 'premium' 
WHERE videoid = killrvideo.sample_uuid('video2');

-- Find when specific content will expire
SELECT videoid, name, 
       TTL(description) as seconds_until_expiry,
       TTL(description)/86400 as days_until_expiry
FROM killrvideo.videos
WHERE videoid = killrvideo.sample_uuid('video1');

-- 9. Content Analysis with Sentiment Scores
-- New in Cassandra 5.0: Sentiment analysis support

-- Find most positive comments
SELECT videoid, comment, sentiment_score
FROM killrvideo.comments
ORDER BY sentiment_score DESC
LIMIT 5;

-- Get average sentiment by video
SELECT videoid, avg(sentiment_score) as avg_sentiment
FROM killrvideo.comments
GROUP BY videoid;

-- Find potentially negative comments for moderation
SELECT videoid, comment, sentiment_score
FROM killrvideo.comments
WHERE sentiment_score < 0.3
LIMIT 5;

-- 10. Using Multiple Features Together
-- Combining Cassandra 5.0 features for powerful queries

-- Find similar educational videos in English with high ratings
SELECT v.videoid, v.name, 
       similarity_cosine(v.content_features, [0.30, 0.10, 0.05, 0.35, 0.15, 0.00, 0.05, 0.10, 0.15, 0.20, 0.35, 0.40, 0.30, 0.10]) as similarity,
       round(vr.rating_total / vr.rating_counter, 1) as avg_rating
FROM killrvideo.videos v
JOIN killrvideo.video_ratings vr ON v.videoid = vr.videoid
WHERE v.category = 'Education' 
AND v.language = 'English'
AND round(vr.rating_total / vr.rating_counter, 1) >= 4.0
ORDER BY ANN OF v.content_features [0.30, 0.10, 0.05, 0.35, 0.15, 0.00, 0.05, 0.10, 0.15, 0.20, 0.35, 0.40, 0.30, 0.10]
LIMIT 3;

-- Personalized recommendations filtered by user tag preferences
SELECT v.videoid, v.name,
       similarity_cosine(up.preference_vector, v.content_features) as similarity_score,
       collection_sum(map_values(map_filter(
         map_entries(up.tag_preferences),
         (k, v) -> k.key IN v.tags
       ))) as tag_score
FROM killrvideo.videos v, killrvideo.user_preferences up
WHERE up.userid = killrvideo.sample_uuid('user1')
AND v.category IN (SELECT map_keys(category_preferences) FROM killrvideo.user_preferences WHERE userid = killrvideo.sample_uuid('user1'))
ORDER BY similarity_score + tag_score DESC
LIMIT 5;