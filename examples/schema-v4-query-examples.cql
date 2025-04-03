-- KillrVideo Sample Queries for Cassandra 4.0
-- Using the sample data to demonstrate key Cassandra 4.0 features

-- 1. Current Time Functions
-- New in Cassandra 4.0: Automatic timestamp generation

-- Insert a user with automatically generated timestamp
INSERT INTO killrvideo.users (userid, email, firstname, lastname, account_status)
VALUES (killrvideo.sample_uuid('new_user'), 'new.user@example.com', 'New', 'User', 'active');
-- Note: created_date is automatically set with currentTimestamp()

-- Add a comment with automatically generated TimeUUID
INSERT INTO killrvideo.comments (videoid, comment, userid)
VALUES (killrvideo.sample_uuid('video1'), 'This is exactly what I needed!', killrvideo.sample_uuid('user4'));
-- Note: commentid is automatically set with currentTimeUUID()

-- 2. Collection Element Access
-- New in Cassandra 4.0: Enhanced collection operations

-- Check if a specific video has the 'tutorial' tag
SELECT videoid, name 
FROM killrvideo.videos 
WHERE videoid = killrvideo.sample_uuid('video1') 
AND tags CONTAINS 'tutorial';

-- Check if a specific video has both 'cassandra' and 'database' tags
SELECT videoid, name 
FROM killrvideo.videos 
WHERE videoid = killrvideo.sample_uuid('video1') 
AND tags CONTAINS 'cassandra' 
AND tags CONTAINS 'database';

-- 3. Arithmetic Operations
-- New in Cassandra 4.0: Enhanced arithmetic operations

-- Calculate days since video was added
SELECT videoid, name, added_date, 
       toDate(currentTimestamp()) - toDate(added_date) AS days_since_added
FROM killrvideo.videos 
WHERE videoid = killrvideo.sample_uuid('video1');

-- Find videos added in the last 30 days
SELECT videoid, name, added_date
FROM killrvideo.videos 
WHERE added_date > currentTimestamp() - 30d;

-- Calculate average view duration for videos
SELECT videoid, 
       views, 
       total_play_time, 
       CAST(total_play_time AS float) / CAST(views AS float) AS avg_view_duration_seconds
FROM killrvideo.video_playback_stats
WHERE videoid = killrvideo.sample_uuid('video1');

-- 4. Traditional Denormalized Tables
-- In Cassandra 4.0, denormalized tables remain essential for high-performance queries

-- Get today's latest videos
SELECT videoid, name, preview_image_location, userid
FROM killrvideo.latest_videos
WHERE day = currentDate()
ORDER BY added_date DESC
LIMIT 5;

-- Get all videos by a specific user
SELECT videoid, name, added_date
FROM killrvideo.user_videos
WHERE userid = killrvideo.sample_uuid('user1')
ORDER BY added_date DESC;

-- Get user's comments
SELECT videoid, commentid, comment
FROM killrvideo.comments_by_user
WHERE userid = killrvideo.sample_uuid('user2');

-- 5. Secondary Indexes
-- In Cassandra 4.0, secondary indexes are improved but still have limitations

-- Find users by email (using secondary index)
SELECT userid, firstname, lastname 
FROM killrvideo.users 
WHERE email = 'john.doe@example.com';

-- Find active users (using secondary index)
SELECT userid, email, firstname, lastname 
FROM killrvideo.users 
WHERE account_status = 'active';

-- 6. User-Defined Functions
-- Enhanced in Cassandra 4.0 with better sandboxing

-- Get average rating for a video
SELECT videoid, killrvideo.avg_rating(rating_counter, rating_total) as average_rating 
FROM killrvideo.video_ratings 
WHERE videoid = killrvideo.sample_uuid('video1');

-- 7. Time-Based Queries
-- Leveraging Cassandra 4.0's time functions

-- Get all user activity for today
SELECT activity_type, activity_id, activity_timestamp 
FROM killrvideo.user_activity 
WHERE userid = killrvideo.sample_uuid('user1') 
AND day = currentDate();

-- Find all videos added on a specific date
SELECT videoid, name, added_date 
FROM killrvideo.latest_videos 
WHERE day = toDate('2023-06-10')
ORDER BY added_date DESC;

-- 8. Using DESCRIBE statements 
-- New in Cassandra 4.0: Server-side support for DESCRIBE

-- Describe the keyspace structure
DESCRIBE KEYSPACE killrvideo;

-- Describe a specific table
DESCRIBE TABLE killrvideo.videos;

-- 9. Enhanced Collection Updates
-- More flexible in Cassandra 4.0

-- Add a tag to a video
UPDATE killrvideo.videos 
SET tags = tags + {'educational'} 
WHERE videoid = killrvideo.sample_uuid('video1');

-- Remove a tag from a video
UPDATE killrvideo.videos 
SET tags = tags - {'outdated'} 
WHERE videoid = killrvideo.sample_uuid('video1');

-- 10. Batch Operations for Data Consistency
-- More reliable in Cassandra 4.0

-- Add a comment and update the comment by user table atomically
BEGIN BATCH
  INSERT INTO killrvideo.comments (videoid, commentid, comment, userid) 
  VALUES (killrvideo.sample_uuid('video5'), now(), 'This JavaScript tutorial is fantastic!', killrvideo.sample_uuid('user2'));
  
  INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid) 
  VALUES (killrvideo.sample_uuid('user2'), now(), 'This JavaScript tutorial is fantastic!', killrvideo.sample_uuid('video5'));
APPLY BATCH;

-- 11. Recommendations and Related Content

-- Get video recommendations for a user
SELECT videoid, name, preview_image_location, rating
FROM killrvideo.video_recommendations
WHERE userid = killrvideo.sample_uuid('user1')
ORDER BY added_date DESC, rating DESC;

-- Get similar videos for a specific video
SELECT userid, name, preview_image_location, rating
FROM killrvideo.video_recommendations_by_video
WHERE videoid = killrvideo.sample_uuid('video1')
ORDER BY rating DESC;