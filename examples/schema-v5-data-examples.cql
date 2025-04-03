-- KillrVideo Sample Data for Cassandra 5.0
-- Sample inserts that demonstrate the model and provide data for query examples
-- Including new Cassandra 5.0 features like vector types, data masking, etc.

-- Create a UUID generator function (for examples only - in production use UUID() function in application code)
CREATE OR REPLACE FUNCTION killrvideo.sample_uuid(seed text)
RETURNS uuid
LANGUAGE java AS '
    return java.util.UUID.nameUUIDFromBytes(seed.getBytes());
';

-----------------------------------------------------------------------------
-- Sample User Data
-----------------------------------------------------------------------------

-- User Credentials - Note the password is masked with SHA-256 
INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked, failed_login_attempts)
VALUES ('john.doe@example.com', 'password123', killrvideo.sample_uuid('user1'), false, 0);

INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked, failed_login_attempts)
VALUES ('jane.smith@example.com', 'securepass456', killrvideo.sample_uuid('user2'), false, 0);

INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked, failed_login_attempts)
VALUES ('bob.johnson@example.com', 'bobspassword', killrvideo.sample_uuid('user3'), false, 0);

INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked, failed_login_attempts)
VALUES ('alice.wang@example.com', 'alicesecure', killrvideo.sample_uuid('user4'), false, 0);

INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked, failed_login_attempts)
VALUES ('carlos.rodriguez@example.com', 'carlospass', killrvideo.sample_uuid('user5'), false, 0);

-- User Information - Note email is masked with inner masking
INSERT INTO killrvideo.users (userid, firstname, lastname, email, account_status, created_date, last_login_date)
VALUES (killrvideo.sample_uuid('user1'), 'John', 'Doe', 'john.doe@example.com', 'active', '2023-01-15 10:30:00', '2023-06-10 08:45:00');

INSERT INTO killrvideo.users (userid, firstname, lastname, email, account_status, created_date, last_login_date)
VALUES (killrvideo.sample_uuid('user2'), 'Jane', 'Smith', 'jane.smith@example.com', 'active', '2023-02-20 14:15:00', '2023-06-12 16:20:00');

INSERT INTO killrvideo.users (userid, firstname, lastname, email, account_status, created_date, last_login_date)
VALUES (killrvideo.sample_uuid('user3'), 'Bob', 'Johnson', 'bob.johnson@example.com', 'inactive', '2023-03-10 09:00:00', '2023-05-05 11:30:00');

INSERT INTO killrvideo.users (userid, firstname, lastname, email, account_status, created_date, last_login_date)
VALUES (killrvideo.sample_uuid('user4'), 'Alice', 'Wang', 'alice.wang@example.com', 'active', '2023-04-05 11:45:00', '2023-06-11 19:10:00');

INSERT INTO killrvideo.users (userid, firstname, lastname, email, account_status, created_date, last_login_date)
VALUES (killrvideo.sample_uuid('user5'), 'Carlos', 'Rodriguez', 'carlos.rodriguez@example.com', 'active', '2023-05-12 16:30:00', '2023-06-13 09:25:00');

-----------------------------------------------------------------------------
-- Sample Video Data
-----------------------------------------------------------------------------

-- Main video table - Note the inclusion of content_features vector
INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, content_features, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video1'), '2023-05-20 14:30:00', 'Learn the basics of Cassandra data modeling', '/videos/cassandra-basics.mp4', 1, 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', {'cassandra', 'database', 'tutorial'}, 
[0.25, 0.10, 0.05, 0.30, 0.15, 0.05, 0.02, 0.08, 0.00, 0.00, 0.30, 0.45, 0.25, 0.00], 
killrvideo.sample_uuid('user1'), 'G', 'Education', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, content_features, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video2'), '2023-05-25 10:15:00', 'My trip to Japan - Tokyo highlights and travel tips', '/videos/japan-trip.mp4', 1, 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', {'travel', 'japan', 'vlog'}, 
[0.05, 0.30, 0.45, 0.15, 0.05, 0.00, 0.00, 0.00, 0.40, 0.35, 0.25, 0.00, 0.10, 0.50], 
killrvideo.sample_uuid('user2'), 'G', 'Travel', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, content_features, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video3'), '2023-06-01 09:45:00', 'Quick and easy pasta carbonara recipe', '/videos/carbonara-recipe.mp4', 1, 'The Perfect Carbonara', '/thumbnails/carbonara.jpg', {'cooking', 'recipe', 'italian'}, 
[0.00, 0.05, 0.10, 0.05, 0.35, 0.45, 0.00, 0.00, 0.20, 0.25, 0.30, 0.35, 0.45, 0.10], 
killrvideo.sample_uuid('user3'), 'G', 'Food', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, content_features, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video4'), '2023-06-05 16:20:00', 'Funny cat compilation - cutest moments', '/videos/cat-compilation.mp4', 1, 'Cute Cat Compilation', '/thumbnails/cats.jpg', {'cats', 'funny', 'pets'}, 
[0.50, 0.35, 0.05, 0.00, 0.00, 0.00, 0.20, 0.30, 0.45, 0.00, 0.05, 0.10, 0.15, 0.00], 
killrvideo.sample_uuid('user4'), 'G', 'Pets', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, content_features, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video5'), '2023-06-10 11:10:00', 'Advanced JavaScript concepts explained', '/videos/js-advanced.mp4', 1, 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', {'programming', 'javascript', 'tutorial'}, 
[0.35, 0.10, 0.05, 0.40, 0.10, 0.00, 0.00, 0.00, 0.15, 0.20, 0.45, 0.35, 0.05, 0.10], 
killrvideo.sample_uuid('user5'), 'G', 'Education', 'English');

-- Latest videos (denormalized)
INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES (toDate('2023-05-20'), '2023-05-20 14:30:00', killrvideo.sample_uuid('video1'), 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', killrvideo.sample_uuid('user1'), 'G', 'Education');

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES (toDate('2023-05-25'), '2023-05-25 10:15:00', killrvideo.sample_uuid('video2'), 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', killrvideo.sample_uuid('user2'), 'G', 'Travel');

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES (toDate('2023-06-01'), '2023-06-01 09:45:00', killrvideo.sample_uuid('video3'), 'The Perfect Carbonara', '/thumbnails/carbonara.jpg', killrvideo.sample_uuid('user3'), 'G', 'Food');

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES (toDate('2023-06-05'), '2023-06-05 16:20:00', killrvideo.sample_uuid('video4'), 'Cute Cat Compilation', '/thumbnails/cats.jpg', killrvideo.sample_uuid('user4'), 'G', 'Pets');

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES (toDate('2023-06-10'), '2023-06-10 11:10:00', killrvideo.sample_uuid('video5'), 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', killrvideo.sample_uuid('user5'), 'G', 'Education');

-----------------------------------------------------------------------------
-- Sample Comments Data
-----------------------------------------------------------------------------

-- Comments on videos - Note the sentiment_score for content analysis
INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (killrvideo.sample_uuid('video1'), now(), 'Great tutorial! Really helped me understand Cassandra better.', killrvideo.sample_uuid('user2'), 0.9);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (killrvideo.sample_uuid('video1'), now(), 'Could you make a follow-up on advanced modeling patterns?', killrvideo.sample_uuid('user3'), 0.7);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (killrvideo.sample_uuid('video2'), now(), 'Loved your Japan vlog! What camera did you use?', killrvideo.sample_uuid('user4'), 0.8);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (killrvideo.sample_uuid('video3'), now(), 'I tried this recipe and it turned out amazing!', killrvideo.sample_uuid('user5'), 0.95);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (killrvideo.sample_uuid('video4'), now(), 'The gray cat at 2:30 is so cute!', killrvideo.sample_uuid('user1'), 0.85);

-- Comments by user (denormalized)
INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (killrvideo.sample_uuid('user2'), now(), 'Great tutorial! Really helped me understand Cassandra better.', killrvideo.sample_uuid('video1'), 0.9);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (killrvideo.sample_uuid('user3'), now(), 'Could you make a follow-up on advanced modeling patterns?', killrvideo.sample_uuid('video1'), 0.7);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (killrvideo.sample_uuid('user4'), now(), 'Loved your Japan vlog! What camera did you use?', killrvideo.sample_uuid('video2'), 0.8);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (killrvideo.sample_uuid('user5'), now(), 'I tried this recipe and it turned out amazing!', killrvideo.sample_uuid('video3'), 0.95);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (killrvideo.sample_uuid('user1'), now(), 'The gray cat at 2:30 is so cute!', killrvideo.sample_uuid('video4'), 0.85);

-----------------------------------------------------------------------------
-- Sample Tag Data
-----------------------------------------------------------------------------

-- Tags metadata with vector representations
INSERT INTO killrvideo.tags (tag, tag_vector, tag_count, related_tags, category)
VALUES ('cassandra', [0.40, 0.35, 0.25, 0.20, 0.15, 0.10, 0.05, 0.00], 1, {'database', 'nosql', 'data-modeling'}, 'Technology');

INSERT INTO killrvideo.tags (tag, tag_vector, tag_count, related_tags, category)
VALUES ('cooking', [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45], 1, {'recipe', 'food', 'culinary'}, 'Food');

INSERT INTO killrvideo.tags (tag, tag_vector, tag_count, related_tags, category)
VALUES ('cats', [0.45, 0.40, 0.35, 0.30, 0.25, 0.20, 0.15, 0.10], 1, {'pets', 'animals', 'cute'}, 'Pets');

INSERT INTO killrvideo.tags (tag, tag_vector, tag_count, related_tags, category)
VALUES ('javascript', [0.25, 0.20, 0.15, 0.30, 0.35, 0.40, 0.10, 0.05], 1, {'programming', 'web-development', 'coding'}, 'Technology');

INSERT INTO killrvideo.tags (tag, tag_vector, tag_count, related_tags, category)
VALUES ('japan', [0.05, 0.10, 0.40, 0.35, 0.30, 0.25, 0.20, 0.15], 1, {'travel', 'asia', 'tokyo'}, 'Travel');

INSERT INTO killrvideo.tags (tag, tag_vector, tag_count, related_tags, category)
VALUES ('tutorial', [0.30, 0.35, 0.25, 0.20, 0.40, 0.15, 0.10, 0.05], 2, {'education', 'learning', 'howto'}, 'Education');

INSERT INTO killrvideo.tags (tag, tag_vector, tag_count, related_tags, category)
VALUES ('travel', [0.05, 0.40, 0.35, 0.30, 0.10, 0.15, 0.20, 0.25], 1, {'vacation', 'tourism', 'adventure'}, 'Travel');

-----------------------------------------------------------------------------
-- Sample Rating Data
-----------------------------------------------------------------------------

-- Video ratings
UPDATE killrvideo.video_ratings 
SET rating_counter = rating_counter + 1, rating_total = rating_total + 5
WHERE videoid = killrvideo.sample_uuid('video1');

UPDATE killrvideo.video_ratings 
SET rating_counter = rating_counter + 1, rating_total = rating_total + 4
WHERE videoid = killrvideo.sample_uuid('video1');

UPDATE killrvideo.video_ratings 
SET rating_counter = rating_counter + 1, rating_total = rating_total + 5
WHERE videoid = killrvideo.sample_uuid('video2');

UPDATE killrvideo.video_ratings 
SET rating_counter = rating_counter + 1, rating_total = rating_total + 3
WHERE videoid = killrvideo.sample_uuid('video3');

UPDATE killrvideo.video_ratings 
SET rating_counter = rating_counter + 1, rating_total = rating_total + 5
WHERE videoid = killrvideo.sample_uuid('video4');

-- User ratings
INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (killrvideo.sample_uuid('video1'), killrvideo.sample_uuid('user2'), 5, '2023-05-21 09:15:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (killrvideo.sample_uuid('video1'), killrvideo.sample_uuid('user3'), 4, '2023-05-22 14:20:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (killrvideo.sample_uuid('video2'), killrvideo.sample_uuid('user4'), 5, '2023-05-26 08:45:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (killrvideo.sample_uuid('video3'), killrvideo.sample_uuid('user5'), 3, '2023-06-02 19:30:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (killrvideo.sample_uuid('video4'), killrvideo.sample_uuid('user1'), 5, '2023-06-06 10:15:00');

-----------------------------------------------------------------------------
-- Sample User Activity and Analytics Data
-----------------------------------------------------------------------------

-- User activity
INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (killrvideo.sample_uuid('user1'), toDate('2023-06-10'), 'view', now(), '2023-06-10 09:15:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (killrvideo.sample_uuid('user1'), toDate('2023-06-10'), 'rate', now(), '2023-06-10 09:30:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (killrvideo.sample_uuid('user2'), toDate('2023-06-11'), 'view', now(), '2023-06-11 14:20:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (killrvideo.sample_uuid('user2'), toDate('2023-06-11'), 'comment', now(), '2023-06-11 14:25:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (killrvideo.sample_uuid('user3'), toDate('2023-06-12'), 'view', now(), '2023-06-12 11:10:00');

-- Video playback statistics
UPDATE killrvideo.video_playback_stats
SET views = views + 10, total_play_time = total_play_time + 1200, complete_views = complete_views + 3, unique_viewers = unique_viewers + 8
WHERE videoid = killrvideo.sample_uuid('video1');

UPDATE killrvideo.video_playback_stats
SET views = views + 8, total_play_time = total_play_time + 960, complete_views = complete_views + 2, unique_viewers = unique_viewers + 6
WHERE videoid = killrvideo.sample_uuid('video2');

UPDATE killrvideo.video_playback_stats
SET views = views + 15, total_play_time = total_play_time + 450, complete_views = complete_views + 10, unique_viewers = unique_viewers + 12
WHERE videoid = killrvideo.sample_uuid('video3');

UPDATE killrvideo.video_playback_stats
SET views = views + 25, total_play_time = total_play_time + 1500, complete_views = complete_views + 15, unique_viewers = unique_viewers + 20
WHERE videoid = killrvideo.sample_uuid('video4');

UPDATE killrvideo.video_playback_stats
SET views = views + 12, total_play_time = total_play_time + 1800, complete_views = complete_views + 7, unique_viewers = unique_viewers + 10
WHERE videoid = killrvideo.sample_uuid('video5');

-- Video engagement metrics using vector type
INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (killrvideo.sample_uuid('video1'), toDate('2023-06-10'), 9, [10.0, 120.0, 3.0, 0.95, 0.8, 2.0, 8.0, 3.0]);

INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (killrvideo.sample_uuid('video1'), toDate('2023-06-10'), 10, [12.0, 140.0, 4.0, 0.92, 0.75, 3.0, 9.0, 4.0]);

INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (killrvideo.sample_uuid('video2'), toDate('2023-06-10'), 9, [8.0, 110.0, 2.0, 0.88, 0.7, 1.0, 7.0, 2.0]);

INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (killrvideo.sample_uuid('video3'), toDate('2023-06-10'), 11, [15.0, 90.0, 5.0, 0.97, 0.85, 4.0, 11.0, 5.0]);

INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (killrvideo.sample_uuid('video4'), toDate('2023-06-10'), 12, [20.0, 150.0, 8.0, 0.99, 0.9, 5.0, 15.0, 8.0]);

-----------------------------------------------------------------------------
-- Sample User Preferences
-----------------------------------------------------------------------------

-- User preferences with vector representation for recommendations
INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (killrvideo.sample_uuid('user1'), 
        [0.30, 0.10, 0.05, 0.35, 0.15, 0.00, 0.05, 0.10, 0.15, 0.20, 0.35, 0.40, 0.30, 0.10],
        {'programming': 0.8, 'technology': 0.7, 'education': 0.6, 'tutorial': 0.5},
        {'Education': 0.8, 'Technology': 0.7, 'Science': 0.4, 'Food': 0.3},
        '2023-06-10 08:00:00');

INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (killrvideo.sample_uuid('user2'), 
        [0.10, 0.30, 0.40, 0.20, 0.00, 0.00, 0.05, 0.05, 0.35, 0.40, 0.30, 0.05, 0.10, 0.50],
        {'travel': 0.9, 'japan': 0.8, 'adventure': 0.7, 'culture': 0.6},
        {'Travel': 0.9, 'Culture': 0.7, 'Food': 0.5, 'Entertainment': 0.4},
        '2023-06-11 09:15:00');

INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (killrvideo.sample_uuid('user3'), 
        [0.05, 0.10, 0.15, 0.05, 0.40, 0.45, 0.05, 0.00, 0.15, 0.20, 0.35, 0.30, 0.45, 0.15],
        {'cooking': 0.9, 'food': 0.8, 'recipe': 0.7, 'italian': 0.6},
        {'Food': 0.9, 'Lifestyle': 0.6, 'Health': 0.5, 'Culture': 0.4},
        '2023-06-09 12:30:00');

INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (killrvideo.sample_uuid('user4'), 
        [0.45, 0.35, 0.10, 0.05, 0.00, 0.05, 0.25, 0.35, 0.40, 0.05, 0.10, 0.15, 0.20, 0.05],
        {'pets': 0.9, 'cats': 0.8, 'animals': 0.7, 'funny': 0.6},
        {'Pets': 0.9, 'Entertainment': 0.7, 'Lifestyle': 0.5, 'Comedy': 0.4},
        '2023-06-11 14:45:00');

INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (killrvideo.sample_uuid('user5'), 
        [0.30, 0.15, 0.10, 0.35, 0.05, 0.05, 0.00, 0.00, 0.20, 0.25, 0.40, 0.30, 0.10, 0.15],
        {'programming': 0.8, 'javascript': 0.7, 'webdev': 0.6, 'tutorial': 0.5},
        {'Technology': 0.8, 'Education': 0.7, 'Science': 0.5, 'Business': 0.4},
        '2023-06-12 11:00:00');

-----------------------------------------------------------------------------
-- Sample Content Moderation Data
-----------------------------------------------------------------------------

-- Content moderation with masked reasons
INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (killrvideo.sample_uuid('flagged1'), 'comment', 'rejected', 'Contains inappropriate language and personal attacks', killrvideo.sample_uuid('moderator1'), '2023-06-10 15:30:00');

INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (killrvideo.sample_uuid('flagged2'), 'video', 'pending', 'Possible copyright violation - check source material', killrvideo.sample_uuid('moderator2'), '2023-06-11 10:45:00');

INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (killrvideo.sample_uuid('flagged3'), 'comment', 'approved', 'False positive - comment is acceptable', killrvideo.sample_uuid('moderator1'), '2023-06-12 14:15:00');

INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (killrvideo.sample_uuid('flagged4'), 'video', 'rejected', 'Contains prohibited content violating community guidelines', killrvideo.sample_uuid('moderator3'), '2023-06-13 09:20:00');

INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (killrvideo.sample_uuid('flagged5'), 'comment', 'pending', 'Potentially offensive content - requires human review', killrvideo.sample_uuid('moderator2'), '2023-06-13 16:45:00');

-----------------------------------------------------------------------------
-- Drop the temporary UUID function when done
-----------------------------------------------------------------------------

DROP FUNCTION IF EXISTS killrvideo.sample_uuid;