-- KillrVideo Sample Data for Cassandra 5.0
-- This script provides sample data inserts to demonstrate the advanced
-- data modeling features in Cassandra 5.0.

-- Use the KillrVideo keyspace
USE killrvideo;

------------------------------------------------------------------
-- User Management Data
------------------------------------------------------------------

-- Using predefined UUIDs for consistent references across queries
-- In a real application, you would generate these with UUID() or from your application logic

-- Define user UUIDs
-- These UUIDs are used consistently throughout the sample data
-- user1 = John Doe
-- user2 = Jane Smith
-- user3 = Bob Johnson
-- user4 = Alice Wang
-- user5 = Carlos Rodriguez

-- Insert user data
-- Note: The email will be automatically masked in queries based on the schema definition
INSERT INTO killrvideo.users (userid, created_date, email, firstname, lastname, account_status, last_login_date)
VALUES (11111111-1111-1111-1111-111111111111, '2023-01-15 10:30:00', 'john.doe@example.com', 'John', 'Doe', 'active', '2023-06-10 08:45:00');

INSERT INTO killrvideo.users (userid, created_date, email, firstname, lastname, account_status, last_login_date)
VALUES (22222222-2222-2222-2222-222222222222, '2023-02-20 14:15:00', 'jane.smith@example.com', 'Jane', 'Smith', 'active', '2023-06-12 16:20:00');

INSERT INTO killrvideo.users (userid, created_date, email, firstname, lastname, account_status, last_login_date)
VALUES (33333333-3333-3333-3333-333333333333, '2023-03-10 09:00:00', 'bob.johnson@example.com', 'Bob', 'Johnson', 'inactive', '2023-05-05 11:30:00');

INSERT INTO killrvideo.users (userid, created_date, email, firstname, lastname, account_status, last_login_date)
VALUES (44444444-4444-4444-4444-444444444444, '2023-04-05 11:45:00', 'alice.wang@example.com', 'Alice', 'Wang', 'active', '2023-06-11 19:10:00');

INSERT INTO killrvideo.users (userid, created_date, email, firstname, lastname, account_status, last_login_date)
VALUES (55555555-5555-5555-5555-555555555555, '2023-05-12 16:30:00', 'carlos.rodriguez@example.com', 'Carlos', 'Rodriguez', 'active', '2023-06-13 09:25:00');

-- Insert user credentials
-- The password will be masked with SHA-256 hash when queried
INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked)
VALUES ('john.doe@example.com', 'password123', 11111111-1111-1111-1111-111111111111, false);

INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked)
VALUES ('jane.smith@example.com', 'securepass456', 22222222-2222-2222-2222-222222222222, false);

INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked)
VALUES ('bob.johnson@example.com', 'bobspassword', 33333333-3333-3333-3333-333333333333, true);

INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked)
VALUES ('alice.wang@example.com', 'alicesecure', 44444444-4444-4444-4444-444444444444, false);

INSERT INTO killrvideo.user_credentials (email, password, userid, account_locked)
VALUES ('carlos.rodriguez@example.com', 'carlospass', 55555555-5555-5555-5555-555555555555, false);

-- Insert login attempt counters
UPDATE killrvideo.login_attempts SET failed_attempts = failed_attempts + 0 WHERE email = 'john.doe@example.com';
UPDATE killrvideo.login_attempts SET failed_attempts = failed_attempts + 1 WHERE email = 'jane.smith@example.com';
UPDATE killrvideo.login_attempts SET failed_attempts = failed_attempts + 5 WHERE email = 'bob.johnson@example.com';
UPDATE killrvideo.login_attempts SET failed_attempts = failed_attempts + 0 WHERE email = 'alice.wang@example.com';
UPDATE killrvideo.login_attempts SET failed_attempts = failed_attempts + 2 WHERE email = 'carlos.rodriguez@example.com';

-- Insert payment information with masked data
INSERT INTO killrvideo.payment_info (userid, payment_id, card_number, card_expiry, billing_address)
VALUES (11111111-1111-1111-1111-111111111111, 12345678-1234-1234-1234-123456789012, '4111111111111111', '2025-06-01', '123 Main St, Anytown, CA 12345');

INSERT INTO killrvideo.payment_info (userid, payment_id, card_number, card_expiry, billing_address)
VALUES (22222222-2222-2222-2222-222222222222, 23456789-2345-2345-2345-234567890123, '5555555555554444', '2026-03-01', '456 Oak Ave, Springfield, IL 67890');

INSERT INTO killrvideo.payment_info (userid, payment_id, card_number, card_expiry, billing_address)
VALUES (44444444-4444-4444-4444-444444444444, 45678901-4567-4567-4567-456789012345, '3782822463100005', '2024-12-01', '789 Pine St, Portland, OR 97123');

------------------------------------------------------------------
-- Video Management Data
------------------------------------------------------------------

-- Define video UUIDs
-- video1 = Cassandra Data Modeling Basics
-- video2 = Japan Travel Vlog
-- video3 = The Perfect Carbonara
-- video4 = Cute Cat Compilation
-- video5 = JavaScript Advanced Topics

-- Insert video data with content feature vectors
-- In a real application, these vectors would be generated by ML models
INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, 
                              preview_image_location, tags, content_features, userid, 
                              content_rating, category, language)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, '2023-05-20 14:30:00', 
        'Learn the basics of Cassandra data modeling', '/videos/cassandra-basics.mp4', 1, 
        'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', 
        {'cassandra', 'database', 'tutorial'}, 
        [0.2, 0.3, 0.1, 0.4, 0.6, 0.2, 0.1, 0.5, 0.3, 0.7, 0.8, 0.1, 0.2, 0.3, 0.4, 0.5], 
        11111111-1111-1111-1111-111111111111, 'G', 'Education', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, 
                              preview_image_location, tags, content_features, userid, 
                              content_rating, category, language)
VALUES (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, '2023-05-25 10:15:00', 
        'My trip to Japan - Tokyo highlights and travel tips', '/videos/japan-trip.mp4', 1, 
        'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', 
        {'travel', 'japan', 'vlog'}, 
        [0.1, 0.8, 0.2, 0.3, 0.1, 0.9, 0.7, 0.2, 0.3, 0.1, 0.2, 0.8, 0.7, 0.1, 0.3, 0.2], 
        22222222-2222-2222-2222-222222222222, 'G', 'Travel', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, 
                              preview_image_location, tags, content_features, userid, 
                              content_rating, category, language)
VALUES (cccccccc-cccc-cccc-cccc-cccccccccccc, '2023-06-01 09:45:00', 
        'Quick and easy pasta carbonara recipe', '/videos/carbonara-recipe.mp4', 1, 
        'The Perfect Carbonara', '/thumbnails/carbonara.jpg', 
        {'cooking', 'recipe', 'italian'}, 
        [0.9, 0.1, 0.3, 0.2, 0.7, 0.1, 0.1, 0.5, 0.8, 0.2, 0.1, 0.3, 0.4, 0.7, 0.1, 0.2], 
        33333333-3333-3333-3333-333333333333, 'G', 'Food', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, 
                              preview_image_location, tags, content_features, userid, 
                              content_rating, category, language)
VALUES (dddddddd-dddd-dddd-dddd-dddddddddddd, '2023-06-05 16:20:00', 
        'Funny cat compilation - cutest moments', '/videos/cat-compilation.mp4', 1, 
        'Cute Cat Compilation', '/thumbnails/cats.jpg', 
        {'cats', 'funny', 'pets'}, 
        [0.2, 0.1, 0.7, 0.9, 0.3, 0.1, 0.8, 0.2, 0.4, 0.1, 0.2, 0.6, 0.7, 0.1, 0.2, 0.3], 
        44444444-4444-4444-4444-444444444444, 'G', 'Pets', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, 
                              preview_image_location, tags, content_features, userid, 
                              content_rating, category, language)
VALUES (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, '2023-06-10 11:10:00', 
        'Advanced JavaScript concepts explained', '/videos/js-advanced.mp4', 1, 
        'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', 
        {'programming', 'javascript', 'tutorial'}, 
        [0.3, 0.6, 0.1, 0.2, 0.7, 0.4, 0.1, 0.3, 0.2, 0.5, 0.7, 0.2, 0.1, 0.6, 0.3, 0.2], 
        55555555-5555-5555-5555-555555555555, 'G', 'Education', 'English');

-- Insert data for latest videos denormalized table
INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES ('2023-05-20', '2023-05-20 14:30:00', aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', 11111111-1111-1111-1111-111111111111, 'G', 'Education');

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES ('2023-05-25', '2023-05-25 10:15:00', bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', 22222222-2222-2222-2222-222222222222, 'G', 'Travel');

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES ('2023-06-01', '2023-06-01 09:45:00', cccccccc-cccc-cccc-cccc-cccccccccccc, 'The Perfect Carbonara', '/thumbnails/carbonara.jpg', 33333333-3333-3333-3333-333333333333, 'G', 'Food');

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES ('2023-06-05', '2023-06-05 16:20:00', dddddddd-dddd-dddd-dddd-dddddddddddd, 'Cute Cat Compilation', '/thumbnails/cats.jpg', 44444444-4444-4444-4444-444444444444, 'G', 'Pets');

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid, content_rating, category)
VALUES ('2023-06-10', '2023-06-10 11:10:00', eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', 55555555-5555-5555-5555-555555555555, 'G', 'Education');

-- Update video playback statistics
UPDATE killrvideo.video_playback_stats 
SET views = views + 120, total_play_time = total_play_time + 3600, complete_views = complete_views + 80, unique_viewers = unique_viewers + 100
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;

UPDATE killrvideo.video_playback_stats 
SET views = views + 85, total_play_time = total_play_time + 2550, complete_views = complete_views + 65, unique_viewers = unique_viewers + 75
WHERE videoid = bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb;

UPDATE killrvideo.video_playback_stats 
SET views = views + 210, total_play_time = total_play_time + 3150, complete_views = complete_views + 180, unique_viewers = unique_viewers + 195
WHERE videoid = cccccccc-cccc-cccc-cccc-cccccccccccc;

UPDATE killrvideo.video_playback_stats 
SET views = views + 320, total_play_time = total_play_time + 4800, complete_views = complete_views + 280, unique_viewers = unique_viewers + 310
WHERE videoid = dddddddd-dddd-dddd-dddd-dddddddddddd;

UPDATE killrvideo.video_playback_stats 
SET views = views + 150, total_play_time = total_play_time + 9000, complete_views = complete_views + 90, unique_viewers = unique_viewers + 130
WHERE videoid = eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee;

------------------------------------------------------------------
-- Tags and Discovery Data
------------------------------------------------------------------

-- Insert tag data with vectors for similarity search
INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('cassandra', [0.8, 0.1, 0.3, 0.2, 0.7, 0.1, 0.5, 0.3], {'database', 'nosql', 'distributed-systems'}, 'Technology');

INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('database', [0.7, 0.2, 0.3, 0.1, 0.6, 0.2, 0.5, 0.4], {'cassandra', 'sql', 'storage'}, 'Technology');

INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('tutorial', [0.5, 0.5, 0.4, 0.3, 0.2, 0.6, 0.3, 0.4], {'education', 'learning', 'how-to'}, 'Education');

INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('travel', [0.1, 0.8, 0.3, 0.7, 0.2, 0.4, 0.1, 0.6], {'vacation', 'tourism', 'adventure'}, 'Lifestyle');

INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('japan', [0.2, 0.7, 0.4, 0.8, 0.1, 0.3, 0.2, 0.5], {'tokyo', 'travel', 'asia'}, 'Location');

INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('cooking', [0.6, 0.3, 0.7, 0.1, 0.8, 0.2, 0.4, 0.1], {'recipe', 'food', 'culinary'}, 'Food');

INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('cats', [0.1, 0.2, 0.8, 0.3, 0.1, 0.7, 0.2, 0.5], {'pets', 'animals', 'kittens'}, 'Animals');

INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('programming', [0.9, 0.2, 0.1, 0.3, 0.7, 0.4, 0.6, 0.2], {'coding', 'development', 'software'}, 'Technology');

INSERT INTO killrvideo.tags (tag, tag_vector, related_tags, category)
VALUES ('javascript', [0.8, 0.3, 0.1, 0.4, 0.6, 0.3, 0.7, 0.1], {'programming', 'web', 'frontend'}, 'Technology');

-- Update tag counts
UPDATE killrvideo.tag_counts SET count = count + 1 WHERE tag = 'cassandra';
UPDATE killrvideo.tag_counts SET count = count + 1 WHERE tag = 'database';
UPDATE killrvideo.tag_counts SET count = count + 1 WHERE tag = 'tutorial';
UPDATE killrvideo.tag_counts SET count = count + 2 WHERE tag = 'travel';
UPDATE killrvideo.tag_counts SET count = count + 1 WHERE tag = 'japan';
UPDATE killrvideo.tag_counts SET count = count + 1 WHERE tag = 'cooking';
UPDATE killrvideo.tag_counts SET count = count + 1 WHERE tag = 'cats';
UPDATE killrvideo.tag_counts SET count = count + 1 WHERE tag = 'programming';
UPDATE killrvideo.tag_counts SET count = count + 1 WHERE tag = 'javascript';

------------------------------------------------------------------
-- Comments System Data
------------------------------------------------------------------

-- Comment TimeUUIDs (predefined for simplicity)
-- In production, you would use currentTimeUUID() function or generate from application code

-- Insert comment data with sentiment scores
INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 30000000-0000-1000-8080-808080808080, 
        'Great tutorial! Really helped me understand Cassandra better.', 
        22222222-2222-2222-2222-222222222222, 0.85);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 30000000-0000-1000-8080-808080808081, 
        'Could you make a follow-up on advanced modeling patterns?', 
        33333333-3333-3333-3333-333333333333, 0.65);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 30000000-0000-1000-8080-808080808082, 
        'Loved your Japan vlog! What camera did you use?', 
        44444444-4444-4444-4444-444444444444, 0.90);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (cccccccc-cccc-cccc-cccc-cccccccccccc, 30000000-0000-1000-8080-808080808083, 
        'I tried this recipe and it turned out amazing!', 
        55555555-5555-5555-5555-555555555555, 0.95);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (dddddddd-dddd-dddd-dddd-dddddddddddd, 30000000-0000-1000-8080-808080808084, 
        'The gray cat at 2:30 is so cute!', 
        11111111-1111-1111-1111-111111111111, 0.80);

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid, sentiment_score)
VALUES (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 30000000-0000-1000-8080-808080808085, 
        'This explanation is confusing and not helpful at all.', 
        22222222-2222-2222-2222-222222222222, 0.15);

-- Insert comments by user (denormalized data)
INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (22222222-2222-2222-2222-222222222222, 30000000-0000-1000-8080-808080808080, 
        'Great tutorial! Really helped me understand Cassandra better.', 
        aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 0.85);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (33333333-3333-3333-3333-333333333333, 30000000-0000-1000-8080-808080808081, 
        'Could you make a follow-up on advanced modeling patterns?', 
        aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 0.65);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (44444444-4444-4444-4444-444444444444, 30000000-0000-1000-8080-808080808082, 
        'Loved your Japan vlog! What camera did you use?', 
        bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 0.90);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (55555555-5555-5555-5555-555555555555, 30000000-0000-1000-8080-808080808083, 
        'I tried this recipe and it turned out amazing!', 
        cccccccc-cccc-cccc-cccc-cccccccccccc, 0.95);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (11111111-1111-1111-1111-111111111111, 30000000-0000-1000-8080-808080808084, 
        'The gray cat at 2:30 is so cute!', 
        dddddddd-dddd-dddd-dddd-dddddddddddd, 0.80);

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid, sentiment_score)
VALUES (22222222-2222-2222-2222-222222222222, 30000000-0000-1000-8080-808080808085, 
        'This explanation is confusing and not helpful at all.', 
        eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 0.15);

------------------------------------------------------------------
-- Ratings and Recommendations Data
------------------------------------------------------------------

-- Update video ratings (counters)
UPDATE killrvideo.video_ratings SET rating_counter = rating_counter + 5, rating_total = rating_total + 23
WHERE videoid = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;

UPDATE killrvideo.video_ratings SET rating_counter = rating_counter + 3, rating_total = rating_total + 14
WHERE videoid = bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb;

UPDATE killrvideo.video_ratings SET rating_counter = rating_counter + 8, rating_total = rating_total + 39
WHERE videoid = cccccccc-cccc-cccc-cccc-cccccccccccc;

UPDATE killrvideo.video_ratings SET rating_counter = rating_counter + 12, rating_total = rating_total + 58
WHERE videoid = dddddddd-dddd-dddd-dddd-dddddddddddd;

UPDATE killrvideo.video_ratings SET rating_counter = rating_counter + 4, rating_total = rating_total + 13
WHERE videoid = eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee;

-- Insert individual user ratings
INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 22222222-2222-2222-2222-222222222222, 5, '2023-05-21 09:15:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 33333333-3333-3333-3333-333333333333, 4, '2023-05-22 14:20:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 44444444-4444-4444-4444-444444444444, 5, '2023-05-26 08:45:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (cccccccc-cccc-cccc-cccc-cccccccccccc, 55555555-5555-5555-5555-555555555555, 5, '2023-06-02 19:30:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (dddddddd-dddd-dddd-dddd-dddddddddddd, 11111111-1111-1111-1111-111111111111, 5, '2023-06-06 10:15:00');

INSERT INTO killrvideo.video_ratings_by_user (videoid, userid, rating, rating_date)
VALUES (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 22222222-2222-2222-2222-222222222222, 2, '2023-06-11 11:20:00');

-- Insert user preferences for recommendations
INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (11111111-1111-1111-1111-111111111111, 
        [0.2, 0.4, 0.3, 0.5, 0.7, 0.2, 0.3, 0.1, 0.6, 0.4, 0.2, 0.8, 0.3, 0.5, 0.1, 0.7],
        {'technology': 0.8, 'education': 0.7, 'cats': 0.6, 'programming': 0.9},
        {'Education': 0.8, 'Technology': 0.9, 'Pets': 0.6},
        '2023-06-10 12:30:00');

INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (22222222-2222-2222-2222-222222222222, 
        [0.7, 0.2, 0.1, 0.3, 0.8, 0.4, 0.5, 0.1, 0.3, 0.6, 0.4, 0.2, 0.5, 0.7, 0.3, 0.2],
        {'travel': 0.9, 'japan': 0.8, 'food': 0.7, 'culture': 0.8},
        {'Travel': 0.9, 'Food': 0.7, 'Lifestyle': 0.8},
        '2023-06-09 09:15:00');

INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (33333333-3333-3333-3333-333333333333, 
        [0.3, 0.8, 0.2, 0.1, 0.4, 0.7, 0.3, 0.5, 0.2, 0.1, 0.6, 0.4, 0.7, 0.2, 0.5, 0.3],
        {'cooking': 0.9, 'recipe': 0.9, 'food': 0.8, 'italian': 0.7},
        {'Food': 0.9, 'Lifestyle': 0.7, 'Education': 0.5},
        '2023-06-08 14:45:00');

INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (44444444-4444-4444-4444-444444444444, 
        [0.1, 0.3, 0.8, 0.6, 0.2, 0.1, 0.7, 0.4, 0.3, 0.2, 0.5, 0.7, 0.1, 0.3, 0.6, 0.2],
        {'cats': 0.9, 'pets': 0.8, 'animals': 0.8, 'funny': 0.7},
        {'Pets': 0.9, 'Animals': 0.8, 'Entertainment': 0.7},
        '2023-06-07 17:30:00');

INSERT INTO killrvideo.user_preferences (userid, preference_vector, tag_preferences, category_preferences, last_updated)
VALUES (55555555-5555-5555-5555-555555555555, 
        [0.6, 0.2, 0.4, 0.7, 0.1, 0.3, 0.8, 0.5, 0.2, 0.4, 0.7, 0.1, 0.5, 0.2, 0.6, 0.3],
        {'programming': 0.9, 'javascript': 0.8, 'development': 0.7, 'tutorial': 0.6},
        {'Technology': 0.9, 'Education': 0.7, 'Science': 0.5},
        '2023-06-06 10:10:00');

------------------------------------------------------------------
-- Content Moderation Data
------------------------------------------------------------------

-- Insert content moderation data
-- Note: the flagged_reason will be masked based on permissions
INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 'video', 'approved', 'No issues found', 
        55555555-5555-5555-5555-555555555555, '2023-05-21 08:45:00');

INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 'video', 'approved', 'Content verified as appropriate', 
        55555555-5555-5555-5555-555555555555, '2023-05-26 10:20:00');

INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (30000000-0000-1000-8080-808080808085, 'comment', 'pending', 'Potentially negative user experience - requires review', 
        null, '2023-06-11 11:25:00');

INSERT INTO killrvideo.content_moderation (contentid, content_type, status, flagged_reason, reviewer, review_date)
VALUES (ffffff00-ffff-ffff-ffff-ffffffffffff, 'video', 'rejected', 'Copyright violation - matches protected content', 
        55555555-5555-5555-5555-555555555555, '2023-06-02 15:10:00');

------------------------------------------------------------------
-- Analytics Data
------------------------------------------------------------------

-- Insert video engagement metrics
-- Using vector type to store multiple metrics in a single column
INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, '2023-06-10', 8, 
        [12.0, 320.0, 5.0, 0.82, 0.7, 3.2, 0.1, 0.9]);  -- [views, watch_time, likes, completion_rate, etc]

INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, '2023-06-10', 9, 
        [18.0, 480.0, 7.0, 0.85, 0.8, 3.5, 0.2, 0.8]);

INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, '2023-06-10', 10, 
        [25.0, 650.0, 10.0, 0.88, 0.75, 4.0, 0.15, 0.9]);

INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, '2023-06-10', 8, 
        [8.0, 240.0, 3.0, 0.75, 0.6, 2.8, 0.08, 0.7]);

INSERT INTO killrvideo.video_engagement (videoid, day, hour, engagement_metrics)
VALUES (cccccccc-cccc-cccc-cccc-cccccccccccc, '2023-06-10', 9, 
        [30.0, 450.0, 15.0, 0.92, 0.85, 4.5, 0.25, 0.95]);

-- Insert user activity data
INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (11111111-1111-1111-1111-111111111111, '2023-06-10', 'view', 50000000-0000-1000-0000-000000000001, 
        '2023-06-10 08:15:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (11111111-1111-1111-1111-111111111111, '2023-06-10', 'rate', 50000000-0000-1000-0000-000000000002, 
        '2023-06-10 08:20:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (11111111-1111-1111-1111-111111111111, '2023-06-10', 'comment', 50000000-0000-1000-0000-000000000003, 
        '2023-06-10 08:25:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (22222222-2222-2222-2222-222222222222, '2023-06-10', 'view', 50000000-0000-1000-0000-000000000004, 
        '2023-06-10 09:10:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (22222222-2222-2222-2222-222222222222, '2023-06-10', 'share', 50000000-0000-1000-0000-000000000005, 
        '2023-06-10 09:15:00');

INSERT INTO killrvideo.user_activity (userid, day, activity_type, activity_id, activity_timestamp)
VALUES (33333333-3333-3333-3333-333333333333, '2023-06-10', 'view', 50000000-0000-1000-0000-000000000006, 
        '2023-06-10 10:05:00');

------------------------------------------------------------------
-- External Integration Data
------------------------------------------------------------------

-- Insert YouTube video import data with feature vectors
INSERT INTO killrvideo.youtube_videos (sourceid, published_at, youtube_video_id, description, name, used, content_features)
VALUES ('youtube-trending', '2023-06-01 00:00:00', 'abcd1234', 
        'Learn advanced Cassandra techniques', 'Cassandra Advanced Techniques', false,
        [0.3, 0.5, 0.1, 0.4, 0.6, 0.2, 0.8, 0.3, 0.1, 0.7, 0.2, 0.5, 0.3, 0.6, 0.1, 0.4]);

INSERT INTO killrvideo.youtube_videos (sourceid, published_at, youtube_video_id, description, name, used, content_features)
VALUES ('youtube-trending', '2023-06-02 00:00:00', 'efgh5678', 
        'Tokyo street food tour - best spots revealed', 'Ultimate Tokyo Food Tour', false,
        [0.2, 0.7, 0.3, 0.5, 0.1, 0.8, 0.4, 0.2, 0.6, 0.3, 0.1, 0.7, 0.4, 0.2, 0.5, 0.3]);

INSERT INTO killrvideo.youtube_videos (sourceid, published_at, youtube_video_id, description, name, used, content_features)
VALUES ('youtube-cooking', '2023-06-03 00:00:00', 'ijkl9012', 
        'Italian pasta secrets revealed by professional chef', 'Professional Pasta Secrets', false,
        [0.8, 0.1, 0.3, 0.5, 0.7, 0.2, 0.3, 0.6, 0.4, 0.1, 0.5, 0.3, 0.7, 0.2, 0.4, 0.1]);

INSERT INTO killrvideo.youtube_videos (sourceid, published_at, youtube_video_id, description, name, used, content_features)
VALUES ('youtube-pets', '2023-06-04 00:00:00', 'mnop3456', 
        'Why cats do those weird things - explained by experts', 'Cat Behavior Explained', false,
        [0.1, 0.2, 0.7, 0.3, 0.1, 0.6, 0.4, 0.2, 0.5, 0.3, 0.7, 0.1, 0.4, 0.3, 0.6, 0.2]);

INSERT INTO killrvideo.youtube_videos (sourceid, published_at, youtube_video_id, description, name, used, content_features)
VALUES ('youtube-tech', '2023-06-05 00:00:00', 'qrst7890', 
        'Modern JavaScript features every developer should know', 'Essential JavaScript Features', false,
        [0.7, 0.2, 0.1, 0.4, 0.6, 0.3, 0.5, 0.2, 0.4, 0.7, 0.3, 0.1, 0.6, 0.2, 0.5, 0.3]);