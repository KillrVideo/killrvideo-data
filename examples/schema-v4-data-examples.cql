-- KillrVideo Sample Data for Cassandra 4.0
-- Sample inserts that demonstrate the model and provide data for query examples

-- Create a UUID generator function (for examples only - in production use UUID() function in application code)
CREATE OR REPLACE FUNCTION killrvideo.sample_uuid(seed text)
RETURNS uuid
LANGUAGE java AS '
    return java.util.UUID.nameUUIDFromBytes(seed.getBytes());
';

-----------------------------------------------------------------------------
-- Sample User Data
-----------------------------------------------------------------------------

-- User Credentials
INSERT INTO killrvideo.user_credentials (email, password, userid)
VALUES ('john.doe@example.com', 'password123', killrvideo.sample_uuid('user1'));

INSERT INTO killrvideo.user_credentials (email, password, userid)
VALUES ('jane.smith@example.com', 'securepass456', killrvideo.sample_uuid('user2'));

INSERT INTO killrvideo.user_credentials (email, password, userid)
VALUES ('bob.johnson@example.com', 'bobspassword', killrvideo.sample_uuid('user3'));

INSERT INTO killrvideo.user_credentials (email, password, userid)
VALUES ('alice.wang@example.com', 'alicesecure', killrvideo.sample_uuid('user4'));

INSERT INTO killrvideo.user_credentials (email, password, userid)
VALUES ('carlos.rodriguez@example.com', 'carlospass', killrvideo.sample_uuid('user5'));

-- User Information
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

-- Main video table
INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video1'), '2023-05-20 14:30:00', 'Learn the basics of Cassandra data modeling', '/videos/cassandra-basics.mp4', 1, 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', {'cassandra', 'database', 'tutorial'}, killrvideo.sample_uuid('user1'), 'G', 'Education', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video2'), '2023-05-25 10:15:00', 'My trip to Japan - Tokyo highlights and travel tips', '/videos/japan-trip.mp4', 1, 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', {'travel', 'japan', 'vlog'}, killrvideo.sample_uuid('user2'), 'G', 'Travel', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video3'), '2023-06-01 09:45:00', 'Quick and easy pasta carbonara recipe', '/videos/carbonara-recipe.mp4', 1, 'The Perfect Carbonara', '/thumbnails/carbonara.jpg', {'cooking', 'recipe', 'italian'}, killrvideo.sample_uuid('user3'), 'G', 'Food', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video4'), '2023-06-05 16:20:00', 'Funny cat compilation - cutest moments', '/videos/cat-compilation.mp4', 1, 'Cute Cat Compilation', '/thumbnails/cats.jpg', {'cats', 'funny', 'pets'}, killrvideo.sample_uuid('user4'), 'G', 'Pets', 'English');

INSERT INTO killrvideo.videos (videoid, added_date, description, location, location_type, name, preview_image_location, tags, userid, content_rating, category, language)
VALUES (killrvideo.sample_uuid('video5'), '2023-06-10 11:10:00', 'Advanced JavaScript concepts explained', '/videos/js-advanced.mp4', 1, 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', {'programming', 'javascript', 'tutorial'}, killrvideo.sample_uuid('user5'), 'G', 'Education', 'English');

-- Latest videos (denormalized)
INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid)
VALUES (toDate('2023-05-20'), '2023-05-20 14:30:00', killrvideo.sample_uuid('video1'), 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', killrvideo.sample_uuid('user1'));

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid)
VALUES (toDate('2023-05-25'), '2023-05-25 10:15:00', killrvideo.sample_uuid('video2'), 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', killrvideo.sample_uuid('user2'));

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid)
VALUES (toDate('2023-06-01'), '2023-06-01 09:45:00', killrvideo.sample_uuid('video3'), 'The Perfect Carbonara', '/thumbnails/carbonara.jpg', killrvideo.sample_uuid('user3'));

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid)
VALUES (toDate('2023-06-05'), '2023-06-05 16:20:00', killrvideo.sample_uuid('video4'), 'Cute Cat Compilation', '/thumbnails/cats.jpg', killrvideo.sample_uuid('user4'));

INSERT INTO killrvideo.latest_videos (day, added_date, videoid, name, preview_image_location, userid)
VALUES (toDate('2023-06-10'), '2023-06-10 11:10:00', killrvideo.sample_uuid('video5'), 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', killrvideo.sample_uuid('user5'));

-- User videos (denormalized)
INSERT INTO killrvideo.user_videos (userid, added_date, videoid, name, preview_image_location)
VALUES (killrvideo.sample_uuid('user1'), '2023-05-20 14:30:00', killrvideo.sample_uuid('video1'), 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg');

INSERT INTO killrvideo.user_videos (userid, added_date, videoid, name, preview_image_location)
VALUES (killrvideo.sample_uuid('user2'), '2023-05-25 10:15:00', killrvideo.sample_uuid('video2'), 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg');

INSERT INTO killrvideo.user_videos (userid, added_date, videoid, name, preview_image_location)
VALUES (killrvideo.sample_uuid('user3'), '2023-06-01 09:45:00', killrvideo.sample_uuid('video3'), 'The Perfect Carbonara', '/thumbnails/carbonara.jpg');

INSERT INTO killrvideo.user_videos (userid, added_date, videoid, name, preview_image_location)
VALUES (killrvideo.sample_uuid('user4'), '2023-06-05 16:20:00', killrvideo.sample_uuid('video4'), 'Cute Cat Compilation', '/thumbnails/cats.jpg');

INSERT INTO killrvideo.user_videos (userid, added_date, videoid, name, preview_image_location)
VALUES (killrvideo.sample_uuid('user5'), '2023-06-10 11:10:00', killrvideo.sample_uuid('video5'), 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg');

-----------------------------------------------------------------------------
-- Sample Comments Data
-----------------------------------------------------------------------------

-- Comments on videos
INSERT INTO killrvideo.comments (videoid, commentid, comment, userid)
VALUES (killrvideo.sample_uuid('video1'), now(), 'Great tutorial! Really helped me understand Cassandra better.', killrvideo.sample_uuid('user2'));

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid)
VALUES (killrvideo.sample_uuid('video1'), now(), 'Could you make a follow-up on advanced modeling patterns?', killrvideo.sample_uuid('user3'));

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid)
VALUES (killrvideo.sample_uuid('video2'), now(), 'Loved your Japan vlog! What camera did you use?', killrvideo.sample_uuid('user4'));

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid)
VALUES (killrvideo.sample_uuid('video3'), now(), 'I tried this recipe and it turned out amazing!', killrvideo.sample_uuid('user5'));

INSERT INTO killrvideo.comments (videoid, commentid, comment, userid)
VALUES (killrvideo.sample_uuid('video4'), now(), 'The gray cat at 2:30 is so cute!', killrvideo.sample_uuid('user1'));

-- Comments by user (denormalized)
INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid)
VALUES (killrvideo.sample_uuid('user2'), now(), 'Great tutorial! Really helped me understand Cassandra better.', killrvideo.sample_uuid('video1'));

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid)
VALUES (killrvideo.sample_uuid('user3'), now(), 'Could you make a follow-up on advanced modeling patterns?', killrvideo.sample_uuid('video1'));

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid)
VALUES (killrvideo.sample_uuid('user4'), now(), 'Loved your Japan vlog! What camera did you use?', killrvideo.sample_uuid('video2'));

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid)
VALUES (killrvideo.sample_uuid('user5'), now(), 'I tried this recipe and it turned out amazing!', killrvideo.sample_uuid('video3'));

INSERT INTO killrvideo.comments_by_user (userid, commentid, comment, videoid)
VALUES (killrvideo.sample_uuid('user1'), now(), 'The gray cat at 2:30 is so cute!', killrvideo.sample_uuid('video4'));

-----------------------------------------------------------------------------
-- Sample Tag Data
-----------------------------------------------------------------------------

-- Tags by first letter
INSERT INTO killrvideo.tags_by_letter (first_letter, tag)
VALUES ('c', 'cassandra');

INSERT INTO killrvideo.tags_by_letter (first_letter, tag)
VALUES ('c', 'cooking');

INSERT INTO killrvideo.tags_by_letter (first_letter, tag)
VALUES ('c', 'cats');

INSERT INTO killrvideo.tags_by_letter (first_letter, tag)
VALUES ('j', 'javascript');

INSERT INTO killrvideo.tags_by_letter (first_letter, tag)
VALUES ('j', 'japan');

INSERT INTO killrvideo.tags_by_letter (first_letter, tag)
VALUES ('t', 'tutorial');

INSERT INTO killrvideo.tags_by_letter (first_letter, tag)
VALUES ('t', 'travel');

-- Videos by tag
INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('cassandra', killrvideo.sample_uuid('video1'), '2023-05-20 14:30:00', 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', '2023-05-20 14:30:00', killrvideo.sample_uuid('user1'));

INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('database', killrvideo.sample_uuid('video1'), '2023-05-20 14:30:00', 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', '2023-05-20 14:30:00', killrvideo.sample_uuid('user1'));

INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('tutorial', killrvideo.sample_uuid('video1'), '2023-05-20 14:30:00', 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', '2023-05-20 14:30:00', killrvideo.sample_uuid('user1'));

INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('travel', killrvideo.sample_uuid('video2'), '2023-05-25 10:15:00', 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', '2023-05-25 10:15:00', killrvideo.sample_uuid('user2'));

INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('japan', killrvideo.sample_uuid('video2'), '2023-05-25 10:15:00', 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', '2023-05-25 10:15:00', killrvideo.sample_uuid('user2'));

INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('cooking', killrvideo.sample_uuid('video3'), '2023-06-01 09:45:00', 'The Perfect Carbonara', '/thumbnails/carbonara.jpg', '2023-06-01 09:45:00', killrvideo.sample_uuid('user3'));

INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('cats', killrvideo.sample_uuid('video4'), '2023-06-05 16:20:00', 'Cute Cat Compilation', '/thumbnails/cats.jpg', '2023-06-05 16:20:00', killrvideo.sample_uuid('user4'));

INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('javascript', killrvideo.sample_uuid('video5'), '2023-06-10 11:10:00', 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', '2023-06-10 11:10:00', killrvideo.sample_uuid('user5'));

INSERT INTO killrvideo.videos_by_tag (tag, videoid, added_date, name, preview_image_location, tagged_date, userid)
VALUES ('tutorial', killrvideo.sample_uuid('video5'), '2023-06-10 11:10:00', 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', '2023-06-10 11:10:00', killrvideo.sample_uuid('user5'));

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
-- Sample User Activity Data
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
SET views = views + 10, total_play_time = total_play_time + 1200, complete_views = complete_views + 3
WHERE videoid = killrvideo.sample_uuid('video1');

UPDATE killrvideo.video_playback_stats
SET views = views + 8, total_play_time = total_play_time + 960, complete_views = complete_views + 2
WHERE videoid = killrvideo.sample_uuid('video2');

UPDATE killrvideo.video_playback_stats
SET views = views + 15, total_play_time = total_play_time + 450, complete_views = complete_views + 10
WHERE videoid = killrvideo.sample_uuid('video3');

UPDATE killrvideo.video_playback_stats
SET views = views + 25, total_play_time = total_play_time + 1500, complete_views = complete_views + 15
WHERE videoid = killrvideo.sample_uuid('video4');

UPDATE killrvideo.video_playback_stats
SET views = views + 12, total_play_time = total_play_time + 1800, complete_views = complete_views + 7
WHERE videoid = killrvideo.sample_uuid('video5');

-----------------------------------------------------------------------------
-- Sample Recommendations Data
-----------------------------------------------------------------------------

-- Video recommendations 
INSERT INTO killrvideo.video_recommendations (userid, added_date, videoid, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('user1'), '2023-06-10 08:00:00', killrvideo.sample_uuid('video5'), killrvideo.sample_uuid('user5'), 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', 4.8);

INSERT INTO killrvideo.video_recommendations (userid, added_date, videoid, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('user2'), '2023-06-10 08:00:00', killrvideo.sample_uuid('video1'), killrvideo.sample_uuid('user1'), 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', 4.5);

INSERT INTO killrvideo.video_recommendations (userid, added_date, videoid, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('user3'), '2023-06-10 08:00:00', killrvideo.sample_uuid('video4'), killrvideo.sample_uuid('user4'), 'Cute Cat Compilation', '/thumbnails/cats.jpg', 4.9);

INSERT INTO killrvideo.video_recommendations (userid, added_date, videoid, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('user4'), '2023-06-10 08:00:00', killrvideo.sample_uuid('video3'), killrvideo.sample_uuid('user3'), 'The Perfect Carbonara', '/thumbnails/carbonara.jpg', 4.2);

INSERT INTO killrvideo.video_recommendations (userid, added_date, videoid, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('user5'), '2023-06-10 08:00:00', killrvideo.sample_uuid('video2'), killrvideo.sample_uuid('user2'), 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', 4.7);

-- Video recommendations by video
INSERT INTO killrvideo.video_recommendations_by_video (videoid, userid, added_date, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('video1'), killrvideo.sample_uuid('user2'), '2023-06-10 08:00:00', killrvideo.sample_uuid('user1'), 'Cassandra Data Modeling Basics', '/thumbnails/cassandra-basics.jpg', 4.5);

INSERT INTO killrvideo.video_recommendations_by_video (videoid, userid, added_date, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('video2'), killrvideo.sample_uuid('user5'), '2023-06-10 08:00:00', killrvideo.sample_uuid('user2'), 'Japan Travel Vlog', '/thumbnails/japan-trip.jpg', 4.7);

INSERT INTO killrvideo.video_recommendations_by_video (videoid, userid, added_date, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('video3'), killrvideo.sample_uuid('user4'), '2023-06-10 08:00:00', killrvideo.sample_uuid('user3'), 'The Perfect Carbonara', '/thumbnails/carbonara.jpg', 4.2);

INSERT INTO killrvideo.video_recommendations_by_video (videoid, userid, added_date, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('video4'), killrvideo.sample_uuid('user3'), '2023-06-10 08:00:00', killrvideo.sample_uuid('user4'), 'Cute Cat Compilation', '/thumbnails/cats.jpg', 4.9);

INSERT INTO killrvideo.video_recommendations_by_video (videoid, userid, added_date, authorid, name, preview_image_location, rating)
VALUES (killrvideo.sample_uuid('video5'), killrvideo.sample_uuid('user1'), '2023-06-10 08:00:00', killrvideo.sample_uuid('user5'), 'JavaScript Advanced Topics', '/thumbnails/javascript.jpg', 4.8);

-----------------------------------------------------------------------------
-- Drop the temporary UUID function when done
-----------------------------------------------------------------------------

DROP FUNCTION IF EXISTS killrvideo.sample_uuid;