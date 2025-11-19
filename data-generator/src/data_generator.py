"""Synthetic data generator using Faker library"""

import random
import uuid
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple
from collections import defaultdict, Counter
from faker import Faker
from dateutil.parser import parse as parse_date
from tqdm import tqdm

from .relationships import RelationshipTracker
from .embeddings import EmbeddingGenerator


class DataGenerator:
    """Generates realistic synthetic data for KillrVideo database"""

    def __init__(self, config: Dict[str, Any], tracker: RelationshipTracker):
        """
        Initialize data generator.

        Args:
            config: Configuration dictionary
            tracker: Relationship tracker for FK management
        """
        self.config = config
        self.tracker = tracker
        self.fake = Faker()

        # Initialize embedding generator
        self.embedder = EmbeddingGenerator()

        # Set random seed for reproducibility
        seed = config.get('dataset', {}).get('random_seed')
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)

    def generate_users(self, num_users: int) -> Tuple[List[Dict], List[Dict]]:
        """
        Generate users and their credentials.

        Args:
            num_users: Number of users to generate

        Returns:
            Tuple of (users list, credentials list)
        """
        users = []
        credentials = []

        print(f"\n👥 Generating {num_users} users...")

        for _ in tqdm(range(num_users)):
            user_id = str(uuid.uuid4())
            email = self.fake.unique.email()

            # Generate user
            user = {
                'userid': user_id,
                'email': email,
                'firstname': self.fake.first_name(),
                'lastname': self.fake.last_name(),
                'created_date': self.fake.date_time_between(start_date='-2y', end_date='now'),
                'account_status': random.choices(
                    ['active', 'inactive', 'suspended'],
                    weights=[90, 8, 2]
                )[0],
                'last_login_date': self.fake.date_time_between(start_date='-30d', end_date='now')
            }
            users.append(user)
            self.tracker.add_user(user)

            # Generate credentials
            credential = {
                'email': email,
                'password': self.fake.sha256(),  # Fake hash
                'userid': user_id,
                'account_locked': random.random() > 0.95  # 5% locked
            }
            credentials.append(credential)

        return users, credentials

    def process_youtube_videos(self, youtube_videos: List[Dict], users: List[Dict]) -> List[Dict]:
        """
        Convert YouTube video metadata to KillrVideo schema.

        Args:
            youtube_videos: List of YouTube video metadata
            users: List of users to assign as video uploaders

        Returns:
            List of video dictionaries
        """
        videos = []

        print(f"\n🎬 Processing {len(youtube_videos)} videos...")

        # Weight users - some are more active uploaders
        active_users = random.sample(users, max(1, len(users) // 3))  # Top 33% are active
        user_weights = [3 if u in active_users else 1 for u in users]

        for yt_video in tqdm(youtube_videos):
            video_id = str(uuid.uuid4())

            # Parse published date
            try:
                added_date = parse_date(yt_video['published_at'])
            except:
                added_date = datetime.now() - timedelta(days=random.randint(1, 730))

            # Categorize video
            title_lower = yt_video['title'].lower()
            category = self._categorize_video(title_lower)

            video = {
                'videoid': video_id,
                'userid': random.choices(users, weights=user_weights)[0]['userid'],
                'added_date': added_date,
                'name': yt_video['title'][:255],
                'description': yt_video['description'][:2000] if yt_video['description'] else '',
                'location': f"https://www.youtube.com/embed/{yt_video['video_id']}",
                'location_type': 1,
                'preview_image_location': yt_video['thumbnail'],
                'tags': yt_video['tags'],
                'category': category,
                'content_rating': 'G',
                'language': 'en',
                'content_features': None  # NULL as requested
            }

            videos.append(video)
            self.tracker.add_video(video)

        # Generate embeddings for video descriptions
        if self.embedder.is_available():
            print("\n🧠 Generating embeddings for video descriptions...")
            descriptions = [v['description'] for v in videos]
            embeddings = self.embedder.generate_batch(descriptions)

            for video, embedding in zip(videos, embeddings):
                video['content_features'] = embedding

        return videos

    def generate_latest_videos(self, videos: List[Dict]) -> List[Dict]:
        """
        Generate denormalized latest_videos table.

        Args:
            videos: List of all videos

        Returns:
            List of latest video entries
        """
        print("\n📅 Generating latest_videos...")

        # Sort by date and take recent 100
        sorted_videos = sorted(videos, key=lambda v: v['added_date'], reverse=True)[:100]

        latest_videos = []
        for video in sorted_videos:
            latest_videos.append({
                'day': video['added_date'].date(),
                'added_date': video['added_date'],
                'videoid': video['videoid'],
                'name': video['name'],
                'preview_image_location': video['preview_image_location'],
                'userid': video['userid'],
                'content_rating': video['content_rating'],
                'category': video['category']
            })

        return latest_videos

    def generate_tags(self, videos: List[Dict]) -> List[Dict]:
        """
        Generate tags table from video tags.

        Args:
            videos: List of all videos

        Returns:
            List of tag dictionaries
        """
        print("\n🏷️  Generating tags...")

        all_tags = self.tracker.tags
        tags = []

        for tag in tqdm(all_tags):
            # Find related tags (tags that co-occur frequently)
            related = self._find_related_tags(tag, videos)

            tags.append({
                'tag': tag,
                'tag_vector': None,  # Will be populated below
                'related_tags': related,
                'category': self._categorize_tag(tag)
            })

        # Generate embeddings for tag names
        if self.embedder.is_available():
            print("\n🧠 Generating embeddings for tags...")
            tag_names = [t['tag'] for t in tags]
            embeddings = self.embedder.generate_batch(tag_names)

            for tag_dict, embedding in zip(tags, embeddings):
                tag_dict['tag_vector'] = embedding

        return tags

    def generate_tag_counts(self, tags: List[Dict], videos: List[Dict]) -> List[Dict]:
        """
        Generate tag_counts counter table.

        Args:
            tags: List of tag dictionaries
            videos: List of all videos

        Returns:
            List of tag count entries
        """
        print("\n📊 Generating tag counts...")

        tag_counts = []
        for tag in tags:
            count = sum(1 for v in videos if tag['tag'] in v.get('tags', set()))
            tag_counts.append({
                'tag': tag['tag'],
                'count': count
            })

        return tag_counts

    def generate_comments(self, videos: List[Dict], users: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Generate comments and comments_by_user.

        Args:
            videos: List of all videos
            users: List of all users

        Returns:
            Tuple of (comments list, comments_by_user list)
        """
        num_comments = random.randint(
            self.config['dataset']['num_comments_min'],
            self.config['dataset']['num_comments_max']
        )

        print(f"\n💬 Generating {num_comments} comments...")

        # Popular videos get more comments
        popular_videos = self.tracker.get_popular_videos(
            self.config['dataset']['popular_video_threshold']
        )
        video_weights = [3 if v in popular_videos else 1 for v in videos]

        comments = []
        comments_by_user = []

        for _ in tqdm(range(num_comments)):
            video = random.choices(videos, weights=video_weights)[0]
            user = random.choice(users)

            # Generate timeuuid (approximated with timestamp)
            comment_id = self._generate_timeuuid()

            # Generate comment text (tech-themed)
            comment_text = self._generate_tech_comment()

            # Sentiment score (skewed positive)
            sentiment = max(0.0, min(1.0, random.gauss(0.7, 0.15)))

            comment = {
                'videoid': video['videoid'],
                'commentid': comment_id,
                'userid': user['userid'],
                'comment': comment_text,
                'sentiment_score': round(sentiment, 3)
            }

            comments.append(comment)
            self.tracker.add_comment(comment)

            # Denormalized by user
            comments_by_user.append({
                'userid': user['userid'],
                'commentid': comment_id,
                'videoid': video['videoid'],
                'comment': comment_text,
                'sentiment_score': round(sentiment, 3)
            })

        return comments, comments_by_user

    def generate_ratings(self, videos: List[Dict], users: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Generate video_ratings_by_user and video_ratings (aggregated).

        Args:
            videos: List of all videos
            users: List of all users

        Returns:
            Tuple of (ratings_by_user list, video_ratings list)
        """
        rating_prob = self.config['dataset']['rating_probability']

        print(f"\n⭐ Generating ratings (probability={rating_prob})...")

        ratings_by_user = []

        # Each user rates a subset of videos
        for user in tqdm(users):
            num_to_rate = random.randint(5, 30)
            videos_to_rate = random.sample(videos, min(num_to_rate, len(videos)))

            for video in videos_to_rate:
                if random.random() < rating_prob:
                    rating = {
                        'videoid': video['videoid'],
                        'userid': user['userid'],
                        'rating': random.choices([1, 2, 3, 4, 5], weights=[2, 3, 10, 35, 50])[0],
                        'rating_date': self.fake.date_time_between(start_date='-1y', end_date='now')
                    }
                    ratings_by_user.append(rating)

        # Aggregate into video_ratings
        print("📊 Aggregating ratings...")
        ratings_by_video = defaultdict(list)
        for rating in ratings_by_user:
            ratings_by_video[rating['videoid']].append(rating['rating'])

        video_ratings = []
        for videoid, ratings_list in ratings_by_video.items():
            video_ratings.append({
                'videoid': videoid,
                'rating_counter': len(ratings_list),
                'rating_total': sum(ratings_list)
            })

        return ratings_by_user, video_ratings

    def generate_playback_stats(self, videos: List[Dict]) -> List[Dict]:
        """
        Generate video_playback_stats counter table.

        Args:
            videos: List of all videos

        Returns:
            List of playback stats entries
        """
        print("\n📺 Generating playback stats...")

        playback_stats = []
        for video in tqdm(videos):
            # Power law distribution for views
            views = int(random.paretovariate(1.5) * 100)
            views = max(10, min(10000, views))  # Clamp to reasonable range

            playback_stats.append({
                'videoid': video['videoid'],
                'views': views,
                'total_play_time': views * random.randint(120, 600),  # seconds
                'complete_views': int(views * random.uniform(0.3, 0.8)),
                'unique_viewers': int(views * random.uniform(0.6, 0.95))
            })

        return playback_stats

    def generate_user_preferences(self, users: List[Dict], videos: List[Dict],
                                   ratings_by_user: List[Dict]) -> List[Dict]:
        """
        Generate user_preferences based on viewing/rating history.

        Args:
            users: List of all users
            videos: List of all videos
            ratings_by_user: List of user ratings

        Returns:
            List of user preference dictionaries
        """
        print("\n🎯 Generating user preferences...")

        # Generate preferences for 70% of users
        active_users = random.sample(users, int(len(users) * 0.7))

        user_preferences = []
        videos_by_id = {v['videoid']: v for v in videos}

        for user in tqdm(active_users):
            # Get user's rated videos
            user_ratings = [r for r in ratings_by_user if r['userid'] == user['userid']]

            # Calculate tag preferences
            tag_scores = defaultdict(float)
            category_scores = defaultdict(float)

            for rating in user_ratings:
                video = videos_by_id.get(rating['videoid'])
                if video:
                    # Weight by rating (higher rated = stronger preference)
                    weight = rating['rating'] / 5.0

                    # Accumulate tag preferences
                    for tag in video.get('tags', set()):
                        tag_scores[tag] += weight

                    # Accumulate category preferences
                    category_scores[video['category']] += weight

            # Normalize scores
            if tag_scores:
                max_tag_score = max(tag_scores.values())
                tag_prefs = {tag: round(score / max_tag_score, 2)
                             for tag, score in tag_scores.items()}
            else:
                tag_prefs = {}

            if category_scores:
                max_cat_score = max(category_scores.values())
                cat_prefs = {cat: round(score / max_cat_score, 2)
                             for cat, score in category_scores.items()}
            else:
                cat_prefs = {}

            user_preferences.append({
                'userid': user['userid'],
                'preference_vector': None,  # Will be populated below
                'tag_preferences': tag_prefs,
                'category_preferences': cat_prefs,
                'last_updated': datetime.now()
            })

        # Generate embeddings from tag preferences
        if self.embedder.is_available():
            print("\n🧠 Generating embeddings for user preferences...")
            # Synthesize text from top tag preferences
            pref_texts = []
            for pref in user_preferences:
                if pref['tag_preferences']:
                    # Get top tags sorted by score
                    top_tags = sorted(pref['tag_preferences'].items(),
                                    key=lambda x: x[1], reverse=True)[:10]
                    text = ' '.join([tag for tag, score in top_tags])
                else:
                    text = ''
                pref_texts.append(text)

            embeddings = self.embedder.generate_batch(pref_texts)

            for pref, embedding in zip(user_preferences, embeddings):
                pref['preference_vector'] = embedding

        return user_preferences

    # Helper methods

    def _categorize_video(self, title: str) -> str:
        """Categorize video based on title keywords"""
        if any(word in title for word in ['tutorial', 'how to', 'guide', 'learn']):
            return 'Tutorial'
        elif any(word in title for word in ['demo', 'example', 'walkthrough']):
            return 'Demo'
        elif any(word in title for word in ['conference', 'talk', 'keynote', 'presentation']):
            return 'Conference'
        elif any(word in title for word in ['workshop', 'training', 'course']):
            return 'Education'
        else:
            return 'Education'

    def _categorize_tag(self, tag: str) -> str:
        """Categorize tag"""
        if tag in ['cassandra', 'astra', 'datastax', 'dse', 'nosql', 'database']:
            return 'Technology'
        elif tag in ['tutorial', 'demo', 'introduction', 'getting_started']:
            return 'Tutorial'
        elif tag in ['conference', 'talk', 'workshop']:
            return 'Event'
        else:
            return 'General'

    def _find_related_tags(self, tag: str, videos: List[Dict], max_related: int = 5) -> set:
        """Find tags that frequently co-occur with the given tag"""
        cooccurrences = Counter()

        for video in videos:
            video_tags = video.get('tags', set())
            if tag in video_tags:
                for other_tag in video_tags:
                    if other_tag != tag:
                        cooccurrences[other_tag] += 1

        # Return top N most common
        related = [t for t, _ in cooccurrences.most_common(max_related)]
        return set(related)

    def _generate_timeuuid(self) -> str:
        """Generate a UUID v1 (time-based) approximation"""
        # For simplicity, use UUID4. In production, use proper timeuuid
        return str(uuid.uuid1())

    def _generate_tech_comment(self) -> str:
        """Generate a tech-themed comment"""
        templates = [
            "Great tutorial! Really helped me understand {}.",
            "Thanks for sharing this. {} is exactly what I needed.",
            "Excellent explanation of {}. Very clear and concise.",
            "This video on {} was super helpful. Keep up the great work!",
            "Love the content! {} makes so much more sense now.",
            "Amazing demo! {} seems really powerful.",
            "Clear and well-explained. {} is fascinating.",
            "Thanks for covering {}. Looking forward to more content!",
        ]

        topics = [
            'Cassandra', 'data modeling', 'CQL', 'distributed databases',
            'NoSQL', 'Astra', 'vector search', 'this topic', 'the architecture',
            'performance optimization', 'scaling', 'consistency'
        ]

        template = random.choice(templates)
        topic = random.choice(topics)
        return template.format(topic)
