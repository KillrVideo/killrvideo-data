"""Relationship tracker for maintaining foreign key integrity"""

from typing import Dict, List, Set, Any
from collections import defaultdict


class RelationshipTracker:
    """Tracks relationships between entities to ensure referential integrity"""

    def __init__(self):
        """Initialize relationship tracker"""
        self.users = []
        self.videos = []
        self.comments = []
        self.tags = set()

        # Indexes for fast lookups
        self.user_ids = set()
        self.user_emails = set()
        self.video_ids = set()
        self.video_by_user = defaultdict(list)
        self.comments_by_video = defaultdict(list)
        self.comments_by_user = defaultdict(list)

    def add_user(self, user: Dict[str, Any]):
        """
        Register a user.

        Args:
            user: User dictionary with userid and email
        """
        self.users.append(user)
        self.user_ids.add(user['userid'])
        self.user_emails.add(user['email'])

    def add_video(self, video: Dict[str, Any]):
        """
        Register a video.

        Args:
            video: Video dictionary with videoid and userid
        """
        self.videos.append(video)
        self.video_ids.add(video['videoid'])
        self.video_by_user[video['userid']].append(video['videoid'])

        # Track tags
        if 'tags' in video and video['tags']:
            self.tags.update(video['tags'])

    def add_comment(self, comment: Dict[str, Any]):
        """
        Register a comment.

        Args:
            comment: Comment dictionary with videoid and userid
        """
        self.comments.append(comment)
        self.comments_by_video[comment['videoid']].append(comment)
        self.comments_by_user[comment['userid']].append(comment)

    def validate(self) -> Dict[str, List[str]]:
        """
        Validate all relationships for referential integrity.

        Returns:
            Dictionary of errors by entity type
        """
        errors = defaultdict(list)

        # Validate videos reference valid users
        for video in self.videos:
            if video['userid'] not in self.user_ids:
                errors['videos'].append(f"Video {video['videoid']} references invalid user {video['userid']}")

        # Validate comments reference valid videos and users
        for comment in self.comments:
            if comment['videoid'] not in self.video_ids:
                errors['comments'].append(f"Comment references invalid video {comment['videoid']}")
            if comment['userid'] not in self.user_ids:
                errors['comments'].append(f"Comment references invalid user {comment['userid']}")

        return dict(errors)

    def get_popular_videos(self, percentage: float = 0.2) -> List[Dict[str, Any]]:
        """
        Get the top N% of videos (can be weighted by any criteria).

        Args:
            percentage: Percentage of videos to consider popular (0.0-1.0)

        Returns:
            List of popular video dictionaries
        """
        count = max(1, int(len(self.videos) * percentage))
        # For now, just return the first N videos
        # In a real implementation, you might sort by creation date or other criteria
        return self.videos[:count]

    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about the dataset.

        Returns:
            Dictionary of entity counts
        """
        return {
            'users': len(self.users),
            'videos': len(self.videos),
            'comments': len(self.comments),
            'tags': len(self.tags),
            'videos_by_user': {
                'min': min((len(v) for v in self.video_by_user.values()), default=0),
                'max': max((len(v) for v in self.video_by_user.values()), default=0),
                'avg': sum(len(v) for v in self.video_by_user.values()) / len(self.video_by_user) if self.video_by_user else 0
            },
            'comments_by_video': {
                'min': min((len(c) for c in self.comments_by_video.values()), default=0),
                'max': max((len(c) for c in self.comments_by_video.values()), default=0),
                'avg': sum(len(c) for c in self.comments_by_video.values()) / len(self.comments_by_video) if self.comments_by_video else 0
            }
        }
