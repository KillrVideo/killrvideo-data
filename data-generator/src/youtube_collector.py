"""YouTube Data API v3 collector for video metadata"""

import re
from typing import List, Dict, Any
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm


class YouTubeCollector:
    """Collects video metadata from YouTube using the Data API v3"""

    def __init__(self, api_key: str):
        """
        Initialize the YouTube API client.

        Args:
            api_key: YouTube Data API v3 key
        """
        self.youtube = build('youtube', 'v3', developerKey=api_key)

    def get_channel_videos(self, channel_id: str, max_results: int = None) -> List[Dict[str, Any]]:
        """
        Get all videos from a YouTube channel.

        Uses the efficient playlistItems.list method (1 API unit per 50 videos)
        instead of search (100 units per query).

        Args:
            channel_id: YouTube channel ID
            max_results: Maximum number of videos to fetch (None = all)

        Returns:
            List of video metadata dictionaries
        """
        try:
            # Step 1: Get the uploads playlist ID (1 API unit)
            channels_response = self.youtube.channels().list(
                id=channel_id,
                part='contentDetails'
            ).execute()

            if not channels_response.get('items'):
                print(f"Channel {channel_id} not found")
                return []

            uploads_playlist_id = channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

            # Step 2: Paginate through all videos (1 unit per 50 videos)
            videos = []
            next_page_token = None

            with tqdm(desc="Fetching channel videos", unit=" videos") as pbar:
                while True:
                    playlist_response = self.youtube.playlistItems().list(
                        playlistId=uploads_playlist_id,
                        part='snippet',
                        maxResults=50,
                        pageToken=next_page_token
                    ).execute()

                    for item in playlist_response['items']:
                        video_data = self._extract_video_data(item['snippet'], source='channel')
                        videos.append(video_data)
                        pbar.update(1)

                        if max_results and len(videos) >= max_results:
                            return videos[:max_results]

                    next_page_token = playlist_response.get('nextPageToken')
                    if not next_page_token:
                        break

            return videos

        except HttpError as e:
            print(f"YouTube API error: {e}")
            return []

    def search_videos(self, query: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        Search for videos by keyword.

        Warning: Uses 100 API units per request!

        Args:
            query: Search query string
            max_results: Maximum number of results (default 100)

        Returns:
            List of video metadata dictionaries
        """
        try:
            videos = []
            next_page_token = None

            with tqdm(desc=f"Searching '{query}'", total=max_results, unit=" videos") as pbar:
                while len(videos) < max_results:
                    search_response = self.youtube.search().list(
                        q=query,
                        part='snippet',
                        type='video',
                        maxResults=min(50, max_results - len(videos)),
                        pageToken=next_page_token
                    ).execute()

                    for item in search_response['items']:
                        video_data = self._extract_video_data(item['snippet'], source='search')
                        # Add video_id from search results
                        video_data['video_id'] = item['id']['videoId']
                        videos.append(video_data)
                        pbar.update(1)

                    next_page_token = search_response.get('nextPageToken')
                    if not next_page_token:
                        break

            return videos

        except HttpError as e:
            print(f"YouTube API error: {e}")
            return []

    def _extract_video_data(self, snippet: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        Extract relevant video metadata from API response.

        Args:
            snippet: YouTube API snippet object
            source: 'channel' or 'search'

        Returns:
            Cleaned video metadata dictionary
        """
        # Extract video ID based on source
        if source == 'channel':
            video_id = snippet['resourceId']['videoId']
        else:
            video_id = snippet.get('videoId', '')

        # Extract and clean title
        title = snippet.get('title', 'Untitled')[:255]

        # Extract description
        description = snippet.get('description', '')[:2000]

        # Extract published date
        published_at = snippet.get('publishedAt', '')

        # Get best quality thumbnail
        thumbnails = snippet.get('thumbnails', {})
        thumbnail_url = (
            thumbnails.get('maxres', {}).get('url') or
            thumbnails.get('high', {}).get('url') or
            thumbnails.get('medium', {}).get('url') or
            thumbnails.get('default', {}).get('url') or
            ''
        )

        # Extract channel info
        channel_title = snippet.get('channelTitle', 'Unknown')

        # Extract tags from title and description
        tags = self._extract_tags(title, description)

        return {
            'video_id': video_id,
            'title': title,
            'description': description,
            'published_at': published_at,
            'thumbnail': thumbnail_url,
            'channel_title': channel_title,
            'tags': tags,
        }

    def _extract_tags(self, title: str, description: str) -> set:
        """
        Extract relevant tags from video title and description.

        Args:
            title: Video title
            description: Video description

        Returns:
            Set of lowercase tags
        """
        text = f"{title} {description}".lower()
        tags = set()

        # Cassandra-related keywords
        keywords = [
            'cassandra', 'astra', 'datastax', 'nosql', 'database',
            'cql', 'distributed', 'scalable', 'nosql database',
            'apache cassandra', 'tutorial', 'demo', 'introduction',
            'getting started', 'workshop', 'conference', 'talk',
            'dse', 'datastar enterprise', 'vector', 'search',
            'data modeling', 'query', 'performance', 'architecture'
        ]

        for keyword in keywords:
            if keyword in text:
                tags.add(keyword.replace(' ', '_'))

        # Ensure at least one tag
        if not tags:
            tags.add('database')

        return tags

    def collect_all_videos(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Collect videos from channel and search queries based on config.

        Args:
            config: Configuration dictionary with youtube settings

        Returns:
            Combined list of unique videos
        """
        all_videos = []
        video_ids_seen = set()

        youtube_config = config.get('youtube', {})
        channel_id = youtube_config.get('channel_id')
        search_queries = youtube_config.get('search_queries', [])
        max_videos = youtube_config.get('max_videos', 500)
        max_search_results = youtube_config.get('max_search_results', 100)

        # Primary: Get channel videos
        if channel_id:
            print(f"\n📹 Collecting videos from channel: {channel_id}")
            channel_videos = self.get_channel_videos(channel_id)
            for video in channel_videos:
                if video['video_id'] not in video_ids_seen:
                    all_videos.append(video)
                    video_ids_seen.add(video['video_id'])

        # Supplementary: Search for additional videos
        if len(all_videos) < max_videos and search_queries:
            for query in search_queries:
                if len(all_videos) >= max_videos:
                    break

                print(f"\n🔍 Searching for: '{query}'")
                search_videos = self.search_videos(query, max_search_results)
                for video in search_videos:
                    if video['video_id'] not in video_ids_seen and len(all_videos) < max_videos:
                        all_videos.append(video)
                        video_ids_seen.add(video['video_id'])

        # Limit to max_videos
        final_videos = all_videos[:max_videos]

        print(f"\n✅ Collected {len(final_videos)} unique videos")
        return final_videos
