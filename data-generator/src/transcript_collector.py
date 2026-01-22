"""YouTube transcript collector using youtube-transcript-api"""

import time
import random
from typing import Dict, Any, Optional, List
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
    IpBlocked,
    RequestBlocked,
)

# Try importing proxy configs (available in newer versions)
try:
    from youtube_transcript_api.proxies import GenericProxyConfig, WebshareProxyConfig
    PROXY_SUPPORT = True
except ImportError:
    PROXY_SUPPORT = False
    GenericProxyConfig = None
    WebshareProxyConfig = None


class TranscriptCollector:
    """Collects video transcripts from YouTube with rate limiting and error handling"""

    def __init__(self, rate_limit_delay: float = 5.0, languages: List[str] = None,
                 max_retries: int = 3, proxy_config: Optional[Dict[str, str]] = None):
        """
        Initialize the transcript collector.

        Args:
            rate_limit_delay: Base seconds to wait between requests (default 5.0)
            languages: Preferred language codes in order (default ['en'])
            max_retries: Maximum retry attempts for rate-limited requests (default 3)
            proxy_config: Optional proxy configuration dict with keys:
                - 'http_url': HTTP proxy URL (e.g., 'http://user:pass@proxy:port')
                - 'https_url': HTTPS proxy URL
                Or for Webshare:
                - 'webshare_username': Webshare proxy username
                - 'webshare_password': Webshare proxy password
        """
        self.rate_limit_delay = rate_limit_delay
        self.languages = languages or ['en']
        self.max_retries = max_retries
        self._last_request_time = 0
        self._consecutive_errors = 0
        self._request_count = 0
        self._ip_blocked_count = 0

        # Configure proxy if provided
        self._proxy_config = None
        if proxy_config and PROXY_SUPPORT:
            if 'webshare_username' in proxy_config:
                self._proxy_config = WebshareProxyConfig(
                    proxy_username=proxy_config['webshare_username'],
                    proxy_password=proxy_config['webshare_password'],
                )
            elif 'http_url' in proxy_config:
                self._proxy_config = GenericProxyConfig(
                    http_url=proxy_config.get('http_url'),
                    https_url=proxy_config.get('https_url'),
                )

    def _rate_limit(self):
        """Apply rate limiting between requests with jitter"""
        elapsed = time.time() - self._last_request_time

        # Add jitter (random 0-1 seconds) to avoid predictable patterns
        jitter = random.uniform(0, 1.0)
        delay = self.rate_limit_delay + jitter

        # If we've had consecutive errors, add exponential backoff
        if self._consecutive_errors > 0:
            backoff = min(60, (2 ** self._consecutive_errors))  # Max 60 seconds
            delay += backoff

        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request_time = time.time()

    def _is_rate_limit_error(self, error_msg: str) -> bool:
        """Check if an error message indicates rate limiting"""
        rate_limit_indicators = [
            "blocking requests",
            "IP",
            "too many requests",
            "blocked",
            "RequestBlocked",
            "IpBlocked"
        ]
        return any(indicator.lower() in error_msg.lower() for indicator in rate_limit_indicators)

    def get_transcript(self, video_id: str, retry_count: int = 0) -> Dict[str, Any]:
        """
        Fetch transcript for a YouTube video with retry logic.

        Args:
            video_id: YouTube video ID
            retry_count: Current retry attempt (internal use)

        Returns:
            Dictionary with transcript data:
            {
                "available": bool,
                "language": str or None,
                "language_code": str or None,
                "is_generated": bool or None,
                "text": str (full text concatenated),
                "segments": list of {start, duration, text} dicts,
                "error": str or None (if unavailable)
            }
        """
        self._rate_limit()
        self._request_count += 1

        try:
            # Create fresh API instance for each request to avoid session-based blocking
            if self._proxy_config:
                api = YouTubeTranscriptApi(proxy_config=self._proxy_config)
            else:
                api = YouTubeTranscriptApi()

            # List available transcripts using instance method
            transcript_list = api.list(video_id)

            # Reset consecutive error counter on success
            self._consecutive_errors = 0

            # Try to find a transcript in preferred languages
            transcript = None
            is_generated = None

            # First try manual transcripts (more accurate)
            for lang in self.languages:
                try:
                    transcript = transcript_list.find_manually_created_transcript([lang])
                    is_generated = False
                    break
                except NoTranscriptFound:
                    continue

            # Fall back to auto-generated
            if transcript is None:
                for lang in self.languages:
                    try:
                        transcript = transcript_list.find_generated_transcript([lang])
                        is_generated = True
                        break
                    except NoTranscriptFound:
                        continue

            # If still not found, try any available transcript
            if transcript is None:
                try:
                    # Get the first available transcript
                    for t in transcript_list:
                        transcript = t
                        is_generated = t.is_generated
                        break
                except Exception:
                    pass

            if transcript is None:
                return {
                    "available": False,
                    "language": None,
                    "language_code": None,
                    "is_generated": None,
                    "text": "",
                    "segments": [],
                    "error": "No transcript found in any language",
                    "error_type": "no_transcript"
                }

            # Fetch the transcript data
            fetched = transcript.fetch()

            # Convert to list of dicts (FetchedTranscript is iterable)
            segments = list(fetched)

            # Combine all text
            full_text = ' '.join(segment.text for segment in segments)

            return {
                "available": True,
                "language": transcript.language,
                "language_code": transcript.language_code,
                "is_generated": is_generated,
                "text": full_text,
                "segments": [
                    {
                        "start": seg.start,
                        "duration": seg.duration,
                        "text": seg.text
                    }
                    for seg in segments
                ],
                "error": None,
                "error_type": None
            }

        except TranscriptsDisabled:
            self._consecutive_errors = 0  # Not a rate limit issue
            return {
                "available": False,
                "language": None,
                "language_code": None,
                "is_generated": None,
                "text": "",
                "segments": [],
                "error": "Transcripts are disabled for this video",
                "error_type": "disabled"
            }

        except VideoUnavailable:
            self._consecutive_errors = 0  # Not a rate limit issue
            return {
                "available": False,
                "language": None,
                "language_code": None,
                "is_generated": None,
                "text": "",
                "segments": [],
                "error": "Video is unavailable",
                "error_type": "video_unavailable"
            }

        except (IpBlocked, RequestBlocked) as e:
            self._ip_blocked_count += 1
            self._consecutive_errors += 1

            # Retry with exponential backoff
            if retry_count < self.max_retries:
                wait_time = min(120, 30 * (2 ** retry_count))  # 30s, 60s, 120s
                print(f"\n⚠️  IP blocked on {video_id}, waiting {wait_time}s before retry {retry_count + 1}/{self.max_retries}...")
                time.sleep(wait_time)
                return self.get_transcript(video_id, retry_count + 1)

            return {
                "available": False,
                "language": None,
                "language_code": None,
                "is_generated": None,
                "text": "",
                "segments": [],
                "error": "IP blocked by YouTube - try again later or use a proxy",
                "error_type": "ip_blocked"
            }

        except Exception as e:
            error_msg = str(e)

            # Check if this is a rate limit error and we can retry
            if self._is_rate_limit_error(error_msg) and retry_count < self.max_retries:
                self._consecutive_errors += 1
                wait_time = min(120, 30 * (2 ** retry_count))  # 30s, 60s, 120s
                print(f"\n⚠️  Rate limited on {video_id}, waiting {wait_time}s before retry {retry_count + 1}/{self.max_retries}...")
                time.sleep(wait_time)
                return self.get_transcript(video_id, retry_count + 1)

            # Track consecutive errors for backoff
            if self._is_rate_limit_error(error_msg):
                self._consecutive_errors += 1
            else:
                self._consecutive_errors = 0

            return {
                "available": False,
                "language": None,
                "language_code": None,
                "is_generated": None,
                "text": "",
                "segments": [],
                "error": f"Error fetching transcript: {error_msg}",
                "error_type": "ip_blocked" if self._is_rate_limit_error(error_msg) else "unknown"
            }

    def get_available_languages(self, video_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get list of available transcript languages for a video.

        Args:
            video_id: YouTube video ID

        Returns:
            List of available transcripts with language info, or None if error
        """
        self._rate_limit()

        try:
            api = YouTubeTranscriptApi()
            transcript_list = api.list(video_id)

            languages = []
            for transcript in transcript_list:
                languages.append({
                    "language": transcript.language,
                    "language_code": transcript.language_code,
                    "is_generated": transcript.is_generated,
                    "is_translatable": transcript.is_translatable
                })

            return languages

        except Exception:
            return None

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about transcript collection.

        Returns:
            Dictionary with collection stats
        """
        return {
            "request_count": self._request_count,
            "ip_blocked_count": self._ip_blocked_count,
            "consecutive_errors": self._consecutive_errors,
            "proxy_enabled": self._proxy_config is not None,
            "proxy_support_available": PROXY_SUPPORT
        }

    @staticmethod
    def has_proxy_support() -> bool:
        """Check if proxy support is available in the installed library."""
        return PROXY_SUPPORT
