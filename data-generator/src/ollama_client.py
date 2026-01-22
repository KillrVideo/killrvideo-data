"""
Ollama API client for generating enhanced video descriptions.

Simple wrapper around the Ollama REST API for generating text completions
from video transcripts.
"""

import time
from typing import Optional

import requests


class OllamaClient:
    """Client for interacting with local Ollama server."""

    DEFAULT_URL = "http://localhost:11434"
    DEFAULT_MODEL = "llama3.2"
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds

    def __init__(
        self,
        base_url: str = DEFAULT_URL,
        model: str = DEFAULT_MODEL,
        timeout: int = 300
    ):
        """
        Initialize Ollama client.

        Args:
            base_url: Ollama server URL (default: http://localhost:11434)
            model: Model name to use (default: llama3.2)
            timeout: Request timeout in seconds (default: 300 for long generations)
        """
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout = timeout

    def health_check(self) -> bool:
        """
        Check if Ollama server is running and accessible.

        Returns:
            True if server is healthy, False otherwise
        """
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def list_models(self) -> list:
        """
        List available models on the Ollama server.

        Returns:
            List of model names
        """
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            response.raise_for_status()
            data = response.json()
            return [m['name'] for m in data.get('models', [])]
        except requests.RequestException:
            return []

    def generate(self, prompt: str, stream: bool = False) -> str:
        """
        Generate a completion from Ollama.

        Args:
            prompt: The prompt to send to the model
            stream: Whether to stream the response (not implemented)

        Returns:
            Generated text response

        Raises:
            ConnectionError: If Ollama server is not accessible
            RuntimeError: If generation fails after retries
        """
        url = f"{self.base_url}/api/generate"
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False
        }

        last_error = None
        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.post(url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                return data.get('response', '')
            except requests.ConnectionError as e:
                raise ConnectionError(
                    f"Cannot connect to Ollama at {self.base_url}. "
                    "Is Ollama running? Try: ollama serve"
                ) from e
            except requests.Timeout as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                continue
            except requests.RequestException as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY)
                continue

        raise RuntimeError(f"Failed to generate after {self.MAX_RETRIES} attempts: {last_error}")


def truncate_transcript(text: str, max_words: int = 6000) -> str:
    """
    Truncate transcript to approximately max_words.

    Args:
        text: Full transcript text
        max_words: Maximum number of words to keep

    Returns:
        Truncated text with note if truncated
    """
    words = text.split()
    if len(words) <= max_words:
        return text

    truncated = ' '.join(words[:max_words])
    return truncated + "\n\n[Transcript truncated for length]"


def generate_enhanced_description(
    client: OllamaClient,
    transcript: str,
    title: str,
    max_words: int = 6000
) -> str:
    """
    Generate an enhanced video description from transcript.

    Args:
        client: OllamaClient instance
        transcript: Full transcript text
        title: Video title
        max_words: Maximum words from transcript to include

    Returns:
        AI-generated description
    """
    # Truncate long transcripts
    transcript_text = truncate_transcript(transcript, max_words)

    prompt = f"""You are creating a video description based on the transcript. The original description contains marketing language and doesn't accurately describe the content.

Video Title: {title}

Transcript:
{transcript_text}

Write a clear, informative description (2-4 paragraphs) that:
- Summarizes the actual content discussed in the video
- Lists the main topics and concepts covered
- Avoids promotional or marketing language
- Helps viewers understand what they'll learn

Description:"""

    return client.generate(prompt)
