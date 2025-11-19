"""Embedding generation using IBM Granite model"""

from typing import List, Optional
import numpy as np

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    print("WARNING: sentence-transformers not installed. Vectors will be NULL.")
    print("Install with: pip install sentence-transformers")


class EmbeddingGenerator:
    """Generates embeddings using IBM Granite 30M English model"""

    def __init__(self, model_name: str = "ibm-granite/granite-embedding-30m-english"):
        """
        Initialize the embedding model.

        Args:
            model_name: HuggingFace model identifier
        """
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            self.model = None
            return

        print(f"📊 Loading embedding model: {model_name}")
        print("   (First run will download ~120MB model)")

        self.model = SentenceTransformer(model_name)
        self.dimensions = self.model.get_sentence_embedding_dimension()

        print(f"✓ Model loaded: {self.dimensions}D embeddings")

    def generate(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding for a single text.

        Args:
            text: Input text

        Returns:
            List of float values or None if model not available
        """
        if not self.model or not text:
            return None

        try:
            embedding = self.model.encode(text, convert_to_numpy=True)
            return embedding.tolist()
        except Exception as e:
            print(f"WARNING: Failed to generate embedding: {e}")
            return None

    def generate_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """
        Generate embeddings for multiple texts efficiently.

        Args:
            texts: List of input texts

        Returns:
            List of embeddings (or None for empty/invalid texts)
        """
        if not self.model:
            return [None] * len(texts)

        # Filter out empty texts but remember their positions
        valid_texts = []
        valid_indices = []

        for i, text in enumerate(texts):
            if text and text.strip():
                valid_texts.append(text)
                valid_indices.append(i)

        if not valid_texts:
            return [None] * len(texts)

        try:
            # Generate embeddings for valid texts
            embeddings = self.model.encode(valid_texts, convert_to_numpy=True, show_progress_bar=True)

            # Reconstruct full list with None for invalid texts
            results = [None] * len(texts)
            for i, embedding in zip(valid_indices, embeddings):
                results[i] = embedding.tolist()

            return results

        except Exception as e:
            print(f"WARNING: Batch embedding generation failed: {e}")
            return [None] * len(texts)

    def is_available(self) -> bool:
        """Check if embedding model is available"""
        return self.model is not None
