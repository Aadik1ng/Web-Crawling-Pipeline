import spacy
import hashlib
from typing import Dict, List, Set, Any, Generator
from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import gc

class TextProcessor:
    """Class to handle text preprocessing, NER, keyword extraction, and deduplication."""
    
    def __init__(self):
        """Initialize the text processor with required models."""
        # Load spaCy model for NER with memory optimization
        self.nlp = spacy.load("en_core_web_sm", disable=['parser', 'textcat'])
        
        # Initialize TF-IDF vectorizer for keyword extraction with memory limits
        self.tfidf = TfidfVectorizer(
            max_features=50,  # Reduced from 100
            stop_words='english',
            ngram_range=(1, 2),
            max_df=1.0,  # Allow all terms
            min_df=1  # Allow all terms
        )
        
        # Initialize deduplication storage with LRU-like behavior
        self.content_hashes: Set[str] = set()
        self.max_hashes = 10000  # Increased from 1000 to allow more unique content
        self.similarity_threshold = 0.95  # Increased from 0.85 to be less strict
    
    def _cleanup_memory(self):
        """Force garbage collection to free memory."""
        gc.collect()
    
    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """
        Extract named entities from text using spaCy.
        
        Args:
            text: Text to process
            
        Returns:
            Dict[str, List[str]]: Dictionary of entity types and their values
        """
        # Process text in chunks to save memory
        chunk_size = 100000  # Process 100KB at a time
        entities = defaultdict(list)
        
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            doc = self.nlp(chunk)
            
            for ent in doc.ents:
                entities[ent.label_].append(ent.text)
            
            # Clean up memory after each chunk
            del doc
            self._cleanup_memory()
        
        return dict(entities)
    
    def extract_keywords(self, text: str, top_n: int = 10) -> List[str]:
        """
        Extract keywords using TF-IDF with memory optimization.
        
        Args:
            text: Text to process
            top_n: Number of top keywords to return
            
        Returns:
            List[str]: List of top keywords
        """
        try:
            # Process text in chunks
            chunk_size = 100000
            all_keywords = []
            
            for i in range(0, len(text), chunk_size):
                chunk = text[i:i + chunk_size]
                
                # Fit and transform the chunk
                tfidf_matrix = self.tfidf.fit_transform([chunk])
                
                # Get feature names (words)
                feature_names = self.tfidf.get_feature_names_out()
                
                # Get top keywords
                scores = tfidf_matrix.toarray()[0]
                top_indices = np.argsort(scores)[-top_n:][::-1]
                
                chunk_keywords = [feature_names[i] for i in top_indices]
                all_keywords.extend(chunk_keywords)
                
                # Clean up memory
                del tfidf_matrix
                del scores
                self._cleanup_memory()
            
            # Return unique keywords
            return list(dict.fromkeys(all_keywords))[:top_n]
        except Exception as e:
            # If TF-IDF fails, return basic keywords
            words = text.lower().split()
            word_freq = defaultdict(int)
            for word in words:
                if len(word) > 3:  # Skip short words
                    word_freq[word] += 1
            
            # Sort by frequency and return top N
            sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
            return [word for word, _ in sorted_words[:top_n]]
    
    def calculate_content_hash(self, text: str) -> str:
        """
        Calculate a hash for the text content.
        
        Args:
            text: Text to hash
            
        Returns:
            str: Hash of the text
        """
        # Process text in chunks to save memory
        chunk_size = 100000
        hasher = hashlib.md5()
        
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i + chunk_size]
            # Normalize chunk (lowercase, remove extra spaces)
            normalized_chunk = ' '.join(chunk.lower().split())
            hasher.update(normalized_chunk.encode())
        
        return hasher.hexdigest()
    
    def is_duplicate(self, text: str) -> bool:
        """
        Check if text is a duplicate based on content hash.
        
        Args:
            text: Text to check
            
        Returns:
            bool: True if duplicate, False otherwise
        """
        content_hash = self.calculate_content_hash(text)
        
        # Implement LRU-like behavior for hash storage
        if len(self.content_hashes) >= self.max_hashes:
            # Remove oldest hashes if we exceed the limit
            self.content_hashes = set(list(self.content_hashes)[-self.max_hashes:])
        
        if content_hash in self.content_hashes:
            return True
        
        self.content_hashes.add(content_hash)
        return False
    
    def process_text(self, text: str) -> Dict[str, Any]:
        """
        Process text with NER, keyword extraction, and deduplication.
        
        Args:
            text: Text to process
            
        Returns:
            Dict[str, Any]: Processed text data
        """
        # Check for duplicates
        if self.is_duplicate(text):
            return None
        
        # Extract entities
        entities = self.extract_entities(text)
        
        # Extract keywords
        keywords = self.extract_keywords(text)
        
        # Clean up memory
        self._cleanup_memory()
        
        return {
            "entities": entities,
            "keywords": keywords,
            "content_hash": self.calculate_content_hash(text)
        } 