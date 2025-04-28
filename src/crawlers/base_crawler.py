import os
import random
import time
import requests
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

import src.utils.config as config


class BaseCrawler(ABC):
    """Base crawler class that all specific crawlers will inherit from."""
    
    def __init__(self, website_config: Dict[str, Any]):
        """
        Initialize the base crawler with website configuration.
        
        Args:
            website_config: Dictionary containing website configuration
                - name: Website name
                - url: Base URL
                - dynamic: Whether the website uses JavaScript rendering
        """
        self.name = website_config.get("name", "")
        self.base_url = website_config.get("url", "")
        self.is_dynamic = website_config.get("dynamic", False)
        self.session = requests.Session()
        self.user_agents = config.USER_AGENTS
        self.delay = config.REQUEST_DELAY
        self.max_retries = config.MAX_RETRIES
        
        # Set random user agent
        self._rotate_user_agent()
    
    def _rotate_user_agent(self) -> None:
        """Rotate the user agent for the session."""
        if self.user_agents:
            self.session.headers.update({"User-Agent": random.choice(self.user_agents)})
    
    def _respect_robots_txt(self, url: str) -> bool:
        """
        Check if the URL is allowed to be crawled based on robots.txt.
        
        Args:
            url: The URL to check
            
        Returns:
            bool: True if allowed, False otherwise
        """
        # TODO: Implement robots.txt checking
        # This is a placeholder implementation
        return True
    
    def _get_url_hash(self, url: str) -> str:
        """
        Generate a hash for the URL.
        
        Args:
            url: The URL to hash
            
        Returns:
            str: Hash of the URL
        """
        return hashlib.md5(url.encode()).hexdigest()
    
    def get_page(self, url: str) -> Optional[str]:
        """
        Get the HTML content of a page with retries and delays.
        
        Args:
            url: URL to fetch
            
        Returns:
            Optional[str]: HTML content of the page or None if failed
        """
        if not self._respect_robots_txt(url):
            print(f"URL {url} is not allowed by robots.txt")
            return None
        
        for attempt in range(self.max_retries):
            try:
                # Rotate user agent on each attempt
                self._rotate_user_agent()
                
                # Add delay between requests
                if attempt > 0:
                    time.sleep(self.delay * (attempt + 1))  # Exponential backoff
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Add delay after successful request
                time.sleep(self.delay)
                
                return response.text
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1}/{self.max_retries} failed for {url}: {str(e)}")
        
        return None
    
    @abstractmethod
    def crawl(self) -> Dict[str, Any]:
        """
        Crawl the website and yield pages as they are crawled.
        
        Yields:
            Dict[str, Any]: Dictionary containing the crawled page data
        """
        pass
    
    @abstractmethod
    def parse(self, html: str) -> Dict[str, Any]:
        """
        Parse the HTML content and extract data.
        
        Args:
            html: HTML content to parse
            
        Returns:
            Dict[str, Any]: Dictionary containing the parsed data
        """
        pass
    
    def get_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add metadata to the crawled data.
        
        Args:
            data: Crawled data
            
        Returns:
            Dict[str, Any]: Data with added metadata
        """
        return {
            **data,
            "crawler": self.name,
            "timestamp": datetime.utcnow().isoformat(),
            "source_url": self.base_url
        } 