import json
import datetime
from typing import Dict, Any, List, Optional
import requests
import feedparser

import config


class NewsApiClient:
    """Client for fetching news from NewsAPI and RSS feeds."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the NewsAPI client.
        
        Args:
            api_key: NewsAPI API key (optional, defaults to config)
        """
        self.api_key = api_key or config.NEWS_API_KEY
        self.base_url = "https://newsapi.org/v2"
        self.keywords = config.NEWS_KEYWORDS
    
    def fetch_news_api(self, query: Optional[str] = None, 
                     days: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """
        Fetch news from NewsAPI.
        
        Args:
            query: Search query (optional, defaults to configured keywords)
            days: Number of days to fetch (default: 1)
            page_size: Number of results per page (default: 100)
            
        Returns:
            Dict[str, Any]: NewsAPI response
        """
        if not self.api_key:
            raise ValueError("NewsAPI API key is not set")
        
        # Use configured keywords if query is not provided
        if not query:
            query = " OR ".join(self.keywords)
        
        # Calculate date range
        today = datetime.datetime.now()
        from_date = (today - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        
        # Make request to NewsAPI
        url = f"{self.base_url}/everything"
        params = {
            "q": query,
            "from": from_date,
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": page_size,
            "apiKey": self.api_key
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Add metadata
            return {
                "source": "newsapi",
                "query": query,
                "timestamp": datetime.datetime.now().isoformat(),
                "articles": data.get("articles", [])
            }
        
        except requests.RequestException as e:
            print(f"Error fetching news from NewsAPI: {str(e)}")
            return {
                "source": "newsapi",
                "query": query,
                "timestamp": datetime.datetime.now().isoformat(),
                "error": str(e),
                "articles": []
            }
    
    def fetch_rss_feed(self, feed_url: str) -> Dict[str, Any]:
        """
        Fetch news from an RSS feed.
        
        Args:
            feed_url: URL of the RSS feed
            
        Returns:
            Dict[str, Any]: RSS feed data
        """
        try:
            feed = feedparser.parse(feed_url)
            
            # Convert feed entries to a common format
            articles = []
            for entry in feed.entries:
                articles.append({
                    "title": entry.get("title", ""),
                    "description": entry.get("description", ""),
                    "content": entry.get("content", entry.get("summary", "")),
                    "url": entry.get("link", ""),
                    "publishedAt": entry.get("published", ""),
                    "source": {
                        "name": feed.feed.get("title", ""),
                        "url": feed.feed.get("link", "")
                    }
                })
            
            # Add metadata
            return {
                "source": "rss",
                "feed_url": feed_url,
                "timestamp": datetime.datetime.now().isoformat(),
                "articles": articles
            }
        
        except Exception as e:
            print(f"Error fetching news from RSS feed {feed_url}: {str(e)}")
            return {
                "source": "rss",
                "feed_url": feed_url,
                "timestamp": datetime.datetime.now().isoformat(),
                "error": str(e),
                "articles": []
            }
    
    def fetch_logistics_news(self) -> Dict[str, Any]:
        """
        Fetch logistics news from multiple sources.
        
        Returns:
            Dict[str, Any]: Combined news data
        """
        all_news = {
            "timestamp": datetime.datetime.now().isoformat(),
            "sources": [],
            "articles": []
        }
        
        # Try NewsAPI if API key is available
        if self.api_key:
            news_api_data = self.fetch_news_api()
            all_news["sources"].append("newsapi")
            all_news["articles"].extend(news_api_data.get("articles", []))
        
        # RSS feeds for logistics news
        logistics_rss_feeds = [
            "https://www.supplychaindive.com/feeds/news/",
            "https://www.freightwaves.com/news/feed",
            "https://theloadstar.com/feed/",
        ]
        
        for feed_url in logistics_rss_feeds:
            rss_data = self.fetch_rss_feed(feed_url)
            if "error" not in rss_data:
                all_news["sources"].append(f"rss:{feed_url}")
                all_news["articles"].extend(rss_data.get("articles", []))
        
        # Deduplicate articles based on URL
        unique_articles = {}
        for article in all_news["articles"]:
            url = article.get("url", "")
            if url and url not in unique_articles:
                unique_articles[url] = article
        
        all_news["articles"] = list(unique_articles.values())
        all_news["total_articles"] = len(all_news["articles"])
        
        return all_news 