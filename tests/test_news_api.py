import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import requests

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.news_api.news_api_client import NewsApiClient


class TestNewsApiClient(unittest.TestCase):
    """Tests for the NewsApiClient class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key"
        self.news_client = NewsApiClient(api_key=self.api_key)
    
    def test_init(self):
        """Test initialization of NewsApiClient."""
        self.assertEqual(self.news_client.api_key, self.api_key)
        self.assertEqual(self.news_client.base_url, "https://newsapi.org/v2")
    
    @patch("src.news_api.news_api_client.requests")
    def test_fetch_news_api_success(self, mock_requests):
        """Test successfully fetching news from NewsAPI."""
        # Create mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "ok",
            "totalResults": 2,
            "articles": [
                {
                    "source": {"id": "source1", "name": "Source 1"},
                    "author": "Author 1",
                    "title": "Title 1",
                    "description": "Description 1",
                    "url": "https://example.com/news/1",
                    "publishedAt": "2023-01-01T00:00:00Z"
                },
                {
                    "source": {"id": "source2", "name": "Source 2"},
                    "author": "Author 2",
                    "title": "Title 2",
                    "description": "Description 2",
                    "url": "https://example.com/news/2",
                    "publishedAt": "2023-01-02T00:00:00Z"
                }
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_response
        
        result = self.news_client.fetch_news_api(query="test query")
        
        self.assertEqual(result["source"], "newsapi")
        self.assertEqual(result["query"], "test query")
        self.assertEqual(len(result["articles"]), 2)
        self.assertEqual(result["articles"][0]["title"], "Title 1")
        self.assertEqual(result["articles"][1]["title"], "Title 2")
        
        # Verify requests.get was called correctly
        mock_requests.get.assert_called_once()
        call_args = mock_requests.get.call_args
        url_arg = call_args[0][0]
        params_arg = call_args[1]["params"]
        
        self.assertEqual(url_arg, "https://newsapi.org/v2/everything")
        self.assertEqual(params_arg["q"], "test query")
        self.assertEqual(params_arg["apiKey"], self.api_key)
    
    @patch("src.news_api.news_api_client.requests.get")
    def test_fetch_news_api_error(self, mock_get):
        """Test error handling when fetching from NewsAPI."""
        # Use RequestException which is caught in the fetch_news_api method
        mock_get.side_effect = requests.RequestException("API Error")
        
        result = self.news_client.fetch_news_api(query="test query")
        
        self.assertEqual(result["source"], "newsapi")
        self.assertEqual(result["query"], "test query")
        self.assertEqual(len(result["articles"]), 0)
        self.assertIn("error", result)
        self.assertEqual(result["error"], "API Error")
    
    @patch("src.news_api.news_api_client.feedparser")
    def test_fetch_rss_feed_success(self, mock_feedparser):
        """Test successfully fetching news from RSS feed."""
        # Create mock parse result
        mock_feed = MagicMock()
        mock_feed.entries = [
            {
                "title": "RSS Title 1",
                "description": "RSS Description 1",
                "summary": "RSS Summary 1",
                "link": "https://example.com/rss/1",
                "published": "Tue, 01 Jan 2023 00:00:00 GMT"
            },
            {
                "title": "RSS Title 2",
                "description": "RSS Description 2",
                "summary": "RSS Summary 2",
                "link": "https://example.com/rss/2",
                "published": "Wed, 02 Jan 2023 00:00:00 GMT"
            }
        ]
        mock_feed.feed = {
            "title": "Test RSS Feed",
            "link": "https://example.com/rss"
        }
        mock_feedparser.parse.return_value = mock_feed
        
        result = self.news_client.fetch_rss_feed("https://example.com/rss")
        
        self.assertEqual(result["source"], "rss")
        self.assertEqual(result["feed_url"], "https://example.com/rss")
        self.assertEqual(len(result["articles"]), 2)
        self.assertEqual(result["articles"][0]["title"], "RSS Title 1")
        self.assertEqual(result["articles"][1]["title"], "RSS Title 2")
        
        # Verify feedparser.parse was called correctly
        mock_feedparser.parse.assert_called_once_with("https://example.com/rss")
    
    @patch("src.news_api.news_api_client.feedparser.parse")
    def test_fetch_rss_feed_error(self, mock_parse):
        """Test error handling when fetching from RSS feed."""
        mock_parse.side_effect = Exception("RSS Error")
        
        result = self.news_client.fetch_rss_feed("https://example.com/rss")
        
        self.assertEqual(result["source"], "rss")
        self.assertEqual(result["feed_url"], "https://example.com/rss")
        self.assertEqual(len(result["articles"]), 0)
        self.assertIn("error", result)
        self.assertEqual(result["error"], "RSS Error")
    
    @patch.object(NewsApiClient, "fetch_news_api")
    @patch.object(NewsApiClient, "fetch_rss_feed")
    def test_fetch_logistics_news(self, mock_fetch_rss, mock_fetch_news_api):
        """Test fetching logistics news from multiple sources."""
        # Mock NewsAPI response
        mock_fetch_news_api.return_value = {
            "source": "newsapi",
            "query": "logistics OR supply chain OR freight OR shipping",
            "articles": [
                {
                    "title": "NewsAPI Title 1",
                    "url": "https://example.com/news/1"
                },
                {
                    "title": "NewsAPI Title 2",
                    "url": "https://example.com/news/2"
                }
            ]
        }
        
        # Mock RSS feed responses
        def mock_rss_side_effect(url):
            if "supplychaindive" in url:
                return {
                    "source": "rss",
                    "feed_url": url,
                    "articles": [
                        {
                            "title": "RSS Title 1",
                            "url": "https://example.com/rss/1"
                        }
                    ]
                }
            elif "freightwaves" in url:
                return {
                    "source": "rss",
                    "feed_url": url,
                    "articles": [
                        {
                            "title": "RSS Title 2",
                            "url": "https://example.com/rss/2"
                        }
                    ]
                }
            else:
                return {
                    "source": "rss",
                    "feed_url": url,
                    "articles": [
                        {
                            "title": "RSS Title 3",
                            "url": "https://example.com/rss/3"
                        }
                    ]
                }
        
        mock_fetch_rss.side_effect = mock_rss_side_effect
        
        result = self.news_client.fetch_logistics_news()
        
        self.assertIn("sources", result)
        self.assertIn("articles", result)
        self.assertIn("total_articles", result)
        
        # Should have 1 NewsAPI source + 3 RSS sources
        self.assertEqual(len(result["sources"]), 4)
        
        # Should have 5 unique articles
        self.assertEqual(result["total_articles"], 5)
        self.assertEqual(len(result["articles"]), 5)


if __name__ == "__main__":
    unittest.main() 