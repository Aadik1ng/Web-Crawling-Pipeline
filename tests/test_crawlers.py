import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import requests

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.crawlers.base_crawler import BaseCrawler
from src.crawlers.static.static_crawler import StaticCrawler
from src.crawlers.dynamic.dynamic_crawler import DynamicCrawler
from src.crawlers.crawler_factory import CrawlerFactory


class MockResponse:
    """Mock HTTP response for testing."""
    
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code
    
    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP Error: {self.status_code}")


class TestBaseCrawler(unittest.TestCase):
    """Tests for the BaseCrawler class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a concrete subclass of BaseCrawler for testing
        class ConcreteCrawler(BaseCrawler):
            def crawl(self):
                return {"test": "data"}
            
            def parse(self, html):
                return {"parsed": html}
        
        self.crawler_cls = ConcreteCrawler
        self.website_config = {
            "name": "test_website",
            "url": "https://example.com",
            "dynamic": False
        }
        
        # Create a mock session
        self.mock_session = MagicMock()
        self.patcher = patch('requests.Session', return_value=self.mock_session)
        self.patcher.start()
        self.crawler = self.crawler_cls(self.website_config)
    
    def tearDown(self):
        """Clean up after tests."""
        self.patcher.stop()
    
    def test_init(self):
        """Test crawler initialization."""
        crawler = self.crawler_cls(self.website_config)
        
        self.assertEqual(crawler.name, "test_website")
        self.assertEqual(crawler.base_url, "https://example.com")
        self.assertEqual(crawler.is_dynamic, False)
    
    def test_get_page_success(self):
        """Test successfully getting a page."""
        # Create a mock response
        mock_response = MagicMock()
        mock_response.text = "test content"
        mock_response.status_code = 200
        self.mock_session.get.return_value = mock_response
        
        # Call the method
        html = self.crawler.get_page("https://example.com")
        
        # Verify the result
        self.assertEqual(html, "test content")
        self.mock_session.get.assert_called_once()
    
    def test_get_page_failure(self):
        """Test handling of request failures."""
        # Setup mock to raise an exception
        self.mock_session.get.side_effect = requests.exceptions.RequestException("Test exception")
        
        # Call the method with a URL that will "fail"
        result = self.crawler.get_page("https://example.com/nonexistent")
        
        # Verify the result is None
        self.assertIsNone(result)
    
    def test_get_metadata(self):
        """Test adding metadata to crawled data."""
        crawler = self.crawler_cls(self.website_config)
        data = {"key": "value"}
        
        metadata = crawler.get_metadata(data)
        
        self.assertEqual(metadata["key"], "value")
        self.assertEqual(metadata["crawler"], "test_website")
        self.assertEqual(metadata["source_url"], "https://example.com")
        self.assertIn("timestamp", metadata)


class TestStaticCrawler(unittest.TestCase):
    """Tests for the StaticCrawler class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.website_config = {
            "name": "test_website",
            "url": "https://example.com",
            "dynamic": False
        }
        
        # Create a mock session
        self.mock_session = MagicMock()
        self.patcher = patch('requests.Session', return_value=self.mock_session)
        self.patcher.start()
        
        # Initialize the crawler
        self.crawler = StaticCrawler(self.website_config)
        
        # Sample HTML for testing
        self.sample_html = """
        <html>
            <head>
                <title>Test Page</title>
                <meta name="description" content="Test description">
            </head>
            <body>
                <h1>Hello, World!</h1>
                <p>This is a test paragraph.</p>
                <a href="/page1">Page 1</a>
                <a href="https://example.com/page2">Page 2</a>
                <a href="https://external.com">External</a>
                <img src="/image.jpg" alt="Test Image">
            </body>
        </html>
        """
    
    def tearDown(self):
        """Clean up after tests."""
        self.patcher.stop()
    
    def test_extract_links(self):
        """Test extracting links from HTML."""
        crawler = StaticCrawler(self.website_config)
        links = crawler.extract_links(self.sample_html, "https://example.com")
        
        self.assertEqual(len(links), 2)
        self.assertIn("https://example.com/page1", links)
        self.assertIn("https://example.com/page2", links)
        self.assertNotIn("https://external.com", links)
    
    def test_extract_images(self):
        """Test extracting images from HTML."""
        crawler = StaticCrawler(self.website_config)
        images = crawler.extract_images(self.sample_html, "https://example.com")
        
        self.assertEqual(len(images), 1)
        self.assertEqual(images[0]["url"], "https://example.com/image.jpg")
        self.assertEqual(images[0]["alt_text"], "Test Image")
    
    def test_extract_text(self):
        """Test extracting text from HTML."""
        crawler = StaticCrawler(self.website_config)
        text = crawler.extract_text(self.sample_html)
        
        self.assertIn("Hello, World!", text)
        self.assertIn("This is a test paragraph.", text)
    
    def test_extract_metadata(self):
        """Test extracting metadata from HTML."""
        crawler = StaticCrawler(self.website_config)
        metadata = crawler.extract_metadata(self.sample_html)
        
        self.assertEqual(metadata["title"], "Test Page")
        self.assertEqual(metadata["description"], "Test description")
    
    def test_crawl(self):
        """Test crawling a website."""
        # Sample HTML for the first page with links to other pages
        first_page_html = """
        <html>
            <head><title>First Page</title></head>
            <body>
                <h1>First Page</h1>
                <a href="https://example.com/page1">Page 1</a>
                <a href="https://example.com/page2">Page 2</a>
            </body>
        </html>
        """
        
        # Sample HTML for the second page
        page1_html = """
        <html>
            <head><title>Page 1</title></head>
            <body>
                <h1>Page 1</h1>
                <p>Content of page 1</p>
            </body>
        </html>
        """
        
        # Set up the mock responses
        def mock_get_response(url, **kwargs):
            mock_response = MagicMock()
            if url == "https://example.com":
                mock_response.text = first_page_html
                mock_response.status_code = 200
            elif url == "https://example.com/page1":
                mock_response.text = page1_html
                mock_response.status_code = 200
            else:
                mock_response.status_code = 404
                raise requests.exceptions.RequestException("Not found")
            return mock_response
                
        self.mock_session.get.side_effect = mock_get_response
        
        # Call the crawl method
        data = self.crawler.crawl()
        
        # Verify the results
        self.assertIn("pages", data)
        self.assertEqual(len(data["pages"]), 2)  # Should have crawled 2 pages
        
        # Check base URL page
        self.assertEqual(data["pages"][0]["url"], "https://example.com")
        self.assertIn("First Page", data["pages"][0]["text"])
        
        # Check page1
        self.assertEqual(data["pages"][1]["url"], "https://example.com/page1")
        self.assertIn("Content of page 1", data["pages"][1]["text"])
        
        # Check metadata
        self.assertIn("crawler", data)
        self.assertEqual(data["crawler"], "test_website")


class TestCrawlerFactory(unittest.TestCase):
    """Tests for the CrawlerFactory class."""
    
    def test_create_static_crawler(self):
        """Test creating a static crawler."""
        website_config = {
            "name": "test_website",
            "url": "https://example.com",
            "dynamic": False
        }
        
        factory = CrawlerFactory()
        crawler = factory.create_crawler(website_config)
        
        self.assertIsInstance(crawler, StaticCrawler)
        self.assertEqual(crawler.name, "test_website")
    
    def test_create_dynamic_crawler(self):
        """Test creating a dynamic crawler."""
        website_config = {
            "name": "test_website",
            "url": "https://example.com",
            "dynamic": True
        }
        
        factory = CrawlerFactory()
        crawler = factory.create_crawler(website_config)
        
        self.assertIsInstance(crawler, DynamicCrawler)
        self.assertEqual(crawler.name, "test_website")


if __name__ == "__main__":
    unittest.main() 