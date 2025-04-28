import unittest
from datetime import datetime
from unittest.mock import MagicMock
from src.crawlers.static.static_crawler import StaticCrawler
from src.crawlers.dynamic.dynamic_crawler import DynamicCrawler

class MockS3Storage:
    def __init__(self):
        self.store_processed_data = MagicMock()
        self.stream_processed_text_data = MagicMock()

class TestCrawlerMetrics(unittest.TestCase):
    def setUp(self):
        """Set up test cases with sample website configurations."""
        self.static_config = {
            "name": "test_static",
            "url": "https://example.com",
            "dynamic": False,
            "page_limit": 10
        }
        
        self.dynamic_config = {
            "name": "test_dynamic",
            "url": "https://example.com",
            "dynamic": True,
            "page_limit": 10,
            "wait_time": 2
        }
        
        self.mock_s3_storage = MockS3Storage()
    
    def test_static_crawler_metrics_initialization(self):
        """Test that StaticCrawler properly initializes metrics."""
        crawler = StaticCrawler(self.static_config)
        
        # Check that metrics are initialized
        self.assertIsNotNone(crawler.metrics)
        self.assertIn("start_time", crawler.metrics)
        self.assertIn("end_time", crawler.metrics)
        self.assertIn("total_pages", crawler.metrics)
        self.assertIn("successful_crawls", crawler.metrics)
        self.assertIn("failed_crawls", crawler.metrics)
        
        # Check initial values
        self.assertIsNone(crawler.metrics["end_time"])
        self.assertEqual(crawler.metrics["total_pages"], 0)
        self.assertEqual(crawler.metrics["successful_crawls"], 0)
        self.assertEqual(crawler.metrics["failed_crawls"], 0)
    
    def test_dynamic_crawler_metrics_initialization(self):
        """Test that DynamicCrawler properly initializes metrics."""
        crawler = DynamicCrawler(self.dynamic_config)
        
        # Check that metrics are initialized
        self.assertIsNotNone(crawler.metrics)
        self.assertIn("start_time", crawler.metrics)
        self.assertIn("end_time", crawler.metrics)
        self.assertIn("total_pages", crawler.metrics)
        self.assertIn("successful_crawls", crawler.metrics)
        self.assertIn("failed_crawls", crawler.metrics)
        
        # Check initial values
        self.assertIsNone(crawler.metrics["end_time"])
        self.assertEqual(crawler.metrics["total_pages"], 0)
        self.assertEqual(crawler.metrics["successful_crawls"], 0)
        self.assertEqual(crawler.metrics["failed_crawls"], 0)
    
    def test_metrics_update_after_crawl(self):
        """Test that metrics are properly updated after a crawl attempt."""
        crawler = StaticCrawler(self.static_config)
        
        # Simulate visiting some URLs
        crawler.visited_urls.add("https://example.com/page1")
        crawler.visited_urls.add("https://example.com/page2")
        
        # Call post-crawl processing with mock S3 storage
        crawler._post_crawl_processing(self.mock_s3_storage)
        
        # Check that metrics were updated
        self.assertIsNotNone(crawler.metrics["end_time"])
        self.assertEqual(crawler.metrics["total_pages"], 2)
        self.assertEqual(crawler.metrics["successful_crawls"], 2)
        self.assertEqual(crawler.metrics["failed_crawls"], 0)
        
        # Verify S3 storage was called
        self.mock_s3_storage.store_processed_data.assert_called()
    
    def test_metrics_with_failed_crawls(self):
        """Test that metrics properly track failed crawls."""
        crawler = StaticCrawler(self.static_config)
        
        # Simulate some successful and failed crawls
        crawler.visited_urls.add("https://example.com/page1")
        crawler.visited_urls.add("https://example.com/page2")
        crawler.metrics["failed_crawls"] = 1
        
        # Call post-crawl processing with mock S3 storage
        crawler._post_crawl_processing(self.mock_s3_storage)
        
        # Check that metrics were updated correctly
        self.assertEqual(crawler.metrics["total_pages"], 2)
        self.assertEqual(crawler.metrics["successful_crawls"], 2)
        self.assertEqual(crawler.metrics["failed_crawls"], 1)
        
        # Verify S3 storage was called
        self.mock_s3_storage.store_processed_data.assert_called()

if __name__ == '__main__':
    unittest.main() 