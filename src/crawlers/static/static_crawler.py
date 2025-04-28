from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re
import urllib.parse
import gc
import json
from datetime import datetime

from src.crawlers.base_crawler import BaseCrawler
from src.utils.text_processor import TextProcessor
from src.storage.s3_storage import S3Storage
from src.utils.logger import CrawlerLogger


class StaticCrawler(BaseCrawler):
    """Crawler for static websites that don't require JavaScript rendering."""
    
    def __init__(self, website_config: Dict[str, Any]):
        """
        Initialize static crawler with website configuration.
        
        Args:
            website_config: Dictionary containing website configuration
        """
        super().__init__(website_config)
        self.visited_urls = set()
        self.page_limit = website_config.get("page_limit", 100)
        self.text_processor = TextProcessor()
        self.logger = CrawlerLogger(f"static_crawler_{self.name}")
        self.metrics = {
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "total_pages": 0,
            "successful_crawls": 0,
            "failed_crawls": 0
        }
    
    def extract_links(self, html: str, base_url: str) -> List[str]:
        """
        Extract links from HTML content.
        
        Args:
            html: HTML content
            base_url: Base URL to resolve relative links
            
        Returns:
            List[str]: List of extracted links
        """
        @self.logger.log_operation("extract_links")
        def _extract_links():
            self.logger.debug(f"Extracting links from {base_url}")
            links = []
            soup = BeautifulSoup(html, 'html.parser')
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Skip empty links, anchors, and javascript links
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue
                
                # Resolve relative URLs
                absolute_url = urllib.parse.urljoin(base_url, href)
                
                # Filter out external links
                if absolute_url.startswith(self.base_url):
                    links.append(absolute_url)
            
            self.logger.debug(f"Found {len(links)} links")
            return links
        
        return _extract_links()
    
    def extract_images(self, html: str, base_url: str) -> List[Dict[str, str]]:
        """
        Extract images from HTML content.
        
        Args:
            html: HTML content
            base_url: Base URL to resolve relative links
            
        Returns:
            List[Dict[str, str]]: List of image data (URL and alt text)
        """
        @self.logger.log_operation("extract_images")
        def _extract_images():
            self.logger.debug(f"Extracting images from {base_url}")
            images = []
            soup = BeautifulSoup(html, 'html.parser')
            
            for img in soup.find_all('img', src=True):
                src = img['src']
                alt = img.get('alt', '')
                
                # Resolve relative URLs
                absolute_url = urllib.parse.urljoin(base_url, src)
                
                images.append({
                    "url": absolute_url,
                    "alt_text": alt
                })
            
            self.logger.debug(f"Found {len(images)} images")
            return images
        
        return _extract_images()
    
    def extract_text(self, html: str) -> str:
        """
        Extract clean text from HTML content.
        
        Args:
            html: HTML content
            
        Returns:
            str: Cleaned text content
        """
        @self.logger.log_operation("extract_text")
        def _extract_text():
            self.logger.debug("Extracting text from HTML")
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()
            
            # Get text
            text = soup.get_text()
            
            # Break into lines and remove leading and trailing space on each
            lines = (line.strip() for line in text.splitlines())
            
            # Break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            
            # Remove blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            self.logger.debug(f"Extracted {len(text)} characters of text")
            return text
        
        return _extract_text()
    
    def extract_metadata(self, html: str) -> Dict[str, str]:
        """
        Extract metadata from HTML content.
        
        Args:
            html: HTML content
            
        Returns:
            Dict[str, str]: Dictionary of metadata
        """
        @self.logger.log_operation("extract_metadata")
        def _extract_metadata():
            self.logger.debug("Extracting metadata from HTML")
            soup = BeautifulSoup(html, 'html.parser')
            metadata = {}
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                metadata['title'] = title_tag.string
            
            # Extract meta tags
            for meta in soup.find_all('meta'):
                name = meta.get('name')
                property_name = meta.get('property')
                content = meta.get('content')
                
                if name and content:
                    metadata[name] = content
                elif property_name and content:
                    metadata[property_name] = content
            
            self.logger.debug(f"Extracted {len(metadata)} metadata items")
            return metadata
        
        return _extract_metadata()
    
    def parse(self, html: str) -> Dict[str, Any]:
        """
        Parse HTML content and extract data.
        
        Args:
            html: HTML content
            
        Returns:
            Dict[str, Any]: Parsed data
        """
        @self.logger.log_operation("parse")
        def _parse():
            self.logger.debug("Parsing HTML content")
            # Extract basic data
            text = self.extract_text(html)
            images = self.extract_images(html, self.base_url)
            metadata = self.extract_metadata(html)
            
            # Process text with NER, keywords, and deduplication
            processed_text = self.text_processor.process_text(text)
            
            # If text is a duplicate, return None
            if processed_text is None:
                self.logger.info("Skipping duplicate content")
                return None
            
            self.logger.debug("HTML parsing completed")
            return {
                "text": text,
                "images": images,
                "metadata": metadata,
                "html": html,
                "entities": processed_text["entities"],
                "keywords": processed_text["keywords"],
                "content_hash": processed_text["content_hash"]
            }
        
        return _parse()
    
    def crawl(self) -> Dict[str, Any]:
        """
        Crawl the website and yield pages as they are crawled.
        
        Yields:
            Dict[str, Any]: Dictionary containing the crawled page data
        """
        @self.logger.log_operation("crawl")
        def _crawl():
            self.logger.info(f"Starting crawl of {self.base_url}")
            to_visit = [self.base_url]
            s3_storage = S3Storage()
            pages_processed = 0
            crawled_data = []
            
            # Create a generator for text processing data
            def text_data_generator():
                nonlocal pages_processed
                while to_visit and len(self.visited_urls) < self.page_limit:
                    url = to_visit.pop(0)
                    
                    if url in self.visited_urls:
                        continue
                    
                    self.logger.info(f"Crawling {url}")
                    
                    html = self.get_page(url)
                    if not html:
                        self.logger.warning(f"Failed to fetch {url}")
                        continue
                    
                    # Mark URL as visited
                    self.visited_urls.add(url)
                    
                    # Parse HTML
                    page_data = self.parse(html)
                    
                    # Skip if page is a duplicate
                    if page_data is None:
                        continue
                    
                    # Add URL and hash
                    page_data["url"] = url
                    page_data["hash"] = self._get_url_hash(url)
                    
                    # Add metadata
                    page_data = self.get_metadata(page_data)
                    
                    # Extract text processing data
                    text_data = {
                        "url": url,
                        "entities": page_data["entities"],
                        "keywords": page_data["keywords"],
                        "content_hash": page_data["content_hash"],
                        "timestamp": page_data["timestamp"]
                    }
                    
                    # Log upload start
                    self.logger.info(f"Starting upload for {url}")
                    
                    # Yield text data for streaming
                    yield text_data
                    
                    # Log upload completion
                    self.logger.info(f"Completed upload for {url}")
                    
                    # Remove text processing data from main page data
                    del page_data["entities"]
                    del page_data["keywords"]
                    del page_data["content_hash"]
                    
                    # Add to crawled data
                    crawled_data.append(page_data)
                    
                    # Update progress
                    pages_processed += 1
                    self.logger.info(f"Progress: {pages_processed} pages processed and uploaded")
                    
                    # Extract links for further crawling
                    links = self.extract_links(html, url)
                    for link in links:
                        if link not in self.visited_urls and link not in to_visit:
                            to_visit.append(link)
                    
                    # Force garbage collection
                    gc.collect()
            
            try:
                # Log upload start
                self.logger.info("Starting S3 upload process")
                
                # Stream text processing data to S3
                s3_storage.stream_processed_text_data(self.name, text_data_generator())
                
                # Log upload completion
                self.logger.info("Completed S3 upload process")
                self.logger.info(f"Crawl completed successfully. Total pages processed: {pages_processed}")
                
                # Post-crawling processing
                self._post_crawl_processing(s3_storage)
                
                return crawled_data
                
            except Exception as e:
                self.logger.error("Crawl failed", e)
                raise
            finally:
                # Save metrics
                self.logger.save_metrics()
                summary = self.logger.get_summary()
                self.logger.info(f"Crawl summary: {json.dumps(summary, indent=2)}")
        
        return _crawl()
    
    def _post_crawl_processing(self, s3_storage: S3Storage):
        """
        Perform post-crawling processing.
        
        Args:
            s3_storage: S3Storage instance
        """
        self.logger.info("Starting post-crawl processing")
        
        try:
            # Update metrics
            self.metrics["end_time"] = datetime.now().isoformat()
            self.metrics["total_pages"] = len(self.visited_urls)
            self.metrics["successful_crawls"] = len(self.visited_urls)
            
            # 1. Generate summary statistics
            self.logger.info("Generating summary statistics")
            summary = {
                "total_pages": self.metrics["total_pages"],
                "successful_crawls": self.metrics["successful_crawls"],
                "failed_crawls": self.metrics["failed_crawls"],
                "start_time": self.metrics["start_time"],
                "end_time": self.metrics["end_time"]
            }
            
            # 2. Store summary in S3
            self.logger.info("Uploading summary to S3")
            s3_storage.store_processed_data(
                self.name,
                summary,
                format='json',
                filename=f"{self.name}_summary.json"
            )
            
            # 3. Generate and store sitemap
            self.logger.info("Generating sitemap")
            sitemap = {
                "urls": list(self.visited_urls),
                "last_updated": datetime.now().isoformat()
            }
            
            self.logger.info("Uploading sitemap to S3")
            s3_storage.store_processed_data(
                self.name,
                sitemap,
                format='json',
                filename=f"{self.name}_sitemap.json"
            )
            
            self.logger.info("Post-crawl processing completed successfully")
            
        except Exception as e:
            self.logger.error("Post-crawl processing failed", e)
            raise 