from typing import Dict, Any, List, Optional
import time
from playwright.sync_api import sync_playwright, Page
import urllib.parse
from datetime import datetime

from src.crawlers.static.static_crawler import StaticCrawler


class DynamicCrawler(StaticCrawler):
    """Crawler for dynamic websites that require JavaScript rendering."""
    
    def __init__(self, website_config: Dict[str, Any]):
        """
        Initialize dynamic crawler with website configuration.
        
        Args:
            website_config: Dictionary containing website configuration
        """
        super().__init__(website_config)
        self.wait_time = website_config.get("wait_time", 5)  # Time to wait for JS to render
        # Initialize metrics if not already initialized by parent
        if not hasattr(self, 'metrics'):
            self.metrics = {
                "start_time": datetime.now().isoformat(),
                "end_time": None,
                "total_pages": 0,
                "successful_crawls": 0,
                "failed_crawls": 0
            }
    
    def get_page(self, url: str) -> Optional[str]:
        """
        Get the HTML content of a page using Playwright for JavaScript rendering.
        
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
                with sync_playwright() as playwright:
                    # Launch browser
                    browser = playwright.chromium.launch(headless=True)
                    context = browser.new_context(
                        user_agent=self.session.headers.get("User-Agent")
                    )
                    page = context.new_page()
                    
                    # Navigate to the page
                    page.goto(url, wait_until="domcontentloaded")
                    
                    # Wait for JavaScript to render
                    time.sleep(self.wait_time)
                    
                    # Get content
                    html = page.content()
                    
                    # Close browser
                    context.close()
                    browser.close()
                    
                    # Add delay after successful request
                    time.sleep(self.delay)
                    
                    return html
            except Exception as e:
                print(f"Attempt {attempt + 1}/{self.max_retries} failed for {url}: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    # Exponential backoff
                    time.sleep(self.delay * (attempt + 1))
        
        return None
    
    def scroll_page(self, page: Page) -> None:
        """
        Scroll the page to load lazy-loaded content.
        
        Args:
            page: Playwright page object
        """
        # Get the height of the page
        height = page.evaluate("document.body.scrollHeight")
        
        # Scroll in steps
        for i in range(0, height, 300):
            page.evaluate(f"window.scrollTo(0, {i})")
            time.sleep(0.5)
        
        # Scroll back to top
        page.evaluate("window.scrollTo(0, 0)")
    
    def handle_cookies_popup(self, page: Page) -> None:
        """
        Attempt to handle common cookie consent popups.
        
        Args:
            page: Playwright page object
        """
        # Try to find and click common cookie acceptance buttons
        selectors = [
            "button[id*='accept']", 
            "button[id*='cookie']",
            "button[class*='accept']",
            "button[class*='cookie']",
            "a[id*='accept']",
            "a[class*='accept']",
            "div[id*='accept']",
            "div[class*='accept']",
            "[aria-label*='accept cookies']",
            "[aria-label*='accept all']"
        ]
        
        for selector in selectors:
            try:
                if page.is_visible(selector):
                    page.click(selector)
                    time.sleep(0.5)
                    break
            except Exception:
                continue 