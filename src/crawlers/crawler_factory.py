from typing import Dict, Any

import config
from src.crawlers.base_crawler import BaseCrawler
from src.crawlers.static.static_crawler import StaticCrawler
from src.crawlers.dynamic.dynamic_crawler import DynamicCrawler


class CrawlerFactory:
    """Factory class to create appropriate crawler instances."""
    
    @staticmethod
    def create_crawler(website_config: Dict[str, Any]) -> BaseCrawler:
        """
        Create a crawler instance based on website configuration.
        
        Args:
            website_config: Dictionary containing website configuration
                - name: Website name
                - url: Base URL
                - dynamic: Whether the website uses JavaScript rendering
                
        Returns:
            BaseCrawler: A crawler instance
        """
        is_dynamic = website_config.get("dynamic", False)
        
        if is_dynamic:
            return DynamicCrawler(website_config)
        else:
            return StaticCrawler(website_config)
    
    @staticmethod
    def create_all_crawlers() -> Dict[str, BaseCrawler]:
        """
        Create crawler instances for all configured websites.
        
        Returns:
            Dict[str, BaseCrawler]: Dictionary of crawler instances keyed by website name
        """
        crawlers = {}
        
        for website_config in config.WEBSITES:
            name = website_config.get("name", "")
            crawlers[name] = CrawlerFactory.create_crawler(website_config)
        
        return crawlers 