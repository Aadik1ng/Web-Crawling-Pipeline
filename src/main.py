"""
Main script to run the web crawler and news API integration.
"""
import os
import sys
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import src.utils.config as config
from src.crawlers.crawler_factory import CrawlerFactory
from src.news_api.news_api_client import NewsApiClient
from src.storage.s3_storage import S3Storage
from src.utils.logger import CrawlerLogger


def crawl_websites(website_names: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Crawl specified websites or all websites if none specified.
    
    Args:
        website_names: List of website names to crawl (optional)
        
    Returns:
        Dict[str, Any]: Crawl results
    """
    logger = CrawlerLogger("main_crawler")
    
    # If no website names specified, crawl all websites
    if not website_names:
        website_names = [website.get("name") for website in config.WEBSITES]
    
    # Initialize results dictionary
    results = {
        "start_time": datetime.now().isoformat(),
        "websites": {},
        "total_pages": 0,
        "successful_crawls": 0,
        "failed_crawls": 0
    }
    
    # Initialize S3 storage
    s3_storage = S3Storage()
    
    # Create crawler factory
    factory = CrawlerFactory()
    
    # Crawl each website
    for website_name in website_names:
        logger.info(f"Starting crawl for {website_name}")
        
        # Find website config
        website_config = None
        for config_item in config.WEBSITES:
            if config_item.get("name") == website_name:
                website_config = config_item
                break
        
        if not website_config:
            error_msg = f"Website configuration not found for {website_name}"
            logger.error(error_msg)
            results["websites"][website_name] = {
                "success": False,
                "error": error_msg
            }
            results["failed_crawls"] += 1
            continue
        
        try:
            # Create crawler
            crawler = factory.create_crawler(website_config)
            
            # Create a generator for streaming data
            def data_generator():
                for page in crawler.crawl():
                    yield page
            
            # Stream data to S3
            s3_key = s3_storage.stream_raw_data(website_name, data_generator())
            
            # Update results
            results["websites"][website_name] = {
                "success": True,
                "s3_key": s3_key
            }
            results["successful_crawls"] += 1
            
            logger.info(f"Successfully crawled {website_name}")
        
        except Exception as e:
            error_msg = f"Error crawling {website_name}: {str(e)}"
            logger.error(error_msg)
            
            results["websites"][website_name] = {
                "success": False,
                "error": str(e)
            }
            results["failed_crawls"] += 1
    
    # Update results with end time
    results["end_time"] = datetime.now().isoformat()
    
    return results


def fetch_news() -> Dict[str, Any]:
    """
    Fetch news from APIs and store in S3.
    
    Returns:
        Dict[str, Any]: News fetch results
    """
    logger = CrawlerLogger("main_news")
    logger.info("Starting news API fetch")
    
    # Initialize results dictionary
    results = {
        "start_time": datetime.now().isoformat(),
        "sources": []
    }
    
    try:
        # Create news client
        news_client = NewsApiClient()
        
        # Fetch logistics news
        data = news_client.fetch_logistics_news()
        
        # Initialize S3 storage
        s3_storage = S3Storage()
        
        # Store raw data in S3
        s3_key = s3_storage.store_raw_data("news_api", data)
        
        # Update results
        results["success"] = True
        results["s3_key"] = s3_key
        results["article_count"] = data.get("total_articles", 0)
        results["sources"] = data.get("sources", [])
        
        logger.info(f"Successfully fetched {results['article_count']} news articles")
    
    except Exception as e:
        error_msg = f"Error fetching news: {str(e)}"
        logger.error(error_msg)
        
        results["success"] = False
        results["error"] = str(e)
    
    # Update results with end time
    results["end_time"] = datetime.now().isoformat()
    
    return results


def save_results(results: Dict[str, Any], filename: str) -> None:
    """
    Save results to a JSON file.
    
    Args:
        results: Results to save
        filename: Filename to save to
    """
    # Create results directory if it doesn't exist
    os.makedirs("results", exist_ok=True)
    
    # Save results
    with open(f"results/{filename}", "w") as f:
        json.dump(results, f, indent=2)


def main():
    """Main function to run the crawler and news API integration."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Web crawler and news API integration")
    parser.add_argument("--websites", nargs="*", help="List of website names to crawl (optional)")
    parser.add_argument("--news", action="store_true", help="Fetch news from APIs")
    parser.add_argument("--all", action="store_true", help="Crawl all websites and fetch news")
    
    args = parser.parse_args()
    
    # Default to all if no options specified
    if not (args.websites or args.news or args.all):
        args.all = True
    
    # Initialize logger
    logger = CrawlerLogger("main")
    
    # Initialize results
    results = {
        "timestamp": datetime.now().isoformat(),
        "crawler_results": None,
        "news_results": None
    }
    
    # Crawl websites
    if args.websites or args.all:
        logger.info("Starting website crawling")
        results["crawler_results"] = crawl_websites(args.websites)
    
    # Fetch news
    if args.news or args.all:
        logger.info("Starting news API fetching")
        results["news_results"] = fetch_news()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    save_results(results, f"crawl_results_{timestamp}.json")
    
    # Print summary
    if results["crawler_results"]:
        print(f"Crawled {results['crawler_results']['successful_crawls']} websites successfully, "
              f"{results['crawler_results']['failed_crawls']} failed, "
              f"{results['crawler_results']['total_pages']} total pages")
    
    if results["news_results"] and results["news_results"].get("success"):
        print(f"Fetched {results['news_results']['article_count']} news articles from "
              f"{len(results['news_results']['sources'])} sources")


if __name__ == "__main__":
    main() 