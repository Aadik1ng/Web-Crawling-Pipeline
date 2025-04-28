# Adding a New Website to the Crawler

This guide explains how to add a new website to the crawler system.

## Method 1: Using the Built-in Crawlers

For most websites, you can simply add a new configuration to the `WEBSITES` list in `config.py`:

```python
WEBSITES = [
    # Existing websites...
    
    # Add your new website here
    {
        "name": "new_website_name",  # Unique identifier for the website
        "url": "https://www.newwebsite.com",  # Base URL of the website
        "dynamic": False,  # Set to True if the site requires JavaScript rendering
        "page_limit": 100  # Optional: Maximum number of pages to crawl
    }
]
```

This method works well for standard websites where our existing crawlers can extract data effectively.

## Method 2: Creating a Custom Crawler

For websites that require special handling, you can create a custom crawler class:

1. Decide whether the website is static or dynamic:
   - If the website doesn't heavily rely on JavaScript, create a new class in `src/crawlers/static/`
   - If the website requires JavaScript rendering, create a new class in `src/crawlers/dynamic/`

2. Create a new Python file for your crawler (e.g., `src/crawlers/static/new_website_crawler.py`):

```python
from typing import Dict, Any
from src.crawlers.static.static_crawler import StaticCrawler  # Or DynamicCrawler for JS sites


class NewWebsiteCrawler(StaticCrawler):
    """Custom crawler for the New Website."""
    
    def __init__(self, website_config: Dict[str, Any]):
        """Initialize with website configuration."""
        super().__init__(website_config)
    
    def parse(self, html: str) -> Dict[str, Any]:
        """
        Parse HTML content with custom logic.
        
        Args:
            html: HTML content
            
        Returns:
            Dict[str, Any]: Parsed data
        """
        # Custom parsing logic here
        data = super().parse(html)  # Use parent class parsing as a starting point
        
        # Add custom parsing logic for this specific website
        # For example, extract specific structured data
        
        return data
```

3. Register your custom crawler in `src/crawlers/crawler_factory.py`:

```python
from src.crawlers.base_crawler import BaseCrawler
from src.crawlers.static.static_crawler import StaticCrawler
from src.crawlers.dynamic.dynamic_crawler import DynamicCrawler
from src.crawlers.static.new_website_crawler import NewWebsiteCrawler  # Import your crawler


class CrawlerFactory:
    """Factory class to create appropriate crawler instances."""
    
    @staticmethod
    def create_crawler(website_config: Dict[str, Any]) -> BaseCrawler:
        """Create a crawler instance based on website configuration."""
        name = website_config.get("name", "")
        is_dynamic = website_config.get("dynamic", False)
        
        # Handle special cases
        if name == "new_website_name":
            return NewWebsiteCrawler(website_config)
        
        # Default case
        if is_dynamic:
            return DynamicCrawler(website_config)
        else:
            return StaticCrawler(website_config)
```

4. Add your website configuration to `config.py`:

```python
WEBSITES = [
    # Existing websites...
    
    # Add your new website with custom crawler
    {
        "name": "new_website_name",  # Must match the name used in CrawlerFactory
        "url": "https://www.newwebsite.com",
        "dynamic": False,  # Still needed for configuration
        "custom_field1": "value1",  # You can add custom fields for your crawler
        "custom_field2": "value2"
    }
]
```

## Testing Your New Crawler

1. Run the crawler for your specific website:

```bash
python src/main.py --websites new_website_name
```

2. Check the logs and results:
   - Look in the `logs/` directory for detailed logs
   - Check the `results/` directory for the crawling output

3. Write unit tests:
   - Create a test file in `tests/` to verify your crawler works correctly

## Best Practices

1. **Respect robots.txt**: Ensure your crawler respects the website's robots.txt file.

2. **Rate Limiting**: Implement proper rate limiting to avoid overloading the website.

3. **Error Handling**: Add robust error handling for website-specific issues.

4. **Documentation**: Document any special behaviors or requirements of your crawler.

5. **Maintainability**: Keep your custom crawler focused on the specific differences from the base crawler. 