"""
Configuration settings for the web crawler.
"""

import os
import csv
from typing import Dict, List, Optional
from pathlib import Path

def load_aws_credentials(credentials_file: str = "rootkey.csv") -> Dict[str, str]:
    """Load AWS credentials from the rootkey.csv file."""
    credentials = {}
    if os.path.exists(credentials_file):
        with open(credentials_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Skip header
            for row in reader:
                if len(row) >= 2:
                    credentials["AWS_ACCESS_KEY_ID"] = row[0]
                    credentials["AWS_SECRET_ACCESS_KEY"] = row[1]
                    break
    return credentials

# Load AWS Credentials from rootkey.csv
aws_creds = load_aws_credentials()
AWS_ACCESS_KEY_ID = aws_creds.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = aws_creds.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = "ap-south-1"  # Mumbai region
S3_BUCKET_NAME = "crawllie"  # Existing bucket name

# S3 Storage Settings
S3_RAW_DATA_PREFIX = "raw"
S3_PROCESSED_DATA_PREFIX = "processed"
S3_TEXT_PROCESSED_PREFIX = "text_processed"

# Crawler Settings
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
]

REQUEST_DELAY = 1  # Delay between requests in seconds
MAX_RETRIES = 1    # Maximum number of retries for failed requests

# Logging Settings
LOG_LEVEL = "INFO"
LOG_DIR = "logs"

# Website Configurations
WEBSITES = [
    # Wikipedia - Static website
    {
        "name": "wikipedia",
        "url": "https://en.wikipedia.org/wiki/Main_Page",  # Main page URL
        "dynamic": False,  # Wikipedia is static
        "page_limit": 100  # You can adjust the number of pages you want to crawl
    },
    
    # OpenStreetMap - Static website
    {
        "name": "openstreetmap",
        "url": "https://www.openstreetmap.org",  # OpenStreetMap homepage URL
        "dynamic": False,  # Static content on the homepage
        "page_limit": 50  # Limit the number of pages to crawl (you can adjust this)
    },
    
    # Project Gutenberg - Free eBooks repository
    {
        "name": "project_gutenberg",
        "url": "https://www.gutenberg.org",  # Homepage of Project Gutenberg
        "dynamic": False,  # Project Gutenberg is static
        "page_limit": 50  # Adjust based on your crawl requirements
    },
    
    # FreeCodeCamp - Free coding tutorials
    {
        "name": "freecodecamp",
        "url": "https://www.freecodecamp.org",  # Homepage URL of FreeCodeCamp
        "dynamic": False,  # FreeCodeCamp is mostly static
        "page_limit": 50  # Adjust this number based on how many pages you need
    },
    
    # Mozilla Developer Network - Free developer resources
    {
        "name": "mozilla_mdns",
        "url": "https://developer.mozilla.org/en-US/",  # MDN homepage URL
        "dynamic": False,  # MDN is static
        "page_limit": 50  # Adjust this to limit how many pages you want to crawl
    }
]


# News API configuration
NEWS_API_KEY = ""  # To be filled with actual API key
NEWS_KEYWORDS = ["logistics", "supply chain", "freight", "shipping"]

# Logging configuration
ALERT_EMAIL = ""  # To be filled with actual email 