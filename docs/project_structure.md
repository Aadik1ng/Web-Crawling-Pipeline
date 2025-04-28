# Project Structure

This document outlines the structure of the web crawler project.

## Directory Structure

```
web-crawler/
├── config.py                  # Configuration settings
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Docker configuration
├── run_crawler.sh             # Shell script to run crawler (Unix)
├── run_crawler.bat            # Batch script to run crawler (Windows)
├── README.md                  # Project documentation
├── .gitignore                 # Git ignore file
│
├── src/                       # Source code
│   ├── __init__.py            # Package initialization
│   ├── main.py                # Main entry point
│   │
│   ├── crawlers/              # Crawler modules
│   │   ├── __init__.py
│   │   ├── base_crawler.py    # Base crawler class
│   │   ├── crawler_factory.py # Factory for creating crawlers
│   │   │
│   │   ├── static/            # Static website crawlers
│   │   │   ├── __init__.py
│   │   │   └── static_crawler.py
│   │   │
│   │   └── dynamic/           # Dynamic website crawlers
│   │       ├── __init__.py
│   │       └── dynamic_crawler.py
│   │
│   ├── storage/               # Storage modules
│   │   ├── __init__.py
│   │   └── s3_storage.py      # AWS S3 storage
│   │
│   ├── news_api/              # News API integration
│   │   ├── __init__.py
│   │   └── news_api_client.py # News API client
│   │
│   ├── utils/                 # Utility functions
│   │   ├── __init__.py
│   │   └── logger.py          # Logging utilities
│   │
│   └── scheduler/             # Scheduling utilities
│       ├── __init__.py
│       └── airflow_dag.py     # Airflow DAG definition
│
├── tests/                     # Unit tests
│   ├── test_crawlers.py       # Tests for crawlers
│   ├── test_storage.py        # Tests for storage
│   └── test_news_api.py       # Tests for news API
│
├── docs/                      # Documentation
│   ├── add_new_website.md     # Guide to adding new websites
│   └── project_structure.md   # This file
│
├── logs/                      # Log files (generated)
└── results/                   # Crawl results (generated)
```

## Key Components

### Crawlers

The crawler system uses a modular design with the following components:

1. **BaseCrawler** (`src/crawlers/base_crawler.py`): Abstract base class that defines the common interface for all crawlers.

2. **StaticCrawler** (`src/crawlers/static/static_crawler.py`): Implementation for static websites that don't require JavaScript rendering.

3. **DynamicCrawler** (`src/crawlers/dynamic/dynamic_crawler.py`): Implementation for dynamic websites that require JavaScript rendering using Playwright.

4. **CrawlerFactory** (`src/crawlers/crawler_factory.py`): Factory class to create the appropriate crawler instance based on website configuration.

### Storage

The storage system handles saving crawled data to AWS S3:

1. **S3Storage** (`src/storage/s3_storage.py`): Handles storing and retrieving data from AWS S3 with partitioning by date and source.

### News API Integration

The news API integration provides access to logistics/supply chain news:

1. **NewsApiClient** (`src/news_api/news_api_client.py`): Client for accessing NewsAPI and RSS feeds.

### Utilities

Various utility modules:

1. **Logger** (`src/utils/logger.py`): Logging and alerting utilities.

### Scheduling

Scheduling components:

1. **Airflow DAG** (`src/scheduler/airflow_dag.py`): Airflow DAG for scheduling crawls.

## Main Execution Flow

The main execution flow is:

1. The user runs `python src/main.py` with appropriate arguments.

2. The main script creates the necessary crawlers using the factory.

3. Each crawler fetches and parses data from its assigned website.

4. The data is stored in AWS S3.

5. Logs and results are saved locally.

## Configuration

The system is configured through `config.py`, which contains:

1. AWS credentials and S3 bucket information
2. Website configurations
3. News API settings
4. Crawler settings (user agents, rate limits, etc.)
5. Logging configuration

## Testing

The `tests/` directory contains unit tests for each component:

1. `test_crawlers.py`: Tests for the crawler classes
2. `test_storage.py`: Tests for the S3 storage module
3. `test_news_api.py`: Tests for the news API client 