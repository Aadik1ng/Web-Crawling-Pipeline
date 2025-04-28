# Web Crawler and News API Integration

A comprehensive web crawling system that crawls logistics/supply chain websites, integrates with news APIs, and stores data in AWS S3.

## Features

- Crawls multiple websites for HTML, text, media, and metadata
- Handles dynamic content (JavaScript-rendered pages) using Playwright
- Respects robots.txt, rate limits, and implements anti-scraping measures
- Integrates with news APIs and RSS feeds
- Stores data in AWS S3 with efficient partitioning
- Implements error logging and monitoring
- Supports scheduling via Apache Airflow

## Requirements

- Python 3.8+
- AWS account with S3 access
- See `requirements.txt` for Python dependencies

## Installation

### Option 1: Local Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd web-crawler
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Install Playwright browsers:
   ```
   python -m playwright install
   ```

### Option 2: Docker Installation

1. Build the Docker image:
   ```
   docker build -t web-crawler .
   ```

## Configuration

1. Set AWS credentials in `config.py` or in environment variables

2. Configure websites to crawl in `config.py`

3. Add NewsAPI key (optional) in `config.py`

## Usage

### Running the Crawler Locally

Run all crawlers and news API:
```
python src/main.py --all
```

Run specific website crawlers:
```
python src/main.py --websites gnosisfreight logisticsoflogistics
```

Run only news API:
```
python src/main.py --news
```

### Running with Docker

Run all crawlers and news API:
```
docker run web-crawler
```

Run specific website crawlers:
```
docker run web-crawler --websites gnosisfreight logisticsoflogistics
```

## AWS S3 Storage Structure

Data is stored in S3 with the following structure:

- Raw data: `raw/{source}/{YYYY}/{MM}/{DD}/{filename}`
- Processed data: `processed/{source}/{YYYY}/{MM}/{DD}/{filename}`

## Adding New Websites

To add a new website to crawl:

1. Add the website configuration to `WEBSITES` in `config.py`
2. If the website requires custom parsing logic, create a new crawler class in `src/crawlers/`

## Scheduling with Airflow

1. Copy the DAG file to your Airflow DAGs folder:
   ```
   cp src/scheduler/airflow_dag.py /path/to/airflow/dags/
   ```

2. Ensure Airflow has access to the project code and dependencies

3. The DAG will run daily by default, crawling all configured websites

## Monitoring and Logs

- Logs are stored in the `logs/` directory
- Alerts can be configured for critical errors

## License

[Specify your license]

## Contributors

[List contributors] 