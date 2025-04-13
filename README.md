# YouTube Analytics Data Pipeline

This project builds a data pipeline to extract video/channel data from the YouTube Data API, store it in AWS S3, transform the data, and load it into AWS RDS — all orchestrated using Apache Airflow.

## Technologies
- YouTube Data API v3
- AWS S3, RDS
- Apache Airflow (Dockerized)
- Python, SQL
- Docker Compose

## Structure
- `dags/`: Airflow DAGs
- `scripts/`: Python scripts for data handling
- `docker/`: Docker setup
- `data/`: Temporary data storage
- `.env`: API keys and credentials (not shared)
