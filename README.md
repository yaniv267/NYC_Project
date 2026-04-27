# NYC Smart City Data Lakehouse

## Project Overview
This project establishes a scalable, real-time data lakehouse for New York City municipal data. Using a Medallion Architecture, we process millions of traffic violations and crash reports to provide actionable urban insights through an interactive Telegram bot and comprehensive Kibana dashboards.

## Team Members
* **Yaniv Ovadia**
* **Eliran Shenhav**
* **Eyal Wechsler**

## Architecture & Data Pipeline
We implemented a three-tier **Medallion Architecture**:
* **Bronze**: Raw data ingestion from various NYC Open Data APIs via Kafka.
* **Silver**: Data cleansing, deduplication, and schema enforcement.
* **Gold**: Advanced analytics, geospatial enrichment (matching coordinates to 1M+ addresses), and risk scoring.

## Technical Highlights & Optimizations
* **Broadcast Hash Joins**: Optimized spatial enrichment by broadcasting the 1M-record address lookup table to executors, eliminating expensive network shuffles.
* **Efficient Aggregations**: Leveraged map-side combiners (ReduceByKey logic) to minimize data transfer during large-scale `groupBy` operations.
* **Observability**: Integrated Logstash for real-time monitoring of data quality and pipeline health within the Spark Silver layer.
* **Dual Serving Layer**: Synchronized processed data to **PostgreSQL** for low-latency bot queries and **Elasticsearch** for high-performance visual analytics in Kibana.

## Tech Stack
* **Processing**: Apache Spark (PySpark)
* **Ingestion**: Apache Kafka
* **Orchestration**: Apache Airflow
* **Storage**: MinIO (S3-compatible Lakehouse)
* **Serving**: PostgreSQL & Elasticsearch
* **Visualization**: Kibana & Telegram Bot API
* **Infrastructure**: Docker & Docker Compose
