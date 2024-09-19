## Table of Contents
- [Production System Architecture](#production-system-architecture)
- [Architecture Components](#architecture-components)
- [Architecture Diagram](#architecture-diagram)
- [Evolution for Near Real Time Updates](#evolution-for-near-real-time-updates)
- [Key Architectural Decisions](#key-architectural-decisions)
- [Conclusion](#Conclusion)

## Production System Architecture
  Overview
  The proposed architecture will support regular ETL jobs to process data from AWS Aurora MySQL databases and expose the resulting datasets to analysts and the finance team. 
  The architecture will also be designed to evolve for near real-time data updates once CDC is enabled and integrated with Kafka.

## Architecture Components

  Data Ingestion Layer
  AWS Aurora MySQL: The source of raw data.
  AWS Data Pipeline: Scheduled to extract data from Aurora MySQL and load it into an S3 bucket.
  
  Data Storage Layer
  Amazon S3: Stores raw data extracted from Aurora MySQL and intermediate data in Parquet format.
  Delta Lake on Databricks: Stores processed data, providing ACID transactions and versioning for time travel queries.
  
  Data Processing Layer
  Databricks: Runs ETL jobs using Apache Spark to process data from S3 and load it into Delta Lake.
  
  Data Serving Layer
  Databricks SQL Analytics: Allows analysts and the finance team to query the processed data using SQL.
  Power BI/Tableau: For data visualization and reporting.
  
  Orchestration and Scheduling
  Apache Airflow: Manages and schedules ETL workflows.

## Architecture Diagram
![As-Is Current Architecture](https://github.com/sonalikapatel84/interview-carsales/blob/main/assets/Current-As-Is-state.png)

## Evolution for Near Real Time Updates

  CDC Integration
  Kafka: Once CDC is enabled, CDC events will be streamed to Kafka.
  Kafka Connect: Captures CDC events from Kafka and writes them to S3 in near real-time.
  ![Future State with Kafka](https://github.com/sonalikapatel84/interview-carsales/blob/main/assets/target%20state.png)
  ![Another Real Time Solution](https://github.com/sonalikapatel84/interview-carsales/blob/main/assets/For-future-Kafka-integration.jpg)
  
  Real-Time Data Processing
  Databricks Structured Streaming: Consumes CDC events from S3 and updates Delta Lake tables in near real-time.

## Key Architectural Decisions

  Data Storage
  Amazon S3: Chosen for its scalability and cost-effectiveness for storing raw and intermediate data.
  Delta Lake: Provides ACID transactions, scalable metadata handling, and time travel capabilities, which are essential for versioning and historical data analysis.
  
  Data Processing
  Databricks: Selected for its powerful Spark-based processing capabilities, ease of integration with S3 and Delta Lake, and support for both batch and streaming data processing.
  
  Orchestration
  Apache Airflow: Chosen for its flexibility and robustness in managing complex ETL workflows and scheduling.

  Query and Analysis
  Databricks SQL Analytics: Provides a SQL interface for analysts and the finance team, ensuring they can query data using familiar SQL syntax.
  Power BI/Tableau: For advanced data visualization and reporting, integrating seamlessly with Databricks.

## Conclusion
  This architecture ensures that the ETL jobs run efficiently and the resulting datasets are readily available for analysis. It is designed to evolve seamlessly to support near real-time data updates, ensuring that analysts and the finance team have     access to the most up-to-date data.
