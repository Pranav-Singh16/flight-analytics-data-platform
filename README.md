# Flight Analytics Data Platform - Airflow + Dataproc Serverless + PySpark + BigQuery

## Overview

This project implements an end-to-end cloud data engineering pipeline to process flight booking data using Google Cloud Platform services.

The pipeline uses:

- **Apache Airflow** for workflow orchestration
- **Google Cloud Storage (GCS)** for storing raw flight booking data
- **Dataproc Serverless** for running PySpark ETL workloads
- **BigQuery** for storing transformed and analytical datasets

The workflow monitors a CSV file arrival in GCS, triggers a PySpark job on Dataproc Serverless, performs data transformations and aggregations, and loads the results into BigQuery.

---

# Architecture

```text
                +----------------+
                | Google Cloud   |
                | Storage (GCS)  |
                +----------------+
                        |
                        |
              GCS File Sensor
                        |
                        v
                +---------------+
                | Apache        |
                | Airflow DAG   |
                +---------------+
                        |
                        |
              Submit PySpark Job
                        |
                        v
              +------------------+
              | Dataproc          |
              | Serverless Spark  |
              +------------------+
                        |
                        |
              Transform & Aggregate
                        |
                        v
                +---------------+
                | BigQuery       |
                | Data Warehouse |
                +---------------+
```

---

# Project Workflow

## 1. File Arrival Detection

Airflow uses `GCSObjectExistenceSensor` to check whether the input CSV file exists in Google Cloud Storage.

Example:

```text
gs://<bucket>/airflow-project/source-dev/flight_booking.csv
```

The sensor:

- Checks for file availability every 30 seconds
- Waits for a maximum of 5 minutes
- Allows the Spark job to run only after the file is detected

---

## 2. PySpark ETL Processing

After the file is detected, Airflow submits a PySpark batch job to Dataproc Serverless.

The Spark job:

1. Reads CSV data from GCS
2. Applies data transformations
3. Creates aggregated analytical datasets
4. Writes output tables into BigQuery

---

# Data Transformations

## Weekend Indicator

Creates a new column to identify weekend bookings.

Logic:

```text
Saturday -> 1
Sunday   -> 1
Other    -> 0
```

---

## Lead Time Category

Classifies bookings based on purchase lead time.

| Purchase Lead | Category |
|---|---|
| Less than 7 days | Last-Minute |
| 7 to 30 days | Short-Term |
| More than 30 days | Long-Term |

---

## Booking Success Rate

Creates a booking performance metric:

```text
booking_complete / num_passengers
```

---

# Analytical Datasets

The pipeline creates three BigQuery tables.

## 1. Transformed Flight Data

Contains:

- Original flight booking records
- Weekend indicator
- Lead time category
- Booking success rate

Example:

```text
flight_data_dev.transformed_table
```

---

## 2. Route Insights

Aggregated information based on flight routes.

Metrics:

- Total bookings
- Average flight duration
- Average length of stay

Example:

```text
flight_data_dev.route_insights_table
```

---

## 3. Origin Insights

Aggregated information based on booking origin.

Metrics:

- Total bookings
- Average booking success rate
- Average purchase lead time

Example:

```text
flight_data_dev.origin_insights_table
```

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Python | Programming language |
| PySpark | Distributed data processing |
| Apache Airflow | Workflow orchestration |
| Google Cloud Storage | Raw data storage |
| Dataproc Serverless | Spark execution environment |
| BigQuery | Analytical data warehouse |
| Google Cloud IAM | Authentication and permissions |

---

# Project Structure

```text
flight-analytics-data-platform/

│
├── airflow/
│   └── dags/
│       └── flight_analytics_pipeline.py
│
├── spark-job/
│   └── spark_transformation_job.py
│
└── README.md
```

---

# Airflow DAG Workflow

The DAG contains two main tasks.

## Task 1: Check File Arrival

Task:

```text
check_file_arrival
```

Purpose:

- Monitor GCS location
- Verify that the input CSV file exists
- Trigger Spark processing after successful detection

---

## Task 2: Run PySpark Job

Task:

```text
run_spark_job_on_dataproc_serverless
```

Purpose:

- Submit Spark batch job to Dataproc Serverless
- Pass runtime parameters
- Execute ETL processing
- Load data into BigQuery

---

# Configuration Management

The DAG uses Airflow Variables for configuration.

Example:

```text
env = dev

gcs_bucket = airflow-projects-gds

bq_project = my-project

bq_dataset = flight_data_dev
```

Table configuration:

```json
{
  "transformed_table": "flight_transformed",
  "route_insights_table": "route_insights",
  "origin_insights_table": "origin_insights"
}
```

---

# Pipeline Execution Flow

```text
Upload CSV File
        |
        v
Google Cloud Storage
        |
        v
Airflow GCS Sensor
        |
        v
Dataproc Serverless Spark
        |
        v
PySpark Transformations
        |
        v
BigQuery Tables
```

---

# Current Implementation

This project currently implements a batch ETL pipeline.

Processing flow:

```text
CSV File
    |
    v
Read from GCS
    |
    v
PySpark Transformations
    |
    v
BigQuery Tables
```

The pipeline currently uses:

```python
.mode("overwrite")
```

when writing data to BigQuery.

The source CSV file remains in GCS after processing.

---

# Future Improvements

Possible enhancements:

## Incremental Processing

- Process only newly arrived files
- Use append mode instead of overwrite
- Track processed files using metadata or watermarking

## Data Quality Checks

- Validate schema before processing
- Check missing values
- Detect duplicate records

## Production Improvements

- Replace hardcoded configurations with Airflow Variables
- Add monitoring and alerting
- Add CI/CD deployment workflow
- Add automated testing
- Add retry and failure notification mechanisms

---

# Key Learning Outcomes

This project demonstrates:

- Building a cloud-based ETL pipeline
- Orchestrating Spark workloads using Airflow
- Running PySpark jobs using Dataproc Serverless
- Loading analytical datasets into BigQuery
- Passing parameters between Airflow and Spark
- Using Google Cloud services for modern data engineering workflows