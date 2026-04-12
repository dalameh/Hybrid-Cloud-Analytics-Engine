# Hybrid-Cloud-Analytics-Engine
Olist ETL PIPELINE using Medallion Architecture on Databricks and POWER BI for visualization. 

# Hybrid-Cloud ERP Analytics Engine

## Project Overview
An enterprise-grade, event-driven data pipeline simulating the continuous, incremental synchronization of on-premise ERP data to a cloud-native analytics environment. Built using AWS and Databricks, this engine implements a strict Medallion Architecture (Bronze, Silver, Gold) via Delta Live Tables (DLT) to serve clean, aggregated metrics to a Power BI dashboard. 

This project demonstrates production-ready infrastructure handling event-driven ingestion, stateful streaming, and automated job orchestration.

## System Architecture

The pipeline follows a decoupled, publish-subscribe model to ensure fault tolerance and scalability:

1. **Synthetic DMS (Data Migration Service):** A Python-based agent simulating an on-premise ERP system continuously writing incremental updates (CDC-style) to the cloud.
2. **AWS Landing Zone (S3):** Raw data is incrementally uploaded to an S3 Bronze bucket.
3. **Event Router (SNS & SQS):** S3 `ObjectCreated` events trigger an SNS topic, which fans out to an SQS queue, decoupling ingestion from computation.
4. **Processing Engine (Databricks DLT):** Databricks Auto Loader consumes the SQS queue, sequentially processing files using Delta Live Tables with built-in checkpointing.
5. **Orchestration & Serving (Databricks Workflows & Power BI):** A scheduled Databricks Job runs the ETL pipeline and triggers a downstream Power BI dataset refresh via REST API.

## Core Technologies
* **Cloud Infrastructure:** AWS S3, SNS, SQS, IAM
* **Data Processing:** Databricks, Apache Spark, Delta Lake, Delta Live Tables (DLT)
* **Ingestion:** Python (Boto3), Databricks Auto Loader (`cloudFiles`)
* **Orchestration:** Databricks Workflows (Jobs)
* **BI & Analytics:** Power BI Service

---

## Pipeline Execution Flow

### 1. Ingestion: Synthetic DMS & Event Queue
* **Incremental Uploads:** The `scripts/synthetic_dms.py` script incrementally pushes new data partitions to `s3://landing-zone-bucket/`. This is a continuous state sync, not a one-time migration.
* **Event Notification:** AWS S3 sends an event configuration to **Amazon SNS**, which publishes the payload to an **Amazon SQS** queue. This prevents Databricks from having to actively list directories (an expensive operation at scale), allowing it to only process exact files dropped into S3.

### 2. Processing: Medallion Architecture & DLT Checkpointing
The data is processed using Delta Live Tables (DLT) to guarantee ACID compliance and maintain stateful execution.

* **Bronze Layer (Raw Ingestion):** * Uses Databricks Auto Loader (`cloudFiles`) configured to listen to the SQS queue.
  * **Checkpointing:** State is maintained in a designated S3 checkpoint directory (`_checkpoints/bronze/`). If the cluster terminates, Auto Loader reads the checkpoint upon restart to resume exactly where it left off, ensuring exactly-once processing.
  * Schema inference and evolution are handled dynamically.
* **Silver Layer (Cleansing & Conforming):**
  * Reads incrementally from the Bronze Delta table as a stream.
  * Applies data quality rules: deduplication, timestamp casting, handling null values, and enforcing schema constraints.
* **Gold Layer (Business Aggregations):**
  * Reads from Silver to create highly refined, query-optimized dimensional models and fact tables.
  * Optimized using `Z-ORDER` clustering for fast downstream BI query performance.

### 3. Orchestration: Automated Job Scheduling
The entire architecture is automated using **Databricks Workflows**.
* **ETL Task:** A scheduled job spins up an automated job cluster, executes the DLT pipeline, and tears down the infrastructure to optimize cloud compute costs.
* **BI Refresh Task:** Upon successful completion of the ETL task, a secondary Python task uses the Power BI REST API to trigger a refresh of the Power BI semantic model, ensuring the dashboard reflects the latest increments.

---

## Repository Structure

```text
├── analytics/
│   └── transformations/
│       ├── 01_bronze_ingestion.py   # Auto Loader & SQS queue consumption
│       ├── 02_silver_cleansing.py   # DLT data quality rules
│       └── 03_gold_aggregations.py  # Business logic & dimensional modeling
├── infrastructure/
│   └── aws_configs/
│       ├── s3_event_sns.json        # SNS/SQS event routing configurations
│       └── iam_roles.json           # Least-privilege execution roles
├── ingestion/
│   └── synthetic_dms.py             # Incremental data upload simulation
├── orchestration/
│   └── power_bi_refresh.py          # API trigger for BI dataset update
└── README.md
