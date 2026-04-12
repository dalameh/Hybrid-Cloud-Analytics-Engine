# Hybrid-Cloud-Analytics-Engine
Olist ETL PIPELINE using Medallion Architecture on Databricks and POWER BI for visualization. 
# Hybrid-Cloud ERP Analytics Engine

## Project Overview
An enterprise-grade, event-driven data pipeline that simulates the continuous synchronization of on-premise ERP data (Olist E-commerce dataset) to a cloud-native analytics environment. 

This engine implements a strict Medallion Architecture (Bronze, Silver, Gold) via Databricks Delta Live Tables (DLT). By utilizing AWS S3 event notifications, SNS fan-out, and SQS queues, the architecture guarantees at-least-once delivery, idempotent processing, and automated state management. The pipeline is fully orchestrated via Databricks Workflows, terminating in a live Power BI dashboard.

## Architecture Diagram

  ┌──────────────────┐
  │  Simulated ERP   │  ← Python Boto3 Agent
  │   (Olist Data)   │    Incremental CDC-style uploads
  └────────┬─────────┘
           │  PUT Object
           ▼
  ┌──────────────────┐
  │   S3 Landing     │  ← s3://hybrid-erp-landing-zone/
  │      Zone        │    Raw Partitioned Data
  └────────┬─────────┘
           │  S3 Event Notification (ObjectCreated)
           ▼
  ┌──────────────────┐
  │   AWS SNS Topic  │  ← erp-pipeline-events
  │  (Fan-out Hub)   │    Decouples ingestion from compute
  └────────┬─────────┘
           │  Publish
           ▼
  ┌──────────────────┐
  │   AWS SQS Queue  │  ← erp-etl-queue
  │  (Work Buffer)   │    Visibility timeout: 300s
  └────────┬─────────┘
           │  Auto Loader consumes events
           ▼
  ┌──────────────────────────────────────────────────────┐
  │             DATABRICKS ETL PIPELINE (DLT)            │
  │                                                      │
  │  [Bronze]    →    [Silver]    →    [Gold]            │
  │  Raw Delta        Cleansing        Aggregations      │
  │                                                      │
  │            ┌──────────────────────────┐              │
  │            │  DLT STATE / CHECKPOINT  │              │
  │            │  Tracks processed files  │              │
  │            │  Guarantees idempotency  │              │
  │            └──────────────────────────┘              │
  └────────────────────────┬─────────────────────────────┘
                           │  Scheduled Job Triggers API
                           ▼
  ┌──────────────────┐
  │   Power BI       │  ← Auto-refreshed semantic model
  │   Dashboard      │    Executive E-commerce Metrics
  └──────────────────┘

## Tech Stack
| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Ingestion** | Python, Boto3 | Simulates continuous on-prem ERP sync |
| **Storage / Events** | AWS S3, SNS, SQS | Event-driven architecture and state buffering |
| **ETL Framework** | Databricks DLT, PySpark | Medallion architecture, incremental processing |
| **Orchestration** | Databricks Workflows | Automated scheduling and compute lifecycle |
| **BI Layer** | Power BI | Cloud-based executive dashboard |

## Repository Structure

```text
Hybrid-Cloud-Analytics-Engine/
├── analytics/
│   └── transformations/
│       ├── 01_bronze.py             # Auto Loader & SQS queue consumption
│       ├── 02_silver.py             # DLT data quality & schema enforcement
│       └── 03_gold.py               # Dimensional modeling & Z-ORDER optimization
├── infrastructure/
│   └── aws_configs/                 # IAM roles, SNS/SQS routing JSONs
├── ingestion/
│   └── simulated_onprem_sync.py     # Python script pushing incremental Olist data
├── orchestration/
│   └── pbi_refresh_hook.py          # REST API trigger for Power BI
└── README.md
