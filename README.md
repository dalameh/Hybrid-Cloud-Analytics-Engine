# Hybrid-Cloud-Analytics-Engine

![Databricks](https://img.shields.io/badge/Databricks-DLT-E64A19?style=flat-square&logo=databricks&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-S3%20%C2%B7%20SNS%20%C2%B7%20SQS-FF9900?style=flat-square&logo=amazonaws&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Medallion%20Architecture-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![PowerBI](https://img.shields.io/badge/Power%20BI-Executive%20Dashboard-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-CDC%20Agent-3776AB?style=flat-square&logo=python&logoColor=white)

> Enterprise-grade, event-driven ERP analytics pipeline — on-premise relational data continuously replicated to AWS, transformed through a Medallion architecture on Databricks, and served to a live Power BI executive dashboard.

---

## Table of Contents

- [Business Problem](#-business-problem)
- [Architecture](#-architecture)
- [Infrastructure — AWS CDK](#-infrastructure--aws-cdk)
- [Continuous CDC Replication](#-continuous-cdc-replication)
- [ETL Pipeline — Databricks DLT](#-etl-pipeline--databricks-dlt)
  - [Bronze — Raw Ingestion & Checkpointing](#bronze--raw-ingestion--checkpointing)
  - [Silver — Data Quality & CDC Upserts](#silver--data-quality--cdc-upserts)
  - [Gold — Star Schema & Query Optimisation](#gold--star-schema--query-optimisation)
- [Idempotency & Consistency Guarantees](#-idempotency--consistency-guarantees)
- [Orchestration — Databricks Workflows](#-orchestration--databricks-workflows)
- [Power BI Dashboard](#-power-bi-dashboard)
- [Repository Structure](#-repository-structure)
- [Tech Stack](#-tech-stack)

---

## 🎯 Business Problem

#### The Challenge: Breaking the Analytical Bottleneck

An enterprise e-commerce platform is currently constrained by an on-premise ERP system that serves as a single point of failure for both operations and intelligence. Running complex analytical queries against the production relational database creates resource contention, degrading OLTP (Online Transactional Processing) performance and risking operational downtime during peak business hours.

Furthermore, the lack of a scalable, cloud-native infrastructure prevents leadership from accessing real-time insights, as the current environment cannot handle the volume, variety, and velocity of modern retail data.

#### The Solution: An Event-Driven Analytical Ecosystem

This project engineers a high-availability, Hybrid-Cloud Analytics Engine designed to decouple operational workloads from analytical processing. By implementing a continuous Change Data Capture (CDC) strategy, we replicate ERP data to AWS without impacting source system performance.

---

## 🏗 Architecture

```
┌─────────────────────────┐
│   On-Prem ERP (Olist)   │  Python CDC agent · ar_h_commit + OP-coded events
│   Relational Database   │  Continuous incremental replication
└───────────┬─────────────┘
            │ 
            │  PUT Object (CSV · Hive-partitioned)
            ▼
┌─────────────────────────┐
│     AWS S3              │  CDK-provisioned landing zone
│     Landing Zone        │  Lifecycle: Infrequent Access (30d)
└───────────┬─────────────┘
            │
            │  s3:ObjectCreated event notification
            ▼
┌─────────────────────────┐
│     AWS SNS             │  erp-pipeline-events topic
│     Event Fan-out       │  Decouples ingestion from compute
└───────────┬─────────────┘
            │
            │  Publish to subscriber
            ▼
┌─────────────────────────┐
│     AWS SQS             │  erp-etl-queue · DLQ: erp-etl-dlq
│     Work Buffer         │  Auto Loader file notification source
└───────────┬─────────────┘
            │
            │  Databricks Auto Loader consumes SQS events
            ▼
┌──────────────────────────────────────────────────────────┐
│                Databricks Delta Live Tables              │
│                                                          │
│   [Bronze]          [Silver]              [Gold]         │
│   Auto Loader   →   DQ Expectations   →  Star Schema     │
│   Raw Delta         MERGE + OP codes     Z-ORDER + Parts │
│   Checkpointed      Schema enforced      Power BI ready  │
│                                                          │
│              DLT State · Auto Loader Checkpoint          │
│              Idempotent · Crash-safe · Schema-evolving   │
└──────────────────────────┬───────────────────────────────┘
                           │
                           │  Databricks Workflow · daily @ midnight
                           ▼
┌─────────────────────────┐
│     Power BI            │  REST API semantic model refresh
│     Executive Dashboard │  Auto-updated after every successful run
└─────────────────────────┘
```

---

## ☁️ Infrastructure — AWS CDK

All AWS infrastructure is provisioned as code using the **AWS CDK**, ensuring the environment is reproducible, version-controlled, and deployable across environments without manual configuration.

### S3 Landing Zone

The S3 bucket is the raw system of record. Incoming files are partitioned by dataset and date using Hive-style keys (`dataset/year=/month=/day=`), enabling efficient partition pruning downstream without full bucket scans. A CDK-managed **lifecycle policy** automatically transitions objects to S3 Infrequent Access after 30 days and expires them at 90 days — keeping storage costs bounded as data accumulates indefinitely. Versioning is enabled to protect against accidental overwrites during replication.

### SNS — Event Fan-out

Every `s3:ObjectCreated` event fires a notification to the SNS topic. SNS decouples the ingestion agent from all downstream compute — the CDC agent has no knowledge of Databricks, SQS, or anything downstream. New consumers (alerting, archival, compliance auditing) can subscribe to the topic without modifying a single line of ingestion code. A **subscription filter policy** scopes delivery to Olist dataset prefixes only, preventing internal S3 operations from triggering the pipeline.

### SQS — Durable Work Buffer

Rather than relying on traditional S3 directory listing—which is an $O(N)$ operation that becomes increasingly expensive and latent as your object count grows—this architecture utilizes Auto Loader with SQS File Notifications. By shifting to an event-driven discovery model, file detection occurs in $O(1)$ constant time. The moment a file lands in S3, a notification is pushed through SNS to SQS, allowing Databricks to pinpoint exactly which new files need processing without ever performing a full bucket scan.

This native AWS event-driven pattern scales to millions of objects without the performance degradation or cost spikes associated with polling. Messages accumulate safely in the queue while the cluster is scaling or mid-run, ensuring no data is lost during compute transitions. A dead-letter queue (DLQ) is integrated to capture and isolate "poison pill" messages that fail repeated processing, preventing malformed records from blocking the pipeline. Furthermore, a visibility timeout of 300 seconds provides the DLT job sufficient time to complete and commit the batch before any re-delivery occurs, maintaining strict processing integrity.

---

## 🔄 Full Load + Continuous CDC Replication (Simulated for the Project)

The on-premise ERP is simulated by a Python Boto3 agent performing **Change Data Capture** against the Olist relational dataset. Each change event carries two critical fields that mirror real CDC tooling (Debezium, AWS DMS, Oracle GoldenGate):

| Field | Purpose |
|:---|:---|
| `AR_H_COMMIT_TIMESTAMP` | Monotonically increasing commit timestamp establishing the exact order of changes across all tables — enabling consistent point-in-time reconstruction of the source database |
| `OP` | Operation code: `I` (insert) · `U` (update) · `D` (delete) — tells the Silver layer exactly which merge, upsert, or tombstone logic to apply |

The agent runs **continuously**, uploading incremental Parquet batches to S3 throughout the day. The DLT pipeline then processes these accumulated CDC events in a single nightly run — separating the concern of data availability (continuous replication) from analytical consistency (daily clean Gold layer for reporting).

---

## ⚙️ ETL Pipeline — Databricks DLT

The pipeline is implemented as a **Databricks Delta Live Tables** pipeline spanning three layers of the Medallion Architecture. DLT manages cluster lifecycle, inter-table dependency resolution, and automatic retry on partial failures — the pipeline code focuses purely on transformation logic.

---

### Bronze — Raw Ingestion & Checkpointing

`analytics/transformations/bronze.py`

The Bronze layer uses **Databricks Auto Loader** in file notification mode, consuming SQS events to discover new S3 objects. Data lands in Bronze as-is — raw, unmodified, with no type casting or business logic applied. The only additions are pipeline metadata columns (`_source_file`, `_ingestion_timestamp`) for end-to-end lineage tracking. Each Olist entity lands in its own Bronze **Delta table**: orders, order items, customers, products, payments, reviews, sellers, and geolocation.

**Auto Loader checkpointing** is the foundation of the pipeline's reliability. A checkpoint directory persists the offset of every file Auto Loader has processed. This means:

- If the cluster is terminated mid-run, the next execution **resumes from the last committed offset** — no files are skipped, none are re-processed
- SQS at-least-once delivery is handled transparently — duplicate notifications for the same S3 file are silently skipped by the checkpoint
- **Schema changes** in upstream Olist exports are tracked in the checkpoint's schema history and automatically merged into the Bronze table, enabling schema evolution without pipeline downtime or manual intervention

---

### Silver — Data Quality & CDC Upserts

`analytics/transformations/silver.py`

Silver reads from Bronze and serves as the pipeline's enforcement and reconciliation layer. Instead of relying on native declarative constraints, this engine utilizes a suite of custom Python validation functions to audit data across three distinct dimensions: Technical Data Quality (schema and null-integrity), Business Logic (domain-specific rules and reference integrity), and Analytic Readiness (metric-validity and distribution checks). This programmatic approach allows for complex, multi-column logic and cross-table validation that exceeds standard expectation syntax.

Beyond quality enforcement, Silver applies the **CDC OP codes** against the **AR_H_COMMIT_TIMESTAMP ordering** to maintain system state. Inserts and updates are merged into Silver tables using Delta's MERGE semantics, keyed on business identifiers such as order_id or customer_id. Deletes are handled as soft-deletes to preserve historical lineage. The result is a consistent, deduplicated Silver layer that accurately reflects the current state of the source ERP, regardless of duplicate SQS notifications or out-of-order event delivery. Comprehensive type casting, timestamp normalization, and critical entity joins—linking orders, customers, and products—are also finalized at this stage to prepare data for the Gold layer.

---

### Gold — Star Schema & Query Optimisation

`analytics/transformations/gold.py`

Gold is the analytics-serving layer. Tables are modelled as a **star schema** with clear separation of fact and dimension tables:

- **Fact tables** — `fact_orders`, `fact_order_items`, `fact_payments` — transactional measures at event grain (revenue, quantities, delivery times)
- **Dimension tables** — `dim_customers`, `dim_products`, `dim_sellers`, `dim_date` — descriptive attributes for Power BI slicing and filtering

Tables are **partitioned** on date columns (`order_month`, `order_year`) so Power BI queries filtering by time period scan only the relevant partitions rather than the full table. **Z-ORDER clustering** is applied on high-selectivity filter columns (`order_status`, `product_category`, `seller_state`), co-locating related values on the same Delta data files — enabling sub-second dashboard response times over large datasets. Pre-aggregated summary tables materialise the most expensive calculations once at pipeline time so the dashboard never recomputes them on the fly.

---

## 🔒 Framework Idempotency & Consistency Guarantees

| Guarantee | Mechanism |
|:---|:---|
| **Crash recovery** | Auto Loader checkpoint resumes from last committed offset on cluster restart |
| **Idempotent processing** | SQS at-least-once + checkpoint file tracking + Silver `MERGE` on business keys = no duplicates |
| **Schema evolution** | Auto Loader schema history auto-merges new upstream columns with no downtime |
| **Delta time travel** | Full transaction log on every table — point-in-time queries and safe rollback after bad runs |
| **Consistent reporting** | Power BI refresh fires only on fully successful pipeline run — partial runs never update the dashboard |

---

## 🕛 Orchestration — Databricks Lakeflow Jobs

The pipeline runs on a **daily cron schedule at midnight** via a Databricks Workflow. The job chains two tasks with strict dependency:

```
[Task 1]  Run DLT Pipeline (Bronze → Silver → Gold)
              ↓  on success only
[Task 2]  Trigger Power BI semantic model refresh
              ↓  on failure
[Alert]   Email / webhook notification
```

This separation is intentional: the CDC agent replicates continuously throughout the day, while the DLT pipeline processes all accumulated events in a single nightly batch — producing a fully reconciled, consistent Gold layer for leadership reporting. Retries up to 2×, with a 10 minute delay, are configured on Task 1 before the workflow alerts and stops.

---

## 📊 Power BI Dashboard

After each successful pipeline run, `orchestration/pbi_refresh_hook.py` calls the **Power BI REST API** using a Service Principal to trigger a full semantic model refresh. The dashboard connects to Gold Delta tables via the Databricks SQL connector and surfaces executive e-commerce metrics:

| Visual | Source Table | Metric |
|:---|:---|:---|
| Total revenue | `fact_orders` | Sum of order value |
| Orders by status | `fact_orders` | Volume by `order_status` |
| Revenue trend | `gold_order_summary` | Monthly revenue over time |
| Top sellers | `dim_sellers` + facts | Revenue per seller |
| Category breakdown | `dim_products` + facts | Revenue by category |
| Delivery SLA | `fact_orders` | Actual vs estimated delivery days |
| Review sentiment | `fact_reviews` | Score distribution by category |

---

## 📁 Repository Structure

```
Hybrid-Cloud-Analytics-Engine/
├── analytics/
│   └── transformations/
│       ├── 01_bronze.py             # Auto Loader · SQS notification · raw Delta tables
│       ├── 02_silver.py             # DLT Expectations · schema enforcement · CDC upserts
│       └── 03_gold.py               # Star schema · Z-ORDER · partition optimisation
├── infrastructure/
│   └── aws_configs/                 # IAM roles · SNS/SQS routing · CDK stack config
├── ingestion/
│   └── simulated_onprem_sync.py     # Python CDC agent · ar_h_commit · OP codes
├── orchestration/
│   └── pbi_refresh_hook.py          # Power BI REST API refresh trigger
└── README.md
```

---

## 🛠 Tech Stack

| Layer | Technology | Purpose |
|:---|:---|:---|
| Ingestion | Python · Boto3 | CDC agent simulating continuous on-prem ERP sync |
| Infrastructure | AWS CDK | Reproducible IaC for S3, SNS, SQS, IAM |
| Storage | AWS S3 | Hive-partitioned raw landing zone with lifecycle management |
| Events | AWS SNS | Fan-out decoupling between ingestion and compute |
| Queue | AWS SQS + DLQ | Durable work buffer with fault-tolerant dead-lettering |
| ETL | Databricks DLT · PySpark | Medallion architecture · Auto Loader · checkpointing |
| Orchestration | Databricks Workflows | Scheduled daily execution · task chaining · alerting |
| BI | Power BI | Executive dashboard · automated semantic model refresh |

---

*Designed for production reliability — idempotent incremental loads, crash-safe Auto Loader checkpointing, event-driven decoupling, and zero-touch nightly orchestration.*
