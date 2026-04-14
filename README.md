# Hybrid-Cloud-Analytics-Engine

![Python](https://img.shields.io/badge/Python-CDC%20Agent%20Script-3776AB?style=flat-square&logo=python&logoColor=white)


![AWS](https://img.shields.io/badge/AWS-S3%20%C2%B7%20SNS%20%C2%B7%20SQS-FF9900?style=flat-square&logo=amazonaws&logoColor=white)


![Databricks](https://img.shields.io/badge/Databricks-DLT%20%C2%B7%20DeltaTable-E64A19?style=flat-square&logo=databricks&logoColor=white)


![PySpark](https://img.shields.io/badge/PySpark-Medallion%20Architecture-E25A1C?style=flat-square&logo=apachespark&logoColor=white)


![PowerBI](https://img.shields.io/badge/Power%20BI-Executive%20Dashboard-F2C811?style=flat-square&logo=powerbi&logoColor=black)

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

## 💼 The Business:

Olist is a Brazilian e-commerce ecosystem that acts as a strategic integrator, connecting thousands of small businesses and local merchants to the country’s largest online marketplaces. By providing a unified logistics and listing platform, it allows sellers to scale their reach while Olist manages the heavy lifting of product distribution and customer service.

## 🎯 Business Problem

#### The Challenge: Breaking the Analytical Bottleneck

The enterprise is currently tethered to an on-premise ERP monolith that was designed for transactional stability, not modern analytical scale. This legacy architecture forces business intelligence queries to compete with real-time operations for the same hardware resources, creating a performance bottleneck that degrades OLTP performance, risking system instability and downtime during peak high-volume retail periods.

To unlock the next phase of growth, leadership is pivoting to a Hybrid-Cloud integration strategy to migrate and synchronize transactional data into a unified cloud data warehouse. By decoupling analytical workloads from the on-premise core, the enterprise aims to:

> Eliminate Resource Contention (Offload Compute): Move heavy compute-intensive queries to the cloud to ensure the local ERP remains fast and responsive for frontline operations.

> Establish a Unified Source of Truth: Centralize fragmented data from multiple local instances into a single, scalable Medallion Architecture on the lakehouse, governed by Unity Catalog

> Executive Visibility: Replace delayed reports with real-time Executive Dashboards to track cross-regional KPIs instantly.

> Enable Advanced Analytics: Provide the infrastructure necessary for high-velocity predictive modeling and real-time reporting that the legacy hardware simply cannot support.

#### The Solution: An Event-Driven Analytical Ecosystem

> This architecture bridges an on-premise ERP monolith to a Databricks Lakehouse using a continuous Change Data Capture (CDC) pipeline. By leveraging AWS DMS (simulated), transactional state changes are replicated into the cloud in real-time, completely offloading analytical overhead from the production OLTP engine.

> The data flows through a governed Medallion architecture within Unity Catalog, where raw relational changes are ultimatley refined into a high-performance Gold-standard star schema. While AWS DMS replicates data continuously, a scheduled Databricks Workflows job runs daily at EOD to process the accumulated changes and push them through the pipeline. To close the loop between engineering and action, this job triggers the Power BI REST API upon completion, ensuring leadership dashboards reflect the latest business state without manual intervention.
---

## 🏗 Architecture

```
┌─────────────────────────┐
│   On-Prem ERP (Olist)   │  Python CDC agent · `AR_H_COMMIT_TIMESTAMP` + OP-coded events
│   Relational Database   │  Continuous incremental replication
└───────────┬─────────────┘
            │ 
            │  PUT Object (Parquet · Hive-partitioned Y/M/D)
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
│   Raw Delta         MERGE + OP Flags    Liquid Clustering│
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

## ☁️ Infrastructure — AWS CDK (IaC)

All AWS infrastructure is provisioned as code using the **AWS CDK**, ensuring the environment is reproducible, version-controlled, and deployable across environments without manual configuration.

### S3 Landing Zone

> The S3 bucket `olist-ecommerce-landing-zone-useast1` is the raw system of record, providing the landing zone for the full load and all on-going replications. 

> Incoming files are partitioned by dataset and date using Hive-style keys (`dataset/year=/month=/day=`), enabling efficient partition pruning downstream without full bucket scans.

> The **lifecycle policy** automatically transitions objects to S3 Infrequent Access after 30 days, Glacier Flexible after 90,  and expires them after 365 days — keeping storage costs bounded as data accumulates indefinitely. 

> Versioning is enabled to protect against accidental overwrites during replication and Amazon S3 managed encryption (SSE-S3) is automatically applied to gaurantee server-side encryption for all objects in S3, using AES-256 encryption to protect data at rest

### S3 Data Lakehouse

> The bucket `olist-ecommerce-prod-useast1-lakehouse` serves as the **Unity Catalog Managed Location**, providing the high-performance storage backbone for the **Bronze, Silver, and Gold** Delta tables. 

> Unlike the raw Landing Zone, data is stored in optimized **Delta format** in a dedicated **_unitystorage/** directory, enabling ACID transactions, time travel, and schema enforcement across the entire pipeline.

> By decoupling compute from storage, this layer ensures that the **Hybrid-Cloud ERP** data remains persistent and accessible to both Databricks SQL and downstream ML workloads without data duplication.

> Centralized governance is enforced through **Managed Storage**, ensuring all physical data access is audited and restricted to the Unity Catalog service principal.

> Amazon S3 managed encryption (SSE-S3) is automatically applied to gaurantee server-side encryption for all objects in S3, using AES-256 encryption to protect data at rest.

### SNS — Event Fan-out

> Every `s3:ObjectCreated` event fires a notification to the `olist-landing-topic` SNS topic. SNS decouples the ingestion agent from all downstream compute — the CDC agent has no knowledge of Databricks, SQS, or anything downstream. 

> Gives opportunity for new consumers (alerting, archival, compliance auditing) to subscribe to the topic without modifying a single line of ingestion code. 


### SQS — Durable Work Buffer

> The `olist-landing-queue` acts as the event-driven backbone of the ingestion layer, subscribing to the olist-landing-topic via SNS. 

> By utilizing Auto Loader with SQS File Notifications, the architecture bypasses the latency of traditional S3 directory listings—an $O(N)$ operation that becomes prohibitively expensive as object counts scale.Instead, file discovery occurs in $O(1)$ constant time. The moment a file lands in S3, a notification is pushed through the SNS-SQS chain, allowing Databricks to pinpoint and ingest new data without performing a full bucket scan.

> This event-driven pattern ensures that even as the ERP history scales to millions of objects, discovery remains instantaneous and cost-effective.To ensure pipeline resilience, a Dead-Letter Queue (DLQ) is integrated (with a maxReceiveCount of 3) to isolate "poison pill" files that fail processing, preventing malformed data from stalling the entire batch. 

> Furthermore, a 300-second visibility timeout ensures the Databricks job has sufficient overhead to commit the batch before any message re-delivery occurs, maintaining strict processing integrity during compute transitions.

---

## 🔄 Full Load + Continuous CDC Replication (Simulated for the Project)

> The on-premise ERP Full Load + **Change Data Capture** is simulated by a **Python Boto3 agent** that replicates the behavior the behavior of DMS's built in replication engine. 

  > State-Aware Replication: The agent promotes source transaction metadata into physical columns (AR_H_COMMIT_TIMESTAMP and OP), ensuring the downstream Medallion pipeline can reconstruct the database state with perfect chronological fidelity.
  | Field | Purpose |
  |:---|:---|
  | `AR_H_COMMIT_TIMESTAMP` | A physical metadata column captured from the source DB's transaction log. It provides a monotonically increasing sequence that allows the pipeline to resolve the "Last Write Wins" logic when multiple updates occur for the same record within a single batch. |
  | `OP` | The operation flags `I` (insert) · `U` (update) · `D` (delete) emitted by the DMS CDC engine — tells the Silver layer exactly which merge, upsert, or delete logic to apply |

  > Engine Logic Simulation: To mirror a production DMS instance, the agent utilizes internal buffering governed by:

  > BatchApplyMemoryLimit: Logic that flushes data to S3 once a specific record volume is reached to optimize storage IOPS.

  > BatchApplyTimeout: A temporal trigger that ensures a "heartbeat" flush occurs periodically


> The agent runs **continuously** (simulated by boto3 script), uploading incremental Parquet batches to `olist-ecommerce-landing-zone-useast1` S3 bucket throughout the day. The DLT pipeline then processes these accumulated CDC events in a single nightly run — separating the concern of data availability (continuous replication) from analytical consistency (daily clean Gold layer for reporting).

---

## ⚙️ ETL Pipeline — Databricks DLT

The pipeline is implemented as a **Databricks Delta Live Tables** pipeline spanning three layers of the Medallion Architecture. DLT manages cluster lifecycle, inter-table dependency resolution, and automatic retry on partial failures — the pipeline code focuses purely on transformation logic.

---

### Bronze — Raw Ingestion & Checkpointing

`analytics/transformations/bronze.py`

> The Bronze layer uses **Databricks Auto Loader** in file notification mode, consuming SQS events to discover new S3 objects. Data lands in Bronze as-is — raw, unmodified, with no type casting or business logic applied.

> The only additions are pipeline metadata columns (`_ingest_date`, `_ingested_at`,`_source_file`, ) for **end-to-end lineage tracking**. Each Olist entity lands in its own Bronze **Delta table**: orders, order items, customers, products, payments, reviews, sellers, and geolocation.

**Auto Loader checkpointing** is the foundation of the pipeline's reliability. A checkpoint directory persists the offset of every file Auto Loader has processed. This means:

- If the cluster is terminated mid-run, the next execution **resumes from the last committed offset** — no files are skipped, none are re-processed
- SQS at-least-once delivery is **handled transparently** — duplicate notifications for the same S3 file are silently skipped by the checkpoint
- **Schema changes** in upstream Olist exports are tracked in the checkpoint's schema history and automatically merged into the Bronze table, enabling schema evolution without pipeline downtime or manual intervention

---

### Silver — Data Quality & CDC Upserts

`analytics/transformations/silver.py`

> Silver reads from Bronze and serves as the pipeline's enforcement and reconciliation layer. Instead of relying on native declarative constraints, this engine utilizes a suite of custom Python validation functions to audit data across three distinct dimensions: Technical Data Quality (schema and null-integrity), Business Logic (domain-specific rules and reference integrity), and Analytic Readiness (metric-validity and distribution checks). This programmatic approach allows for complex, multi-column logic and cross-table validation that exceeds standard expectation syntax.

> Data that fails pre-defined Data Quality (DQ) checks is diverted into dedicated **Quarantine Tables** within Unity Catalog.

  > Auditability: Every quarantined record is persisted with a rejection_reason metadata column (e.g., Negative Price, Null Order_ID), allowing for rapid root-cause analysis without stalling the main pipeline.

  > Non-Blocking Logic: By shunting "poison pill" records into a side-table rather than failing the entire job, the pipeline maintains high availability for the 99% of data that is healthy.

  > Continuous Improvement: These tables serve as a feedback loop for the on-premise ERP team to identify upstream data entry errors or legacy system bugs.

> Beyond quality enforcement, Silver applies the **CDC OP Flags** against the **AR_H_COMMIT_TIMESTAMP ordering** to maintain system state. Inserts and updates are merged into Silver tables using Delta's MERGE semantics, keyed on business identifiers such as order_id or customer_id. Deletes are hard-deleted to ensure silver and gold layers are sources of truth. The result is a consistent, deduplicated Silver layer that accurately reflects the current state of the source ERP, regardless of duplicate SQS notifications or out-of-order event delivery. Comprehensive type casting, timestamp normalization, and critical entity joins—linking orders, customers, and products—are also finalized at this stage to prepare data for the Gold layer. Thee changes ultimatley propogate downstream to the gold layer. 

---

### Gold — Star Schema & Query Optimisation

`analytics/transformations/gold.py`

Gold is the analytics-serving layer. Tables are modelled as a **star schema** with clear separation of fact and dimension tables:

- **Fact tables** —  `fact_order_items`, `fact_payments`, `fact_reviews` — transactional measures at event grain (revenue, quantities, delivery times)
- **Dimension tables** — `dim_orders`, `dim_customers`, `dim_products`, `dim_sellers`, `dim_date` — descriptive attributes for Power BI slicing and filtering

> Tables are **partitioned** strategically on columns that align with high frequency access patterns —such as `customer_state` —ensuring that queries, such as Power BI queries, filtering by geography scan only the relevant data folders. To further optimize performance, the architecture utilizes **Liquid Clustering** on high-selectivity columns. Unlike traditional Z-Ordering, Liquid Clustering dynamically adjusts data layout over time, co-locating related values to enable sub-second response times as the dataset evolves.
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

After each successful pipeline run, the **Power BI REST API** is called using a Service Principal to trigger a full semantic model refresh. The dashboard connects to Gold Delta tables via the Databricks SQL connector and surfaces executive e-commerce metrics:


PUT THE DASHBOARD HERE

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
│   └── simulated_onprem_sync.py     # Python CDC agent · AR_H_COMMIT_TIMESTAMP· OP Flags
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
