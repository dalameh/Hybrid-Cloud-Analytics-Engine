# Hybrid-Cloud-Analytics-Engine

<div align="center">

![Python](https://img.shields.io/badge/Python-Scripts-3776AB?style=flat-square&logo=python&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-S3%20%C2%B7%20SNS%20%C2%B7%20SQS-FF9900?style=flat-square&logo=amazonaws&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-DeltaTables%20%C2%B7%20DLT%20ETL%20Pipelines%20%C2%B7%20LakeFlow%20Jobs%20%C2%B7%20Dashboards-E64A19?style=flat-square&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Medallion%20Architecture-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![PowerBI](https://img.shields.io/badge/Power%20BI-Executive%20Dashboard-F2C811?style=flat-square&logo=powerbi&logoColor=black)

**Enterprise-grade, event-driven ERP analytics pipeline — on-premise relational data continuously replicated to AWS, transformed through a Medallion architecture on Databricks, and served to a live Power BI executive dashboard.**

</div>

---

## Table of Contents

- [Business Context](#-business-context)
  - [The Business](#the-business)
  - [The Challenge](#the-challenge-breaking-the-analytical-bottleneck)
  - [The Solution](#the-solution-an-event-driven-analytical-ecosystem)
- [Architecture](#-architecture)
- [Infrastructure — AWS CDK](#️-infrastructure--aws-cdk-iac)
  - [S3 Landing Zone](#s3-landing-zone)
  - [S3 Data Lakehouse](#s3-data-lakehouse)
  - [SNS — Event Fan-out](#sns--event-fan-out)
  - [SQS — Durable Work Buffer](#sqs--durable-work-buffer)
- [Continuous CDC Replication — AWS DMS](#-continuous-cdc-replication-simulated--aws-dms)
- [Unity Catalog Governance](#️-unity-catalog-governance)
- [ETL Pipeline — Databricks DLT Pipeline](#️-etl-pipeline--databricks-dlt)
  - [Bronze — Raw Ingestion & Checkpointing](#bronze--raw-ingestion--checkpointing)
  - [Silver — Data Quality & CDC Upserts](#silver--data-quality--cdc-upserts)
  - [Gold — Star Schema & Query Optimisation](#gold--star-schema--query-optimisation)
- [Idempotency & Consistency Guarantees](#-idempotency--consistency-guarantees)
- [Orchestration — Databricks Lakeflow Jobs](#-orchestration--databricks-lakeflow-jobs)
- [Databricks Dashboard](#-databricks-dashboard)
- [Power BI Dashboard](#-power-bi-dashboard)
- [Repository Structure](#-repository-structure)
- [Tech Stack](#️-tech-stack)
- [Planned Improvements](#-planned-improvements)

---

## 💼 Business Context

### The Business

Olist is a Brazilian e-commerce ecosystem that acts as a strategic integrator, connecting thousands of small businesses and local merchants to the country's largest online marketplaces. By providing a unified logistics and listing platform, it allows sellers to scale their reach while Olist manages the heavy lifting of product distribution and customer service.

### The Challenge: Breaking the Analytical Bottleneck

The enterprise is currently tethered to an **on-premise ERP monolith** that was designed for transactional stability, not modern analytical scale. This legacy architecture forces business intelligence queries to compete with real-time operations for the same hardware resources, creating a performance bottleneck that degrades OLTP performance, risking system instability and downtime during peak high-volume retail periods.

To unlock the next phase of growth, leadership is pivoting to a **Hybrid-Cloud integration strategy** to migrate and synchronize transactional data into a **unified cloud data lakehouse**. By decoupling analytical workloads from the on-premise core, the enterprise aims to:

| Goal | Description |
|:---|:---|
| **Eliminate Resource Contention** | Offload compute-intensive queries to the cloud to ensure the local ERP remains fast and responsive for frontline operations |
| **Unified Source of Truth** | Centralize fragmented data from multiple local instances into a single, scalable Medallion Architecture governed by Unity Catalog |
| **Executive Visibility** | Replace delayed reports with real-time Executive Dashboards to track cross-regional KPIs instantly |
| **Enable Advanced Analytics** | Provide the infrastructure necessary for high-velocity predictive modeling and real-time reporting the legacy hardware cannot support |

### The Solution: An Event-Driven Analytical Ecosystem

This architecture bridges an on-premise ERP monolith to a Databricks Lakehouse using a continuous Change Data Capture (CDC) pipeline. By leveraging **AWS DMS (simulated)**, transactional state changes are replicated into the cloud in real-time, completely offloading analytical overhead from the production OLTP engine.

The data flows through a governed **Medallion architecture within Unity Catalog**, where raw relational changes are ultimately refined into a high-performance Gold-standard star schema. While AWS DMS replicates data continuously, a scheduled **Databricks Lakeflow job** runs daily at EOD to process the accumulated changes and push them through the pipeline. To close the loop between engineering and action, this job triggers the **Power BI REST API** and Databricks Dashboard upon completion, ensuring up to date leadership dashboards that reflect the latest business state without manual intervention.

---

## 🏗 Architecture

```
┌─────────────────────────┐
│   On-Prem ERP (Olist)   │  Python CDC agent · AR_H_COMMIT_TIMESTAMP + OP-coded events
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
└──────────────────────┬───────────────────────────────────┘
                       │
                       │  Databricks Workflow · daily @ midnight
                       ▼
┌─────────────────────────┐
│     Power BI            │  REST API semantic model refresh
│     Executive Dashboard │  Auto-updated after every successful run
└─────────────────────────┘
```

---

## ☁️ Infrastructure — AWS CDK + CloudFormation (IaC)

All AWS infrastructure is provisioned as code using the **AWS CDK (Cloud Development Kit)**, which synthesizes high-level constructs into **CloudFormation** templates to ensure the environment is reproducible, version-controlled, and deployable across environments without manual configuration.

### S3 Landing Zone

The S3 bucket `olist-ecommerce-landing-zone-useast1` is the raw system of record, providing the landing zone for the full load and all on-going replications.

- **Hive-style partitioning** (`dataset/year=/month=/day=`) enables efficient partition pruning downstream without full bucket scans
- **Event notifications** enabled to send all `s3:ObjectCreated:*`, `s3:ObjectRemoved:*`, and `s3:LifecycleExpiration:*`events to sns topic `olist-landing-topic` (imperative to have all three events enabled to ensure the file events service within Databricks can track the full lifecycle of files in its cache)
- **Lifecycle policy** automatically transitions objects to S3 Infrequent Access after 30 days, Glacier Flexible after 90, and expires them after 365 days — keeping storage costs bounded as data accumulates
- **Versioning** is enabled to protect against accidental overwrites during replication
- **SSE-S3 encryption** (AES-256) is automatically applied to guarantee server-side encryption for all objects at rest

<img width="1869" height="767" alt="image" src="https://github.com/user-attachments/assets/d8604d10-db2b-44c5-bedc-49cf64726f27" />

### S3 Data Lakehouse

The bucket `olist-ecommerce-prod-useast1-lakehouse` serves as the **Unity Catalog Managed Location**, providing the high-performance storage backbone for the Bronze, Silver, and Gold Delta tables.

- Data is stored in optimized **Delta format** in a dedicated `_unitystorage/` directory, enabling ACID transactions, time travel, and schema enforcement across the entire pipeline
- **Decoupled compute from storage** ensures ERP data remains persistent and accessible to both Databricks SQL and potential downstream ML workloads without data duplication
- **Managed Storage** enforces centralized governance, auditing all physical data access and restricting it to the Unity Catalog service principal
- **SSE-S3 encryption** (AES-256) applied identically to the landing zone

<img width="1876" height="747" alt="image" src="https://github.com/user-attachments/assets/07725ddf-c074-448f-ad0a-1f56ed1fc3d3" />

### SNS — Event Fan-out

Every `s3:ObjectCreated:*`, `s3:ObjectRemoved:*`, and `s3:LifecycleExpiration:*` event fires a notification to the `olist-landing-topic` SNS topic. SNS decouples the ingestion agent from all downstream compute — the CDC agent has no knowledge of Databricks, SQS, or anything downstream. This also allows new consumers (alerting, archival, compliance auditing) to subscribe to the topic without modifying a single line of ingestion code.

<img width="1609" height="517" alt="image" src="https://github.com/user-attachments/assets/8d3b1a1f-25eb-4a94-8a8e-c3ec7465930f" />

### SQS — Durable Work Buffer

The `olist-landing-queue` acts as the event-driven backbone of the ingestion layer, subscribing to `olist-landing-topic` via SNS.

- **O(1) file discovery**: By utilizing Auto Loader with SQS File Notifications, the architecture bypasses the latency of traditional S3 directory listings — an $O(N)$ operation that becomes prohibitively expensive as object counts scale. The moment a file lands in S3, a notification is pushed through the SNS→SQS chain, allowing Databricks to pinpoint and ingest new data without performing a full bucket scan
- **Dead-Letter Queue (DLQ)** with `maxReceiveCount: 3` isolates "poison pill" files that fail processing, preventing malformed data from stalling the entire batch
- **300-second visibility timeout** ensures the Databricks job has sufficient overhead to commit the batch before any message re-delivery occurs, maintaining strict processing integrity during compute transitions

<img width="1650" height="360" alt="image" src="https://github.com/user-attachments/assets/9d11bb26-8d3d-46be-8127-601a79bfcf5a" />
<img width="1804" height="703" alt="image" src="https://github.com/user-attachments/assets/cef2464d-e1ab-4ebb-a994-ce31bb7cf0d7" />
<img width="875" height="340" alt="image" src="https://github.com/user-attachments/assets/b47bff3d-d987-4956-b14b-2676738513e5" />


---

## 🔄 Continuous CDC Replication (Simulated) — AWS DMS

> `ingestion/dms_synthetic_batching.py`

> `ingestion/dms_synthetic_run.py`

The on-premise ERP Full Load + Change Data Capture is simulated by a **Python Boto3 agent** that replicates the behavior of DMS's built-in replication engine.

**Promoted metadata columns:**

| Field | Purpose |
|:---|:---|
| `AR_H_COMMIT_TIMESTAMP` | A physical metadata column captured from the source DB's transaction log. Provides a monotonically increasing sequence that allows the pipeline to resolve "Last Write Wins" logic when multiple updates occur for the same record within a single batch |
| `OP` | Operation flags `I` (insert) · `U` (update) · `D` (delete) emitted by the DMS CDC engine — tells the Silver layer exactly which merge, upsert, or delete logic to apply |

**Engine logic simulation mirrors a production DMS instance via:**

- `BatchApplyMemoryLimit` — flushes data to S3 once a specific record volume is reached to optimize storage IOPS
- `BatchApplyTimeout` — a temporal trigger that ensures a "heartbeat" flush occurs periodically

The agent runs **continuously**, uploading incremental Parquet batches to S3 throughout the day. The DLT pipeline then processes these accumulated CDC events in a single nightly run — separating the concern of data availability (continuous replication) from analytical consistency (daily clean Gold layer for reporting).

<img width="1026" height="819" alt="image" src="https://github.com/user-attachments/assets/ee31bc0b-c042-4d04-a8cd-20f52f8c5cdd" />

---

## ⚙️ Unity Catalog Governance

> `pipeline/catalog_schema_builder.ipynb`

I implemented **Unity Catalog** to provide a centralized governance layer for the **Unified Cloud Data Lakehouse**, moving beyond a simple "bucket of files" to a managed enterprise asset.

### Infrastructure Setup

I established **External Locations** to securely map S3 storage to Databricks compute. 
* **`olist_raw_landing_zone`**: Ingestion point for raw CDC changes. Enables File Events with provided `olist-landing-queue` url.
* **`olist_datalakehouse_root`**: Storage backbone for all processed data.

### Medallion Catalog Architecture
I architected the **`olist_prod`** catalog with a **Managed Location**, allowing the system to automatically handle physical file layout and metadata. This catalog is structured into medallion schemas:

```sql
CREATE CATALOG IF NOT EXISTS olist_prod 
MANAGED LOCATION 's3://olist-ecommerce-prod-useast1-lakehouse/';

CREATE SCHEMA IF NOT EXISTS olist_prod.bronze;
CREATE SCHEMA IF NOT EXISTS olist_prod.silver;
CREATE SCHEMA IF NOT EXISTS olist_prod.gold;
```

**Impact:** This setup decouples storage from compute while providing **Unified Security** and **Data Lineage** across the entire hybrid-cloud pipeline.

<img width="1662" height="425" alt="image" src="https://github.com/user-attachments/assets/1718a66a-2d77-4696-97f3-8cff557d954a" />
<img width="1153" height="669" alt="image" src="https://github.com/user-attachments/assets/699e1f34-563f-467e-96bb-551ab4a685c3" />

---

## ⚙️ ETL Pipeline — Databricks DLT

The pipeline is implemented as a **Databricks Delta Live Tables** pipeline spanning three layers of the Medallion Architecture. DLT manages cluster lifecycle, inter-table dependency resolution, and automatic retry on partial failures — the pipeline code focuses purely on transformation logic.

---

### Bronze — Raw Ingestion & Checkpointing

> `analytics/transformations/bronze.py`

The Bronze layer uses **Databricks Auto Loader** in file notification mode, consuming SQS events to discover new S3 objects. Data lands in Bronze as-is — raw, unmodified, with no type casting or business logic applied.

The only additions are pipeline metadata columns (`_ingested_at`, `_source_file`) for **end-to-end lineage tracking**. Each Olist entity lands in its own Bronze Delta table: `orders`, `order_items`, `customers`, `products`, `payments`, `reviews`, `sellers`, and `geolocation`.

**Auto Loader checkpointing** is the foundation of the pipeline's reliability. A checkpoint directory persists the offset of every file Auto Loader has processed:

- If the cluster is terminated mid-run, the next execution **resumes from the last committed offset** — no files are skipped, none are re-processed
- SQS at-least-once delivery is **handled transparently** — duplicate notifications for the same S3 file are silently skipped by the checkpoint
- **Schema changes** in upstream Olist exports are tracked in the checkpoint's schema history and automatically merged into the Bronze table, enabling schema evolution without pipeline downtime or manual intervention

<img width="1128" height="430" alt="image" src="https://github.com/user-attachments/assets/b13ac2ec-79d8-4e02-9f27-75f4068c8e18" />
<img width="1125" height="398" alt="image" src="https://github.com/user-attachments/assets/894021bc-8d99-4639-8999-3e9dbde58d24" />

---

### Silver — Data Quality & CDC Upserts

> `analytics/transformations/silver.py`

Silver reads from Bronze and serves as the pipeline's enforcement and reconciliation layer. Instead of relying on native declarative constraints, this engine utilizes a suite of custom Python validation functions to audit data across three distinct dimensions:

| Dimension | Description |
|:---|:---|
| **Technical Data Quality** | Schema and null-integrity validation |
| **Business Logic** | Domain-specific rules and reference integrity checks |
| **Analytic Readiness** | Metric-validity and distribution checks |

This programmatic approach allows for complex, multi-column logic and cross-table validation that exceeds standard expectation syntax.

**Quarantine Tables:** Data that fails pre-defined DQ checks is diverted into dedicated Quarantine Tables within Unity Catalog:

- **Auditability** — every quarantined record is persisted with a `_quarantine_reason` metadata column (e.g., `Negative Price`, `Null PK`), allowing for rapid root-cause analysis without stalling the main pipeline
- **Non-Blocking Logic** — by shunting "poison pill" records into a side-table rather than failing the entire job, the pipeline maintains high availability for the 99% of data that is healthy and ensures no misleading analytics to end-users.
- **Continuous Improvement** — these tables serve as a feedback loop for the on-premise ERP team to identify upstream data entry errors or legacy system bugs

<img width="1216" height="361" alt="image" src="https://github.com/user-attachments/assets/71aa2ec5-d82d-4a30-8436-308b16c96bc6" />

**CDC reconciliation:** Beyond quality enforcement, Silver applies the CDC OP Flags against the `AR_H_COMMIT_TIMESTAMP` ordering to maintain system state. Inserts and updates are merged into Silver tables using Delta's `MERGE` semantics, keyed on business identifiers such as `order_id` or `customer_id`. Deletes are hard-deleted to ensure Silver and Gold layers remain sources of truth, **regardless of duplicate SQS notifications or out-of-order event delivery**. Comprehensive type casting, timestamp normalization, and critical entity joins — linking orders, customers, and products — are also finalized at this stage.

<img width="1126" height="442" alt="image" src="https://github.com/user-attachments/assets/262fa5bb-d93a-4722-ba1d-f93a80733ce6" />
<img width="1128" height="394" alt="image" src="https://github.com/user-attachments/assets/c4be2962-70c2-4e98-b43e-4d16ff077bc4" />

---

### Gold — Star Schema & Query Optimisation

> `analytics/transformations/gold.py`

Gold is the analytics-serving layer, modelled as a **star schema** with clear separation of fact and dimension tables:

All changed in silver are propogated downstream to gold, and transformed to the star schema for analytics

**Fact Tables** — transactional measures at event grain:

| Table | Measures |
|:---|:---|
| `fact_order_items` | Revenue, quantities, delivery times |
| `fact_payments`    | Payment values, installments |
| `fact_reviews`     | Review scores, response times |

**Dimension Tables** — descriptive attributes for Power BI slicing:

`dim_orders` · `dim_customers` · `dim_products` · `dim_sellers` · `dim_date`

**Query optimisation strategy:**

- **Partition pruning** on high-frequency access columns (i.e. `customer_state` for dim_customers, `purchase_date` for fact_order_items, and so on) — Power BI queries filtering by geography scan only the relevant data folders
- **Liquid Clustering** on high-selectivity columns — unlike traditional Z-Ordering, Liquid Clustering dynamically adjusts data layout over time, co-locating related values for sub-second response times as the dataset evolves

<img width="1690" height="769" alt="image" src="https://github.com/user-attachments/assets/46bd26fc-13bf-4a45-ad8b-75a105ff6841" />

---

## 🔒 Idempotency & Consistency Guarantees

| Guarantee | Mechanism |
|:---|:---|
| **Crash recovery** | Auto Loader checkpoint resumes from last committed offset on cluster restart |
| **Idempotent processing** | SQS at-least-once + checkpoint file tracking + Silver `MERGE` on business keys = no duplicates |
| **Schema evolution** | Auto Loader schema history auto-merges new upstream columns with no downtime |
| **Delta time travel** | Full transaction log on every table — point-in-time queries and safe rollback after bad runs |
| **Consistent reporting** | Power BI refresh fires only on fully successful pipeline run — partial runs never update the dashboard |

---

## 🕛 Orchestration — Databricks Lakeflow Jobs

The pipeline runs on a **daily cron schedule at midnight** via a Databricks Workflow. The DLT pipeline processes all accumulated events in a single nightly batch — producing a fully reconciled, consistent Gold layer for leadership reporting for the next morning. Retries up to 2×, with a 10-minute delay, are configured on Task 1 before the workflow alerts and stops. The job chains two tasks:

<img width="1654" height="647" alt="image" src="https://github.com/user-attachments/assets/4a4de16d-5733-48b0-a749-aa3f688478a7" />

### Full Load:
<img width="1337" height="826" alt="image" src="https://github.com/user-attachments/assets/e4467af0-76c7-461c-9c1b-f258596a39d9" />


### Change Data Capture:
<img width="1651" height="823" alt="image" src="https://github.com/user-attachments/assets/0319f1a3-ea13-4285-8e3c-4ea70029d0ee" />

---

## 📊 Databricks Dashboard

After each successful pipeline run, the published Databricks Dashboard is refresh the gold tables and aggregations used to contruct the dashboard.

<img width="1721" height="680" alt="image" src="https://github.com/user-attachments/assets/9034d2ab-8a37-4c9c-929f-c9284298293a" />
<img width="1718" height="541" alt="image" src="https://github.com/user-attachments/assets/c29cf39d-e40e-4306-945d-d51f2cf9a1aa" />

---


## 📊 Power BI Dashboard

After each successful pipeline run, the **Power BI REST API** is called using a Service Principal to trigger a full semantic model refresh. The dashboard connects to Gold Delta tables via the Databricks SQL connector and surfaces executive e-commerce metrics.

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|:---|:---|:---|
| **Ingestion** | Python · Boto3 | CDC agent simulating continuous on-prem ERP sync |
| **Infrastructure** | AWS CDK | Reproducible IaC for S3, SNS, SQS, IAM |
| **Storage** | AWS S3 | Hive-partitioned raw landing zone with lifecycle management |
| **Events** | AWS SNS | Fan-out decoupling between ingestion and compute |
| **Queue** | AWS SQS + DLQ | Durable work buffer with fault-tolerant dead-lettering |
| **ETL** | Databricks DLT · PySpark | Medallion architecture · Auto Loader · checkpointing |
| **Orchestration** | Databricks Workflows | Scheduled daily execution · task chaining · alerting |
| **BI** | Power BI | Executive dashboard · automated semantic model refresh |

---

## 🔮 Planned Improvements

| Improvement | Description |
|:---|:---|
| **Late Arrival Reconciliation** | Handle late-arriving dimension records by writing placeholder rows for foreign keys that don't yet exist in the target dimension table (e.g., an `order_item` references a `product_id` that hasn't replicated yet). The placeholder preserves referential integrity in the star schema, marks the record as `unresolved`, and a reconciliation job back-fills the full attributes once the parent dimension record eventually lands. This prevents silent data loss and keeps fact table grain intact without blocking the pipeline. |
| **SQS Subscription Filter Policy** | Scope SQS delivery to Olist dataset prefixes only, preventing internal S3 operations from triggering the pipeline unnecessarily |
| **SQS PrivateLink** | Route SQS traffic over a VPC private endpoint to eliminate public internet traversal |
| **DLQ → Quarantine Bridge** | Automatically promote DLQ messages into the Silver quarantine tables for unified auditability and root-cause analysis alongside DQ failures |
| **Slack Notification** | Utilize native Databricks alerts to alert a Slack Channel of job start and job success/failure |
---

<div align="center">

*Designed for production reliability — idempotent incremental loads, crash-safe Auto Loader checkpointing, event-driven decoupling, and zero-touch nightly orchestration.*

</div>
