# Hybrid-Cloud-Analytics-Engine

> Bridging on-premises relational ERP data to a cloud-native analytics platform — event-driven, medallion-architected, and continuously replicated.

---

## The Business Problem

Many enterprises operate mission-critical transactional data on on-premises relational databases — systems built for OLTP workloads, not analytics. Running complex analytical queries directly against production ERP systems degrades performance, introduces risk, and limits the depth of insight available to decision-makers.

This engine solves that problem. It continuously replicates an on-premises ERP (modelled on the Olist Brazilian E-commerce dataset) to a cloud-native analytics environment, transforming raw relational records into a clean, performant, Power BI-ready data warehouse — without ever touching the source system during business hours.

---

## Architecture Overview

```
┌─────────────────────┐
│  On-Prem ERP (Olist)│  Simulated via Python CDC agent
│  Relational Database│  ar_h_commit + OP-coded change events
└──────────┬──────────┘
           │  Continuous replication (CDC)
           ▼
┌─────────────────────┐
│   AWS S3            │  CDK-provisioned landing zone
│   Landing Zone      │  Hive-partitioned raw storage
│                     │  Lifecycle: Glacier (30d) → Expire (90d)
└──────────┬──────────┘
           │  s3:ObjectCreated event notification
           ▼
┌─────────────────────┐
│   AWS SNS           │  Fan-out hub — erp-pipeline-events
│   (Event Bus)       │  Decouples ingestion from compute
└──────────┬──────────┘
           │  Publish
           ▼
┌─────────────────────┐
│   AWS SQS           │  Durable work buffer — erp-etl-queue
│   (Message Queue)   │  DLQ for failed message handling
└──────────┬──────────┘
           │  Auto Loader file notification
           ▼
┌──────────────────────────────────────────────────┐
│           Databricks Delta Live Tables           │
│                                                  │
│  [Bronze] → [Silver] → [Gold]                    │
│  Raw Delta   DQ + Merge  Star Schema             │
│                                                  │
│         DLT Checkpoint State                     │
│         Idempotent · Crash-safe                  │
└──────────────────┬───────────────────────────────┘
                   │  Databricks Workflow (daily @ midnight)
                   ▼
┌─────────────────────┐
│   Power BI          │  Semantic model refresh via REST API
│   Dashboard         │  Executive E-commerce Metrics
└─────────────────────┘
```

---

## Infrastructure — AWS CDK

All AWS infrastructure is provisioned as code using the **AWS CDK**, ensuring the environment is reproducible, version-controlled, and environment-agnostic.

**S3 Landing Zone** is the raw system of record. Files arrive partitioned by dataset and date, giving the downstream pipeline efficient partition pruning without full bucket scans. A lifecycle policy automatically transitions objects to **S3 Glacier** after 30 days and expires them at 90 days — keeping storage costs bounded as the dataset grows. Versioning is enabled to protect against accidental overwrites during replication.

**SNS** sits immediately downstream of S3. Every `ObjectCreated` event fires a notification to the `erp-pipeline-events` topic, which fans out to all subscribers. This decoupling is intentional — the CDC agent has no knowledge of Databricks, and new consumers (alerting, archival, auditing) can be attached to the topic without modifying ingestion code.

**SQS** acts as the durable buffer between the event bus and the compute layer. Rather than Databricks polling S3 directly (expensive at scale), Auto Loader consumes SQS messages to discover new files — a pattern that scales to millions of objects without degradation. A dead-letter queue captures any message that fails repeated processing, preventing poison records from blocking the queue indefinitely.

---

## Continuous Replication — CDC Agent

The on-premises ERP is simulated by a Python agent that performs **Change Data Capture** against the Olist relational dataset. Each change event carries two critical fields:

- **`ar_h_commit`** — a monotonically increasing commit timestamp that establishes the exact order of changes across all tables, enabling consistent point-in-time reconstruction of the source database
- **`OP`** — the operation code: `I` (insert), `U` (update), or `D` (delete), allowing the pipeline to apply the correct merge or tombstone logic downstream

This mirrors the format produced by real CDC tools (Debezium, AWS DMS, Oracle GoldenGate). The agent runs continuously, uploading incremental batches to S3 — simulating the steady stream of transactional changes that would flow from a live ERP.

---

## ETL Pipeline — Databricks Delta Live Tables

The pipeline is implemented as a **Databricks DLT** pipeline spanning three layers of the Medallion Architecture. DLT manages cluster lifecycle, dependency resolution between tables, and automatic retry on partial failures.

### Bronze — Raw Ingestion with Checkpointing

The Bronze layer uses **Databricks Auto Loader** in file notification mode, consuming SQS events to discover new S3 objects. Auto Loader maintains a **checkpoint** — a persistent record of every file it has processed, stored in DBFS alongside the pipeline state. This checkpoint is the foundation of the pipeline's reliability guarantees:

- If the cluster is terminated mid-run, the next execution resumes from the last committed offset — no files are skipped, no files are re-processed
- SQS delivers messages at-least-once; the checkpoint ensures **idempotency** — duplicate notifications for the same file are silently ignored
- Schema changes in the upstream Olist data are tracked in the checkpoint's schema history and automatically merged into the Bronze table, enabling **schema evolution without pipeline downtime**

Raw records land in Bronze as **Delta tables** — unchanged from the source, but now versioned, ACID-compliant, and time-travel capable. Metadata columns (`_source_file`, `_ingestion_timestamp`) are appended for lineage tracking.

### Silver — Data Quality, Deduplication & Upserts

Silver reads from Bronze and applies the pipeline's data quality layer via **DLT Expectations**. Every table has explicit rules governing what constitutes a valid record. Violations are handled with defined severity:

- Soft warnings log the violation but retain the row for investigation
- Hard drops remove records that violate business key constraints
- Pipeline halts are reserved for critical integrity failures that would corrupt downstream aggregations

Beyond quality enforcement, Silver performs **deduplication and upserts** using Delta's `MERGE` semantics keyed on business identifiers (e.g., `order_id`, `customer_id`). This is where the CDC `OP` codes are applied — inserts and updates are merged into the table, deletes are handled as soft-deletes or tombstones. The result is a **consistent, deduplicated Silver layer** that accurately reflects the current state of the source ERP, regardless of how many times the same change event was delivered.

Type casting, timestamp normalisation, and referential joins between Olist entities (orders → customers, orders → items → products) are also resolved at this layer.

### Gold — Star Schema, Partitioning & Z-ORDER

Gold is the analytics-serving layer. Tables are modelled as a **star schema** with clear separation of fact and dimension tables:

- **Fact tables** — `fact_orders`, `fact_order_items`, `fact_payments` — contain transactional measures (revenue, quantities, delivery times) at the grain of individual events
- **Dimension tables** — `dim_customers`, `dim_products`, `dim_sellers`, `dim_geography` — contain descriptive attributes used for slicing and filtering in Power BI

Tables are **partitioned** on high-selectivity date columns (e.g., `order_month`, `order_year`) so Power BI queries that filter by time period scan only the relevant partitions rather than the full table.

**Z-ORDER clustering** is applied on the columns most frequently used in Power BI filter predicates (e.g., `order_status`, `product_category`, `seller_state`). Z-ORDER co-locates related values on the same Delta data files, meaning queries filtering on these columns skip the vast majority of storage reads — critical for sub-second dashboard response times over large datasets.

Pre-aggregated summary tables (e.g., `gold_order_summary`, `gold_seller_performance`) materialise the most expensive calculations once at pipeline time, so the dashboard never recomputes them on the fly.

---

## Orchestration — Databricks Workflows

The pipeline runs on a **daily schedule at midnight** via a Databricks Workflow. The job runs the full DLT pipeline — Bronze through Gold — and, on successful completion, executes the Power BI refresh hook as a post-pipeline task.

The scheduling pattern is deliberate: the DLT pipeline processes the day's accumulated CDC batches in a single nightly run, while the CDC agent continues replicating continuously throughout the day. This separates the concerns of **data availability** (continuous CDC → S3) from **analytical consistency** (daily DLT run producing a clean, fully-merged Gold layer for reporting).

The Workflow enforces strict task dependency — the Power BI refresh only fires if the DLT pipeline completes without errors. Failed runs trigger an alert without updating the dashboard, ensuring the business never reports on partial or inconsistent data.

---

## Power BI Dashboard

After each successful pipeline run, `pbi_refresh_hook.py` calls the **Power BI REST API** using a Service Principal to trigger a full semantic model refresh. The dashboard connects to the Gold layer Delta tables via the Databricks SQL connector and surfaces executive e-commerce metrics: revenue trends, order volumes, seller performance, delivery SLAs, customer geography, and review sentiment — all reflecting the prior day's fully reconciled data.

---

## Dataset

[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k orders across 8 relational tables covering orders, customers, products, sellers, payments, reviews, and geolocation.

---

*Designed for production reliability: idempotent incremental loads, crash-safe checkpointing, event-driven decoupling, and zero-touch nightly orchestration.*
