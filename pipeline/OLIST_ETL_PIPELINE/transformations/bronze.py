from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# =========================================================
# BRONZE LAKEFLOW PIPELINE - OLIST LANDING -> BRONZE
# =========================================================
# PURPOSE
# -------
# Ingest raw Parquet files from the landing zone into Bronze Delta tables
# using Databricks Auto Loader (cloudFiles) inside a Declarative pipeline.
#
# DESIGN NOTES
# ------------
# - One explicit dp table per source table (better for lineage/UI)
# - Shared helper functions for DRY logic
# - Auto Loader tracks new files incrementally
# - The pipeline manages checkpoints, stream state, and schema tracking
#   internally — do NOT set cloudFiles.schemaLocation manually
# - Bronze remains append-only and preserves raw structure
#
# EXPECTED LANDING LAYOUT
# -----------------------
# s3://<bucket>/landing/orders/upload_date=YYYY-MM-DD/file.parquet
# s3://<bucket>/landing/order_items/upload_date=YYYY-MM-DD/file.parquet
# etc.
#
# PIPELINE SETTINGS (recommended)
# -----------------------------------
# Target Catalog : olist_prod
# Target Schema  : bronze
# Pipeline Mode  : Triggered
# =========================================================


# =========================================================
# 1. CONFIG
# =========================================================
BUCKET = "olist-ecommerce-landing-bucket"

# Shared table properties for all Bronze tables
BRONZE_TABLE_PROPERTIES = {
    "quality": "bronze",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "pipelines.autoOptimize.managed": "true",
}

# =========================================================
# 2. SHARED HELPERS
# =========================================================
def read_landing_table(table_name: str) -> DataFrame:
    """
    Reads a landing-zone table as a streaming Auto Loader source.

    Assumes landing files are stored in Hive-style partition folders:
      s3://<bucket>/landing/<table_name>/upload_date=YYYY-MM-DD/file.parquet

    Important:
    - The pipeline manages stream state, checkpoints, and schema tracking
      internally. Do NOT set cloudFiles.schemaLocation manually.
    """
    landing_path = f"s3://{BUCKET}/landing/{table_name}/"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.partitionColumns", "upload_date")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .load(landing_path)
    )

def add_bronze_metadata(df: DataFrame) -> DataFrame:
    """
    Adds standardized Bronze metadata columns to every ingested table.

    Added columns:
    - ingest_date      : DATE derived from upload_date (used for partitioning)
    - ingested_at      : TIMESTAMP combining upload_date + current wall-clock time
    - source_file_path : full source file path from Auto Loader metadata
    """
    return (
        df.select(
            "*",
            F.to_date(F.col("upload_date")).alias("ingest_date"),

            # This timestamp is used to make the audit trail look more realistic
            # to the static datatset used for this project
            # For a real-world pipeline, this would be F.current_timestamp(
            F.to_timestamp(
                F.concat(
                    F.col("upload_date"),
                    F.lit(" "),
                    F.date_format(F.current_timestamp(), "HH:mm:ss"),
                )
            ).alias("ingested_at"),
            F.col("_metadata.file_path").alias("source_file_path"),
        )
    )


# =========================================================
# 3. BRONZE TABLES
# =========================================================

@dp.table(
    name="orders",
    comment="Bronze landing ingestion for orders",
    partition_cols=["ingest_date"],
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def orders():
    return add_bronze_metadata(read_landing_table("orders"))


@dp.table(
    name="order_items",
    comment="Bronze landing ingestion for order_items",
    partition_cols=["ingest_date"],
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def order_items():
    return add_bronze_metadata(read_landing_table("order_items"))


@dp.table(
    name="payments",
    comment="Bronze landing ingestion for payments",
    partition_cols=["ingest_date"],
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def payments():
    return add_bronze_metadata(read_landing_table("payments"))


@dp.table(
    name="reviews",
    comment="Bronze landing ingestion for reviews",
    partition_cols=["ingest_date"],
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def reviews():
    return add_bronze_metadata(read_landing_table("reviews"))


@dp.table(
    name="products",
    comment="Bronze landing ingestion for products",
    partition_cols=["ingest_date"],
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def products():
    return add_bronze_metadata(read_landing_table("products"))


@dp.table(
    name="sellers",
    comment="Bronze landing ingestion for sellers",
    partition_cols=["ingest_date"],
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def sellers():
    return add_bronze_metadata(read_landing_table("sellers"))


@dp.table(
    name="customers",
    comment="Bronze landing ingestion for customers",
    partition_cols=["ingest_date"],
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def customers():
    return add_bronze_metadata(read_landing_table("customers"))


@dp.table(
    name="product_category_name_translation",
    comment="Bronze landing ingestion for product_category_name_translation",
    partition_cols=["ingest_date"],
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def product_category_name_translation():
    return add_bronze_metadata(read_landing_table("product_category_name_translation"))