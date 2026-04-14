from pyspark import pipelines as dp
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import BooleanType, StringType, TimestampType


# =========================================================
# SILVER SDP PIPELINE - OLIST BRONZE -> SILVER
# =========================================================
# PURPOSE
# -------
# Transform Bronze Delta tables into Silver, applying:
#   - Type casting
#   - Data quality checks with per-rule quarantine routing
#   - LAD (late-arriving dimension) placeholder support
#   - CDC handling (Insert / Update / Delete via DMS Op column)
#
# - The pattern per Silver table is:
#       1. @dp.temporary_view("<table>_clean_cdc")      — DQ-filtered streaming view; CDC source
#       2. @dp.table("<table>_quarantine")              — rejected rows (append-only)
#       3. dp.create_streaming_table("<table>")         — explicit target table
#       4. dp.create_auto_cdc_flow(target="<table>", source="<table>_clean_cdc", ...)
#
# SDP PIPELINE SETTINGS
# ------------------------------------
# Target Catalog : olist_prod
# Target Schema  : bronze  (Silver tables override via fully qualified names)
# Pipeline Mode  : Triggered
# =========================================================


# =========================================================
# 1. CONFIG
# =========================================================
CATALOG       = "olist_prod"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

CDC_OP_COL     = "OP"
CDC_COMMIT_COL = "AR_H_COMMIT_TIMESTAMP"

VALID_BRAZIL_STATES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}
VALID_PAYMENT_TYPES  = {"credit_card", "boleto", "voucher", "debit_card", "not_defined"}
VALID_ORDER_STATUSES = {
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "created", "approved",
}

SILVER_TABLE_PROPERTIES = {
    "quality": "silver",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact":   "true",
    "pipelines.autoOptimize.managed":   "true",
    "delta.feature.timestampNtz": "supported"
}
QUARANTINE_TABLE_PROPERTIES = {
    **SILVER_TABLE_PROPERTIES,
    "quality": "quarantine",
}


# =========================================================
# 2. SHARED TRANSFORM HELPERS
# =========================================================

def trim_all_strings(df: DataFrame) -> DataFrame:
    """Single select replaces chained withColumn calls — avoids deep plan nesting."""
    return df.select([
        F.trim(F.col(f.name)).alias(f.name)
        if isinstance(f.dataType, StringType)
        else F.col(f.name)
        for f in df.schema.fields
    ])


def cast_columns(df: DataFrame, casts: Dict[str, str]) -> DataFrame:
    for col_name, target_type in casts.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
            if target_type == "string":
                df = df.withColumn(col_name, F.lower(F.col(col_name)))
    return df


def add_silver_metadata(df: DataFrame) -> DataFrame:
    # This timestamp is used to make the audit trail look more realistic
    # to the static datatset used for this project
    # For a real-world pipeline, this would be F.current_timestamp(
    return df.withColumn(
        "_silver_processed_at", 
        F.to_timestamp(
            F.concat(
                F.to_date(F.col("_ingested_at")),
                F.lit(" "),
                F.date_format(F.current_timestamp(), "HH:mm:ss")
            ),
            "yyyy-MM-dd HH:mm:ss" # Prevents parsing failures
        ).cast("timestamp_ntz")) # source of truth (ntz)

def split_on_null_pk(df: DataFrame, pk_cols: List[str]) -> Tuple[DataFrame, DataFrame]:
    """Returns (valid_rows, null_pk_rows)."""
    condition = F.lit(True)
    for c in pk_cols:
        condition = condition & F.col(c).isNotNull()
    return df.filter(condition), df.filter(~condition)


def _apply_rules(
    df: DataFrame,
    rules: List[Tuple[Column, str]],
) -> Tuple[DataFrame, List[Tuple[DataFrame, str]]]:
    """
    Sequential rule runner. Each rule selects CLEAN rows; failures are bucketed
    with a specific reason string so quarantine records carry one precise,
    actionable label. Returns (clean_df, [(rejected_df, reason), ...]).
    """
    rejected_buckets: List[Tuple[DataFrame, str]] = []
    remaining = df
    for condition, reason in rules:
        clean    = remaining.filter(condition)
        rejected = remaining.filter(~condition)
        rejected_buckets.append((rejected, reason))
        remaining = clean
    return remaining, rejected_buckets


def collect_rejected(
    rejected_buckets: List[Tuple[DataFrame, str]],
    fallback_df: DataFrame,
) -> DataFrame:
    """
    Union all rejected buckets into a single DataFrame with a quarantine_reason
    column. Empty buckets are included in the union — Spark optimises away
    empty partitions at execution time, so no eager .count() is needed.
    """
    tagged = [
        df.withColumn("quarantine_reason", F.lit(reason))
        for df, reason in rejected_buckets
    ]
    if not tagged:
        return fallback_df.limit(0).withColumn(
            "quarantine_reason", F.lit(None).cast(StringType())
        )
    result = tagged[0]
    for extra in tagged[1:]:
        result = result.unionByName(extra, allowMissingColumns=True)
    return result


def add_quarantine_metadata(df: DataFrame, table_name: str) -> DataFrame:
    return (
        df
        .withColumn("_quarantine_table_name", F.lit(table_name))
        .withColumn("_quarantine_date", F.to_date(F.col("_ingested_at")))
        .withColumn("_quarantine_failed_at", 
                F.to_timestamp(
                    F.concat(
                        F.to_date(F.col("_ingested_at")),
                        F.lit(" "),
                        F.date_format(F.current_timestamp(), "HH:mm:ss"),
                    ), 
                    "yyyy-MM-dd HH:mm:ss"
                ).cast("timestamp_ntz")
        ))
        # .withColumn("_quarantine_failed_at",  F.current_timestamp())
        # .withColumn("_quarantine_date",       F.to_date(F.current_timestamp()

# =========================================================
# 3. PER-TABLE BASE TRANSFORMS
# =========================================================
# _base_<table>(df) applies trim + cast + derived columns that are needed by
# BOTH the clean path and the quarantine path. DQ rules run on top of this.
# This ensures purchase_date, product_category_name substitution, etc. are
# present in quarantine rows — matching non-SDP transform_table step 1 & 2.

def _base_customers(df: DataFrame) -> DataFrame:
    return trim_all_strings(df)


def _base_sellers(df: DataFrame) -> DataFrame:
    return trim_all_strings(df)


def _base_products(df: DataFrame) -> DataFrame:
    _PRODUCT_CASTS = {
        "product_name_lenght":        "double",
        "product_description_lenght": "double",
        "product_photos_qty":         "double",
        "product_weight_g":           "double",
        "product_length_cm":          "double",
        "product_height_cm":          "double",
        "product_width_cm":           "double",
    }
    df = trim_all_strings(df)
    df = cast_columns(df, _PRODUCT_CASTS)
    # NULL category → sentinel value (matches non-SDP transform_table step 2)
    df = df.withColumn(
        "product_category_name",
        F.when(F.col("product_category_name").isNull(), "sem categoria")
         .otherwise(F.col("product_category_name")),
    )
    rename_dict = {"product_name_lenght": "product_name_length", "product_description_lenght": "product_description_length"}
    df = df.withColumnsRenamed(rename_dict)
    return df


def _base_product_category_name_translation(df: DataFrame) -> DataFrame:
    return trim_all_strings(df)


def _base_orders(df: DataFrame) -> DataFrame:
    _ORDER_CASTS = {
        "order_purchase_timestamp":      "timestamp_ntz",
        "order_approved_at":             "timestamp_ntz",
        "order_delivered_carrier_date":  "timestamp_ntz",
        "order_delivered_customer_date": "timestamp_ntz",
        "order_estimated_delivery_date": "timestamp_ntz",
    }
    df = trim_all_strings(df)
    df = cast_columns(df, _ORDER_CASTS)
    # Partition helper — must be present in both clean and quarantine rows
    if "purchase_date" not in df.columns:
        df = df.withColumn("purchase_date", F.to_date(F.col("order_purchase_timestamp")))
    return df


def _base_order_items(df: DataFrame) -> DataFrame:
    _ORDER_ITEMS_CASTS = {
        "order_item_id":       "int",
        "shipping_limit_date": "timestamp_ntz",
        "price":               "double",
        "freight_value":       "double",
    }
    df = trim_all_strings(df)
    df = cast_columns(df, _ORDER_ITEMS_CASTS)
    return df


def _base_payments(df: DataFrame) -> DataFrame:
    _PAYMENT_CASTS = {
        "payment_sequential":   "int",
        "payment_installments": "int",
        "payment_value":        "double",
    }
    df = trim_all_strings(df)
    df = cast_columns(df, _PAYMENT_CASTS)
    return df


def _base_reviews(df: DataFrame) -> DataFrame:
    _REVIEW_CASTS = {
        "review_score":            "int",
        "review_creation_date":    "date",
        "review_answer_timestamp": "timestamp_ntz",
    }
    df = trim_all_strings(df)
    df = cast_columns(df, _REVIEW_CASTS)
    return df


# =========================================================
# 4. PER-TABLE DQ RULES + CLEAN / QUARANTINE SPLIT
# =========================================================
# Pattern for every table:
#   _rules_<table>(df)      — runs _apply_rules on a pre-transformed df,
#                             returns (clean_df, rejected_buckets)
#   _clean_<table>(df)      — clean rows ready for create_auto_cdc_flow source
#   _quarantine_<table>(df) — rejected rows ready for quarantine table
#
# Op='D' rows BYPASS DQ in _clean_* (they have no payload to validate)
# and are EXCLUDED from _quarantine_* (they are not bad data).
# This mirrors non-SDP transform_table's CDC split exactly.

# ─── customers ────────────────────────────────────────────────────────────

def _rules_customers(df: DataFrame) -> Tuple[DataFrame, List[Tuple[DataFrame, str]]]:
    return _apply_rules(df, [
        (F.col("customer_state").isin(VALID_BRAZIL_STATES), "invalid_state"),
    ])


def _clean_customers(df: DataFrame) -> DataFrame:
    df = _base_customers(df)

    # Op='D' rows bypass DQ — pass through directly
    if CDC_OP_COL in df.columns:
        deletes     = df.filter(F.col(CDC_OP_COL) == "D")
        non_deletes = df.filter(F.col(CDC_OP_COL) != "D")
    else:
        deletes     = df.limit(0)
        non_deletes = df

    clean, _ = _rules_customers(non_deletes)
    clean, _ = split_on_null_pk(clean, ["customer_id"])
    clean     = add_silver_metadata(clean)

    if CDC_OP_COL in df.columns:
        clean = clean.unionByName(deletes, allowMissingColumns=True)
    return clean


def _quarantine_customers(df: DataFrame) -> DataFrame:
    df = _base_customers(df)

    # Deletes are not bad data — exclude from quarantine
    if CDC_OP_COL in df.columns:
        df = df.filter(F.col(CDC_OP_COL) != "D")

    # _, buckets = _rules_customers(df)
    # clean, _   = _rules_customers(df)
    clean, buckets = _rules_customers(df)
    _, null_pk = split_on_null_pk(clean, ["customer_id"])
    null_pk    = null_pk.withColumn("quarantine_reason", F.lit("null_pk"))
    result     = collect_rejected(buckets, df)
    result     = result.unionByName(null_pk, allowMissingColumns=True)
    return add_quarantine_metadata(result, "customers")


# ─── sellers ──────────────────────────────────────────────────────────────

def _rules_sellers(df: DataFrame) -> Tuple[DataFrame, List[Tuple[DataFrame, str]]]:
    return _apply_rules(df, [
        (F.col("seller_state").isin(VALID_BRAZIL_STATES), "invalid_state"),
    ])


def _clean_sellers(df: DataFrame) -> DataFrame:
    df = _base_sellers(df)

    if CDC_OP_COL in df.columns:
        deletes     = df.filter(F.col(CDC_OP_COL) == "D")
        non_deletes = df.filter(F.col(CDC_OP_COL) != "D")
    else:
        deletes     = df.limit(0)
        non_deletes = df

    clean, _ = _rules_sellers(non_deletes)
    clean, _ = split_on_null_pk(clean, ["seller_id"])
    clean     = add_silver_metadata(clean)
    if CDC_OP_COL in df.columns:
        clean = clean.unionByName(deletes, allowMissingColumns=True)
    return clean


def _quarantine_sellers(df: DataFrame) -> DataFrame:
    df = _base_sellers(df)

    if CDC_OP_COL in df.columns:
        df = df.filter(F.col(CDC_OP_COL) != "D")

    # _, buckets = _rules_sellers(df)
    # clean, _   = _rules_sellers(df)
    clean, buckets = _rules_sellers(df)
    _, null_pk = split_on_null_pk(clean, ["seller_id"])
    null_pk    = null_pk.withColumn("quarantine_reason", F.lit("null_pk"))
    result     = collect_rejected(buckets, df)
    result     = result.unionByName(null_pk, allowMissingColumns=True)
    return add_quarantine_metadata(result, "sellers")


# ─── products ─────────────────────────────────────────────────────────────

def _rules_products(df: DataFrame) -> Tuple[DataFrame, List[Tuple[DataFrame, str]]]:
    return _apply_rules(df, [
        (F.col("product_weight_g").isNull()  | (F.col("product_weight_g")  > 0), "non_positive_weight"),
        (F.col("product_length_cm").isNull() | (F.col("product_length_cm") > 0), "non_positive_length"),
        (F.col("product_height_cm").isNull() | (F.col("product_height_cm") > 0), "non_positive_height"),
        (F.col("product_width_cm").isNull()  | (F.col("product_width_cm")  > 0), "non_positive_width"),
    ])


def _clean_products(df: DataFrame) -> DataFrame:
    df = _base_products(df)

    if CDC_OP_COL in df.columns:
        deletes     = df.filter(F.col(CDC_OP_COL) == "D")
        non_deletes = df.filter(F.col(CDC_OP_COL) != "D")
    else:
        deletes     = df.limit(0)
        non_deletes = df

    clean, _ = _rules_products(non_deletes)
    clean, _ = split_on_null_pk(clean, ["product_id"])
    clean     = add_silver_metadata(clean)

    if CDC_OP_COL in df.columns:
        clean = clean.unionByName(deletes, allowMissingColumns=True)
    return clean


def _quarantine_products(df: DataFrame) -> DataFrame:
    # _base_products already handles trim + cast — no double-trim here
    df = _base_products(df)

    if CDC_OP_COL in df.columns:
        df = df.filter(F.col(CDC_OP_COL) != "D")

    # _, buckets = _rules_products(df)
    # clean, _   = _rules_products(df)
    clean, buckets = _rules_products(df)
    _, null_pk = split_on_null_pk(clean, ["product_id"])
    null_pk    = null_pk.withColumn("quarantine_reason", F.lit("null_pk"))
    result     = collect_rejected(buckets, df)
    result     = result.unionByName(null_pk, allowMissingColumns=True)
    return add_quarantine_metadata(result, "products")


# ─── product_category_name_translation ────────────────────────────────────

def _clean_product_category_name_translation(df: DataFrame) -> DataFrame:
    df = _base_product_category_name_translation(df)

    if CDC_OP_COL in df.columns:
        deletes     = df.filter(F.col(CDC_OP_COL) == "D")
        non_deletes = df.filter(F.col(CDC_OP_COL) != "D")
    else:
        deletes     = df.limit(0)
        non_deletes = df

    clean, _ = split_on_null_pk(non_deletes, ["product_category_name"])
    clean     = add_silver_metadata(clean)

    if CDC_OP_COL in df.columns:
        clean = clean.unionByName(deletes, allowMissingColumns=True)
    return clean


def _quarantine_product_category_name_translation(df: DataFrame) -> DataFrame:
    df = _base_product_category_name_translation(df)

    if CDC_OP_COL in df.columns:
        df = df.filter(F.col(CDC_OP_COL) != "D")

    _, null_pk = split_on_null_pk(df, ["product_category_name"])
    null_pk    = null_pk.withColumn("quarantine_reason", F.lit("null_pk"))
    return add_quarantine_metadata(null_pk, "product_category_name_translation")


# ─── orders ───────────────────────────────────────────────────────────────

def _rules_orders(df: DataFrame) -> Tuple[DataFrame, List[Tuple[DataFrame, str]]]:
    return _apply_rules(df, [
        (F.col("order_status").isin(VALID_ORDER_STATUSES), "invalid_order_status"),
        (F.col("order_purchase_timestamp").isNotNull(),    "null_purchase_timestamp"),
        (F.col("customer_id").isNotNull(),                 "null_customer_id"),
    ])


def _clean_orders(df: DataFrame) -> DataFrame:
    df = _base_orders(df)

    if CDC_OP_COL in df.columns:
        deletes     = df.filter(F.col(CDC_OP_COL) == "D")
        non_deletes = df.filter(F.col(CDC_OP_COL) != "D")
    else:
        deletes     = df.limit(0)
        non_deletes = df

    clean, _ = _rules_orders(non_deletes)
    clean, _ = split_on_null_pk(clean, ["order_id"])
    clean     = add_silver_metadata(clean)

    if CDC_OP_COL in df.columns:
        clean = clean.unionByName(deletes, allowMissingColumns=True)
    return clean


def _quarantine_orders(df: DataFrame) -> DataFrame:
    df = _base_orders(df)

    if CDC_OP_COL in df.columns:
        df = df.filter(F.col(CDC_OP_COL) != "D")

    # _, buckets = _rules_orders(df)
    # clean, _   = _rules_orders(df)
    clean, buckets = _rules_orders(df)
    _, null_pk = split_on_null_pk(clean, ["order_id"])
    null_pk    = null_pk.withColumn("quarantine_reason", F.lit("null_pk"))
    result     = collect_rejected(buckets, df)
    result     = result.unionByName(null_pk, allowMissingColumns=True)
    return add_quarantine_metadata(result, "orders")


# ─── order_items ──────────────────────────────────────────────────────────

def _rules_order_items(df: DataFrame) -> Tuple[DataFrame, List[Tuple[DataFrame, str]]]:
    return _apply_rules(df, [
        (F.col("price")         >= 0,     "negative_price"),
        (F.col("freight_value") >= 0,     "negative_freight_value"),
        (F.col("order_id").isNotNull(),   "null_order_id"),
        (F.col("product_id").isNotNull(), "null_product_id"),
        (F.col("seller_id").isNotNull(),  "null_seller_id"),
    ])


def _clean_order_items(df: DataFrame) -> DataFrame:
    df = _base_order_items(df)

    if CDC_OP_COL in df.columns:
        deletes     = df.filter(F.col(CDC_OP_COL) == "D")
        non_deletes = df.filter(F.col(CDC_OP_COL) != "D")
    else:
        deletes     = df.limit(0)
        non_deletes = df

    clean, _ = _rules_order_items(non_deletes)
    clean, _ = split_on_null_pk(clean, ["order_id", "order_item_id"])
    clean     = add_silver_metadata(clean)

    if CDC_OP_COL in df.columns:
        clean = clean.unionByName(deletes, allowMissingColumns=True)
    return clean


def _quarantine_order_items(df: DataFrame) -> DataFrame:
    df = _base_order_items(df)

    if CDC_OP_COL in df.columns:
        df = df.filter(F.col(CDC_OP_COL) != "D")

    # _, buckets = _rules_order_items(df)
    # clean, _   = _rules_order_items(df)
    clean, buckets = _rules_order_items(df)
    _, null_pk = split_on_null_pk(clean, ["order_id", "order_item_id"])
    null_pk    = null_pk.withColumn("quarantine_reason", F.lit("null_pk"))
    result     = collect_rejected(buckets, df)
    result     = result.unionByName(null_pk, allowMissingColumns=True)
    return add_quarantine_metadata(result, "order_items")


# ─── payments ─────────────────────────────────────────────────────────────

def _rules_payments(df: DataFrame) -> Tuple[DataFrame, List[Tuple[DataFrame, str]]]:
    return _apply_rules(df, [
        (F.col("payment_type").isin(VALID_PAYMENT_TYPES), "invalid_payment_type"),
        (F.col("payment_value")        >= 0,              "negative_payment_value"),
        (F.col("payment_installments") >  0,              "invalid_installments"),
    ])


def _clean_payments(df: DataFrame) -> DataFrame:
    df = _base_payments(df)

    if CDC_OP_COL in df.columns:
        deletes     = df.filter(F.col(CDC_OP_COL) == "D")
        non_deletes = df.filter(F.col(CDC_OP_COL) != "D")
    else:
        deletes     = df.limit(0)
        non_deletes = df

    clean, _ = _rules_payments(non_deletes)
    clean, _ = split_on_null_pk(clean, ["order_id", "payment_sequential"])
    clean     = add_silver_metadata(clean)

    if CDC_OP_COL in df.columns:
        clean = clean.unionByName(deletes, allowMissingColumns=True)
    return clean


def _quarantine_payments(df: DataFrame) -> DataFrame:
    df = _base_payments(df)

    if CDC_OP_COL in df.columns:
        df = df.filter(F.col(CDC_OP_COL) != "D")

    # _, buckets = _rules_payments(df)
    # clean, _   = _rules_payments(df)
    clean, buckets = _rules_payments(df)
    _, null_pk = split_on_null_pk(clean, ["order_id", "payment_sequential"])
    null_pk    = null_pk.withColumn("quarantine_reason", F.lit("null_pk"))
    result     = collect_rejected(buckets, df)
    result     = result.unionByName(null_pk, allowMissingColumns=True)
    return add_quarantine_metadata(result, "payments")


# ─── reviews ──────────────────────────────────────────────────────────────

def _rules_reviews(df: DataFrame) -> Tuple[DataFrame, List[Tuple[DataFrame, str]]]:
    return _apply_rules(df, [
        (F.col("review_score").between(1, 5), "invalid_review_score"),
    ])


def _clean_reviews(df: DataFrame) -> DataFrame:
    df = _base_reviews(df)

    if CDC_OP_COL in df.columns:
        deletes     = df.filter(F.col(CDC_OP_COL) == "D")
        non_deletes = df.filter(F.col(CDC_OP_COL) != "D")
    else:
        deletes     = df.limit(0)
        non_deletes = df

    clean, _ = _rules_reviews(non_deletes)
    clean, _ = split_on_null_pk(clean, ["review_id", "order_id"])
    clean     = add_silver_metadata(clean)

    if CDC_OP_COL in df.columns:
        clean = clean.unionByName(deletes, allowMissingColumns=True)
    return clean


def _quarantine_reviews(df: DataFrame) -> DataFrame:
    df = _base_reviews(df)

    if CDC_OP_COL in df.columns:
        df = df.filter(F.col(CDC_OP_COL) != "D")

    # _, buckets = _rules_reviews(df)
    # clean, _   = _rules_reviews(df)
    clean, buckets = _rules_reviews(df)
    _, null_pk = split_on_null_pk(clean, ["review_id", "order_id"])
    null_pk    = null_pk.withColumn("quarantine_reason", F.lit("null_pk"))
    result     = collect_rejected(buckets, df)
    result     = result.unionByName(null_pk, allowMissingColumns=True)
    return add_quarantine_metadata(result, "reviews")


# =========================================================
# 5. SILVER TABLES VIA create_auto_cdc_flow()
# =========================================================
# Pattern per table:
#   @dp.temporary_view("<table>_clean_cdc")           — streaming view; CDC source (not materialized)
#   dp.create_streaming_table("catalog.silver.<table>") — explicit target table declaration
#   dp.create_auto_cdc_flow(...)                        — CDC merge into the Silver target table
#
# _clean_cdc views are pipeline-private temporary views (unqualified names).
# They are NOT persisted as tables — they exist only during pipeline execution.
# All published Silver tables use fully qualified catalog.schema.table names.
#
# create_auto_cdc_flow() parameters:
#   sequence_by  = coalesce(CDC_COMMIT_COL, _ingested_at)
#                  Mirrors non-SDP dedupe_latest_by_pk ordering:
#                  DMS commit timestamp is authoritative; _ingested_at is the
#                  fallback for pre-CDC / backfill rows without AR_H_COMMIT_TIMESTAMP.
#   apply_as_deletes = F.col(CDC_OP_COL) == "D"
#                  Replaces the non-SDP .whenMatchedDelete(condition="s.Op = 'D'").
#   stored_as_scd_type = 1  — overwrite in-place (no history rows).
#   ignore_null_updates   — not set here; used only for LAD placeholder merges.
#
# SDP handles deduplication within a batch internally using sequence_by,
# so dedupe_latest_by_pk is NOT called in the clean CDC source functions.
#
# Dimensions are declared before facts so LAD views (section 7) can
# reference them via spark.read.table().

# ─── Dimensions ───────────────────────────────────────────────────────────

@dp.temporary_view(name="customers_clean_cdc")
def customers_clean_cdc():
    return _clean_customers(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.customers",
    comment="Silver customers table",
    table_properties=SILVER_TABLE_PROPERTIES,
    partition_cols=["customer_state"],
)

dp.create_auto_cdc_flow(
    target           = f"{CATALOG}.{SILVER_SCHEMA}.customers",
    source           = "customers_clean_cdc",
    keys             = ["customer_id"],
    sequence_by      = F.coalesce(F.col(CDC_COMMIT_COL), F.col("_ingested_at")),
    apply_as_deletes = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
    name             = "customers_cdc",
)


@dp.temporary_view(name="sellers_clean_cdc")
def sellers_clean_cdc():
    return _clean_sellers(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.sellers")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sellers",
    comment="Silver sellers table",
    table_properties=SILVER_TABLE_PROPERTIES,
    partition_cols=['seller_state']
)

dp.create_auto_cdc_flow(
    target           = f"{CATALOG}.{SILVER_SCHEMA}.sellers",
    source           = "sellers_clean_cdc",
    keys             = ["seller_id"],
    sequence_by      = F.coalesce(F.col(CDC_COMMIT_COL), F.col("_ingested_at")),
    apply_as_deletes = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
    name             = "sellers_cdc",
)


@dp.temporary_view(name="products_clean_cdc")
def products_clean_cdc():
    return _clean_products(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.products")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.products",
    comment="Silver products table",
    table_properties=SILVER_TABLE_PROPERTIES,
    partition_cols=['product_category_name']
)

dp.create_auto_cdc_flow(
    target           = f"{CATALOG}.{SILVER_SCHEMA}.products",
    source           = "products_clean_cdc",
    keys             = ["product_id"],
    sequence_by      = F.coalesce(F.col(CDC_COMMIT_COL), F.col("_ingested_at")),
    apply_as_deletes = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
    name             = "products_cdc",
)


@dp.temporary_view(name="product_category_name_translation_clean_cdc")
def product_category_name_translation_clean_cdc():
    return _clean_product_category_name_translation(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.product_category_name_translation")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.product_category_name_translation",
    comment="Silver product_category_name_translation table",
    table_properties=SILVER_TABLE_PROPERTIES,
)

dp.create_auto_cdc_flow(
    target           = f"{CATALOG}.{SILVER_SCHEMA}.product_category_name_translation",
    source           = "product_category_name_translation_clean_cdc",
    keys             = ["product_category_name"],
    sequence_by      = F.coalesce(F.col(CDC_COMMIT_COL), F.col("_ingested_at")),
    apply_as_deletes = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


# ─── Facts ────────────────────────────────────────────────────────────────

@dp.temporary_view(name="orders_clean_cdc")
def orders_clean_cdc():
    return _clean_orders(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.orders",
    comment="Silver orders table",
    table_properties=SILVER_TABLE_PROPERTIES,
    partition_cols=['purchase_date']
)

dp.create_auto_cdc_flow(
    target           = f"{CATALOG}.{SILVER_SCHEMA}.orders",
    source           = "orders_clean_cdc",
    keys             = ["order_id"],
    sequence_by      = F.coalesce(F.col(CDC_COMMIT_COL), F.col("_ingested_at")),
    apply_as_deletes = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


@dp.temporary_view(name="order_items_clean_cdc")
def order_items_clean_cdc():
    return _clean_order_items(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.order_items")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.order_items",
    comment="Silver order_items table",
    table_properties=SILVER_TABLE_PROPERTIES,
)

dp.create_auto_cdc_flow(
    target           = f"{CATALOG}.{SILVER_SCHEMA}.order_items",
    source           = "order_items_clean_cdc",
    keys             = ["order_id", "order_item_id"],
    sequence_by      = F.coalesce(F.col(CDC_COMMIT_COL), F.col("_ingested_at")),
    apply_as_deletes = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


@dp.temporary_view(name="payments_clean_cdc")
def payments_clean_cdc():
    return _clean_payments(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.payments")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.payments",
    comment="Silver payments table",
    table_properties=SILVER_TABLE_PROPERTIES,
)

dp.create_auto_cdc_flow(
    target           = f"{CATALOG}.{SILVER_SCHEMA}.payments",
    source           = "payments_clean_cdc",
    keys             = ["order_id", "payment_sequential"],
    sequence_by      = F.coalesce(F.col(CDC_COMMIT_COL), F.col("_ingested_at")),
    apply_as_deletes = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


@dp.temporary_view(name="reviews_clean_cdc")
def reviews_clean_cdc():
    return _clean_reviews(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.reviews")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.reviews",
    comment="Silver reviews table",
    table_properties=SILVER_TABLE_PROPERTIES,
)

dp.create_auto_cdc_flow(
    target           = f"{CATALOG}.{SILVER_SCHEMA}.reviews",
    source           = "reviews_clean_cdc",
    keys             = ["review_id", "order_id"],
    sequence_by      = F.coalesce(F.col(CDC_COMMIT_COL), F.col("_ingested_at")),
    apply_as_deletes = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


# =========================================================
# 6. QUARANTINE TABLES
# =========================================================
# Each source table has a companion quarantine @dp.table that reads the
# same bronze stream independently and keeps only rejected rows.
# Append-only, partitioned by _quarantine_date.
#
# Op='D' rows are excluded from quarantine in all _quarantine_* functions —
# deletes are not bad data.

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.customers_quarantine",
    comment="Quarantine for rejected customers rows",
    partition_cols=["_quarantine_date"],
    table_properties=QUARANTINE_TABLE_PROPERTIES,
)
def customers_quarantine():
    return _quarantine_customers(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sellers_quarantine",
    comment="Quarantine for rejected sellers rows",
    partition_cols=["_quarantine_date"],
    table_properties=QUARANTINE_TABLE_PROPERTIES,
)
def sellers_quarantine():
    return _quarantine_sellers(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.sellers")
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.products_quarantine",
    comment="Quarantine for rejected products rows",
    partition_cols=["_quarantine_date"],
    table_properties=QUARANTINE_TABLE_PROPERTIES,
)
def products_quarantine():
    return _quarantine_products(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.products")
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.product_category_name_translation_quarantine",
    comment="Quarantine for rejected product_category_name_translation rows",
    partition_cols=["_quarantine_date"],
    table_properties=QUARANTINE_TABLE_PROPERTIES,
)
def product_category_name_translation_quarantine():
    return _quarantine_product_category_name_translation(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.product_category_name_translation")
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.orders_quarantine",
    comment="Quarantine for rejected orders rows",
    partition_cols=["_quarantine_date"],
    table_properties=QUARANTINE_TABLE_PROPERTIES,
)
def orders_quarantine():
    return _quarantine_orders(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders")
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.order_items_quarantine",
    comment="Quarantine for rejected order_items rows",
    partition_cols=["_quarantine_date"],
    table_properties=QUARANTINE_TABLE_PROPERTIES,
)
def order_items_quarantine():
    return _quarantine_order_items(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.order_items")
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.payments_quarantine",
    comment="Quarantine for rejected payments rows",
    partition_cols=["_quarantine_date"],
    table_properties=QUARANTINE_TABLE_PROPERTIES,
)
def payments_quarantine():
    return _quarantine_payments(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.payments")
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.reviews_quarantine",
    comment="Quarantine for rejected reviews rows",
    partition_cols=["_quarantine_date"],
    table_properties=QUARANTINE_TABLE_PROPERTIES,
)
def reviews_quarantine():
    return _quarantine_reviews(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.reviews")
    )
