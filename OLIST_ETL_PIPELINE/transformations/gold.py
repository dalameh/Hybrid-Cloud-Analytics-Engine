from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, StringType, TimestampType,
    IntegerType, DoubleType, DateType, LongType,
)

# =========================================================
# GOLD SDP PIPELINE - OLIST SILVER -> GOLD (STAR SCHEMA)
# =========================================================
# PURPOSE
# -------
# Transform Silver tables into a Kimball-style star schema
# optimised for executive dashboards and BI tooling.
#
# SCHEMA OVERVIEW
# ---------------
# DIMENSIONS (SCD Type 1 via create_auto_cdc_flow):
#   dim_date        — dense calendar spine (generated, not streamed)
#   dim_customer    — one row per customer_id; carries customer_unique_id
#                     for true human-level loyalty analysis
#   dim_seller      — one row per seller_id
#   dim_product     — one row per product_id; enriched with English category
#   dim_order       — one row per order_id; owns all delivery timing +
#                     on-time flag; promoted from degenerate to full dim
#                     because order-level metrics (on-time %, order volume)
#                     need a clean, non-redundant home
#
# FACTS (append-only, immutable grain):
#   fact_order_items — grain: order_id + order_item_id
#                      Central GMV fact. One row = one physical unit shipped.
#                      FKs to all dims. Carries price, freight, gmv_item.
#   fact_payments    — grain: order_id + payment_sequential
#                      Isolated to avoid fan-out with items. Owns all
#                      payment method and installment analysis.
#   fact_reviews     — grain: review_id + order_id
#                      Isolated to avoid fan-out with items. Owns all
#                      CSAT / satisfaction analysis.
#
# SURROGATE KEYS
# --------------
# All dims and facts use surrogate keys (bigint) derived via F.hash()
# (Murmur3, 32-bit) over the natural business key(s). This is:
#   - Deterministic : same input always produces the same SK
#   - Distributed-safe : no sequence generator needed in Spark/SDP
#   - Reproducible  : re-running the pipeline produces identical SKs
#   - Storage-efficient : replaces 36-char UUID strings with 8-byte bigints
#                         for faster joins and smaller fact tables
#
# For datasets >100M rows swap F.hash() → F.xxhash64() for lower collision
# probability (64-bit vs 32-bit key space).
#
# Z-ORDERING
# ----------
# Applied via "pipelines.clusteringColumns" table property.
# SDP (DBR 13+) reads this property and applies Z-ordering automatically
# on each pipeline run. Clustering columns are chosen per table based on
# the dominant BI access pattern (what the dashboard filters on first).
#
# DASHBOARD QUERY MAP — every KPI and visual, single-hop joins only
# -----------------------------------------------------------------
# Total GMV              → SUM(fact_order_items.gmv_item)
# Order Volume           → COUNT(DISTINCT fact_order_items.order_id)
# AOV (seller-contract)  → GMV / COUNT(DISTINCT order_id)
# AOV (true cart)        → GMV / COUNT(DISTINCT fact_reviews.review_id)
# CSAT                   → AVG(fact_reviews.review_score)
# On-Time Delivery %     → AVG(CAST(dim_order.is_on_time AS INT))
#                          WHERE is_delivered = TRUE
# Monthly GMV trend      → fact_order_items JOIN dim_date ON purchase_date_sk
# GMV by State (customer)→ fact_order_items JOIN dim_customer ON customer_sk
# GMV by State (seller)  → fact_order_items JOIN dim_seller ON seller_sk
# Category Leaderboard   → fact_order_items JOIN dim_product ON product_sk
# Delivery vs CSAT       → fact_reviews JOIN dim_order ON order_sk
# Seller Pareto          → fact_order_items JOIN dim_seller ON seller_sk
# Payment method donut   → fact_payments GROUP BY payment_type
# Installment behaviour  → fact_payments GROUP BY payment_installments
# True shopping trips    → COUNT(DISTINCT fact_reviews.review_id)
#
# All Gold table names use fully qualified catalog.schema.table notation
# because the pipeline default schema is bronze. Stage views are temporary
# (pipeline-private, unqualified names).
#
# STREAMING FROM CDC TARGETS (Change Data Feed)
# ----------------------------------------------
# All Silver tables are CDC targets (updated via dp.create_auto_cdc_flow
# with MERGE). Streaming reads use readChangeFeed=true to capture all
# change types (insert, update, delete) from the Delta Change Data Feed.
# update_preimage rows are filtered out (we only need the post-image).
# The _change_type column drives apply_as_deletes in Gold CDC flows,
# ensuring deletes in Silver propagate correctly to Gold.
#
# SDP PIPELINE SETTINGS (recommended)
# ------------------------------------
# Target Catalog : olist_prod
# Target Schema  : bronze  (Gold tables override via fully qualified names)
# Pipeline Mode  : Triggered
# =========================================================


# =========================================================
# 1. CONFIG
# =========================================================
CATALOG       = "olist_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

CDC_OP_COL     = "OP"
CDC_COMMIT_COL = "AR_H_COMMIT_TIMESTAMP"

# Dense calendar spine — covers full Olist dataset + buffer either side
DATE_SPINE_START = "2016-01-01"
DATE_SPINE_END   = "2019-12-31"


# =========================================================
# 2. HELPERS
# =========================================================

def surrogate_key(*natural_key_cols) -> F.Column:
    """
    Deterministic surrogate key via Spark Murmur3 hash (F.hash, 32-bit).
    Always positive — F.abs() ensures BI tools never see negative dimension keys.
    Cast to LongType (bigint) for storage efficiency and join performance.

    Usage:
        surrogate_key(F.col("order_id"))
        surrogate_key(F.col("order_id"), F.col("order_item_id").cast("string"))
    """
    return F.abs(F.hash(*natural_key_cols)).cast(LongType())


def gold_table_properties(cluster_cols: list) -> dict:
    """
    Standard Gold table properties with Z-ordering via liquid clustering.
    pipelines.clusteringColumns is read by SDP to auto-apply ZORDER on each run.
    """
    return {
        "quality":                          "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "pipelines.autoOptimize.managed":   "true",
        "pipelines.clusteringColumns":      ",".join(cluster_cols),
    }


def silver(table: str) -> str:
    """Fully-qualified Silver table path for spark.read / spark.readStream."""
    return f"{CATALOG}.{SILVER_SCHEMA}.{table}"


def gold(table: str) -> str:
    """Fully-qualified Gold table path."""
    return f"{CATALOG}.{GOLD_SCHEMA}.{table}"


# =========================================================
# 3. DIMENSIONS
# =========================================================

# ─── dim_date ─────────────────────────────────────────────────────────────
# Dense calendar spine. Generated once as a static batch table — NOT
# streamed from Silver. Every date in range exists regardless of whether
# any transactions occurred, so BI time-series charts never show silent gaps.
#
# Z-order: date_actual — all time-series dashboard filters anchor here.

@dp.materialized_view(
    name=gold("dim_date"),
    comment="Dense calendar spine 2016–2019 covering full Olist dataset range",
    table_properties=gold_table_properties(["date_actual"]),
)
def dim_date() -> DataFrame:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    date_range = spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{DATE_SPINE_START}'),
                to_date('{DATE_SPINE_END}'),
                interval 1 day
            )
        ) AS date_actual
    """)

    return (
        date_range
        .withColumn("date_sk",
            surrogate_key(F.col("date_actual").cast(StringType()))
        )
        .withColumn("year",           F.year("date_actual"))
        .withColumn("quarter",        F.quarter("date_actual"))
        .withColumn("month",          F.month("date_actual"))
        .withColumn("month_name",     F.date_format("date_actual", "MMMM"))
        .withColumn("month_short",    F.date_format("date_actual", "MMM"))
        .withColumn("year_month",     F.date_format("date_actual", "yyyy-MM"))
        .withColumn("week_of_year",   F.weekofyear("date_actual"))
        .withColumn("day_of_month",   F.dayofmonth("date_actual"))
        .withColumn("day_of_week",    F.dayofweek("date_actual"))  # 1=Sun, 7=Sat
        .withColumn("day_name",       F.date_format("date_actual", "EEEE"))
        .withColumn("day_short",      F.date_format("date_actual", "EEE"))
        .withColumn("is_weekend",
            F.dayofweek("date_actual").isin([1, 7]).cast(BooleanType())
        )
        .withColumn("fiscal_year",    F.year("date_actual"))
        .withColumn("fiscal_quarter", F.quarter("date_actual"))
        # This timestamp is used to make the audit trail look more realistic
        # to the static datatset used for this project
        # For a real-world pipeline, this would be F.current_timestamp(
        # .withColumn("gold_processed_at", F.to_timestamp(
        #         F.concat(
        #             F.col("silver_processed_at"),
        #             F.lit(" "),
        #             F.date_format(F.current_timestamp(), "HH:mm:ss"),
        #         )))
        .select(
            "date_sk", "date_actual", "year", "quarter", "month",
            "month_name", "month_short", "year_month",
            "week_of_year", "day_of_month", "day_of_week",
            "day_name", "day_short", "is_weekend",
            "fiscal_year", "fiscal_quarter", 
            # "gold_processed_at",
        )
    )


# ─── dim_customer ─────────────────────────────────────────────────────────
# One row per customer_id (the checkout session token).
# Carries customer_unique_id so analysts can GROUP BY the actual human.
#
# ANALYST NOTE:
#   FK on fact_order_items is customer_sk → customer_id (session token).
#   For repeat-purchase / loyalty: GROUP BY dim_customer.customer_unique_id.
#   NEVER use COUNT(DISTINCT customer_id) for unique humans —
#   that is COUNT(DISTINCT order_id) in disguise.
#
# Z-order: customer_state — geographic market concentration queries.

@dp.temporary_view(name="dim_customer_stage")
def dim_customer_stage() -> DataFrame:
    return (
        spark.readStream.option("readChangeFeed", "true").table(silver("customers"))
        .filter(F.col("_change_type") != "update_preimage")
        .withColumn("customer_sk", surrogate_key(F.col("customer_id")))
        # This timestamp is used to make the audit trail look more realistic
        # to the static datatset used for this project
        # For a real-world pipeline, this would be F.current_timestamp(
        .withColumn("gold_processed_at", F.to_timestamp(
                F.concat(
                    F.col("silver_processed_at"),
                    F.lit(" "),
                    F.date_format(F.current_timestamp(), "HH:mm:ss"),
                )))
        .select(
            "customer_sk",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "is_placeholder",
            "silver_processed_at",
            "gold_processed_at",
            CDC_OP_COL,
            CDC_COMMIT_COL,
        )
    )


dp.create_streaming_table(
    name=gold("dim_customer"),
    comment="Gold dimension: one row per customer_id with surrogate key",
    table_properties=gold_table_properties(["customer_state"]),
)

dp.create_auto_cdc_flow(
    target             = gold("dim_customer"),
    source             = "dim_customer_stage",
    keys               = ["customer_sk"],
    sequence_by        = F.coalesce(F.col(CDC_COMMIT_COL), F.col("silver_processed_at")),
    apply_as_deletes   = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


# ─── dim_seller ───────────────────────────────────────────────────────────
# One row per seller_id.
# Z-order: seller_state — seller geographic concentration (Pareto analysis).

@dp.temporary_view(name="dim_seller_stage")
def dim_seller_stage() -> DataFrame:
    return (
        spark.readStream.option("readChangeFeed", "true").table(silver("sellers"))
        .filter(F.col("_change_type") != "update_preimage")
        .withColumn("seller_sk", surrogate_key(F.col("seller_id")))
        # This timestamp is used to make the audit trail look more realistic
        # to the static datatset used for this project
        # For a real-world pipeline, this would be F.current_timestamp(
        .withColumn("gold_processed_at", F.to_timestamp(
                F.concat(
                    F.col("silver_processed_at"),
                    F.lit(" "),
                    F.date_format(F.current_timestamp(), "HH:mm:ss"),
                )))
        .select(
            "seller_sk",
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "is_placeholder",
            "silver_processed_at",
            "gold_processed_at",
            CDC_OP_COL,
            CDC_COMMIT_COL,
        )
    )


dp.create_streaming_table(
    name=gold("dim_seller"),
    comment="Gold dimension: one row per seller_id with surrogate key",
    table_properties=gold_table_properties(["seller_state"]),
)

dp.create_auto_cdc_flow(
    target             = gold("dim_seller"),
    source             = "dim_seller_stage",
    keys               = ["seller_sk"],
    sequence_by        = F.coalesce(F.col(CDC_COMMIT_COL), F.col("silver_processed_at")),
    apply_as_deletes   = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


# ─── dim_product ──────────────────────────────────────────────────────────
# One row per product_id. Enriched with English category translation.
# NULL category already defaulted to "sem categoria" in Silver _base_products.
# Falls back to Portuguese name if no translation row exists.
#
# NOTE: spark.readStream + spark.read (batch) join is valid in SDP — the batch
# side (translations) is snapshotted at the start of each pipeline run.
#
# Z-order: product_category_name_english — category leaderboard queries.

@dp.temporary_view(name="dim_product_stage")
def dim_product_stage() -> DataFrame:
    products = (
        spark.readStream.option("readChangeFeed", "true").table(silver("products"))
        .filter(F.col("_change_type") != "update_preimage")
    )
    translations = spark.read.table(silver("product_category_name_translation"))

    return (
        products
        .join(
            translations.select(
                "product_category_name",
                "product_category_name_english",
            ),
            on="product_category_name",
            how="left",
        )
        .withColumn(
            "product_category_name_english",
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name"),
            ),
        )
        .withColumn("product_sk", surrogate_key(F.col("product_id")))
        # This timestamp is used to make the audit trail look more realistic
        # to the static datatset used for this project
        # For a real-world pipeline, this would be F.current_timestamp(
        .withColumn("gold_processed_at", F.to_timestamp(
                F.concat(
                    F.col("silver_processed_at"),
                    F.lit(" "),
                    F.date_format(F.current_timestamp(), "HH:mm:ss"),
                )))
        .select(
            "product_sk",
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "is_placeholder",
            "silver_processed_at",
            "gold_processed_at",
            CDC_OP_COL,
            CDC_COMMIT_COL,
        )
    )


dp.create_streaming_table(
    name=gold("dim_product"),
    comment="Gold dimension: one row per product_id with English category",
    table_properties=gold_table_properties(["product_category_name_english"]),
)

dp.create_auto_cdc_flow(
    target             = gold("dim_product"),
    source             = "dim_product_stage",
    keys               = ["product_sk"],
    sequence_by        = F.coalesce(F.col(CDC_COMMIT_COL), F.col("silver_processed_at")),
    apply_as_deletes   = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


# ─── dim_order ────────────────────────────────────────────────────────────
# One row per order_id. Promoted to a full dimension (not degenerate) because
# it owns all delivery timing attributes that would otherwise be redundantly
# repeated across every item row in fact_order_items.
#
# KEY DERIVATIONS:
#   delivery_days       : actual days from purchase → customer receipt
#   estimated_days      : promised days from purchase → estimated delivery
#   delivery_delta_days : actual − estimated (negative=early, positive=late)
#   is_on_time          : delivered_date <= estimated_date
#                         NULL when not yet delivered — intentional.
#                         Filter WHERE is_delivered = TRUE before AVG()
#                         to get correct On-Time Delivery %.
#   is_delivered        : order_status == 'delivered'
#
# DASHBOARD FORMULAS:
#   On-Time % → AVG(CAST(is_on_time AS INT)) WHERE is_delivered = TRUE
#   Lead time → AVG(delivery_days) GROUP BY dim_date.year_month
#
# Z-order: purchase_date_sk, order_status — time-range + status filters.

@dp.temporary_view(name="dim_order_stage")
def dim_order_stage() -> DataFrame:
    orders = (
        spark.readStream.option("readChangeFeed", "true").table(silver("orders"))
        .filter(F.col("_change_type") != "update_preimage")
    )
    dim_date_ = spark.read.table(gold("dim_date"))

    return (
        orders
        # Resolve purchase_date_sk — purchase_date (Date) added in Silver _base_orders
        .join(
            dim_date_.select(
                F.col("date_sk").alias("purchase_date_sk"),
                F.col("date_actual").alias("_join_date"),
            ),
            on=F.col("purchase_date") == F.col("_join_date"),
            how="left",
        )
        .drop("_join_date")
        .withColumn("order_sk",    surrogate_key(F.col("order_id")))
        .withColumn("customer_sk", surrogate_key(F.col("customer_id")))
        # ── Delivery timing derivations ──
        .withColumn(
            "delivery_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff(
                    F.col("order_delivered_customer_date"),
                    F.col("order_purchase_timestamp"),
                ),
            ).cast(IntegerType()),
        )
        .withColumn(
            "estimated_days",
            F.when(
                F.col("order_estimated_delivery_date").isNotNull(),
                F.datediff(
                    F.col("order_estimated_delivery_date"),
                    F.col("order_purchase_timestamp"),
                ),
            ).cast(IntegerType()),
        )
        .withColumn(
            "delivery_delta_days",
            F.when(
                F.col("delivery_days").isNotNull() &
                F.col("estimated_days").isNotNull(),
                F.col("delivery_days") - F.col("estimated_days"),
            ).cast(IntegerType()),
        )
        .withColumn(
            # NULL when not delivered — do not include in on-time denominator
            "is_on_time",
            F.when(
                F.col("order_delivered_customer_date").isNotNull() &
                F.col("order_estimated_delivery_date").isNotNull(),
                (
                    F.col("order_delivered_customer_date") <=
                    F.col("order_estimated_delivery_date")
                ).cast(BooleanType()),
            ),
        )
        .withColumn(
            "is_delivered",
            (F.col("order_status") == "delivered").cast(BooleanType()),
        )
        # This timestamp is used to make the audit trail look more realistic
        # to the static datatset used for this project
        # For a real-world pipeline, this would be F.current_timestamp(
        .withColumn("gold_processed_at", F.to_timestamp(
                F.concat(
                    F.col("silver_processed_at"),
                    F.lit(" "),
                    F.date_format(F.current_timestamp(), "HH:mm:ss"),
                )))
        .select(
            # Surrogate keys
            "order_sk",
            "customer_sk",
            "purchase_date_sk",
            # Natural keys
            "order_id",
            "customer_id",
            # Attributes
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            # Derived delivery metrics
            "delivery_days",
            "estimated_days",
            "delivery_delta_days",
            "is_on_time",
            "is_delivered",
            # Metadata
            "silver_processed_at",
            "gold_processed_at",
            CDC_OP_COL,
            CDC_COMMIT_COL,
        )
    )


dp.create_streaming_table(
    name=gold("dim_order"),
    comment="Gold dimension: one row per order_id with delivery metrics",
    table_properties=gold_table_properties(["purchase_date_sk", "order_status"]),
)

dp.create_auto_cdc_flow(
    target             = gold("dim_order"),
    source             = "dim_order_stage",
    keys               = ["order_sk"],
    sequence_by        = F.coalesce(F.col(CDC_COMMIT_COL), F.col("silver_processed_at")),
    apply_as_deletes   = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


# =========================================================
# 4. FACTS
# =========================================================
# All facts use create_auto_cdc_flow (SCD Type 1) to handle late-arriving
# corrections from source CDC. The grain is enforced by the surrogate key.
#
# FK PATTERN — every fact carries both:
#   <dim>_sk  : surrogate key for clean dim joins
#   <natural> : natural business key preserved for debugging / raw access
#
# Stage views resolve all SKs before create_auto_cdc_flow writes the final fact.

# ─── fact_order_items ─────────────────────────────────────────────────────
# GRAIN: order_id + order_item_id
# One row = one physical unit shipped by one seller.
# The central GMV fact — correct grain for all revenue and category analysis
# without fan-out risk.
#
# KEY MEASURES:
#   price         : unit selling price (excl. freight)
#   freight_value : shipping cost for this specific unit
#   gmv_item      : price + freight_value — total revenue per unit
#                   SUM(gmv_item) = Total GMV (the top line)
#
# AOV NOTE:
#   Seller-contract AOV = SUM(gmv_item) / COUNT(DISTINCT order_id)
#   True cart-level AOV = SUM(gmv_item) / COUNT(DISTINCT fact_reviews.review_id)
#   One shopping cart shreds into N order_ids (one per seller), so
#   COUNT(DISTINCT order_id) inflates trip count. Both are computable from
#   this table; documentation in BI layer is the analyst's responsibility.
#
# Z-order: purchase_date_sk, order_sk — time-range + order lookups dominate.

@dp.temporary_view(name="fact_order_items_stage")
def fact_order_items_stage() -> DataFrame:
    items = (
        spark.readStream.option("readChangeFeed", "true").table(silver("order_items"))
        .filter(F.col("_change_type") != "update_preimage")
    )

    # Resolve order_sk, customer_sk, purchase_date_sk from dim_order snapshot
    dim_order_ = spark.read.table(gold("dim_order")).select(
        "order_sk",
        "order_id",
        "customer_sk",
        "purchase_date_sk",
    )

    return (
        items
        .join(dim_order_, on="order_id", how="left")
        .withColumn("order_item_sk",
            surrogate_key(
                F.col("order_id"),
                F.col("order_item_id").cast(StringType()),
            )
        )
        .withColumn("product_sk", surrogate_key(F.col("product_id")))
        .withColumn("seller_sk",  surrogate_key(F.col("seller_id")))
        .withColumn(
            "gmv_item",
            (F.col("price") + F.col("freight_value")).cast(DoubleType()),
        )
        # This timestamp is used to make the audit trail look more realistic
        # to the static datatset used for this project
        # For a real-world pipeline, this would be F.current_timestamp(
        .withColumn("gold_processed_at", F.to_timestamp(
                F.concat(
                    F.col("silver_processed_at"),
                    F.lit(" "),
                    F.date_format(F.current_timestamp(), "HH:mm:ss"),
                )))
        .select(
            # Surrogate keys
            "order_item_sk",
            "order_sk",
            "product_sk",
            "seller_sk",
            "customer_sk",
            "purchase_date_sk",
            # Natural keys (preserved for debugging)
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            # Measures
            "price",
            "freight_value",
            "gmv_item",
            # Attributes
            "shipping_limit_date",
            # Metadata
            "silver_processed_at",
            "gold_processed_at",
            CDC_OP_COL,
            CDC_COMMIT_COL,
        )
    )


dp.create_streaming_table(
    name=gold("fact_order_items"),
    comment="Gold fact: one row per order_id + order_item_id with GMV measures",
    table_properties=gold_table_properties(["purchase_date_sk", "order_sk"]),
)

dp.create_auto_cdc_flow(
    target             = gold("fact_order_items"),
    source             = "fact_order_items_stage",
    keys               = ["order_item_sk"],
    sequence_by        = F.coalesce(F.col(CDC_COMMIT_COL), F.col("silver_processed_at")),
    apply_as_deletes   = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


# ─── fact_payments ────────────────────────────────────────────────────────
# GRAIN: order_id + payment_sequential
# Isolated from items to prevent fan-out (N items × M payment methods
# would multiply rows and corrupt all payment aggregations).
#
# KEY MEASURES:
#   payment_value             : amount paid via this method/sequential entry
#   payment_installments      : number of monthly instalments chosen
#   monthly_instalment_value  : payment_value / payment_installments
#                               (derived; avoids repeated division in BI)
#
# DASHBOARD FORMULAS:
#   Payment method mix    → COUNT(*) or SUM(payment_value) GROUP BY payment_type
#   Instalment behaviour  → AVG(payment_installments) GROUP BY order value bucket
#   Total order payment   → SUM(payment_value) GROUP BY order_id
#
# Z-order: purchase_date_sk, payment_type — time-range + method filters.

@dp.temporary_view(name="fact_payments_stage")
def fact_payments_stage() -> DataFrame:
    payments = (
        spark.readStream.option("readChangeFeed", "true").table(silver("payments"))
        .filter(F.col("_change_type") != "update_preimage")
    )

    dim_order_ = spark.read.table(gold("dim_order")).select(
        "order_sk",
        "order_id",
        "purchase_date_sk",
    )

    return (
        payments
        .join(dim_order_, on="order_id", how="left")
        .withColumn("payment_sk",
            surrogate_key(
                F.col("order_id"),
                F.col("payment_sequential").cast(StringType()),
            )
        )
        .withColumn(
            "monthly_instalment_value",
            F.when(
                F.col("payment_installments") > 0,
                (F.col("payment_value") / F.col("payment_installments")).cast(DoubleType()),
            ),
        )
        # This timestamp is used to make the audit trail look more realistic
        # to the static datatset used for this project
        # For a real-world pipeline, this would be F.current_timestamp(
        .withColumn("gold_processed_at", F.to_timestamp(
                F.concat(
                    F.col("silver_processed_at"),
                    F.lit(" "),
                    F.date_format(F.current_timestamp(), "HH:mm:ss"),
                )))
        .select(
            # Surrogate keys
            "payment_sk",
            "order_sk",
            "purchase_date_sk",
            # Natural keys
            "order_id",
            "payment_sequential",
            # Attributes & measures
            "payment_type",
            "payment_installments",
            "payment_value",
            "monthly_instalment_value",
            # Metadata
            "silver_processed_at",
            "gold_processed_at",
            CDC_OP_COL,
            CDC_COMMIT_COL,
        )
    )


dp.create_streaming_table(
    name=gold("fact_payments"),
    comment="Gold fact: one row per order_id + payment_sequential",
    table_properties=gold_table_properties(["purchase_date_sk", "payment_type"]),
)

dp.create_auto_cdc_flow(
    target             = gold("fact_payments"),
    source             = "fact_payments_stage",
    keys               = ["payment_sk"],
    sequence_by        = F.coalesce(F.col(CDC_COMMIT_COL), F.col("silver_processed_at")),
    apply_as_deletes   = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)


# ─── fact_reviews ─────────────────────────────────────────────────────────
# GRAIN: review_id + order_id
# Isolated from items to prevent fan-out (one review × k items in the order
# would inflate CSAT averages if joined naively at item grain).
#
# KEY MEASURES:
#   review_score           : 1–5 star rating; AVG = CSAT
#   has_comment_title      : boolean; engagement depth signal
#   has_comment_message    : boolean; engagement depth signal
#
# CRITICAL — TRUE SHOPPING TRIP COUNT:
#   COUNT(DISTINCT review_id) = true number of shopping cart trips.
#   One review covers one seller's fulfilment box regardless of how many
#   order_ids were in the original cart. This is the correct denominator
#   for cart-level AOV and trip-level conversion metrics.
#
# DELIVERY × CSAT SCATTER (Visual D in dashboard spec):
#   SELECT
#       dim_order.delivery_days       AS x,
#       AVG(fact_reviews.review_score) AS y,
#       COUNT(*)                       AS bubble_size
#   FROM fact_reviews
#   JOIN dim_order USING (order_sk)
#   GROUP BY dim_order.delivery_days
#
# Z-order: purchase_date_sk, review_score — time-range + score filters.

@dp.temporary_view(name="fact_reviews_stage")
def fact_reviews_stage() -> DataFrame:
    reviews = (
        spark.readStream.option("readChangeFeed", "true").table(silver("reviews"))
        .filter(F.col("_change_type") != "update_preimage")
    )

    dim_order_ = spark.read.table(gold("dim_order")).select(
        "order_sk",
        "order_id",
        "purchase_date_sk",
    )

    return (
        reviews
        .join(dim_order_, on="order_id", how="left")
        .withColumn("review_sk",
            surrogate_key(F.col("review_id"), F.col("order_id"))
        )
        .withColumn(
            "has_comment_title",
            F.col("review_comment_title").isNotNull().cast(BooleanType()),
        )
        .withColumn(
            "has_comment_message",
            F.col("review_comment_message").isNotNull().cast(BooleanType()),
        )
        # This timestamp is used to make the audit trail look more realistic
        # to the static datatset used for this project
        # For a real-world pipeline, this would be F.current_timestamp(
        .withColumn("gold_processed_at", F.to_timestamp(
                F.concat(
                    F.col("silver_processed_at"),
                    F.lit(" "),
                    F.date_format(F.current_timestamp(), "HH:mm:ss"),
                )))
        .select(
            # Surrogate keys
            "review_sk",
            "order_sk",
            "purchase_date_sk",
            # Natural keys
            "review_id",
            "order_id",
            # Measures & attributes
            "review_score",
            "review_creation_date",
            "review_answer_timestamp",
            "has_comment_title",
            "has_comment_message",
            # Metadata
            "silver_processed_at",
            "gold_processed_at",
            CDC_OP_COL,
            CDC_COMMIT_COL,
        )
    )


dp.create_streaming_table(
    name=gold("fact_reviews"),
    comment="Gold fact: one row per review_id + order_id with CSAT measures",
    table_properties=gold_table_properties(["purchase_date_sk", "review_score"]),
)

dp.create_auto_cdc_flow(
    target             = gold("fact_reviews"),
    source             = "fact_reviews_stage",
    keys               = ["review_sk"],
    sequence_by        = F.coalesce(F.col(CDC_COMMIT_COL), F.col("silver_processed_at")),
    apply_as_deletes   = F.col(CDC_OP_COL) == "D",
    stored_as_scd_type = 1,
)