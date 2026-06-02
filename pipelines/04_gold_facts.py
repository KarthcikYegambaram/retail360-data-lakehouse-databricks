# =============================================================================
# pipelines/04_gold_facts.py
# GOLD LAYER — gold_fact_sales (Star Schema Fact Table)
#
# Catalog : workspace
# Schema  : retail360
#
# FIX: Removed monotonically_increasing_id() for customer_key and product_key
#      monotonically_increasing_id() is not supported on streaming DataFrames.
#      Natural keys (customer_id, product_id) are used as foreign keys instead.
#      Delta Lake joins efficiently on string keys — surrogate int keys are
#      unnecessary in a Databricks lakehouse star schema.
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name    = "gold_fact_sales",
    comment = "Gold fact table: star schema centre. Joins silver_orders_clean with gold dimensions.",
    table_properties = {
        "quality":                            "gold",
        "pipelines.autoOptimize.managed":     "true",
        "delta.autoOptimize.optimizeWrite":   "true",
        "delta.autoOptimize.autoCompact":     "true",
        "delta.dataSkippingNumIndexedCols":   "8",
    },
    partition_cols = ["order_year"],
)
@dp.expect_or_fail("fact_order_id_not_null",    "order_id IS NOT NULL")
@dp.expect_or_fail("fact_customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect_or_fail("fact_product_id_not_null",  "product_id IS NOT NULL")
@dp.expect_or_fail("fact_sales_positive",       "sales > 0")
def gold_fact_sales():

    # ── Read silver orders (streaming) ────────────────────────────────────
    orders = dp.read_stream("silver_orders_clean")

    # ── dim_customer: batch read, filter to current SCD records ──────────
    # FIX: removed monotonically_increasing_id() — not supported on streams
    #      customer_id is the natural foreign key for the fact table
    dim_customer = (
        dp.read("gold_dim_customer")
        .filter(F.col("__END_AT").isNull())
        .select(
            F.col("customer_id"),
            F.col("full_name"),
            F.col("segment"),
            F.col("city_type"),
            F.col("region"),
            F.col("outlet_type"),
            F.col("state"),
        )
    )

    # ── dim_product: batch read, filter to current SCD records ───────────
    # FIX: removed monotonically_increasing_id() — not supported on streams
    #      product_id is the natural foreign key for the fact table
    dim_product = (
        dp.read("gold_dim_product")
        .filter(F.col("__END_AT").isNull())
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("sub_category"),
            F.col("category_of_goods"),
        )
    )

    # ── dim_date: batch read ──────────────────────────────────────────────
    dim_date = (
        dp.read("gold_dim_date")
        .select(
            F.col("full_date"),
            F.col("date_key"),
            F.col("year_month"),
            F.col("quarter_label"),
            F.col("is_weekend"),
        )
    )

    return (
        orders
        .join(dim_customer, on="customer_id", how="left")
        .join(dim_product,  on="product_id",  how="left")
        .join(dim_date, orders["order_date"] == dim_date["full_date"], how="left")
        .select(
            # ── Keys (natural keys as foreign keys) ───────────────────────
            orders["order_id"],
            orders["customer_id"],
            orders["product_id"],
            dim_date["date_key"],

            # ── Dates ─────────────────────────────────────────────────────
            orders["order_date"],
            orders["ship_date"],
            orders["sales_date"],

            # ── Measures ──────────────────────────────────────────────────
            orders["sales"],
            orders["quantity"],
            orders["discount"],
            orders["profit"],
            orders["net_sales"],
            orders["profit_margin_pct"],
            orders["shipping_days"],

            # ── Degenerate dimension ───────────────────────────────────────
            orders["ship_mode"],

            # ── Conformed attributes from dim_customer ─────────────────────
            dim_customer["full_name"],
            dim_customer["segment"],
            dim_customer["city_type"],
            dim_customer["region"],
            dim_customer["outlet_type"],
            dim_customer["state"],

            # ── Conformed attributes from dim_product ──────────────────────
            dim_product["product_name"],
            dim_product["category_of_goods"],
            dim_product["sub_category"],

            # ── Date attributes ────────────────────────────────────────────
            dim_date["year_month"],
            dim_date["quarter_label"],
            dim_date["is_weekend"],

            # ── Partition helper ───────────────────────────────────────────
            F.year(orders["order_date"]).cast("string").alias("order_year"),

            # ── Audit ──────────────────────────────────────────────────────
            orders["_source_file"],
            orders["_ingestion_ts"],
        )
    )