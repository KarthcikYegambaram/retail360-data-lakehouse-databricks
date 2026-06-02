# =============================================================================
# pipelines/02_silver_cleansing.py
# SILVER LAYER — Cleansed and standardized tables
#
# Catalog : workspace
# Schema  : retail360
#
# FIX: Replaced ROW_NUMBER() window deduplication with dropDuplicates()
#      ROW_NUMBER() over non-time windows is not supported on streaming
#      DataFrames — Structured Streaming only supports time-based windows.
#
# Deduplication strategy:
#   customers_clean → dropDuplicates(["customer_id"])
#   products_clean  → dropDuplicates(["product_id"])
#   orders_clean    → withWatermark + dropDuplicates(["order_id"])
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# =============================================================================
# TABLE 1: silver_customers_clean
# =============================================================================
@dp.table(
    name    = "silver_customers_clean",
    comment = "Silver: cleansed and deduped customer records. One row per customer_id.",
    table_properties = {
        "quality":                          "silver",
        "pipelines.autoOptimize.managed":   "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)
@dp.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect_or_drop("first_name_not_null",  "first_name IS NOT NULL")
@dp.expect_or_drop("valid_dob",
    "dob IS NULL OR (dob < current_date() AND dob > date('1900-01-01'))"
)
@dp.expect("valid_segment",     "segment IN ('Consumer', 'Corporate')")
@dp.expect("valid_city_type",   "city_type IN ('Tier 1', 'Tier 2', 'Village')")
@dp.expect("valid_outlet_type", "outlet_type IN ('Large', 'Medium', 'Small')")
@dp.expect("valid_region",      "region IN ('East', 'South', 'West', 'North')")
def silver_customers_clean():
    return (
        dp.read_stream("bronze_raw_sales")
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("first_name")).alias("first_name"),
            F.trim(F.col("last_name")).alias("last_name"),
            F.concat_ws(" ",
                F.trim(F.col("first_name")),
                F.trim(F.col("last_name"))
            ).alias("full_name"),
            F.to_date(F.col("date_of_birth_raw"), "yyyy-MM-dd").alias("dob"),
            F.trim(F.upper(F.col("segment"))).alias("segment"),
            F.trim(F.col("city_type")).alias("city_type"),
            F.trim(F.col("region")).alias("region"),
            F.trim(F.col("outlet_type")).alias("outlet_type"),
            F.trim(F.col("state")).alias("state"),
            F.trim(F.col("country")).alias("country"),
            F.col("_ingestion_ts"),
            F.col("_source_file"),
        )
        # FIX: replaced Window + row_number() with dropDuplicates()
        # Keeps first occurrence of each customer_id seen in the stream
        .dropDuplicates(["customer_id"])
    )


# =============================================================================
# TABLE 2: silver_products_clean
# =============================================================================
@dp.table(
    name    = "silver_products_clean",
    comment = "Silver: cleansed and deduped product records. One row per product_id.",
    table_properties = {
        "quality":                          "silver",
        "pipelines.autoOptimize.managed":   "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)
@dp.expect_or_drop("product_id_not_null",   "product_id IS NOT NULL")
@dp.expect_or_drop("product_name_not_null", "product_name IS NOT NULL")
@dp.expect_or_drop("category_not_null",     "category_of_goods IS NOT NULL")
@dp.expect("valid_category",
    "category_of_goods IN ('Fast Food','Electric Appliances','Dairy Products',"
    "'Sessional Fruits & Vegetables','Furniture','Household Items')"
)
def silver_products_clean():
    return (
        dp.read_stream("bronze_raw_sales")
        .select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.trim(F.col("sub_category")).alias("sub_category"),
            F.trim(F.col("category_of_goods")).alias("category_of_goods"),
            F.col("_ingestion_ts"),
            F.col("_source_file"),
        )
        # FIX: replaced Window + row_number() with dropDuplicates()
        # Keeps first occurrence of each product_id seen in the stream
        .dropDuplicates(["product_id"])
    )


# =============================================================================
# TABLE 3: silver_orders_clean
# =============================================================================
@dp.table(
    name    = "silver_orders_clean",
    comment = "Silver: cleansed, validated and typed order transactions. Deduped on order_id.",
    table_properties = {
        "quality":                          "silver",
        "pipelines.autoOptimize.managed":   "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
    partition_cols = ["order_year"],
)
@dp.expect_or_drop("order_id_not_null",
    "order_id IS NOT NULL AND LENGTH(TRIM(order_id)) > 0")
@dp.expect_or_drop("customer_id_not_null",
    "customer_id IS NOT NULL AND LENGTH(TRIM(customer_id)) > 0")
@dp.expect_or_drop("product_id_not_null",
    "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect_or_drop("sales_positive",
    "sales IS NOT NULL AND sales > 0 AND sales <= 100000")
@dp.expect_or_drop("quantity_in_range",
    "quantity IS NOT NULL AND quantity >= 1 AND quantity <= 10")
@dp.expect_or_drop("discount_in_range",
    "discount IS NOT NULL AND discount >= 0.0 AND discount <= 0.5")
@dp.expect_or_drop("profit_not_null",    "profit IS NOT NULL")
@dp.expect_or_drop("order_date_not_null","order_date IS NOT NULL")
@dp.expect_or_drop("ship_date_not_null", "ship_date IS NOT NULL")
@dp.expect("ship_after_order",  "ship_date >= order_date")
@dp.expect("valid_ship_mode",
    "ship_mode IN ('Same Day','Standard Class','Second Class','First Class')")
@dp.expect("valid_segment",     "segment IN ('Consumer','Corporate')")
@dp.expect("valid_region",      "region IN ('East','South','West','North')")
@dp.expect("profit_margin_sane","(profit / sales) <= 5.0")
def silver_orders_clean():
    return (
        dp.read_stream("bronze_raw_sales")
        .select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.to_date(F.col("order_date_raw"),  "yyyy-MM-dd").alias("order_date"),
            F.to_date(F.col("ship_date_raw"),   "yyyy-MM-dd").alias("ship_date"),
            F.to_date(F.col("sales_date_raw"),  "yyyy-MM-dd").alias("sales_date"),
            F.col("sales").cast("double"),
            F.col("quantity").cast("int"),
            F.col("discount").cast("double"),
            F.col("profit").cast("double"),
            F.round(F.col("sales") * (1.0 - F.col("discount")), 2).alias("net_sales"),
            F.round(
                F.when(F.col("sales") > 0, F.col("profit") / F.col("sales"))
                 .otherwise(F.lit(None)), 4
            ).alias("profit_margin_pct"),
            F.datediff(
                F.to_date(F.col("ship_date_raw"),  "yyyy-MM-dd"),
                F.to_date(F.col("order_date_raw"), "yyyy-MM-dd")
            ).alias("shipping_days"),
            F.trim(F.col("segment")).alias("segment"),
            F.trim(F.col("ship_mode")).alias("ship_mode"),
            F.trim(F.col("outlet_type")).alias("outlet_type"),
            F.trim(F.col("city_type")).alias("city_type"),
            F.trim(F.col("region")).alias("region"),
            F.trim(F.col("state")).alias("state"),
            F.trim(F.col("country")).alias("country"),
            F.col("postal_code"),
            F.trim(F.col("category_of_goods")).alias("category_of_goods"),
            F.trim(F.col("sub_category")).alias("sub_category"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.col("year"),
            F.col("_source_file"),
            F.col("_ingestion_ts"),
        )
        # FIX: replaced Window + row_number() with watermark + dropDuplicates()
        # Watermark tells Spark how long to wait for late-arriving duplicates.
        # 24 hours is safe for batch-like triggered pipeline runs.
        .withWatermark("_ingestion_ts", "24 hours")
        .dropDuplicates(["order_id"])
        .withColumn("order_year",  F.year(F.col("order_date")).cast("string"))
        .withColumn("order_month", F.month(F.col("order_date")).cast("string"))
    )


# =============================================================================
# SILVER QUARANTINE — no deduplication needed, captures all failing rows
# =============================================================================
@dp.table(
    name    = "silver_quarantine",
    comment = "Silver quarantine: rows rejected from silver_orders_clean.",
    table_properties = {"quality": "quarantine"},
)
def silver_quarantine():
    return (
        dp.read_stream("bronze_raw_sales")
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("sales"),
            F.col("quantity"),
            F.col("discount"),
            F.col("profit"),
            F.col("order_date_raw"),
            F.col("ship_date_raw"),
            F.col("_source_file"),
            F.col("_ingestion_ts"),
            F.when(F.col("order_id").isNull(), F.lit("order_id IS NULL"))
            .when(F.col("sales").isNull() | (F.col("sales") <= 0), F.lit("sales NULL or non-positive"))
            .when(
                F.col("quantity").isNull() | (F.col("quantity") < 1) | (F.col("quantity") > 10),
                F.lit("quantity out of range [1,10]")
            )
            .when(
                F.col("discount").isNull() | (F.col("discount") < 0) | (F.col("discount") > 0.5),
                F.lit("discount out of range [0.0, 0.5]")
            )
            .when(F.col("profit").isNull(), F.lit("profit IS NULL"))
            .otherwise(F.lit("other")).alias("failure_reason")
        )
        .filter(
            F.col("order_id").isNull() |
            F.col("sales").isNull() | (F.col("sales") <= 0) |
            F.col("quantity").isNull() |
            (F.col("quantity") < 1) | (F.col("quantity") > 10) |
            F.col("discount").isNull() |
            (F.col("discount") < 0) | (F.col("discount") > 0.5) |
            F.col("profit").isNull()
        )
    )