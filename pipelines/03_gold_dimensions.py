# =============================================================================
# pipelines/03_gold_dimensions.py
# GOLD LAYER — Dimension tables with SCD Type II
#
# FREE EDITION FIX: single schema, layer-prefixed table names
#   Catalog : workspace
#   Schema  : retail360
#
#   gold_dim_customer  ← was dim_customer in 03_gold
#   gold_dim_product   ← was dim_product  in 03_gold
#   gold_dim_date      ← was dim_date     in 03_gold
#
# Reads FROM : silver_customers_clean, silver_products_clean
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# =============================================================================
# GOLD_DIM_CUSTOMER — SCD Type II
# =============================================================================

@dp.temporary_view(name = "stg_dim_customer")
def stg_dim_customer():
    return (
        dp.read_stream("silver_customers_clean")    # ← updated ref
        .select(
            F.col("customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("full_name"),
            F.col("dob"),
            F.col("segment"),
            F.col("city_type"),
            F.col("region"),
            F.col("outlet_type"),
            F.col("state"),
            F.col("country"),
            F.col("_ingestion_ts"),
            F.col("_source_file"),
        )
    )

dp.create_streaming_table(
    name    = "gold_dim_customer",              # prefixed: gold_
    comment = "Gold SCD Type II customer dimension. Tracks changes in segment, city_type, region.",
    table_properties = {
        "quality":                          "gold",
        "pipelines.autoOptimize.managed":   "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)

dp.create_auto_cdc_flow(
    target             = "gold_dim_customer",   # ← updated ref
    source             = "stg_dim_customer",
    keys               = ["customer_id"],
    sequence_by        = F.col("_ingestion_ts"),
    stored_as_scd_type = 2,
    column_list        = [
        "customer_id", "first_name", "last_name", "full_name",
        "dob", "segment", "city_type", "region",
        "outlet_type", "state", "country", "_source_file",
    ],
)


# =============================================================================
# GOLD_DIM_PRODUCT — SCD Type II
# =============================================================================

@dp.temporary_view(name = "stg_dim_product")
def stg_dim_product():
    return (
        dp.read_stream("silver_products_clean")     # ← updated ref
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("sub_category"),
            F.col("category_of_goods"),
            F.col("_ingestion_ts"),
            F.col("_source_file"),
        )
    )

dp.create_streaming_table(
    name    = "gold_dim_product",               # prefixed: gold_
    comment = "Gold SCD Type II product dimension. Tracks changes in product_name, sub_category.",
    table_properties = {
        "quality":                          "gold",
        "pipelines.autoOptimize.managed":   "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
)

dp.create_auto_cdc_flow(
    target             = "gold_dim_product",    # ← updated ref
    source             = "stg_dim_product",
    keys               = ["product_id"],
    sequence_by        = F.col("_ingestion_ts"),
    stored_as_scd_type = 2,
    column_list        = [
        "product_id", "product_name",
        "sub_category", "category_of_goods", "_source_file",
    ],
)


# =============================================================================
# GOLD_DIM_DATE — Static date spine
# =============================================================================
@dp.materialized_view(
    name    = "gold_dim_date",                  # prefixed: gold_
    comment = "Gold static date spine covering 2015-2025.",
    table_properties = {
        "quality":                        "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_dim_date():
    return (
        spark.sql("""
            SELECT
                date_format(date, 'yyyyMMdd')       AS date_key,
                date                                AS full_date,
                year(date)                          AS year,
                quarter(date)                       AS quarter,
                month(date)                         AS month,
                date_format(date, 'MMMM')           AS month_name,
                date_format(date, 'MMM')            AS month_short,
                weekofyear(date)                    AS week_of_year,
                dayofmonth(date)                    AS day_of_month,
                dayofweek(date)                     AS day_of_week,
                date_format(date, 'EEEE')           AS day_name,
                date_format(date, 'EEE')            AS day_short,
                CASE WHEN dayofweek(date) IN (1,7)
                     THEN true ELSE false END        AS is_weekend,
                CASE WHEN dayofweek(date) IN (1,7)
                     THEN false ELSE true END        AS is_weekday,
                date_format(date, 'yyyy-MM')        AS year_month,
                concat('Q', quarter(date), '-',
                       year(date))                  AS quarter_label
            FROM (
                SELECT explode(
                    sequence(
                        to_date('2015-01-01'),
                        to_date('2025-12-31'),
                        interval 1 day
                    )
                ) AS date
            )
        """)
    )