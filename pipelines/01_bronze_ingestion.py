# =============================================================================
# pipelines/01_bronze_ingestion.py
# BRONZE LAYER — Auto Loader → bronze_raw_sales Delta table
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CATALOG          = "workspace"
INGESTION_SCHEMA = "retail360"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
LANDING_PATH                 = f"/Volumes/{CATALOG}/{INGESTION_SCHEMA}/raw_data/"
SCHEMA_CHECKPOINT            = f"/Volumes/{CATALOG}/{INGESTION_SCHEMA}/raw_data/_checkpoints/bronze_schema/"
QUARANTINE_SCHEMA_CHECKPOINT = f"/Volumes/{CATALOG}/{INGESTION_SCHEMA}/raw_data/_checkpoints/bronze_quarantine_schema/"

# ---------------------------------------------------------------------------
# Explicit schema
# ---------------------------------------------------------------------------
RAW_SCHEMA = StructType([
    StructField("Customer ID",       StringType(),  True),
    StructField("Customer Name",     StringType(),  True),
    StructField("Last Name",         StringType(),  True),
    StructField("Date of Birth",     StringType(),  True),
    StructField("Sales",             DoubleType(),  True),
    StructField("Year",              IntegerType(), True),
    StructField("Outlet Type",       StringType(),  True),
    StructField("City Type",         StringType(),  True),
    StructField("Category of Goods", StringType(),  True),
    StructField("Region",            StringType(),  True),
    StructField("Country",           StringType(),  True),
    StructField("Segment",           StringType(),  True),
    StructField("Sales Date",        StringType(),  True),
    StructField("Order ID",          StringType(),  True),
    StructField("Order Date",        StringType(),  True),
    StructField("Ship Date",         StringType(),  True),
    StructField("Ship Mode",         StringType(),  True),
    StructField("State",             StringType(),  True),
    StructField("Postal Code",       LongType(),    True),
    StructField("Product ID",        StringType(),  True),
    StructField("Sub-Category",      StringType(),  True),
    StructField("Product Name",      StringType(),  True),
    StructField("Quantity",          IntegerType(), True),
    StructField("Discount",          DoubleType(),  True),
    StructField("Profit",            DoubleType(),  True),
])

# ---------------------------------------------------------------------------
# BRONZE TABLE — bronze_raw_sales
# ---------------------------------------------------------------------------
@dp.table(
    name    = "bronze_raw_sales",       # prefixed: bronze_
    comment = "Bronze layer: raw incremental ingestion from Volume landing zone. Append-only.",
    table_properties = {
        "quality":                          "bronze",
        "pipelines.autoOptimize.managed":   "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
    partition_cols = ["_ingestion_year", "_ingestion_month"],
)
@dp.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("sales_positive",    "sales > 0")
@dp.expect_or_drop("quantity_valid",    "quantity >= 1 AND quantity <= 10")
def bronze_raw_sales():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",             "csv")
        .option("cloudFiles.inferColumnTypes",   "false")
        .option("cloudFiles.schemaLocation",     SCHEMA_CHECKPOINT)
        .option("cloudFiles.maxFilesPerTrigger", "1000")
        .option("header",                        "true")
        .option("rescuedDataColumn",             "_rescued_data")
        .schema(RAW_SCHEMA)
        .load(LANDING_PATH)
        .withColumnRenamed("Customer ID",        "customer_id")
        .withColumnRenamed("Customer Name",      "first_name")
        .withColumnRenamed("Last Name",          "last_name")
        .withColumnRenamed("Date of Birth",      "date_of_birth_raw")
        .withColumnRenamed("Sales",              "sales")
        .withColumnRenamed("Year",               "year")
        .withColumnRenamed("Outlet Type",        "outlet_type")
        .withColumnRenamed("City Type",          "city_type")
        .withColumnRenamed("Category of Goods",  "category_of_goods")
        .withColumnRenamed("Region",             "region")
        .withColumnRenamed("Country",            "country")
        .withColumnRenamed("Segment",            "segment")
        .withColumnRenamed("Sales Date",         "sales_date_raw")
        .withColumnRenamed("Order ID",           "order_id")
        .withColumnRenamed("Order Date",         "order_date_raw")
        .withColumnRenamed("Ship Date",          "ship_date_raw")
        .withColumnRenamed("Ship Mode",          "ship_mode")
        .withColumnRenamed("State",              "state")
        .withColumnRenamed("Postal Code",        "postal_code")
        .withColumnRenamed("Product ID",         "product_id")
        .withColumnRenamed("Sub-Category",       "sub_category")
        .withColumnRenamed("Product Name",       "product_name")
        .withColumnRenamed("Quantity",           "quantity")
        .withColumnRenamed("Discount",           "discount")
        .withColumnRenamed("Profit",             "profit")
        .withColumn("_source_file",            F.col("_metadata.file_path"))
        .withColumn("_ingestion_ts",           F.current_timestamp())
        .withColumn("_ingestion_year",         F.year(F.current_timestamp()).cast("string"))
        .withColumn("_ingestion_month",        F.month(F.current_timestamp()).cast("string"))
        .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
    )


# ---------------------------------------------------------------------------
# BRONZE QUARANTINE TABLE — bronze_quarantine
# ---------------------------------------------------------------------------
@dp.table(
    name    = "bronze_quarantine",      # prefixed: bronze_
    comment = "Bronze quarantine: records rejected from bronze_raw_sales due to DQ failures.",
    table_properties = {"quality": "quarantine"},
)
def bronze_quarantine():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",             "csv")
        .option("cloudFiles.inferColumnTypes",   "false")
        .option("cloudFiles.schemaLocation",     QUARANTINE_SCHEMA_CHECKPOINT)
        .option("cloudFiles.maxFilesPerTrigger", "1000")
        .option("header",                        "true")
        .option("rescuedDataColumn",             "_rescued_data")
        .schema(RAW_SCHEMA)
        .load(LANDING_PATH)
        .withColumnRenamed("Customer ID",        "customer_id")
        .withColumnRenamed("Customer Name",      "first_name")
        .withColumnRenamed("Last Name",          "last_name")
        .withColumnRenamed("Date of Birth",      "date_of_birth_raw")
        .withColumnRenamed("Sales",              "sales")
        .withColumnRenamed("Year",               "year")
        .withColumnRenamed("Outlet Type",        "outlet_type")
        .withColumnRenamed("City Type",          "city_type")
        .withColumnRenamed("Category of Goods",  "category_of_goods")
        .withColumnRenamed("Region",             "region")
        .withColumnRenamed("Country",            "country")
        .withColumnRenamed("Segment",            "segment")
        .withColumnRenamed("Sales Date",         "sales_date_raw")
        .withColumnRenamed("Order ID",           "order_id")
        .withColumnRenamed("Order Date",         "order_date_raw")
        .withColumnRenamed("Ship Date",          "ship_date_raw")
        .withColumnRenamed("Ship Mode",          "ship_mode")
        .withColumnRenamed("State",              "state")
        .withColumnRenamed("Postal Code",        "postal_code")
        .withColumnRenamed("Product ID",         "product_id")
        .withColumnRenamed("Sub-Category",       "sub_category")
        .withColumnRenamed("Product Name",       "product_name")
        .withColumnRenamed("Quantity",           "quantity")
        .withColumnRenamed("Discount",           "discount")
        .withColumnRenamed("Profit",             "profit")
        .withColumn("_source_file",  F.col("_metadata.file_path"))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_failure_reason",
            F.when(F.col("order_id").isNull(), F.lit("order_id IS NULL"))
            .when(F.col("sales").isNull() | (F.col("sales") <= 0), F.lit("sales is NULL or <= 0"))
            .when(
                F.col("quantity").isNull() | (F.col("quantity") < 1) | (F.col("quantity") > 10),
                F.lit("quantity out of range [1, 10]")
            ).otherwise(F.lit("unknown"))
        )
        .filter(
            F.col("order_id").isNull() |
            F.col("sales").isNull() | (F.col("sales") <= 0) |
            F.col("quantity").isNull() |
            (F.col("quantity") < 1) | (F.col("quantity") > 10)
        )
    )