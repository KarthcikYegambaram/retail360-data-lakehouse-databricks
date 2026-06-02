# =============================================================================
# config/pipeline_config.py
# retail360 Data Lakehouse — Shared Configuration
# =============================================================================

# ── Catalog & Schema ──────────────────────────────────────────────────────────
CATALOG          = "workspace"
SCHEMA           = "retail360"
INGESTION_SCHEMA = "00_ingestionlayer"

# ── Volume Paths ──────────────────────────────────────────────────────────────
LANDING_PATH = (
    f"/Volumes/retail360_data_lakehouse/{INGESTION_SCHEMA}/raw_data/"
)
SCHEMA_CHECKPOINT = (
    f"/Volumes/retail360_data_lakehouse/{INGESTION_SCHEMA}"
    f"/raw_data/_checkpoints/bronze_schema/"
)
QUARANTINE_CHECKPOINT = (
    f"/Volumes/retail360_data_lakehouse/{INGESTION_SCHEMA}"
    f"/raw_data/_checkpoints/bronze_quarantine_schema/"
)

# ── Bronze Table Names ────────────────────────────────────────────────────────
BRONZE_RAW_SALES  = "bronze_raw_sales"
BRONZE_QUARANTINE = "bronze_quarantine"

# ── Silver Table Names ────────────────────────────────────────────────────────
SILVER_CUSTOMERS  = "silver_customers_clean"
SILVER_PRODUCTS   = "silver_products_clean"
SILVER_ORDERS     = "silver_orders_clean"
SILVER_QUARANTINE = "silver_quarantine"

# ── Gold Dimension Table Names ────────────────────────────────────────────────
GOLD_DIM_CUSTOMER = "gold_dim_customer"
GOLD_DIM_PRODUCT  = "gold_dim_product"
GOLD_DIM_DATE     = "gold_dim_date"

# ── Gold Fact Table Name ──────────────────────────────────────────────────────
GOLD_FACT_SALES   = "gold_fact_sales"

# ── Gold KPI Table Names ──────────────────────────────────────────────────────
GOLD_KPI_REGION_CATEGORY = "gold_kpi_sales_by_region_category"
GOLD_KPI_SEGMENT         = "gold_kpi_customer_segment_performance"
GOLD_KPI_PRODUCT         = "gold_kpi_product_performance"
GOLD_KPI_MONTHLY_TREND   = "gold_kpi_monthly_sales_trend"
GOLD_KPI_OUTLET_CITY     = "gold_kpi_outlet_city_analysis"
