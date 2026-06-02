# Data Dictionary — retail360 Data Lakehouse

## Source Data (25 columns)

| Column | Type | Description |
|---|---|---|
| Customer ID | String | Unique customer identifier |
| Customer Name | String | Customer first name |
| Last Name | String | Customer last name |
| Date of Birth | String | Customer DOB (cast to date in Silver) |
| Sales | Double | Order sales amount |
| Year | Integer | Sales year |
| Outlet Type | String | Large / Medium / Small |
| City Type | String | Tier 1 / Tier 2 / Village |
| Category of Goods | String | Product category |
| Region | String | East / West / North / South |
| Country | String | Country of sale |
| Segment | String | Consumer / Corporate |
| Sales Date | String | Date of sale |
| Order ID | String | Unique order identifier |
| Order Date | String | Date order placed |
| Ship Date | String | Date order shipped |
| Ship Mode | String | Same Day / First / Second / Standard Class |
| State | String | State of delivery |
| Postal Code | Long | Delivery postal code |
| Product ID | String | Unique product identifier |
| Sub-Category | String | Product sub-category |
| Product Name | String | Full product name |
| Quantity | Integer | Items ordered (1–10) |
| Discount | Double | Discount applied (0.0–0.5) |
| Profit | Double | Profit on order |

---

## Bronze Layer

### bronze_raw_sales
Raw ingested data with audit columns. Append-only.

| Column | Type | Description |
|---|---|---|
| All source columns (renamed) | Various | Cleaned column names (underscores, lowercase) |
| _source_file | String | Source CSV file path |
| _ingestion_ts | Timestamp | When the row was ingested |
| _ingestion_year | String | Partition column — ingestion year |
| _ingestion_month | String | Partition column — ingestion month |
| _file_modification_time | Timestamp | Source file modification timestamp |
| _rescued_data | String | JSON of any fields that didn't match schema |

### bronze_quarantine
Rows rejected from bronze_raw_sales by DQ expectations.

| Column | Added | Description |
|---|---|---|
| _failure_reason | Bronze | Why the row was rejected |

---

## Silver Layer

### silver_orders_clean
Cleansed transactional order data with derived columns.

| Column | Type | Description |
|---|---|---|
| order_id | String | Primary key |
| customer_id | String | FK → silver_customers_clean |
| product_id | String | FK → silver_products_clean |
| order_date | Date | Cast from string |
| ship_date | Date | Cast from string |
| sales_date | Date | Cast from string |
| sales | Double | Order revenue |
| quantity | Integer | Items ordered |
| discount | Double | Discount fraction |
| profit | Double | Profit amount |
| net_sales | Double | sales × (1 - discount) |
| profit_margin_pct | Double | profit / sales |
| shipping_days | Integer | ship_date - order_date |
| order_year | String | Partition column |
| order_month | String | Month of order |

### silver_customers_clean
One row per customer (deduped). Entity dimension.

| Column | Type | Description |
|---|---|---|
| customer_id | String | Primary key |
| first_name | String | Trimmed first name |
| last_name | String | Trimmed last name |
| full_name | String | Concatenated full name |
| dob | Date | Date of birth |
| segment | String | Consumer / Corporate |
| city_type | String | Tier 1 / Tier 2 / Village |
| region | String | East / West / North / South |
| outlet_type | String | Large / Medium / Small |
| state | String | State |
| country | String | Country |

### silver_products_clean
One row per product (deduped). Entity dimension.

| Column | Type | Description |
|---|---|---|
| product_id | String | Primary key |
| product_name | String | Cleaned product name |
| sub_category | String | Sub-category |
| category_of_goods | String | Top-level category |

---

## Gold Layer

### gold_dim_customer (SCD Type II)
Full history of customer attribute changes.

| Column | Type | Description |
|---|---|---|
| customer_id | String | Natural key |
| full_name | String | Customer full name |
| segment | String | Consumer / Corporate |
| city_type | String | Tier 1 / Tier 2 / Village |
| region | String | East / West / North / South |
| outlet_type | String | Large / Medium / Small |
| state | String | State |
| country | String | Country |
| __START_AT | Timestamp | When this version became active |
| __END_AT | Timestamp | When this version expired (NULL = current) |

### gold_dim_product (SCD Type II)
Full history of product attribute changes.

| Column | Type | Description |
|---|---|---|
| product_id | String | Natural key |
| product_name | String | Product name |
| sub_category | String | Sub-category |
| category_of_goods | String | Top-level category |
| __START_AT | Timestamp | Version start |
| __END_AT | Timestamp | Version end (NULL = current) |

### gold_dim_date (Static)
Date spine covering 2015–2025.

| Column | Type | Description |
|---|---|---|
| date_key | String | YYYYMMDD format key |
| full_date | Date | Calendar date |
| year | Integer | Year number |
| quarter | Integer | Quarter (1–4) |
| month | Integer | Month number |
| month_name | String | Full month name |
| week_of_year | Integer | ISO week number |
| day_of_week | Integer | Day number (1=Sun) |
| day_name | String | Full day name |
| is_weekend | Boolean | True for Sat/Sun |
| year_month | String | YYYY-MM format |
| quarter_label | String | Q1-2026 format |

### gold_fact_sales (Star Schema Fact)
Central fact table — orders joined to current dimension records.

| Column | Type | Description |
|---|---|---|
| order_id | String | Degenerate dimension (order key) |
| customer_id | String | FK → gold_dim_customer |
| product_id | String | FK → gold_dim_product |
| date_key | String | FK → gold_dim_date |
| sales | Double | Order revenue |
| quantity | Integer | Items ordered |
| discount | Double | Discount applied |
| profit | Double | Profit |
| net_sales | Double | Revenue after discount |
| profit_margin_pct | Double | Profit / Sales ratio |
| shipping_days | Integer | Days to ship |
| ship_mode | String | Shipping method |
| order_year | String | Partition column |

---

## KPI Tables (Materialized Views)

| Table | Grain | Key Metrics |
|---|---|---|
| gold_kpi_sales_by_region_category | region × category × year | total_sales, total_profit, overall_margin, total_orders |
| gold_kpi_customer_segment_performance | segment × city_type × outlet_type × year | unique_customers, avg_revenue_per_customer, avg_shipping_days |
| gold_kpi_product_performance | product_id × sub_category × category | total_sales, avg_profit_margin, sales_rank_in_category |
| gold_kpi_monthly_sales_trend | year_month × region × category | monthly_sales, mom_growth_pct, prev_month_sales |
| gold_kpi_outlet_city_analysis | outlet_type × city_type × region × year | profit_pct, avg_shipping_days, pct_same_day, pct_standard_class |
