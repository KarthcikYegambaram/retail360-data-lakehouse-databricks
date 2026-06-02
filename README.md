# 🏪 retail360 — Data Lakehouse Pipeline

> End-to-end Medallion Architecture pipeline built on **Databricks Lakeflow Spark Declarative Pipelines (SDP)**, ingesting raw retail CSV data through Bronze → Silver → Gold layers with SCD Type II dimensions and KPI aggregations.

---

## 📌 Project Overview

This project demonstrates a production-style data engineering pipeline for a retail sales domain. Raw CSV files land in a cloud Volume, are incrementally ingested using Auto Loader, cleansed through a Silver layer, and transformed into a Star Schema Gold layer ready for BI consumption.

**Domain:** Retail Sales Analytics  
**Data:** 25-column retail transactions — orders, customers, products, regions, shipments  
**Records:** ~10,000+ sales transactions across multiple regions and categories  

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│              CSV Files (retail sales transactions)              │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Auto Loader (cloudFiles)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  │  workspace.retail360                           │
│                                                                 │
│   bronze_raw_sales          bronze_quarantine                   │
│   (append-only, raw)        (DQ rejected rows)                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  cleanse + validate + deduplicate
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  │  workspace.retail360                           │
│                                                                 │
│   silver_customers_clean    silver_products_clean               │
│   silver_orders_clean       silver_quarantine                   │
└────────┬────────────────────────────┬───────────────────────────┘
         │  SCD Type II (CDC flow)    │  Star Schema join
         ▼                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  │  workspace.retail360                             │
│                                                                 │
│  Dimensions              Fact                KPIs               │
│  gold_dim_customer ──┐                                          │
│  gold_dim_product  ──┼──▶ gold_fact_sales ──▶ gold_kpi_*  (5)  │
│  gold_dim_date     ──┘                                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Platform | Databricks (Free Edition) |
| Pipeline API | Lakeflow Spark Declarative Pipelines — 2026 (`from pyspark import pipelines as dp`) |
| Storage format | Delta Lake |
| Ingestion | Auto Loader (cloudFiles) |
| Orchestration | Lakeflow Pipelines (Triggered mode) |
| Governance | Unity Catalog |
| Language | Python (PySpark) |

---

## 📂 Repository Structure

```
retail360-data-lakehouse/
│
├── README.md
├── .gitignore
│
├── pipelines/
│   ├── 01_bronze_ingestion.py       # Auto Loader → raw_sales + quarantine
│   ├── 02_silver_cleansing.py       # Cleanse, validate, deduplicate
│   ├── 03_gold_dimensions.py        # SCD Type II dims + static date spine
│   ├── 04_gold_facts.py             # Star schema fact table
│   └── 05_gold_aggregations.py      # 5 KPI materialized views
│
├── config/
│   └── pipeline_config.py           # Catalog, schema, path constants
│
└── docs/
    └── data_dictionary.md           # Table and column descriptions
```

---

## 🗂️ Pipeline Layers

### Bronze — Raw Ingestion
- Auto Loader watches `/Volumes/.../raw_data/` for new CSV files
- Exactly-once ingestion via checkpoint
- Explicit schema — no type inference drift
- `rescuedDataColumn` captures unexpected fields safely
- DQ rules: `order_id NOT NULL`, `sales > 0`, `quantity BETWEEN 1 AND 10`
- Failed rows routed to `bronze_quarantine` with `_failure_reason`

### Silver — Cleansed & Validated
- Type casting (dates, doubles, ints)
- Column standardisation (trim, uppercase, rename)
- Streaming-safe deduplication using `dropDuplicates()` + `withWatermark()`
- Warn-only expectations for non-critical attributes
- `silver_quarantine` captures downstream DQ failures

### Gold — Business Layer
- **Dimensions:** SCD Type II via `dp.create_auto_cdc_flow()` — full history preserved, `__END_AT IS NULL` identifies current records
- **Fact table:** Star schema join of orders + current dimension records
- **KPI tables (5):** Pre-aggregated `@dp.materialized_view` tables for BI

---

## 📊 Gold Tables

| Table | Type | Description |
|---|---|---|
| `gold_dim_customer` | SCD Type II | Customer master with segment, region, outlet history |
| `gold_dim_product` | SCD Type II | Product master with category, sub-category history |
| `gold_dim_date` | Static | Date spine 2015–2025 with week, quarter, weekend flags |
| `gold_fact_sales` | Fact | Star schema — orders joined to all dimensions |
| `gold_kpi_sales_by_region_category` | Materialized View | Revenue, profit, orders by region × category |
| `gold_kpi_customer_segment_performance` | Materialized View | Metrics by segment × city tier × outlet type |
| `gold_kpi_product_performance` | Materialized View | Product-level sales rank within category |
| `gold_kpi_monthly_sales_trend` | Materialized View | MoM growth rate with lag comparison |
| `gold_kpi_outlet_city_analysis` | Materialized View | Outlet × city profitability with ship mode mix |

---

## ⚙️ Key Engineering Decisions

| Decision | Reason |
|---|---|
| `@dp.table` for Bronze/Silver/Fact | Streaming sources — incremental processing via `readStream` |
| `@dp.materialized_view` for Gold KPIs | Batch reads from fact table — full recompute each run |
| `dropDuplicates()` instead of `ROW_NUMBER()` | `ROW_NUMBER()` window function not supported on streaming DataFrames |
| Natural keys instead of `monotonically_increasing_id()` | `monotonically_increasing_id()` not supported on streaming DataFrames |
| Single schema (`workspace.retail360`) | Databricks Free Edition restriction — single catalog/schema per pipeline |
| `rescuedDataColumn` on Auto Loader | Prevents silent data loss on schema mismatches |

---

## 🚀 How to Run

**Prerequisites:**
- Databricks workspace (Free Edition or above)
- Unity Catalog enabled
- Volume created at `/Volumes/retail360_data_lakehouse/00_ingestionlayer/raw_data/`

**Setup:**
```sql
-- Create target schema
CREATE SCHEMA IF NOT EXISTS workspace.retail360;
```

**Steps:**
1. Upload pipeline files to Databricks workspace
2. Create a new **ETL Pipeline** in Lakeflow Pipelines Editor
3. Add all 5 files as **Transformation** source files
4. Set **Catalog:** `workspace` | **Schema:** `retail360`
5. Drop a CSV file into the landing Volume
6. Click **Start → Full refresh** (first run only)
7. Subsequent runs: **Start** (incremental — Auto Loader picks up only new files)

---

## 📐 Data Quality Framework

| Layer | Expectation | Behaviour |
|---|---|---|
| Bronze | `order_id IS NOT NULL` | Drop row → quarantine |
| Bronze | `sales > 0` | Drop row → quarantine |
| Bronze | `quantity BETWEEN 1 AND 10` | Drop row → quarantine |
| Silver | `order_date IS NOT NULL` | Drop row → quarantine |
| Silver | `discount BETWEEN 0 AND 0.5` | Drop row → quarantine |
| Silver | `ship_mode IN (valid values)` | Warn only — keep row |
| Gold Fact | `order_id IS NOT NULL` | Halt pipeline |
| Gold Fact | `sales > 0` | Halt pipeline |

---

## 💡 Concepts Demonstrated

- ✅ Medallion Architecture (Bronze → Silver → Gold)
- ✅ Auto Loader incremental ingestion with schema enforcement
- ✅ Lakeflow SDP 2026 API (`from pyspark import pipelines as dp`)
- ✅ Data quality expectations (warn / drop / fail)
- ✅ SCD Type II with CDC flow (`dp.create_auto_cdc_flow`)
- ✅ Star Schema design (fact + 3 dimensions)
- ✅ Streaming-safe deduplication (`dropDuplicates` + watermark)
- ✅ Quarantine pattern for rejected records
- ✅ Pre-aggregated KPI materialized views
- ✅ Unity Catalog governance

---

## 👤 Karthick

**Karthick** — Data Engineer  
Built as a portfolio project demonstrating Databricks data engineering capabilities.
