# =============================================================================
# pipelines/05_gold_aggregations.py
# GOLD LAYER — KPI Aggregation tables
#
# FREE EDITION FIX: single schema, layer-prefixed table names
#   Catalog : workspace
#   Schema  : retail360
#
#   gold_kpi_sales_by_region_category       ← was kpi_sales_by_region_category
#   gold_kpi_customer_segment_performance   ← was kpi_customer_segment_performance
#   gold_kpi_product_performance            ← was kpi_product_performance
#   gold_kpi_monthly_sales_trend            ← was kpi_monthly_sales_trend
#   gold_kpi_outlet_city_analysis           ← was kpi_outlet_city_analysis
#
# Reads FROM : gold_fact_sales
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# KPI 1: Sales by Region & Category
# =============================================================================
@dp.materialized_view(
    name    = "gold_kpi_sales_by_region_category",  # prefixed: gold_
    comment = "Gold KPI: Total sales, profit and orders by region and category.",
    table_properties = {
        "quality":                        "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_kpi_sales_by_region_category():
    return (
        dp.read("gold_fact_sales")                  # ← updated ref
        .groupBy("region", "category_of_goods", "order_year")
        .agg(
            F.round(F.sum("sales"),         2).alias("total_sales"),
            F.round(F.sum("profit"),        2).alias("total_profit"),
            F.round(F.sum("net_sales"),     2).alias("total_net_sales"),
            F.countDistinct("order_id")      .alias("total_orders"),
            F.sum("quantity")                .alias("total_quantity"),
            F.round(F.avg("discount"),      4).alias("avg_discount"),
            F.round(F.avg("profit_margin_pct"), 4).alias("avg_profit_margin"),
            F.round(F.sum("profit") / F.sum("sales"), 4).alias("overall_margin"),
        )
        .withColumn("_gold_updated_ts", F.current_timestamp())
        .orderBy("region", "category_of_goods", "order_year")
    )


# =============================================================================
# KPI 2: Customer Segment Performance
# =============================================================================
@dp.materialized_view(
    name    = "gold_kpi_customer_segment_performance",  # prefixed: gold_
    comment = "Gold KPI: Revenue and order metrics by segment, city tier, outlet type.",
    table_properties = {
        "quality":                        "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_kpi_customer_segment_performance():
    return (
        dp.read("gold_fact_sales")                  # ← updated ref
        .groupBy("segment", "city_type", "outlet_type", "order_year")
        .agg(
            F.countDistinct("customer_id")   .alias("unique_customers"),
            F.countDistinct("order_id")      .alias("total_orders"),
            F.round(F.sum("sales"),       2) .alias("total_sales"),
            F.round(F.sum("profit"),      2) .alias("total_profit"),
            F.round(F.avg("sales"),       2) .alias("avg_order_value"),
            F.round(
                F.sum("sales") / F.countDistinct("customer_id"), 2
            ).alias("avg_revenue_per_customer"),
            F.round(F.avg("shipping_days"), 1).alias("avg_shipping_days"),
            F.round(F.avg("discount"),    4)  .alias("avg_discount"),
        )
        .withColumn("_gold_updated_ts", F.current_timestamp())
    )


# =============================================================================
# KPI 3: Product Performance
# =============================================================================
@dp.materialized_view(
    name    = "gold_kpi_product_performance",       # prefixed: gold_
    comment = "Gold KPI: Product-level sales, profit and volume metrics.",
    table_properties = {
        "quality":                        "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_kpi_product_performance():
    window = Window.partitionBy("category_of_goods").orderBy(F.col("total_sales").desc())
    return (
        dp.read("gold_fact_sales")                  # ← updated ref
        .groupBy("product_id", "sub_category", "category_of_goods")
        .agg(
            F.countDistinct("order_id")        .alias("total_orders"),
            F.sum("quantity")                  .alias("total_quantity_sold"),
            F.round(F.sum("sales"),         2) .alias("total_sales"),
            F.round(F.sum("profit"),        2) .alias("total_profit"),
            F.round(F.avg("sales"),         2) .alias("avg_order_sales"),
            F.round(F.avg("profit_margin_pct"), 4).alias("avg_profit_margin"),
            F.round(F.avg("discount"),      4) .alias("avg_discount"),
            F.countDistinct("customer_id")     .alias("unique_customers"),
        )
        .withColumn("sales_rank_in_category", F.rank().over(window))
        .withColumn("_gold_updated_ts", F.current_timestamp())
    )


# =============================================================================
# KPI 4: Monthly Sales Trend (MoM)
# =============================================================================
@dp.materialized_view(
    name    = "gold_kpi_monthly_sales_trend",       # prefixed: gold_
    comment = "Gold KPI: Month-over-month sales trend with MoM growth rate.",
    table_properties = {
        "quality":                        "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_kpi_monthly_sales_trend():
    window_trend = (
        Window
        .partitionBy("region", "category_of_goods")
        .orderBy("year_month")
    )
    monthly = (
        dp.read("gold_fact_sales")                  # ← updated ref
        .groupBy("year_month", "region", "category_of_goods")
        .agg(
            F.round(F.sum("sales"),   2).alias("monthly_sales"),
            F.round(F.sum("profit"),  2).alias("monthly_profit"),
            F.countDistinct("order_id") .alias("monthly_orders"),
            F.sum("quantity")           .alias("monthly_quantity"),
            F.round(F.avg("discount"), 4).alias("avg_discount"),
        )
    )
    return (
        monthly
        .withColumn("prev_month_sales", F.lag("monthly_sales").over(window_trend))
        .withColumn("mom_growth_pct",
            F.round(
                F.when(
                    F.col("prev_month_sales").isNotNull() & (F.col("prev_month_sales") > 0),
                    (F.col("monthly_sales") - F.col("prev_month_sales"))
                    / F.col("prev_month_sales") * 100
                ).otherwise(F.lit(None)), 2
            )
        )
        .withColumn("_gold_updated_ts", F.current_timestamp())
        .orderBy("year_month", "region", "category_of_goods")
    )


# =============================================================================
# KPI 5: Outlet & City Tier Analysis
# =============================================================================
@dp.materialized_view(
    name    = "gold_kpi_outlet_city_analysis",      # prefixed: gold_
    comment = "Gold KPI: Outlet type vs city tier profitability analysis.",
    table_properties = {
        "quality":                        "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_kpi_outlet_city_analysis():
    return (
        dp.read("gold_fact_sales")                  # ← updated ref
        .groupBy("outlet_type", "city_type", "region", "order_year")
        .agg(
            F.countDistinct("order_id")      .alias("total_orders"),
            F.countDistinct("customer_id")   .alias("unique_customers"),
            F.round(F.sum("sales"),       2) .alias("total_sales"),
            F.round(F.sum("profit"),      2) .alias("total_profit"),
            F.round(F.avg("sales"),       2) .alias("avg_order_value"),
            F.round(F.avg("profit_margin_pct"), 4).alias("avg_profit_margin"),
            F.round(F.sum("profit") / F.sum("sales") * 100, 2).alias("profit_pct"),
            F.round(F.avg("shipping_days"), 1).alias("avg_shipping_days"),
            F.round(
                F.sum(F.when(F.col("ship_mode") == "Same Day", 1).otherwise(0))
                / F.count("*") * 100, 1
            ).alias("pct_same_day"),
            F.round(
                F.sum(F.when(F.col("ship_mode") == "Standard Class", 1).otherwise(0))
                / F.count("*") * 100, 1
            ).alias("pct_standard_class"),
        )
        .withColumn("_gold_updated_ts", F.current_timestamp())
    )