# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 03 - Gold Aggregated KPI Analytical Marts
# MAGIC 
# MAGIC **Purpose**: Generate high-performance pre-aggregated summary tables for Databricks SQL BI Dashboards:
# MAGIC 1. `gold.agg_daily_sales_performance`
# MAGIC 2. `gold.agg_customer_lifetime_value`

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "retailpulse_catalog"
GOLD_FACT_ORDERS = f"{CATALOG}.gold.fact_sales_orders"
GOLD_DIM_CUSTOMERS = f"{CATALOG}.gold.dim_customers"

DAILY_SALES_MART = f"{CATALOG}.gold.agg_daily_sales_performance"
CUSTOMER_LTV_MART = f"{CATALOG}.gold.agg_customer_lifetime_value"

# COMMAND ----------

fact_orders = spark.table(GOLD_FACT_ORDERS)
dim_cust = spark.table(GOLD_DIM_CUSTOMERS)

# 1. Aggregate Daily Sales Performance Mart
daily_kpi_df = (
    fact_orders
    .groupBy("order_date")
    .agg(
        F.sum("total_amount").cast("decimal(15,2)").alias("total_revenue"),
        F.count("order_id").cast("int").alias("total_orders_placed"),
        F.sum(F.when(F.col("order_status") == "Delivered", 1).otherwise(0)).cast("int").alias("completed_orders"),
        F.sum(F.when(F.col("order_status") == "Cancelled", 1).otherwise(0)).cast("int").alias("cancelled_orders"),
        F.sum(F.when(F.col("order_status") == "Returned", 1).otherwise(0)).cast("int").alias("returned_orders"),
        F.avg("total_amount").cast("decimal(10,2)").alias("average_order_value"),
        F.sum(F.when(F.col("payment_method") == "UPI", 1).otherwise(0)).cast("int").alias("upi_payment_count"),
        F.sum(F.when(F.col("payment_method").isin("Credit Card", "Debit Card"), 1).otherwise(0)).cast("int").alias("card_payment_count")
    )
    .withColumn(
        "return_rate_pct",
        F.round((F.col("returned_orders") / F.col("total_orders_placed")) * 100, 2).cast("decimal(5,2)")
    )
    .withColumnRenamed("order_date", "sales_date")
    .withColumn("_generated_at", F.current_timestamp())
)

daily_kpi_df.write.format("delta").mode("overwrite").saveAsTable(DAILY_SALES_MART)
print(f"Refreshed Daily Sales Performance Mart: {DAILY_SALES_MART}")

# COMMAND ----------

# 2. Customer Lifetime Value (LTV) & Segmentation Mart
customer_agg = (
    fact_orders
    .groupBy("customer_id")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").cast("int").alias("total_orders"),
        F.sum("total_amount").cast("decimal(15,2)").alias("total_lifetime_spend"),
        F.avg("total_amount").cast("decimal(10,2)").alias("avg_order_value")
    )
)

customer_ltv_df = (
    dim_cust.alias("c")
    .join(customer_agg.alias("a"), F.col("c.customer_id") == F.col("a.customer_id"), "inner")
    .withColumn(
        "customer_segment",
        F.when(F.col("total_lifetime_spend") > 50000, "VIP (>50k)")
        .when((F.col("total_lifetime_spend") >= 15000) & (F.col("total_lifetime_spend") <= 50000), "Regular (15k-50k)")
        .otherwise("Occasional (<15k)")
    )
    .withColumn(
        "churn_risk_flag",
        F.when(F.datediff(F.current_date(), F.col("last_order_date")) > 90, True).otherwise(False)
    )
    .withColumnRenamed("full_name", "customer_name")
    .withColumn("_generated_at", F.current_timestamp())
    .select(
        "c.customer_id", "customer_name", "city", "first_order_date",
        "last_order_date", "total_orders", "total_lifetime_spend",
        "avg_order_value", "customer_segment", "churn_risk_flag",
        "_generated_at"
    )
)

customer_ltv_df.write.format("delta").mode("overwrite").saveAsTable(CUSTOMER_LTV_MART)
print(f"Refreshed Customer Lifetime Value Mart: {CUSTOMER_LTV_MART}")

