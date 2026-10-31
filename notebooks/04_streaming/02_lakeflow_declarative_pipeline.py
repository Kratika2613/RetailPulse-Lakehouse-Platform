# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 04 - Lakeflow Spark Declarative Pipeline (DLT)
# MAGIC 
# MAGIC **Purpose**: End-to-end declarative pipeline utilizing Lakeflow Spark Declarative Pipelines (Delta Live Tables).
# MAGIC 
# MAGIC **Highlights**:
# MAGIC - Automatic DAG resolution.
# MAGIC - Built-in Data Quality Expectations (`@dlt.expect_or_drop`).
# MAGIC - Seamless Bronze -> Silver -> Gold state management.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# ==============================================================================
# 1. BRONZE DECLARATIVE TABLES (Auto Loader Streaming Tables)
# ==============================================================================

@dlt.table(
    name="dlt_bronze_orders",
    comment="Raw streaming orders landed into Bronze via Lakeflow Auto Loader",
    table_properties={"quality": "bronze"}
)
def dlt_bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/retailpulse_catalog/bronze/batch_landing/orders.csv")
    )

# ==============================================================================
# 2. SILVER DECLARATIVE TABLES (With Data Quality Expectations)
# ==============================================================================

@dlt.table(
    name="dlt_silver_orders",
    comment="Cleansed, deduplicated orders adhering to quality expectations",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "CAST(total_amount AS DOUBLE) > 0")
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL AND customer_id != 'NULL'")
def dlt_silver_orders():
    return (
        dlt.read_stream("dlt_bronze_orders")
        .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(12,2)"))
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("shipping_city", F.initcap(F.trim(F.col("shipping_city"))))
    )

# ==============================================================================
# 3. GOLD DECLARATIVE ANALYTICAL MART
# ==============================================================================

@dlt.table(
    name="dlt_gold_daily_metrics",
    comment="Gold analytical mart aggregated directly by Lakeflow",
    table_properties={"quality": "gold"}
)
def dlt_gold_daily_metrics():
    return (
        dlt.read("dlt_silver_orders")
        .groupBy("order_date")
        .agg(
            F.sum("total_amount").alias("daily_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("total_amount").alias("average_order_value")
        )
    )

