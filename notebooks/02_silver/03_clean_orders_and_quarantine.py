# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 02 - Silver Orders Transformation & Quarantine Routing
# MAGIC 
# MAGIC **Purpose**: Deduplicate orders, validate financial & referential integrity, and route bad records into a Quarantine Dead-Letter Queue (DLQ).
# MAGIC 
# MAGIC **Industry Pattern**: Quarantine Pattern (Fail-safe ingestion without crashing downstream pipelines).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "retailpulse_catalog"
BRONZE_ORDERS = f"{CATALOG}.bronze.raw_orders"
SILVER_ORDERS = f"{CATALOG}.silver.orders"
QUARANTINE_TABLE = f"{CATALOG}.silver.quarantine_orders_dlq"

# COMMAND ----------

# 1. Read Bronze Orders
raw_orders_df = spark.table(BRONZE_ORDERS)

# 2. Deduplication using Window ROW_NUMBER
window_spec = Window.partitionBy("order_id").orderBy(
    F.col("order_timestamp").desc(),
    F.col("_ingest_timestamp").desc()
)

deduped_orders_df = (
    raw_orders_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

# 3. Data Quality Gate: Identify Valid vs Quarantine-bound Records
# Quality Rules:
# - total_amount MUST be strictly > 0
# - customer_id MUST NOT be null or placeholder 'CUST-99999'

condition_valid = (
    (F.col("total_amount") > 0) &
    (F.col("customer_id").isNotNull()) &
    (F.col("customer_id") != "NULL") &
    (F.col("customer_id") != "CUST-99999")
)

# 3A. Valid Orders Transformation
valid_orders_df = (
    deduped_orders_df
    .filter(condition_valid)
    .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_date", F.to_date(F.col("order_timestamp")))
    .withColumn("total_amount", F.col("total_amount").cast("decimal(12,2)"))
    .withColumn("order_status", F.trim(F.initcap(F.col("order_status"))))
    .withColumn("payment_method", F.trim(F.col("payment_method")))
    .withColumn("shipping_city", F.trim(F.initcap(F.col("shipping_city"))))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .select(
        "order_id", "customer_id", "order_timestamp", "order_date",
        "total_amount", "order_status", "payment_method",
        "shipping_city", "_silver_processed_at"
    )
)

# 3B. Invalid Records for Quarantine DLQ
quarantine_df = (
    deduped_orders_df
    .filter(~condition_valid)
    .withColumn("source_table", F.lit(BRONZE_ORDERS))
    .withColumn("record_payload", F.to_json(F.struct([F.col(c) for c in raw_orders_df.columns])))
    .withColumn(
        "failed_rule",
        F.when(F.col("total_amount") <= 0, "NEGATIVE_OR_ZERO_AMOUNT")
        .when((F.col("customer_id").isNull()) | (F.col("customer_id") == "NULL"), "MISSING_CUSTOMER_ID")
        .when(F.col("customer_id") == "CUST-99999", "ORPHAN_CUSTOMER_ID")
        .otherwise("UNKNOWN_INTEGRITY_VIOLATION")
    )
    .withColumn(
        "failure_details",
        F.concat(
            F.lit("Violated data quality gate with amount="),
            F.coalesce(F.col("total_amount").cast("string"), F.lit("NULL")),
            F.lit(" and customer_id="),
            F.coalesce(F.col("customer_id"), F.lit("NULL"))
        )
    )
    .withColumn("quarantined_at", F.current_timestamp())
    .select("source_table", "record_payload", "failed_rule", "failure_details", "quarantined_at")
)

# COMMAND ----------

# 4. Upsert Valid Orders into Silver Orders Table
orders_delta = DeltaTable.forName(spark, SILVER_ORDERS)

(
    orders_delta.alias("target")
    .merge(
        valid_orders_df.alias("source"),
        "target.order_id = source.order_id AND target.order_date = source.order_date"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# 5. Append Bad Records to Quarantine DLQ
quarantine_df.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)

print("Orders processing complete:")
print(f" - Clean orders merged: {valid_orders_df.count()}")
print(f" - Quarantined records isolated: {quarantine_df.count()}")

