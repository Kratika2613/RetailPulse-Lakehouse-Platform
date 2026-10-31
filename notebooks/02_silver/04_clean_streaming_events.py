# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 02 - Silver Streaming Order Events
# MAGIC 
# MAGIC **Purpose**: Parse raw JSON payloads, unnest metadata, and structure streaming telemetry into `silver.order_events`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = "retailpulse_catalog"
BRONZE_EVENTS = f"{CATALOG}.bronze.raw_order_events"
SILVER_EVENTS = f"{CATALOG}.silver.order_events"

# COMMAND ----------

# Schema for unnesting the raw 'metadata' JSON field
metadata_schema = StructType([
    StructField("app_version", StringType(), True),
    StructField("network_type", StringType(), True)
])

bronze_events_df = spark.table(BRONZE_EVENTS)

parsed_events_df = (
    bronze_events_df
    .filter(F.col("event_id").isNotNull())
    .withColumn("parsed_meta", F.from_json(F.col("metadata"), metadata_schema))
    .withColumn("app_version", F.col("parsed_meta.app_version"))
    .withColumn("network_type", F.col("parsed_meta.network_type"))
    .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
    .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
    .withColumn("device_type", F.trim(F.initcap(F.col("device_type"))))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .select(
        "event_id", "order_id", "customer_id", "event_type",
        "event_timestamp", "device_type", "ip_address",
        "app_version", "network_type", "_silver_processed_at"
    )
)

# COMMAND ----------

# Idempotent write to Silver events
(
    parsed_events_df.write
    .format("delta")
    .mode("append")
    .partitionBy("event_type")
    .saveAsTable(SILVER_EVENTS)
)

print(f"Processed and partitioned streaming events into {SILVER_EVENTS}")

