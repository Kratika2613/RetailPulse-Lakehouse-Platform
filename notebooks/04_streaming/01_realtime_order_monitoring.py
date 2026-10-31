# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 04 - Real-Time Streaming Order Velocity Monitoring
# MAGIC 
# MAGIC **Purpose**: Calculate real-time sliding window aggregates of order events (Event Velocity & Bottlenecks) directly over streaming data using PySpark Structured Streaming with Watermarking.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "retailpulse_catalog"
SILVER_EVENTS = f"{CATALOG}.silver.order_events"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/bronze/checkpoints/streaming_velocity_monitor"

# COMMAND ----------

# Stream from Silver order events with 10-minute watermark
events_stream = (
    spark.readStream
    .table(SILVER_EVENTS)
    .withWatermark("event_timestamp", "10 minutes")
)

# 5-minute sliding window with 1-minute slide
windowed_velocity_df = (
    events_stream
    .groupBy(
        F.window("event_timestamp", "5 minutes", "1 minute"),
        "event_type"
    )
    .agg(
        F.count("event_id").alias("event_count"),
        F.approx_count_distinct("customer_id").alias("distinct_active_users")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "event_type",
        "event_count",
        "distinct_active_users"
    )
)

# COMMAND ----------

# Write streaming aggregates to in-memory console / gold table
query = (
    windowed_velocity_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.gold.agg_realtime_order_velocity")
)

query.awaitTermination()
print("Real-time windowed velocity aggregation complete.")

