# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 01 - Streaming Ingestion via Auto Loader
# MAGIC 
# MAGIC Streams JSON event payloads from the landing volume into the `bronze.raw_order_events` Delta table.
# MAGIC 
# MAGIC **Why Auto Loader (`cloudFiles`)?**:
# MAGIC - Standard file streaming needs expensive directory listings (S3/cloud storage API limits). Auto Loader scales efficiently using cloud file notification queues.
# MAGIC - `cloudFiles.schemaEvolutionMode = 'addNewColumns'` lets the stream adapt if mobile apps push new telemetry properties without failing the pipeline.
# MAGIC - Checkpointing stores exact byte offsets so we never reprocess or skip events on restart.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Pipeline paths inside Unity Catalog Volumes
CATALOG = "retailpulse_catalog"
SCHEMA = "bronze"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/streaming_landing"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/raw_order_events"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.raw_order_events"

# COMMAND ----------

# Base contract schema for incoming event payloads
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("device_type", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("metadata", StringType(), True) # Nested JSON string, parsed later in Silver
])

# COMMAND ----------

# Setup Auto Loader stream
streaming_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .schema(event_schema)
    .load(LANDING_PATH)
    # Add audit columns for traceability
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

# Trigger with availableNow=True: Processes all available files in micro-batches
# and terminates the stream, saving cluster costs (Production cost-saver)
query = (
    streaming_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()
print(f"Auto Loader micro-batch completed successfully into {TARGET_TABLE}.")
