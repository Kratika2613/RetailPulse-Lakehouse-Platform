# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Master Pipeline Orchestrator Runner
# MAGIC 
# MAGIC **Purpose**: Programmatic sequential execution of the RetailPulse Medallion Pipeline with performance telemetry logging.

# COMMAND ----------

import time
from datetime import datetime

pipeline_stages = [
    ("Bronze Batch Ingestion", "notebooks/01_bronze/01_batch_ingest_copy_into", 600),
    ("Bronze Streaming Ingestion", "notebooks/01_bronze/02_streaming_ingest_autoloader", 600),
    ("Silver Customers Cleaning", "notebooks/02_silver/01_clean_customers", 600),
    ("Silver Products Cleaning", "notebooks/02_silver/02_clean_products", 600),
    ("Silver Orders & Quarantine", "notebooks/02_silver/03_clean_orders_and_quarantine", 600),
    ("Silver Streaming Events", "notebooks/02_silver/04_clean_streaming_events", 600),
    ("Gold Star Schema Dimensions & Facts", "notebooks/03_gold/01_build_star_schema_dims_facts", 600),
    ("Gold Analytical KPI Marts", "notebooks/03_gold/02_agg_kpi_business_metrics", 600),
]

execution_audit = []
pipeline_start = time.time()

print(f"🚀 [START] RetailPulse Master Pipeline Triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

for stage_name, notebook_path, timeout in pipeline_stages:
    stage_start = time.time()
    print(f"\n▶ Executing: {stage_name} ({notebook_path})...")
    try:
        # In Databricks runtime, dbutils.notebook.run runs child notebooks
        status = dbutils.notebook.run(notebook_path, timeout)
        duration = round(time.time() - stage_start, 2)
        execution_audit.append((stage_name, "SUCCESS", duration, "Completed successfully"))
        print(f"  ✅ Completed in {duration}s")
    except Exception as e:
        duration = round(time.time() - stage_start, 2)
        execution_audit.append((stage_name, "FAILED", duration, str(e)))
        print(f"  ❌ FAILED in {duration}s. Reason: {e}")
        raise e

total_duration = round(time.time() - pipeline_start, 2)
print(f"\n🏁 [FINISH] RetailPulse Pipeline Completed in {total_duration}s")

