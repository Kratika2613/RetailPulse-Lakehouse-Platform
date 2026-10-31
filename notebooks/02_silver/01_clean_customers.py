# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 02 - Silver Customers Transformation
# MAGIC 
# MAGIC **Purpose**: Clean, standardize, and deduplicate raw customer records.
# MAGIC 
# MAGIC **Transformations**:
# MAGIC 1. **Deduplication**: Keep the most recently signed-up record for any duplicate `customer_id`.
# MAGIC 2. **Standardization**: Title-case cities (e.g. `mUMBAI` -> `Mumbai`), clean whitespaces, lowercase email.
# MAGIC 3. **Data Quality Validation**: Email regex pattern matching.
# MAGIC 4. **Upsert (MERGE INTO)**: Idempotent merge into `silver.customers`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "retailpulse_catalog"
BRONZE_TABLE = f"{CATALOG}.bronze.raw_customers"
SILVER_TABLE = f"{CATALOG}.silver.customers"

# COMMAND ----------

# 1. Read Bronze Customers
bronze_df = spark.table(BRONZE_TABLE)

# 2. Deduplication & Standardization Pipeline
# Regex pattern for RFC 5322 standard email validation
EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

window_spec = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc(), F.col("_ingest_timestamp").desc())

cleansed_customers_df = (
    bronze_df
    # Remove records with missing customer identifier
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != "NULL"))
    # Window function to pick most authoritative record per customer_id
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    # Clean and standardize string attributes
    .withColumn("first_name", F.trim(F.initcap(F.col("first_name"))))
    .withColumn("last_name", F.trim(F.initcap(F.col("last_name"))))
    .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .withColumn("city", F.trim(F.initcap(F.col("city"))))
    .withColumn("state", F.trim(F.initcap(F.col("state"))))
    .withColumn("country", F.trim(F.initcap(F.col("country"))))
    .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("is_valid_email", F.col("email").rlike(EMAIL_REGEX))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .select(
        "customer_id", "first_name", "last_name", "full_name", "email",
        "phone", "city", "state", "country", "signup_date",
        "is_valid_email", "_silver_processed_at"
    )
)

# COMMAND ----------

# 3. Upsert into Silver Delta Table (MERGE INTO - SCD Type 1)
silver_delta = DeltaTable.forName(spark, SILVER_TABLE)

(
    silver_delta.alias("target")
    .merge(
        cleansed_customers_df.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"Upserted clean customer records into {SILVER_TABLE}")

