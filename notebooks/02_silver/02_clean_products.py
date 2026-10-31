# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 02 - Silver Products Transformation
# MAGIC 
# MAGIC **Purpose**: Clean, type-cast, and enforce business integrity constraints on product catalog data.
# MAGIC 
# MAGIC **Business Rules**:
# MAGIC 1. `price > 0`: Negative prices are invalid catalog anomalies.
# MAGIC 2. `stock_quantity >= 0`: Inventory cannot be negative.
# MAGIC 3. `is_active`: Boolean type casting.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "retailpulse_catalog"
BRONZE_TABLE = f"{CATALOG}.bronze.raw_products"
SILVER_TABLE = f"{CATALOG}.silver.products"

# COMMAND ----------

bronze_products_df = spark.table(BRONZE_TABLE)

# Filter out bad records and standardize
valid_products_df = (
    bronze_products_df
    .filter(
        (F.col("product_id").isNotNull()) &
        (F.col("price") > 0) &
        (F.col("stock_quantity") >= 0)
    )
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.initcap(F.col("category"))))
    .withColumn("price", F.col("price").cast("decimal(10,2)"))
    .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
    .withColumn("is_active", F.when(F.lower(F.col("is_active")) == "true", True).otherwise(False))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .select(
        "product_id", "product_name", "category", "price",
        "stock_quantity", "is_active", "_silver_processed_at"
    )
)

# COMMAND ----------

# Upsert (MERGE INTO)
silver_products_delta = DeltaTable.forName(spark, SILVER_TABLE)

(
    silver_products_delta.alias("target")
    .merge(
        valid_products_df.alias("source"),
        "target.product_id = source.product_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"Validated products successfully merged into {SILVER_TABLE}")

