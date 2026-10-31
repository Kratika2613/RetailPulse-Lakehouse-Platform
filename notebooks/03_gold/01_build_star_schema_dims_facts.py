# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 03 - Gold Star Schema Dimensional Models
# MAGIC 
# MAGIC **Purpose**: Construct conformed Dimensions (`dim_customers`, `dim_products`) and central Transactional Fact (`fact_sales_orders`).
# MAGIC 
# MAGIC **Design Pattern**: Kimball Dimensional Modeling on Delta Lake.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "retailpulse_catalog"

SILVER_CUSTOMERS = f"{CATALOG}.silver.customers"
SILVER_PRODUCTS = f"{CATALOG}.silver.products"
SILVER_ORDERS = f"{CATALOG}.silver.orders"

GOLD_DIM_CUSTOMERS = f"{CATALOG}.gold.dim_customers"
GOLD_DIM_PRODUCTS = f"{CATALOG}.gold.dim_products"
GOLD_FACT_ORDERS = f"{CATALOG}.gold.fact_sales_orders"

# COMMAND ----------

# 1. Build Dimension: Customers
silver_cust_df = spark.table(SILVER_CUSTOMERS)

dim_customers_df = (
    silver_cust_df
    .withColumn("customer_tenure_days", F.datediff(F.current_date(), F.col("signup_date")))
    .withColumn("_updated_at", F.current_timestamp())
    .select(
        "customer_id", "full_name", "email", "phone",
        "city", "state", "signup_date", "customer_tenure_days",
        "_updated_at"
    )
)

dim_customers_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIM_CUSTOMERS)
print(f"Refreshed {GOLD_DIM_CUSTOMERS}")

# COMMAND ----------

# 2. Build Dimension: Products with Price Tiers
silver_prod_df = spark.table(SILVER_PRODUCTS)

dim_products_df = (
    silver_prod_df
    .withColumn(
        "price_tier",
        F.when(F.col("price") < 1000, "Budget")
        .when((F.col("price") >= 1000) & (F.col("price") <= 5000), "Mid-Range")
        .otherwise("Premium")
    )
    .withColumn("_updated_at", F.current_timestamp())
    .select(
        "product_id", "product_name", "category", "price",
        "price_tier", "stock_quantity", "is_active", "_updated_at"
    )
)

dim_products_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIM_PRODUCTS)
print(f"Refreshed {GOLD_DIM_PRODUCTS}")

# COMMAND ----------

# 3. Build Fact: Sales Orders
silver_orders_df = spark.table(SILVER_ORDERS)

fact_sales_orders_df = (
    silver_orders_df
    .withColumn("is_completed", F.col("order_status") == "Delivered")
    .withColumn("is_returned", F.col("order_status") == "Returned")
    .withColumn("_updated_at", F.current_timestamp())
    .select(
        "order_id", "customer_id", "order_timestamp", "order_date",
        "total_amount", "order_status", "payment_method",
        "shipping_city", "is_completed", "is_returned", "_updated_at"
    )
)

(
    fact_sales_orders_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .saveAsTable(GOLD_FACT_ORDERS)
)
print(f"Refreshed {GOLD_FACT_ORDERS}")

