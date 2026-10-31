# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Module 01 - Batch Ingestion via COPY INTO
# MAGIC 
# MAGIC Ingests raw batch CSV files (Customers, Products, Orders) from landing volumes into Bronze Delta tables.
# MAGIC 
# MAGIC **Engineering Decisions**:
# MAGIC - `COPY INTO` is preferred over standard `spark.read.csv().write` because it natively tracks loaded files in the Delta log, making re-runs completely idempotent (no duplicate rows if triggered multiple times).
# MAGIC - We attach `_ingest_timestamp` and `_source_file` to every record to ensure data lineage and traceability back to source files.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to the project catalog and bronze landing schema
# MAGIC USE CATALOG retailpulse_catalog;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Load Customers CSV
# MAGIC -- Note: We treat raw columns as strings initially to avoid ingestion failures on format errors.
# MAGIC -- Cleansing and casting happen in the Silver layer.
# MAGIC COPY INTO bronze.raw_customers
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         first_name,
# MAGIC         last_name,
# MAGIC         email,
# MAGIC         phone,
# MAGIC         city,
# MAGIC         state,
# MAGIC         country,
# MAGIC         signup_date,
# MAGIC         current_timestamp() AS _ingest_timestamp,
# MAGIC         input_file_name() AS _source_file
# MAGIC     FROM '/Volumes/retailpulse_catalog/bronze/batch_landing/customers.csv'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'false',
# MAGIC     'emptyValue' = 'NULL'
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC     'mergeSchema' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Load Products Catalog CSV
# MAGIC COPY INTO bronze.raw_products
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         product_id,
# MAGIC         product_name,
# MAGIC         category,
# MAGIC         CAST(price AS DOUBLE) AS price,
# MAGIC         CAST(stock_quantity AS INT) AS stock_quantity,
# MAGIC         is_active,
# MAGIC         current_timestamp() AS _ingest_timestamp,
# MAGIC         input_file_name() AS _source_file
# MAGIC     FROM '/Volumes/retailpulse_catalog/bronze/batch_landing/products.csv'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'false'
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC     'mergeSchema' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Load Historical Batch Orders CSV
# MAGIC COPY INTO bronze.raw_orders
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         order_id,
# MAGIC         customer_id,
# MAGIC         order_timestamp,
# MAGIC         CAST(total_amount AS DOUBLE) AS total_amount,
# MAGIC         order_status,
# MAGIC         payment_method,
# MAGIC         shipping_city,
# MAGIC         current_timestamp() AS _ingest_timestamp,
# MAGIC         input_file_name() AS _source_file
# MAGIC     FROM '/Volumes/retailpulse_catalog/bronze/batch_landing/orders.csv'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'false'
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC     'mergeSchema' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity check: Ensure records landed successfully in Bronze
# MAGIC SELECT 'raw_customers' AS table_name, count(*) AS record_count FROM bronze.raw_customers
# MAGIC UNION ALL
# MAGIC SELECT 'raw_products' AS table_name, count(*) AS record_count FROM bronze.raw_products
# MAGIC UNION ALL
# MAGIC SELECT 'raw_orders' AS table_name, count(*) AS record_count FROM bronze.raw_orders;
