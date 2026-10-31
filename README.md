# RetailPulse Lakehouse Platform

An end-to-end retail data engineering project built on Databricks using PySpark, Delta Lake, and the Medallion Architecture (Bronze -> Silver -> Gold).

---

## Tech Stack
- **Platform:** Databricks
- **Processing:** PySpark
- **Storage:** Delta Lake
- **SQL & Analytics:** Databricks SQL
- **Orchestration:** Databricks Workflows

---

## Pipeline Workflow

1. **Bronze Layer (Raw Ingestion):**
   - Batch ingestion of Customers, Products, and Orders CSV data using `COPY INTO`.
   - Streaming ingestion of real-time JSON order events using Databricks `Auto Loader` (`cloudFiles`).

2. **Silver Layer (Cleaned & Curated):**
   - Deduplication using PySpark `row_number()` window ranking.
   - Quality checks: Invalid records (negative prices, missing customer IDs) routed to `quarantine_orders_dlq`.
   - Idempotent upserts into Delta tables using `MERGE INTO`.

3. **Gold Layer (Analytics):**
   - Star Schema dimensional modeling (`dim_customers`, `dim_products`, `fact_sales_orders`).
   - Pre-aggregated tables for daily sales performance and customer lifetime value (LTV).
