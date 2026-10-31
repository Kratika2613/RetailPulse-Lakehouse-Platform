-- ==============================================================================
-- RetailPulse - Gold Layer DDL (Dimensional Models & Pre-Computed KPI Marts)
-- Schema: retailpulse_catalog.gold
-- ==============================================================================

USE CATALOG retailpulse_catalog;
USE SCHEMA gold;

-- 1. Dimension: Customers (Conformed Dimension with Customer Profiling)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate Key',
    customer_id STRING NOT NULL COMMENT 'Natural Business Key',
    full_name STRING NOT NULL,
    email STRING,
    phone STRING,
    city STRING NOT NULL,
    state STRING NOT NULL,
    signup_date DATE NOT NULL,
    customer_tenure_days INT COMMENT 'Days since registration',
    _updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (customer_id)
COMMENT 'Conformed customer dimension table';

-- 2. Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate Key',
    product_id STRING NOT NULL COMMENT 'Natural Business SKU',
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    price_tier STRING COMMENT 'Derived tier: Budget (<1000), Mid-Range (1000-5000), Premium (>5000)',
    stock_quantity INT NOT NULL,
    is_active BOOLEAN NOT NULL,
    _updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Conformed product dimension table';

-- 3. Fact Table: Sales Orders
CREATE TABLE IF NOT EXISTS fact_sales_orders (
    order_key BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate Key',
    order_id STRING NOT NULL COMMENT 'Natural order ID',
    customer_id STRING NOT NULL COMMENT 'Foreign key to dim_customers',
    order_timestamp TIMESTAMP NOT NULL,
    order_date DATE NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    order_status STRING NOT NULL,
    payment_method STRING NOT NULL,
    shipping_city STRING NOT NULL,
    is_completed BOOLEAN NOT NULL COMMENT 'Flag: Delivered orders',
    is_returned BOOLEAN NOT NULL COMMENT 'Flag: Returned orders',
    _updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (order_date)
COMMENT 'Central retail sales transactional fact table';

-- 4. Analytical Mart: Daily Sales Performance
CREATE TABLE IF NOT EXISTS agg_daily_sales_performance (
    sales_date DATE NOT NULL COMMENT 'Primary aggregation grain',
    total_revenue DECIMAL(15, 2) NOT NULL COMMENT 'Gross sales in INR',
    total_orders_placed INT NOT NULL,
    completed_orders INT NOT NULL,
    cancelled_orders INT NOT NULL,
    returned_orders INT NOT NULL,
    average_order_value DECIMAL(10, 2) NOT NULL,
    upi_payment_count INT NOT NULL,
    card_payment_count INT NOT NULL,
    return_rate_pct DECIMAL(5, 2) NOT NULL,
    _generated_at TIMESTAMP
)
USING DELTA
COMMENT 'Daily executive sales performance summary mart for BI dashboards';

-- 5. Analytical Mart: Customer Lifetime Value & RFM Segmentation
CREATE TABLE IF NOT EXISTS agg_customer_lifetime_value (
    customer_id STRING NOT NULL COMMENT 'Customer identifier',
    customer_name STRING NOT NULL,
    city STRING NOT NULL,
    first_order_date DATE,
    last_order_date DATE,
    total_orders INT NOT NULL,
    total_lifetime_spend DECIMAL(15, 2) NOT NULL,
    avg_order_value DECIMAL(10, 2) NOT NULL,
    customer_segment STRING NOT NULL COMMENT 'VIP (>50,000 spend), Regular (10,000-50,000), Occasional (<10,000)',
    churn_risk_flag BOOLEAN NOT NULL COMMENT 'True if no order placed in last 90 days',
    _generated_at TIMESTAMP
)
USING DELTA
COMMENT 'Customer lifetime value (LTV) and RFM segmentation mart';

