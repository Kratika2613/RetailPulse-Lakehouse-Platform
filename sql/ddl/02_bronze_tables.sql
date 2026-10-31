-- ==============================================================================
-- RetailPulse - Bronze Layer DDL (Raw Landing Delta Tables)
-- Schema: retailpulse_catalog.bronze
-- ==============================================================================

USE CATALOG retailpulse_catalog;
USE SCHEMA bronze;

-- 1. Bronze Customers
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id STRING COMMENT 'Raw business customer ID',
    first_name STRING COMMENT 'Customer first name',
    last_name STRING COMMENT 'Customer last name',
    email STRING COMMENT 'Raw email string',
    phone STRING COMMENT 'Customer telephone number',
    city STRING COMMENT 'Raw city name with potential case inconsistencies',
    state STRING COMMENT 'State name',
    country STRING COMMENT 'Country name',
    signup_date STRING COMMENT 'Signup date as raw string',
    _ingest_timestamp TIMESTAMP COMMENT 'System timestamp when row was loaded into Bronze',
    _source_file STRING COMMENT 'Full source file path from landing volume'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Bronze raw customer snapshot table';

-- 2. Bronze Products
CREATE TABLE IF NOT EXISTS raw_products (
    product_id STRING COMMENT 'Raw SKU identifier',
    product_name STRING COMMENT 'Product commercial description',
    category STRING COMMENT 'Merchandising category',
    price DOUBLE COMMENT 'Raw catalog price',
    stock_quantity INT COMMENT 'Inventory count',
    is_active STRING COMMENT 'Catalog active flag',
    _ingest_timestamp TIMESTAMP COMMENT 'System timestamp when row was loaded into Bronze',
    _source_file STRING COMMENT 'Source file path'
)
USING DELTA
COMMENT 'Bronze raw product master snapshot table';

-- 3. Bronze Orders
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id STRING COMMENT 'Raw order transaction identifier',
    customer_id STRING COMMENT 'Associated customer ID',
    order_timestamp STRING COMMENT 'Raw transaction timestamp string',
    total_amount DOUBLE COMMENT 'Raw monetary transaction amount',
    order_status STRING COMMENT 'Order fulfillment status',
    payment_method STRING COMMENT 'Payment instrument',
    shipping_city STRING COMMENT 'Destination delivery city',
    _ingest_timestamp TIMESTAMP COMMENT 'System timestamp when row was loaded into Bronze',
    _source_file STRING COMMENT 'Source file path'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Bronze raw historical orders batch table';

-- 4. Bronze Streaming Order Events
CREATE TABLE IF NOT EXISTS raw_order_events (
    event_id STRING COMMENT 'Unique event tracking UUID',
    order_id STRING COMMENT 'Correlated order transaction ID',
    customer_id STRING COMMENT 'Customer performing the event',
    event_type STRING COMMENT 'Status code: ORDER_PLACED, PAYMENT_CONFIRMED, etc.',
    event_timestamp STRING COMMENT 'ISO timestamp from mobile/web client',
    device_type STRING COMMENT 'Client platform (Android, iOS, Web)',
    ip_address STRING COMMENT 'Origin client IP',
    metadata STRING COMMENT 'Raw serialized JSON string containing device telemetry',
    _ingest_timestamp TIMESTAMP COMMENT 'Streaming ingestion timestamp',
    _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema drift handling'
)
USING DELTA
COMMENT 'Bronze raw streaming order events ingested via Auto Loader';

