-- ==============================================================================
-- RetailPulse - Silver Layer DDL (Curated, Deduplicated & Validated Tables)
-- Schema: retailpulse_catalog.silver
-- ==============================================================================

USE CATALOG retailpulse_catalog;
USE SCHEMA silver;

-- 1. Silver Customers (Cleaned, Standardized, Deduplicated)
CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING NOT NULL COMMENT 'Primary key: Unique customer identifier',
    first_name STRING NOT NULL COMMENT 'Standardized trimmed customer first name',
    last_name STRING NOT NULL COMMENT 'Standardized trimmed customer last name',
    full_name STRING COMMENT 'Derived full customer name',
    email STRING COMMENT 'Validated email address with valid regex format',
    phone STRING COMMENT 'Standardized phone number',
    city STRING NOT NULL COMMENT 'Title-cased city name (e.g. Mumbai, Bengaluru)',
    state STRING NOT NULL COMMENT 'State name',
    country STRING NOT NULL COMMENT 'Country standardized',
    signup_date DATE NOT NULL COMMENT 'Standardized signup date',
    is_valid_email BOOLEAN COMMENT 'Flag indicating valid email structure',
    _silver_processed_at TIMESTAMP COMMENT 'Processing timestamp for auditing'
)
USING DELTA
CLUSTER BY (customer_id, city)
COMMENT 'Silver conformed customer master table (SCD Type 1)';

-- 2. Silver Products (Validated List Prices and Inventory)
CREATE TABLE IF NOT EXISTS products (
    product_id STRING NOT NULL COMMENT 'Primary key: Product SKU identifier',
    product_name STRING NOT NULL COMMENT 'Cleaned product name',
    category STRING NOT NULL COMMENT 'Standardized merchandising category',
    price DECIMAL(10, 2) NOT NULL COMMENT 'Verified positive price in INR',
    stock_quantity INT NOT NULL COMMENT 'Verified non-negative stock count',
    is_active BOOLEAN NOT NULL COMMENT 'Boolean operational flag',
    _silver_processed_at TIMESTAMP COMMENT 'Processing timestamp'
)
USING DELTA
CLUSTER BY (category, product_id)
COMMENT 'Silver validated product catalog table';

-- 3. Silver Orders (Deduplicated, Parsed Timestamps, Validated Amounts)
CREATE TABLE IF NOT EXISTS orders (
    order_id STRING NOT NULL COMMENT 'Primary key: Unique order transaction ID',
    customer_id STRING NOT NULL COMMENT 'Referential customer ID',
    order_timestamp TIMESTAMP NOT NULL COMMENT 'Parsed transaction timestamp',
    order_date DATE NOT NULL COMMENT 'Partitioning date key',
    total_amount DECIMAL(12, 2) NOT NULL COMMENT 'Verified positive transaction value',
    order_status STRING NOT NULL COMMENT 'Standardized lifecycle state',
    payment_method STRING NOT NULL COMMENT 'Payment mode',
    shipping_city STRING NOT NULL COMMENT 'Destination city',
    _silver_processed_at TIMESTAMP COMMENT 'Processing timestamp'
)
USING DELTA
PARTITIONED BY (order_date)
COMMENT 'Silver cleansed, validated and deduplicated sales order transactions';

-- 4. Silver Order Events (Cleaned Real-Time Streaming Telemetry)
CREATE TABLE IF NOT EXISTS order_events (
    event_id STRING NOT NULL COMMENT 'Primary key: Unique event tracking ID',
    order_id STRING NOT NULL COMMENT 'Correlated sales order ID',
    customer_id STRING COMMENT 'Associated customer ID',
    event_type STRING NOT NULL COMMENT 'Standardized event status lifecycle',
    event_timestamp TIMESTAMP NOT NULL COMMENT 'Parsed client timestamp with timezone',
    device_type STRING NOT NULL COMMENT 'Client device platform',
    ip_address STRING COMMENT 'Source IP address',
    app_version STRING COMMENT 'Extracted from metadata',
    network_type STRING COMMENT 'Extracted network interface (4G, 5G, WiFi)',
    _silver_processed_at TIMESTAMP COMMENT 'Processing timestamp'
)
USING DELTA
PARTITIONED BY (event_type)
COMMENT 'Silver parsed and structured streaming order lifecycle events';

