-- ==============================================================================
-- RetailPulse - Inventory & Category Merchandising Analytics
-- Target Engine: Databricks SQL Serverless Warehouse
-- ==============================================================================

USE CATALOG retailpulse_catalog;

-- 1. Category Revenue & Sales Contribution
SELECT 
    p.category,
    COUNT(DISTINCT p.product_id) AS active_sku_count,
    ROUND(AVG(p.price), 2) AS avg_product_price,
    SUM(p.stock_quantity) AS total_inventory_units
FROM gold.dim_products p
GROUP BY p.category
ORDER BY total_inventory_units DESC;

-- 2. Low Stock Alerts (Products needing restocking)
SELECT 
    product_id,
    product_name,
    category,
    price,
    stock_quantity,
    price_tier
FROM gold.dim_products
WHERE stock_quantity < 50 AND is_active = true
ORDER BY stock_quantity ASC;

-- 3. Data Quality Audit: Quarantined Records Summary
SELECT 
    failed_rule,
    COUNT(*) AS rejected_record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_failures,
    MIN(quarantined_at) AS earliest_failure,
    MAX(quarantined_at) AS latest_failure
FROM silver.quarantine_orders_dlq
GROUP BY failed_rule
ORDER BY rejected_record_count DESC;

