-- ==============================================================================
-- RetailPulse - Customer LTV, RFM & Churn Risk Analytics
-- Target Engine: Databricks SQL Serverless Warehouse
-- Dashboard: Customer Intelligence & Retention Tab
-- ==============================================================================

USE CATALOG retailpulse_catalog;
USE SCHEMA gold;

-- 1. Customer Segmentation Breakdown (VIP vs Regular vs Occasional)
SELECT 
    customer_segment,
    COUNT(customer_id) AS total_customers,
    ROUND(SUM(total_lifetime_spend), 2) AS aggregate_segment_spend,
    ROUND(AVG(total_lifetime_spend), 2) AS avg_spend_per_customer,
    ROUND(AVG(total_orders), 1) AS avg_orders_per_customer
FROM agg_customer_lifetime_value
GROUP BY customer_segment
ORDER BY aggregate_segment_spend DESC;

-- 2. Churn Risk Warning List (High Value Customers Inactive for >90 Days)
SELECT 
    customer_id,
    customer_name,
    city,
    last_order_date,
    total_orders,
    total_lifetime_spend,
    customer_segment
FROM agg_customer_lifetime_value
WHERE churn_risk_flag = true AND customer_segment IN ('VIP (>50k)', 'Regular (15k-50k)')
ORDER BY total_lifetime_spend DESC
LIMIT 20;

-- 3. Top 10 Most Valuable Customers (Leaderboard Table)
SELECT 
    customer_id,
    customer_name,
    city,
    total_orders,
    total_lifetime_spend,
    avg_order_value,
    customer_segment
FROM agg_customer_lifetime_value
ORDER BY total_lifetime_spend DESC
LIMIT 10;

