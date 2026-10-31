-- ==============================================================================
-- RetailPulse - Executive Sales & Financial KPIs
-- Target Engine: Databricks SQL Serverless Warehouse
-- Dashboard: RetailPulse Executive Overview
-- ==============================================================================

USE CATALOG retailpulse_catalog;
USE SCHEMA gold;

-- 1. High-Level KPI Summary (Single Value Cards)
SELECT 
    SUM(total_revenue) AS total_gross_revenue,
    SUM(completed_orders) AS total_completed_orders,
    ROUND(AVG(average_order_value), 2) AS overall_avg_order_value,
    ROUND(AVG(return_rate_pct), 2) AS overall_return_rate_percentage
FROM agg_daily_sales_performance;

-- 2. Daily Revenue & Order Volume Trend (Time Series Line Chart)
SELECT 
    sales_date,
    total_revenue,
    completed_orders,
    cancelled_orders,
    returned_orders,
    average_order_value
FROM agg_daily_sales_performance
ORDER BY sales_date ASC;

-- 3. Payment Method Market Share (Donut / Pie Chart)
SELECT 
    payment_method,
    COUNT(order_id) AS total_transactions,
    ROUND(SUM(total_amount), 2) AS total_volume_inr,
    ROUND(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER (), 2) AS percentage_share
FROM fact_sales_orders
GROUP BY payment_method
ORDER BY total_transactions DESC;

-- 4. Geographic Performance by Destination City (Bar Chart)
SELECT 
    shipping_city,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_sales_inr,
    ROUND(AVG(total_amount), 2) AS city_aov
FROM fact_sales_orders
WHERE is_completed = true
GROUP BY shipping_city
ORDER BY total_sales_inr DESC
LIMIT 10;

