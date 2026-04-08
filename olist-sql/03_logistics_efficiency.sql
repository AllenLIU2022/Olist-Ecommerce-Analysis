-- ==================================================================================
-- MODULE: Logistics Efficiency & Shipping Performance
-- LAYER: Gold Layer
-- DESCRIPTION: Analysis of delivery lead times, carrier performance, 
--              and state-level efficiency ratios to identify bottlenecks.
-- ==================================================================================

-- ----------------------------------------------------------------------------------
-- 2.1 Delivery Time Segmentation
-- Goal: Break down the total wait time into seller processing vs. carrier transit.
-- ----------------------------------------------------------------------------------
SELECT 
    order_id,
    -- Seller Response Time
    DATEDIFF(day, order_approved_at, order_delivered_carrier_date) AS seller_process_days,
    -- Carrier Transit Time
    DATEDIFF(day, order_delivered_carrier_date, order_delivered_customer_date) AS carrier_delivery_days,
    -- Total Customer Wait Time
    DATEDIFF(day, order_purchase_timestamp, order_delivered_customer_date) AS total_wait_days
FROM gold_orders
WHERE order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL
ORDER BY total_wait_days DESC;


-- ----------------------------------------------------------------------------------
-- 2.2 State-Level Efficiency Ratio (Strategic Insight)
-- Goal: Benchmark each state's performance against the national average.
-- Insight: States with a ratio > 1.0 are slower than average, suggesting a need for 
--          Regional Distribution Centers (Fulfillment Centers).
-- ----------------------------------------------------------------------------------
WITH National_Avg AS (
    SELECT AVG(DATEDIFF(day, order_purchase_timestamp, order_delivered_customer_date) * 1.0) AS national_avg_days
    FROM gold_orders
    WHERE order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL
),
State_Metrics AS (
    SELECT 
        c.customer_state,
        COUNT(o.order_id) AS total_orders,
        AVG(DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date) * 1.0) AS state_avg_days,
        SUM(CASE WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1 ELSE 0 END) AS on_time_orders
    FROM gold_orders o
    JOIN gold_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
    GROUP BY c.customer_state
)
SELECT 
    sm.customer_state,
    sm.total_orders,
    ROUND(sm.state_avg_days, 2) AS avg_delivery_days,
    -- Efficiency Ratio = State Avg / National Avg
    ROUND(sm.state_avg_days / (SELECT national_avg_days FROM National_Avg), 2) AS efficiency_ratio,
    ROUND(CAST(sm.on_time_orders AS FLOAT) / sm.total_orders * 100, 2) AS on_time_rate_pcnt
FROM State_Metrics sm
ORDER BY efficiency_ratio DESC;


-- ----------------------------------------------------------------------------------
-- 2.3 Delay Rate & Root Cause Analysis
-- Goal: Determine if delays are caused by Sellers (Packing) or Carriers (Shipping).
-- ----------------------------------------------------------------------------------
-- Monthly Delay Trend
SELECT 
    FORMAT(order_purchase_timestamp, 'yyyy-MM') AS order_month,
    COUNT(order_id) AS total_orders,
    AVG(CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1.0 ELSE 0.0 END) AS monthly_delay_rate
FROM gold_orders
WHERE order_status = 'delivered'
GROUP BY FORMAT(order_purchase_timestamp, 'yyyy-MM')
ORDER BY order_month;

-- Responsible Party Identification (Example for March 2018)
SELECT 
    AVG(DATEDIFF(day, order_approved_at, order_delivered_carrier_date) * 1.0) AS avg_seller_time,
    AVG(DATEDIFF(day, order_delivered_carrier_date, order_delivered_customer_date) * 1.0) AS avg_carrier_time
FROM gold_orders
WHERE FORMAT(order_purchase_timestamp, 'yyyy-MM') = '2018-03'
  AND order_status = 'delivered';