-- ==================================================================================
-- MODULE: Seller Operation Performance 
-- LAYER: Gold Layer (Analytical Views)
-- DESCRIPTION: Focuses on GMV growth, order health (cancellation funnel), and seller ecosystem concentration.
-- ==================================================================================

-- ----------------------------------------------------------------------------------
-- 1.1 Monthly GMV & Average Order Value Trend
-- Analyzing if growth is driven by volume or price increases.
-- ----------------------------------------------------------------------------------
SELECT 
    FORMAT(o.order_purchase_timestamp, 'yyyy-MM') AS purchase_month, 
    COUNT(DISTINCT o.order_id) AS total_orders,                    
    SUM(i.price + i.freight_value) AS monthly_gmv,                 
    SUM(i.price) / COUNT(DISTINCT o.order_id) AS avg_item_value    
FROM gold_orders AS o
JOIN gold_items AS i ON o.order_id = i.order_id
WHERE o.order_status = 'delivered' 
GROUP BY FORMAT(o.order_purchase_timestamp, 'yyyy-MM')
ORDER BY purchase_month;


-- ----------------------------------------------------------------------------------
-- 1.2 Order Health: Funnel & Cancellation Analysis
-- Pinpointing where the "leakage" happens in the order lifecycle.
-- ----------------------------------------------------------------------------------

-- Overall Status Percentage
SELECT 
    order_status,
    COUNT(order_id) AS order_count,
    CAST(COUNT(order_id) AS FLOAT) / SUM(COUNT(order_id)) OVER() AS percentage
FROM gold_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- Cancellation "Death Stage" Analysis
-- High 'Before Approval' suggests payment friction; 'After Approval' suggests logistics issues.
WITH canceled_orders AS (
    SELECT 
        order_id,
        CASE 
            WHEN order_delivered_customer_date IS NOT NULL THEN 'After Delivery (Return)'
            WHEN order_delivered_carrier_date IS NOT NULL THEN 'During Shipping'
            WHEN order_approved_at IS NOT NULL THEN 'After Approval'
            ELSE 'Before Approval (User Regret/Payment Issue)'
        END AS death_stage
    FROM gold_orders
    WHERE order_status = 'canceled'
)
SELECT 
    death_stage,
    COUNT(order_id) AS canceled_count,
    ROUND(COUNT(order_id) * 1.0 / SUM(COUNT(order_id)) OVER(), 4) AS stage_percentage
FROM canceled_orders
GROUP BY death_stage
ORDER BY canceled_count DESC;


-- ----------------------------------------------------------------------------------
-- 1.3 Seller Ecosystem: Pareto (80/20) & Concentration Analysis
-- Checking for market monopoly vs. a healthy long-tail seller base.
-- ----------------------------------------------------------------------------------
WITH seller_ranks AS (
    SELECT 
        seller_id,
        SUM(price + freight_value) AS seller_gmv,
        SUM(price + freight_value) / SUM(SUM(price + freight_value)) OVER() AS pcnt
    FROM gold_items
    GROUP BY seller_id
),
cumulative_ranks AS (
    SELECT 
        seller_id,
        pcnt,
        SUM(pcnt) OVER(ORDER BY pcnt DESC) AS cumulative_pcnt,
        ROW_NUMBER() OVER(ORDER BY pcnt DESC) AS seller_rank
    FROM seller_ranks
)
SELECT * FROM cumulative_ranks 
WHERE cumulative_pcnt <= 0.81; -- Shows top sellers contributing to 80% of GMV


-- ----------------------------------------------------------------------------------
-- 1.4 Seller Group Comparison: Core vs. Long-tail
-- Analyzing if a small group of sellers causes the majority of cancellations.
-- ----------------------------------------------------------------------------------
WITH seller_stats AS (
    SELECT 
        i.seller_id,
        SUM(i.price + i.freight_value) AS seller_gmv,
        COUNT(o.order_id) AS total_orders,
        SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
        SUM(SUM(i.price + i.freight_value)) OVER(ORDER BY SUM(i.price + i.freight_value) DESC) / 
        SUM(SUM(i.price + i.freight_value)) OVER() AS cumulative_gmv_pcnt
    FROM gold_orders o
    JOIN gold_items i ON o.order_id = i.order_id
    GROUP BY i.seller_id
),
seller_groups AS (
    SELECT 
        CASE WHEN cumulative_gmv_pcnt <= 0.80 THEN 'Core Sellers' ELSE 'Long-tail Sellers' END AS seller_group,
        total_orders,
        canceled_orders
    FROM seller_stats
)
SELECT 
    seller_group,
    SUM(total_orders) AS total_orders,
    SUM(canceled_orders) AS total_canceled,
    CAST(SUM(canceled_orders) AS FLOAT) / SUM(total_orders) AS group_cancel_rate
FROM seller_groups
GROUP BY seller_group;