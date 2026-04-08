-- ==================================================================================
-- MODULE: Customer Behavior & Payment Insights
-- LAYER: Gold Layer
-- DESCRIPTION: Analysis of customer distribution, repurchase rates, and the 
--              psychological impact of payment installments on review scores.
-- ==================================================================================

-- ----------------------------------------------------------------------------------
-- 3.1 Geographic Distribution & Average Order Value (AOV)
-- Goal: Identify high-value regions and customer density by state.
-- ----------------------------------------------------------------------------------
SELECT 
    c.customer_state, 
    COUNT(DISTINCT i.order_id) AS total_orders, 
    SUM(i.price) / COUNT(DISTINCT o.order_id) AS aov,
    AVG(i.price) AS avg_item_price
FROM gold_items AS i 
LEFT JOIN gold_orders AS o ON i.order_id = o.order_id
LEFT JOIN gold_customers AS c ON o.customer_id = c.customer_id
WHERE c.customer_state IS NOT NULL
GROUP BY c.customer_state
ORDER BY aov DESC;


-- ----------------------------------------------------------------------------------
-- 3.2 Repurchase Rate Analysis
-- Goal: Measure customer loyalty. (Note: Olist usually shows low repurchase due to 
--       the nature of the dataset's timeframe and marketplace model).
-- ----------------------------------------------------------------------------------
WITH orders_per_person AS (
    SELECT customer_unique_id, COUNT(o.order_id) AS total_orders
    FROM gold_customers AS c 
    LEFT JOIN gold_orders AS o ON c.customer_id = o.customer_id
    WHERE o.customer_id IS NOT NULL 
    GROUP BY customer_unique_id
)
SELECT 
    COUNT(*) AS total_customers,
    SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
    CAST(SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) AS repurchase_rate
FROM orders_per_person;


-- ----------------------------------------------------------------------------------
-- 3.3 Payment Preferences & Credit Card Installments
-- Goal: Understand how financing (installments) drives high-value purchases.
-- ----------------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN payment_installments = 1 THEN '1. Single Payment'
        WHEN payment_installments BETWEEN 2 AND 6 THEN '2. Short-term (2-6)'
        WHEN payment_installments > 6 THEN '3. Long-term (>6)'
        ELSE '4. Others'
    END AS installment_bucket,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(payment_value) AS total_revenue,
    SUM(payment_value) / COUNT(DISTINCT order_id) AS aov
FROM gold_payments
WHERE payment_type = 'credit_card'
GROUP BY 
    CASE 
        WHEN payment_installments = 1 THEN '1. Single Payment'
        WHEN payment_installments BETWEEN 2 AND 6 THEN '2. Short-term (2-6)'
        WHEN payment_installments > 6 THEN '3. Long-term (>6)'
        ELSE '4. Others'
    END
ORDER BY installment_bucket;


-- ----------------------------------------------------------------------------------
-- 3.4 The "Repayment Pain" Hypothesis
-- Goal: Correlating installment length with review scores.
-- Insight: Higher installments often correlate with slightly lower satisfaction, 
--          possibly due to long-term financial commitment.
-- ----------------------------------------------------------------------------------
SELECT 
    pa.payment_installments,
    AVG(CAST(r.review_score AS FLOAT)) AS avg_score,
    AVG(DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date)) AS avg_delivery_days,
    COUNT(DISTINCT pa.order_id) AS total_orders
FROM gold_payments pa
JOIN gold_reviews r ON pa.order_id = r.order_id
JOIN gold_orders o ON pa.order_id = o.order_id
WHERE pa.payment_type = 'credit_card'
GROUP BY pa.payment_installments
ORDER BY pa.payment_installments;