-- ==================================================================================
-- MODULE: Product Category & Quality Control
-- DESCRIPTION: Analysis of sales distribution by category and identifying 
--              high-risk products through review keyword mining.
-- ==================================================================================

-- ----------------------------------------------------------------------------------
-- 4.1 Sales Contribution by Category
-- Goal: Identify top-performing categories by GMV percentage.
-- ----------------------------------------------------------------------------------
SELECT 
    category, 
    SUM(i.price) AS total_revenue, 
    ROUND(SUM(i.price) * 100.0 / SUM(SUM(i.price)) OVER(), 2) AS revenue_percentage
FROM (
    SELECT 
        ca.product_category_name_english AS category, 
        p.product_id
    FROM gold_category AS ca 
    JOIN gold_products AS p ON ca.product_category_name = p.product_category_name
) AS ce
JOIN gold_items AS i ON ce.product_id = i.product_id
GROUP BY category
ORDER BY total_revenue DESC;


-- ----------------------------------------------------------------------------------
-- 4.2 Potential Refund Risk by Category
-- Goal: Using 1-star review rates as a proxy for product dissatisfaction and refunds.
-- Recommendation: Categories with high 1-star rates (e.g., Fashion) may need 
--                 better size guides or quality inspections.
-- ----------------------------------------------------------------------------------
SELECT 
    ca.product_category_name_english AS category,
    COUNT(o.order_id) AS total_orders,
    -- Ratio of 1-star reviews
    AVG(CASE WHEN r.review_score = 1 THEN 1.0 ELSE 0.0 END) AS low_score_rate
FROM gold_category ca
JOIN gold_products p ON ca.product_category_name = p.product_category_name
JOIN gold_items i ON i.product_id = p.product_id
JOIN gold_reviews r ON r.order_id = i.order_id
JOIN gold_orders o ON o.order_id = r.order_id 
GROUP BY ca.product_category_name_english
HAVING COUNT(o.order_id) > 100 -- Filtering for statistical significance
ORDER BY low_score_rate DESC;


-- ----------------------------------------------------------------------------------
-- 4.3 Review Keyword Mining: Refund & Return Detection
-- Goal: Identify specific categories mentioned alongside "refund" or "return" keywords.
-- Insight: This helps distinguish between "I didn't like it" (low score) 
--          and "I want my money back" (refund mention).
-- ----------------------------------------------------------------------------------
SELECT 
    ca.product_category_name_english AS category,
    -- 1-Star Rate
    AVG(CASE WHEN r.review_score = 1 THEN 1.0 ELSE 0.0 END) AS low_score_rate,
    -- Keyword detection: 'reembolso' (refund) or 'devolução' (return)
    AVG(CASE WHEN r.review_comment_message LIKE '%reembolso%' 
               OR r.review_comment_message LIKE '%devolução%' THEN 1.0 ELSE 0.0 END) AS refund_mention_rate
FROM gold_orders o
JOIN gold_reviews r ON o.order_id = r.order_id
JOIN gold_items i ON o.order_id = i.order_id
JOIN gold_products p i.product_id = p.product_id
JOIN gold_category ca ON p.product_category_name = ca.product_category_name
GROUP BY ca.product_category_name_english
HAVING COUNT(DISTINCT o.order_id) > 100
ORDER BY low_score_rate DESC;