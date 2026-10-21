CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.city,
    c.loyalty_tier,
    COUNT(DISTINCT o.order_id) AS delivered_orders,
    SUM(o.quantity) AS units_purchased,
    ROUND(SUM(o.order_amount), 2) AS total_revenue,
    ROUND(AVG(o.order_amount), 2) AS avg_order_value,
    MAX(CAST(o.order_ts AS DATE)) AS last_delivered_order_date
FROM stg_orders o
JOIN stg_customers c
    ON o.customer_id = c.customer_id
   AND c.dq_status = 'PASS'
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY
    c.customer_id,
    c.full_name,
    c.city,
    c.loyalty_tier;
