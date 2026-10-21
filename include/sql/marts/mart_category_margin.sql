CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS delivered_orders,
    SUM(o.quantity) AS units_sold,
    ROUND(SUM(o.order_amount), 2) AS net_revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2) AS cost_of_goods,
    ROUND(
        SUM(o.order_amount) - SUM(o.quantity * p.cost_price),
        2
    ) AS gross_margin,
    ROUND(
        100.0 * (
            SUM(o.order_amount) - SUM(o.quantity * p.cost_price)
        ) / NULLIF(SUM(o.order_amount), 0),
        2
    ) AS gross_margin_pct
FROM stg_orders AS o
JOIN stg_products AS p
    ON o.product_id = p.product_id
   AND p.dq_status = 'PASS'
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY p.category;
