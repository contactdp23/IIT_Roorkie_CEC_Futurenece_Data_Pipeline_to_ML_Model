CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    s.zone,
    o.store_id,
    s.store_name,
    COUNT(DISTINCT o.order_id) AS delivered_orders,
    ROUND(SUM(o.order_amount), 2) AS net_revenue,
    ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes,
    COUNT(*) FILTER (
        WHERE o.delivery_minutes <= 15
    ) AS orders_within_15_min,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE o.delivery_minutes <= 15
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS pct_within_15_min
FROM stg_orders AS o
JOIN stg_stores AS s
    ON o.store_id = s.store_id
   AND s.dq_status = 'PASS'
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY
    CAST(o.order_ts AS DATE),
    s.zone,
    o.store_id,
    s.store_name;
