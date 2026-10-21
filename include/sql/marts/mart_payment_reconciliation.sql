CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH payment_summary AS (
    SELECT
        order_id,
        COUNT(*) FILTER (
            WHERE payment_status = 'SUCCESS'
        ) AS successful_payment_count,
        ROUND(SUM(amount_paid) FILTER (
            WHERE payment_status = 'SUCCESS'
        ), 2) AS amount_paid,
        COUNT(*) FILTER (
            WHERE payment_status = 'FAILED'
        ) AS failed_payment_count,
        COUNT(*) FILTER (
            WHERE payment_status = 'REFUNDED'
        ) AS refunded_payment_count,
        COUNT(*) FILTER (
            WHERE payment_status = 'SUCCESS'
              AND dq_status = 'WARNING'
        ) AS successful_payment_warnings,
        COUNT(*) FILTER (
            WHERE payment_status = 'SUCCESS'
              AND (gateway_ref IS NULL OR TRIM(gateway_ref) = '')
        ) AS missing_gateway_ref_count,
        STRING_AGG(DISTINCT payment_mode, ', ') AS recorded_payment_modes
    FROM stg_payments
    GROUP BY order_id
),
combined AS (
    SELECT
        COALESCE(o.order_id, p.order_id) AS order_id,
        o.order_status,
        o.payment_mode,
        o.order_amount AS expected_order_amount,
        p.amount_paid,
        COALESCE(p.successful_payment_count, 0) AS successful_payment_count,
        COALESCE(p.failed_payment_count, 0) AS failed_payment_count,
        COALESCE(p.refunded_payment_count, 0) AS refunded_payment_count,
        COALESCE(p.successful_payment_warnings, 0) AS successful_payment_warnings,
        COALESCE(p.missing_gateway_ref_count, 0) AS missing_gateway_ref_count,
        p.recorded_payment_modes
    FROM stg_orders o
    FULL OUTER JOIN payment_summary p
        ON o.order_id = p.order_id
    WHERE o.order_id IS NULL OR o.dq_status = 'PASS'
)
SELECT
    order_id,
    order_status,
    COALESCE(payment_mode, recorded_payment_modes) AS payment_mode,
    expected_order_amount,
    amount_paid,
    successful_payment_count,
    ROUND(amount_paid - expected_order_amount, 2) AS amount_difference,
    CASE
        WHEN order_status IS NULL
            THEN 'successful_payment_without_valid_order'
        WHEN successful_payment_count > 0
             AND ABS(amount_paid - expected_order_amount) > 0.01
            THEN 'payment_amount_mismatch'
        WHEN successful_payment_count > 1
            THEN 'multiple_successful_payments'
        WHEN successful_payment_count > 0
             AND missing_gateway_ref_count > 0
            THEN 'successful_payment_missing_gateway_ref'
        WHEN order_status = 'DELIVERED'
             AND successful_payment_count = 0
             AND payment_mode = 'Cash on Delivery'
             AND failed_payment_count = 0
             AND refunded_payment_count = 0
            THEN 'cod_collection_unconfirmed'
        WHEN order_status = 'DELIVERED'
             AND successful_payment_count = 0
             AND failed_payment_count > 0
            THEN 'failed_payment_recorded'
        WHEN order_status = 'DELIVERED'
             AND successful_payment_count = 0
             AND refunded_payment_count > 0
            THEN 'refunded_without_successful_payment'
        WHEN order_status = 'DELIVERED'
             AND successful_payment_count = 0
            THEN 'delivered_order_without_successful_payment'
        ELSE 'matched'
    END AS reconciliation_status
FROM combined
WHERE
       order_status IS NULL
    OR (successful_payment_count > 0
        AND ABS(amount_paid - expected_order_amount) > 0.01)
    OR successful_payment_count > 1
    OR (successful_payment_count > 0
        AND missing_gateway_ref_count > 0)
    OR (order_status = 'DELIVERED' AND successful_payment_count = 0);
