
-- NammaMart ELT: Order staging

CREATE OR REPLACE TABLE stg_orders AS
WITH cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRY_CAST(NULLIF(TRIM(order_ts), '') AS TIMESTAMP) AS order_ts,
        TRIM(customer_id) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(product_id) AS product_id,
        TRY_CAST(NULLIF(TRIM(quantity), '') AS INTEGER) AS quantity,
        TRY_CAST(NULLIF(TRIM(unit_price), '') AS DECIMAL(12,2))
            AS unit_price,
        TRY_CAST(NULLIF(TRIM(discount_pct), '') AS DECIMAL(8,2))
            AS discount_pct,
        TRY_CAST(NULLIF(TRIM(order_amount), '') AS DECIMAL(12,2))
            AS order_amount,
        CASE UPPER(TRIM(payment_mode))
            WHEN 'UPI' THEN 'UPI'
            WHEN 'CARD' THEN 'Card'
            WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
            WHEN 'COD' THEN 'Cash on Delivery'
            WHEN 'WALLET' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status,
        TRIM(channel) AS channel,
        TRY_CAST(NULLIF(TRIM(delivery_minutes), '') AS INTEGER)
            AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(order_id)
            ORDER BY
                CASE
                    WHEN _source_file = 'orders_2026-10-01_increment.csv'
                    THEN 1
                    ELSE 0
                END DESC,
                _loaded_at DESC,
                TRIM(order_ts) DESC
        ) AS row_num
    FROM raw_orders
),
deduplicated AS (
    SELECT *
    FROM cleaned
    WHERE row_num = 1
),
checked AS (
    SELECT
        *,
        CASE
            WHEN order_id IS NULL OR order_id = ''
                THEN 'missing_order_id'
            WHEN order_ts IS NULL
                THEN 'invalid_order_ts'
            WHEN customer_id IS NULL OR customer_id = ''
                THEN 'missing_customer_id'
            WHEN store_id IS NULL OR store_id = ''
                THEN 'missing_store_id'
            WHEN product_id IS NULL OR product_id = ''
                THEN 'missing_product_id'
            WHEN quantity IS NULL OR quantity <= 0
                THEN 'invalid_quantity'
            WHEN unit_price IS NULL OR unit_price <= 0
                THEN 'invalid_unit_price'
            WHEN discount_pct IS NULL
                 OR discount_pct < 0 OR discount_pct > 100
                THEN 'invalid_discount_pct'
            WHEN order_amount IS NULL OR order_amount < 0
                THEN 'invalid_order_amount'
            WHEN payment_mode NOT IN (
                'UPI', 'Card', 'Cash on Delivery', 'Wallet'
            )
                THEN 'invalid_payment_mode'
            WHEN order_status NOT IN (
                'DELIVERED', 'CANCELLED', 'RETURNED'
            )
                THEN 'invalid_order_status'
            WHEN order_status = 'DELIVERED'
                 AND delivery_minutes IS NULL
                THEN 'missing_delivery_minutes'
            WHEN delivery_minutes IS NOT NULL
                 AND (delivery_minutes < 0 OR delivery_minutes >= 120)
                THEN 'invalid_delivery_minutes'
            WHEN ABS(
                order_amount -
                (quantity * unit_price * (1 - discount_pct / 100.0))
            ) > 1.00
                THEN 'order_amount_mismatch'
            
WHEN NOT EXISTS (
    SELECT 1
    FROM stg_customers c
    WHERE c.customer_id = deduplicated.customer_id
      AND c.dq_status IN ('PASS', 'WARNING')
) THEN 'customer_not_found'

WHEN NOT EXISTS (
    SELECT 1
    FROM stg_products p
    WHERE p.product_id = deduplicated.product_id
      AND p.dq_status = 'PASS'
) THEN 'product_not_found'

WHEN NOT EXISTS (
    SELECT 1
    FROM stg_stores s
    WHERE s.store_id = deduplicated.store_id
      AND s.dq_status = 'PASS'
) THEN 'store_not_found'

            ELSE ''
        END AS failure_reason
    FROM deduplicated
)
SELECT
    order_id,
    order_ts,
    customer_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    order_amount,
    payment_mode,
    order_status,
    channel,
    delivery_minutes,
    _loaded_at,
    _source_file,
    _run_id,
    CASE WHEN failure_reason <> '' THEN 'FAIL' ELSE 'PASS' END
        AS dq_status,
    failure_reason
FROM checked;


