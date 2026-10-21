
-- NammaMart ELT: Payment staging

CREATE OR REPLACE TABLE stg_payments AS
WITH cleaned AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        TRY_CAST(NULLIF(TRIM(payment_ts), '') AS TIMESTAMP) AS payment_ts,
        CASE UPPER(TRIM(payment_mode))
            WHEN 'UPI' THEN 'UPI'
            WHEN 'CARD' THEN 'Card'
            WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
            WHEN 'COD' THEN 'Cash on Delivery'
            WHEN 'WALLET' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        TRY_CAST(NULLIF(TRIM(amount_paid), '') AS DECIMAL(12,2))
            AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        NULLIF(TRIM(gateway_ref), '') AS gateway_ref,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(payment_id)
            ORDER BY _loaded_at DESC, TRIM(payment_ts) DESC
        ) AS row_num
    FROM raw_payments
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
            WHEN payment_id IS NULL OR payment_id = ''
                THEN 'missing_payment_id'
            WHEN order_id IS NULL OR order_id = ''
                THEN 'missing_order_id'
            WHEN payment_ts IS NULL
                THEN 'invalid_payment_ts'
            WHEN amount_paid IS NULL OR amount_paid <= 0
                THEN 'invalid_amount_paid'
            WHEN payment_status NOT IN ('SUCCESS', 'REFUNDED', 'FAILED')
                THEN 'invalid_payment_status'
            WHEN payment_mode NOT IN (
                'UPI', 'Card', 'Cash on Delivery', 'Wallet'
            )
                THEN 'invalid_payment_mode'
            ELSE ''
        END AS critical_failure,
        CASE
            WHEN gateway_ref IS NULL THEN 'missing_gateway_ref'
            ELSE ''
        END AS warning_failure
    FROM deduplicated
)
SELECT
    payment_id,
    order_id,
    payment_ts,
    payment_mode,
    amount_paid,
    payment_status,
    gateway_ref,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN critical_failure <> '' THEN 'FAIL'
        WHEN warning_failure <> '' THEN 'WARNING'
        ELSE 'PASS'
    END AS dq_status,
    CONCAT_WS(
        ';',
        NULLIF(critical_failure, ''),
        NULLIF(warning_failure, '')
    ) AS failure_reason
FROM checked;
