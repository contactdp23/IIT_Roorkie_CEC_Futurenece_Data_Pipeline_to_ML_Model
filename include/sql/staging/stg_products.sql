-- NammaMart ELT: Product staging
-- Raw product records are preserved in raw_products.
-- Business assumption: prefer catalogue entries not marked "(old)".
-- This resolves the known obsolete product versions deterministically.

CREATE OR REPLACE TABLE stg_products AS
WITH cleaned AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(sub_category) AS sub_category,
        TRIM(brand) AS brand,
        TRY_CAST(NULLIF(TRIM(mrp), '') AS DECIMAL(12,2)) AS mrp,
        TRY_CAST(NULLIF(TRIM(cost_price), '') AS DECIMAL(12,2)) AS cost_price,
        TRIM(unit) AS unit,
        UPPER(TRIM(is_perishable)) AS is_perishable,
        TRY_CAST(NULLIF(TRIM(launch_date), '') AS DATE) AS launch_date,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(product_id)
            ORDER BY
                CASE
                    WHEN LOWER(TRIM(product_name)) LIKE '%(old)%' THEN 1
                    ELSE 0
                END ASC,
                _loaded_at DESC,
                TRIM(product_name) ASC
        ) AS row_num
    FROM raw_products
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
            WHEN product_id IS NULL OR product_id = ''
                THEN 'missing_product_id'
            WHEN product_name IS NULL OR product_name = ''
                THEN 'missing_product_name'
            WHEN category IS NULL OR category = ''
                THEN 'missing_category'
            WHEN mrp IS NULL OR mrp <= 0
                THEN 'invalid_mrp'
            WHEN cost_price IS NULL OR cost_price < 0
                THEN 'invalid_cost_price'
            WHEN cost_price > mrp
                THEN 'cost_price_exceeds_mrp'
            WHEN launch_date IS NULL
                THEN 'invalid_launch_date'
            ELSE ''
        END AS failure_reason
    FROM deduplicated
)
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    mrp,
    cost_price,
    unit,
    is_perishable,
    launch_date,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN failure_reason <> '' THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    failure_reason
FROM checked;
