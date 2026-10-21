
-- NammaMart ELT: Store staging

CREATE OR REPLACE TABLE stg_stores AS
WITH cleaned AS (
    SELECT
        TRIM(store_id) AS store_id,
        TRIM(store_name) AS store_name,
        UPPER(TRIM(zone)) AS zone_upper,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(NULLIF(TRIM(open_date), '') AS DATE) AS open_date,
        TRIM(manager_name) AS manager_name,
        TRY_CAST(NULLIF(TRIM(capacity_orders_per_day), '') AS INTEGER)
            AS capacity_orders_per_day,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(store_id)
            ORDER BY _loaded_at DESC, TRIM(store_name) DESC
        ) AS row_num
    FROM raw_stores
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
            WHEN store_id IS NULL OR store_id = ''
                THEN 'missing_store_id'
            WHEN store_name IS NULL OR store_name = ''
                THEN 'missing_store_name'
            WHEN zone_upper IS NULL
                 OR zone_upper NOT IN ('NORTH', 'SOUTH', 'EAST', 'WEST')
                THEN 'invalid_zone'
            WHEN pincode IS NULL
                 OR NOT REGEXP_FULL_MATCH(pincode, '[0-9]{6}')
                THEN 'invalid_pincode'
            WHEN open_date IS NULL
                THEN 'invalid_open_date'
            WHEN capacity_orders_per_day IS NULL
                 OR capacity_orders_per_day <= 0
                THEN 'invalid_capacity'
            ELSE ''
        END AS failure_reason
    FROM deduplicated
)
SELECT
    store_id,
    store_name,
    CASE zone_upper
        WHEN 'NORTH' THEN 'North'
        WHEN 'SOUTH' THEN 'South'
        WHEN 'EAST' THEN 'East'
        WHEN 'WEST' THEN 'West'
        ELSE zone_upper
    END AS zone,
    area,
    pincode,
    open_date,
    manager_name,
    capacity_orders_per_day,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN failure_reason <> '' THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    failure_reason
FROM checked;
