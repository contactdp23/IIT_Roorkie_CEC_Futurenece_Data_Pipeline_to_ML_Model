
-- NammaMart ELT: Customer staging
-- Raw data remains unchanged. PII is masked in staging.

CREATE OR REPLACE TABLE stg_customers AS
WITH conflicting_ids AS (
    SELECT TRIM(customer_id) AS customer_id
    FROM raw_customers
    WHERE TRIM(customer_id) <> ''
    GROUP BY TRIM(customer_id)
    HAVING COUNT(DISTINCT CONCAT_WS(
        '|',
        TRIM(full_name),
        TRIM(email),
        TRIM(phone),
        TRIM(city),
        TRIM(area),
        TRIM(pincode),
        TRIM(signup_date),
        UPPER(TRIM(loyalty_tier)),
        UPPER(TRIM(is_active))
    )) > 1
),
cleaned AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(full_name) AS full_name,
        LOWER(TRIM(email)) AS email_clean,
        REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g') AS phone_digits,
        TRIM(city) AS city,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(NULLIF(TRIM(signup_date), '') AS DATE) AS signup_date,
        CASE UPPER(TRIM(loyalty_tier))
    WHEN 'BRONZE' THEN 'Bronze'
    WHEN 'SILVER' THEN 'Silver'
    WHEN 'GOLD' THEN 'Gold'
    WHEN 'PLATINUM' THEN 'Platinum'
    ELSE TRIM(loyalty_tier)
END AS loyalty_tier,
        UPPER(TRIM(is_active)) AS is_active,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(customer_id)
            ORDER BY _loaded_at DESC, TRIM(email) DESC, TRIM(phone) DESC
        ) AS row_num
    FROM raw_customers
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
            WHEN customer_id IS NULL OR customer_id = ''
                THEN 'missing_customer_id'
            WHEN full_name IS NULL OR full_name = ''
                THEN 'missing_full_name'
            WHEN EXISTS (
                SELECT 1
                FROM conflicting_ids ci
                WHERE ci.customer_id = deduplicated.customer_id
            )
                THEN 'conflicting_duplicate_customer_id'
            WHEN pincode IS NULL OR NOT REGEXP_FULL_MATCH(pincode, '[0-9]{6}')
                THEN 'invalid_pincode'
            WHEN signup_date IS NULL
                THEN 'invalid_signup_date'
            WHEN is_active NOT IN ('Y', 'N')
                THEN 'invalid_is_active'
            ELSE ''
        END AS critical_failure,
        CASE
            WHEN email_clean IS NULL OR email_clean = ''
                 OR NOT REGEXP_FULL_MATCH(
                     email_clean,
                     '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
                 )
                THEN 'invalid_email'
            WHEN phone_digits IS NULL
                 OR NOT REGEXP_FULL_MATCH(phone_digits, '[0-9]{10}')
                THEN 'invalid_phone'
            ELSE ''
        END AS warning_failure
    FROM deduplicated
)
SELECT
    customer_id,
    full_name,
    CASE
        WHEN email_clean IS NULL OR email_clean = ''
             OR NOT REGEXP_FULL_MATCH(
                 email_clean,
                 '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
             )
            THEN NULL
        ELSE
            LEFT(SPLIT_PART(email_clean, '@', 1), 2)
            || '***@' || SPLIT_PART(email_clean, '@', 2)
    END AS masked_email,
    CASE
        WHEN REGEXP_FULL_MATCH(phone_digits, '[0-9]{10}')
            THEN LEFT(phone_digits, 2) || '******' || RIGHT(phone_digits, 2)
        ELSE NULL
    END AS masked_phone,
    city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
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
