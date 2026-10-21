
-- NammaMart ELT: Raw ingestion layer
-- Preserve CSV values as VARCHAR; add ingestion metadata.
-- This is a baseline load. Incremental append handling will be
-- implemented in the Airflow ELT DAG.

CREATE OR REPLACE TABLE raw_orders AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'orders.csv' AS _source_file,
    'initial_load' AS _run_id
FROM read_csv(
    '/usr/local/airflow/include/data/nammamart/orders.csv',
    all_varchar = true,
    header = true
);

CREATE OR REPLACE TABLE raw_payments AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'payments.csv' AS _source_file,
    'initial_load' AS _run_id
FROM read_csv(
    '/usr/local/airflow/include/data/nammamart/payments.csv',
    all_varchar = true,
    header = true
);

CREATE OR REPLACE TABLE raw_customers AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'customers.csv' AS _source_file,
    'initial_load' AS _run_id
FROM read_csv(
    '/usr/local/airflow/include/data/nammamart/customers.csv',
    all_varchar = true,
    header = true
);

CREATE OR REPLACE TABLE raw_products AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'products.csv' AS _source_file,
    'initial_load' AS _run_id
FROM read_csv(
    '/usr/local/airflow/include/data/nammamart/products.csv',
    all_varchar = true,
    header = true
);

CREATE OR REPLACE TABLE raw_stores AS
SELECT
    *,
    CURRENT_TIMESTAMP AS _loaded_at,
    'stores.csv' AS _source_file,
    'initial_load' AS _run_id
FROM read_csv(
    '/usr/local/airflow/include/data/nammamart/stores.csv',
    all_varchar = true,
    header = true
);
