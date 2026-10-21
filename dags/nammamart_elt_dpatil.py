
from pathlib import Path
from datetime import timedelta
import subprocess
import sys
import pendulum

from airflow.sdk import dag, task

BASE_DIR = Path("/usr/local/airflow/include")
DATA_DIR = BASE_DIR / "data" / "nammamart"
SQL_DIR = BASE_DIR / "sql"
STAGING_DIR = SQL_DIR / "staging"
MARTS_DIR = SQL_DIR / "marts"
WAREHOUSE_PATH = BASE_DIR / "warehouse_dpatil.duckdb"

SOURCE_FILES = [
    "orders.csv",
    "payments.csv",
    "customers.csv",
    "products.csv",
    "stores.csv",
    "orders_2026-10-01_increment.csv",
]

STAGING_FILES = [
    "stg_customers.sql",
    "stg_products.sql",
    "stg_stores.sql",
    "stg_payments.sql",
    "stg_orders.sql",
]

MART_FILES = [
    "mart_daily_store_kpis.sql",
    "mart_category_margin.sql",
    "mart_delivery_sla.sql",
    "mart_payment_reconciliation.sql",
    "mart_top_customers.sql",
]


@dag(
    dag_id="nammamart_elt_dpatil",
    description="NammaMart ELT pipeline: incremental raw ingestion, SQL staging and marts",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "dhanesh",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["nammamart", "elt", "assignment"],
)
def nammamart_elt_dpatil():

    @task
    def check_environment():
        import duckdb

        if not WAREHOUSE_PATH.exists():
            raise FileNotFoundError(
                f"Warehouse not found: {WAREHOUSE_PATH}"
            )

        missing_sources = [
            name for name in SOURCE_FILES
            if not (DATA_DIR / name).is_file()
        ]
        if missing_sources:
            raise FileNotFoundError(
                f"Missing source files: {missing_sources}"
            )

        missing_sql = [
            str(path)
            for path in (
                [STAGING_DIR / name for name in STAGING_FILES]
                + [MARTS_DIR / name for name in MART_FILES]
            )
            if not path.is_file()
        ]
        if missing_sql:
            raise FileNotFoundError(
                f"Missing SQL files: {missing_sql}"
            )

        with duckdb.connect(
            str(WAREHOUSE_PATH), read_only=True
        ) as con:
            tables = {
                row[0]
                for row in con.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                """).fetchall()
            }

        required_tables = {
            "raw_orders",
            "raw_payments",
            "raw_customers",
            "raw_products",
            "raw_stores",
            "raw_ingestion_log",
        }
        missing_tables = sorted(required_tables - tables)

        if missing_tables:
            raise RuntimeError(
                f"Missing raw tables: {missing_tables}"
            )

        print("ELT environment validation passed.")
        print(f"Source files checked: {len(SOURCE_FILES)}")
        print(f"Staging SQL files checked: {len(STAGING_FILES)}")
        print(f"Mart SQL files checked: {len(MART_FILES)}")

    @task(pool="nammamart_duckdb_pool")
    def ingest_increment():
        import re

        script = BASE_DIR / "ingest_increment.py"

        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=300,
        )

        print(result.stdout)
        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                "Incremental raw ingestion failed."
            )

        match = re.search(
            r"SUCCESS: appended (\d+) rows",
            result.stdout,
        )

        if match:
            rows_loaded = int(match.group(1))
        elif "SKIPPED:" in result.stdout:
            rows_loaded = 0
        else:
            raise RuntimeError(
                "Could not determine rows loaded from ingestion output."
            )

        import csv

        source_path = DATA_DIR / "orders_2026-10-01_increment.csv"
        with source_path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as source_file:
            rows_extracted = sum(1 for _ in csv.DictReader(source_file))

        print(f"Rows extracted from incremental file: {rows_extracted}")
        print(f"Rows newly loaded in this run: {rows_loaded}")
        return {
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
        }

    @task(pool="nammamart_duckdb_pool")
    def build_staging():
        import duckdb

        with duckdb.connect(str(WAREHOUSE_PATH)) as con:
            for filename in STAGING_FILES:
                sql_path = STAGING_DIR / filename
                sql_text = sql_path.read_text(encoding="utf-8")

                print(f"Executing staging SQL: {filename}")
                con.execute(sql_text)

        print("All staging tables rebuilt successfully.")

    @task(pool="nammamart_duckdb_pool")
    def build_marts():
        import duckdb

        with duckdb.connect(str(WAREHOUSE_PATH)) as con:
            for filename in MART_FILES:
                sql_path = MARTS_DIR / filename
                sql_text = sql_path.read_text(encoding="utf-8")

                print(f"Executing mart SQL: {filename}")
                con.execute(sql_text)

        print("All five reporting marts rebuilt.")


    @task(pool="nammamart_duckdb_pool")
    def validate_outputs(ingestion_metrics):
        import duckdb

        from datetime import datetime, timezone
        from airflow.sdk import get_current_context

        context = get_current_context()
        run_id = str(context["run_id"])
        dag_id = str(context["dag"].dag_id)
        started_at = datetime.now(timezone.utc)

        expected_marts = {
            "mart_daily_store_kpis",
            "mart_category_margin",
            "mart_delivery_sla",
            "mart_payment_reconciliation",
            "mart_top_customers",
        }

        checks = []

        with duckdb.connect(str(WAREHOUSE_PATH)) as con:
            tables = {
                row[0] for row in con.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                """).fetchall()
            }

            missing = sorted(expected_marts - tables)
            if missing:
                raise RuntimeError(f"Missing reporting marts: {missing}")

            for mart in sorted(expected_marts):
                count = con.execute(
                    f'SELECT COUNT(*) FROM "{mart}"'
                ).fetchone()[0]
                print(f"{mart}: {count} rows")

            raw_orders = con.execute(
                "SELECT COUNT(*) FROM raw_orders"
            ).fetchone()[0]

            # Each query returns (rows_checked, rows_failed).
            check_sql = [
                (
                    "orders_missing_order_id", "completeness", "high",
                    """
                    SELECT COUNT(*),
                           COUNT(*) FILTER (
                               WHERE order_id IS NULL OR TRIM(order_id) = ''
                           )
                    FROM stg_orders
                    """
                ),
                (
                    "duplicate_staged_order_ids", "uniqueness", "critical",
                    """
                    SELECT COUNT(*),
                           COUNT(*) FILTER (WHERE order_id IS NOT NULL)
                    FROM (
                        SELECT order_id
                        FROM stg_orders
                        WHERE order_id IS NOT NULL
                        GROUP BY order_id
                        HAVING COUNT(*) > 1
                    ) d
                    """
                ),
                (
                    "invalid_staged_order_status", "validity", "critical",
                    """
                    SELECT COUNT(*),
                           COUNT(*) FILTER (
                               WHERE order_status IS NULL
                                  OR UPPER(TRIM(order_status)) NOT IN
                                     ('DELIVERED', 'CANCELLED', 'RETURNED')
                           )
                    FROM stg_orders
                    """
                ),
                (
                    "order_amount_formula_mismatch", "accuracy", "critical",
                    """
                    SELECT COUNT(*),
                           COUNT(*) FILTER (
                               WHERE quantity IS NOT NULL
                                 AND unit_price IS NOT NULL
                                 AND discount_pct IS NOT NULL
                                 AND order_amount IS NOT NULL
                                 AND ABS(
                                     order_amount -
                                     ROUND(
                                         quantity * unit_price *
                                         (1 - discount_pct / 100.0), 2
                                     )
                                 ) > 0.01
                           )
                    FROM stg_orders
                    """
                ),
                (
                    "delivered_orders_customer_missing", "consistency", "high",
                    """
                    SELECT COUNT(*),
                           COUNT(*) FILTER (
                               WHERE c.customer_id IS NULL
                           )
                    FROM stg_orders o
                    LEFT JOIN stg_customers c
                      ON o.customer_id = c.customer_id
                    WHERE UPPER(TRIM(o.order_status)) = 'DELIVERED'
                    """
                ),
                (
                    "delivered_orders_missing_delivery_time",
                    "timeliness", "high",
                    """
                    SELECT COUNT(*),
                           COUNT(*) FILTER (
                               WHERE delivery_minutes IS NULL
                           )
                    FROM stg_orders
                    WHERE UPPER(TRIM(order_status)) = 'DELIVERED'
                    """
                ),
            ]

            checked_at = datetime.now(timezone.utc)

            for check_name, dimension, severity, sql in check_sql:
                rows_checked, rows_failed = con.execute(sql).fetchone()
                status = "PASS" if rows_failed == 0 else "FAIL"

                checks.append({
                    "check_name": check_name,
                    "dimension": dimension,
                    "severity": severity,
                    "rows_checked": int(rows_checked),
                    "rows_failed": int(rows_failed),
                    "status": status,
                })

                con.execute("""
                    INSERT INTO dq_results
                    (run_id, dag_id, check_name, dimension, severity,
                     rows_checked, rows_failed, status, checked_at)
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dq_results
                        WHERE run_id = ? AND check_name = ?
                    )
                """, [
                    run_id, dag_id, check_name, dimension, severity,
                    int(rows_checked), int(rows_failed), status, checked_at,
                    run_id, check_name,
                ])

                print(
                    f"DQ {check_name}: {status} "
                    f"({rows_failed}/{rows_checked} failed)"
                )

            failed_orders = con.execute("""
                SELECT COUNT(*)
                FROM stg_orders
                WHERE dq_status = 'FAIL'
            """).fetchone()[0]
            ended_at = datetime.now(timezone.utc)

            con.execute("""
                INSERT INTO pipeline_runs
                (run_id, dag_id, start_time, end_time, rows_extracted,
                 rows_loaded, rows_quarantined, status)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM pipeline_runs WHERE run_id = ?
                )
            """, [
                run_id, dag_id, started_at, ended_at,
                int(ingestion_metrics["rows_extracted"]),
                int(ingestion_metrics["rows_loaded"]),
                int(failed_orders), "SUCCESS", run_id,
            ])

        print(f"Raw orders retained: {raw_orders}")
        print("ELT output validation completed.")
        return {"run_id": run_id, "dq_checks": checks}

    env = check_environment()
    ingested = ingest_increment()
    staging = build_staging()
    marts = build_marts()
    validated = validate_outputs(ingested)

    env >> ingested >> staging >> marts >> validated


nammamart_elt_dpatil()




