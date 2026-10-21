from pathlib import Path
import hashlib
import csv
import duckdb

DATA_DIR = Path("/usr/local/airflow/include/data/nammamart")
WAREHOUSE = "/usr/local/airflow/include/warehouse_dpatil.duckdb"

BASE_FILES = {
    "orders.csv": "raw_orders",
    "payments.csv": "raw_payments",
    "customers.csv": "raw_customers",
    "products.csv": "raw_products",
    "stores.csv": "raw_stores",
}

con = duckdb.connect(WAREHOUSE)

try:
    for filename, table in BASE_FILES.items():
        path = DATA_DIR / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing source file: {path}")

        digest = hashlib.sha256(path.read_bytes()).hexdigest()

        with path.open("r", encoding="utf-8-sig", newline="") as f:
            row_count = sum(1 for _ in csv.DictReader(f))

        existing = con.execute(
            "SELECT COUNT(*) FROM raw_ingestion_log WHERE source_file = ? AND content_sha256 = ?",
            [filename, digest],
        ).fetchone()[0]

        if existing:
            print(f"Already registered: {filename}")
            continue

        actual_raw_count = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE _source_file = ?",
            [filename],
        ).fetchone()[0]

        if actual_raw_count != row_count:
            raise RuntimeError(
                f"{filename}: CSV has {row_count} rows but {table} "
                f"has {actual_raw_count} rows for this source. "
                "Baseline registration stopped."
            )

        con.execute(
            """
            INSERT INTO raw_ingestion_log
                (source_file, content_sha256, row_count, run_id, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            [filename, digest, row_count, "baseline_registration", "registered"],
        )
        print(f"Registered {filename}: {row_count} rows")

finally:
    con.close()
