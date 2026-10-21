from pathlib import Path
import hashlib
import uuid
import duckdb

DATA_DIR = Path("/usr/local/airflow/include/data/nammamart")
WAREHOUSE = "/usr/local/airflow/include/warehouse_dpatil.duckdb"
FILENAME = "orders_2026-10-01_increment.csv"
TABLE = "raw_orders"

def main():
    path = DATA_DIR / FILENAME
    if not path.exists():
        raise FileNotFoundError(f"Missing incremental file: {path}")

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    run_id = "elt_increment_" + uuid.uuid4().hex[:12]

    con = duckdb.connect(WAREHOUSE)
    try:
        con.execute("BEGIN TRANSACTION")

        already_loaded = con.execute(
            """
            SELECT COUNT(*)
            FROM raw_ingestion_log
            WHERE source_file = ? AND content_sha256 = ?
              AND status = 'registered'
            """,
            [FILENAME, digest],
        ).fetchone()[0]

        if already_loaded:
            con.execute("COMMIT")
            print(f"SKIPPED: {FILENAME} version already ingested.")
            return

        before_count = con.execute(
            "SELECT COUNT(*) FROM raw_orders"
        ).fetchone()[0]

        con.execute(
            """
            INSERT INTO raw_orders
            SELECT
                *,
                CURRENT_TIMESTAMP AS _loaded_at,
                ? AS _source_file,
                ? AS _run_id
            FROM read_csv(?, all_varchar = true, header = true)
            """,
            [FILENAME, run_id, str(path)],
        )

        after_count = con.execute(
            "SELECT COUNT(*) FROM raw_orders"
        ).fetchone()[0]
        inserted = after_count - before_count

        expected = con.execute(
            "SELECT COUNT(*) FROM read_csv(?, all_varchar = true, header = true)",
            [str(path)],
        ).fetchone()[0]

        if inserted != expected:
            raise RuntimeError(
                f"Row-count mismatch: expected {expected}, inserted {inserted}"
            )

        con.execute(
            """
            INSERT INTO raw_ingestion_log
                (source_file, content_sha256, row_count, run_id, status)
            VALUES (?, ?, ?, ?, 'registered')
            """,
            [FILENAME, digest, inserted, run_id],
        )

        con.execute("COMMIT")
        print(f"SUCCESS: appended {inserted} rows from {FILENAME}")
        print(f"Raw orders before: {before_count}")
        print(f"Raw orders after:  {after_count}")
        print(f"Run ID: {run_id}")

    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    main()
