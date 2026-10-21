from pathlib import Path
import re

import pandas as pd
import pendulum

from airflow.sdk import dag, task
from airflow.sdk import get_current_context

BASE_DIR = Path("/usr/local/airflow/include")
DATA_DIR = BASE_DIR / "data" / "nammamart"
QUARANTINE_DIR = BASE_DIR / "quarantine" / "dpatil"
STAGING_DIR = BASE_DIR / "staging" / "dpatil"

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

VALID_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
PAYMENT_MODE_MAP = {
    "UPI": "UPI",
    "CARD": "Card",
    "CASH ON DELIVERY": "Cash on Delivery",
    "COD": "Cash on Delivery",
    "WALLET": "Wallet",
}


@dag(
    dag_id="nammamart_etl_dpatil",
    description="NammaMart ETL extraction and orders validation",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "dhanesh",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
    },
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl_dpatil():

    @task
    def validate_other_source(source_metadata: dict) -> dict:
        """Validate payments, customers, products, and stores."""
        name = source_metadata["source_name"]
        filename = source_metadata["filename"]
        path = DATA_DIR / filename

        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df = df.fillna("")
        original_columns = list(df.columns)

        # Normalize known variations before applying validation rules.
        if name == "payments":
            df["payment_mode"] = (
                df["payment_mode"].str.strip().str.upper()
                .map(PAYMENT_MODE_MAP)
                .fillna(df["payment_mode"].str.strip())
            )
        elif name == "customers":
            tier_map = {
                "BRONZE": "Bronze",
                "SILVER": "Silver",
                "GOLD": "Gold",
                "PLATINUM": "Platinum",
            }
            df["loyalty_tier"] = (
                df["loyalty_tier"].str.strip().str.upper()
                .map(tier_map).fillna(df["loyalty_tier"].str.strip())
            )
        elif name == "stores":
            df["zone"] = df["zone"].str.strip().str.title()

        # Remove exact duplicate records for customers/products.
        if name in {"customers", "products"}:
            df = df.drop_duplicates().reset_index(drop=True)

        # Business assumption: product records marked "(old)" are
        # obsolete. Prefer the current catalogue record for each ID.
        # Keep customer conflict handling conservative.
        if name == "products":
            df["_is_old_product"] = (
                df["product_name"]
                .astype(str)
                .str.strip()
                .str.lower()
                .str.contains(r"\(old\)", regex=True, na=False)
            )

            df = (
                df.sort_values(
                    by=["_is_old_product", "product_name"],
                    ascending=[True, True],
                    kind="stable",
                )
                .drop_duplicates(subset=["product_id"], keep="first")
                .drop(columns=["_is_old_product"])
                .reset_index(drop=True)
            )

        key_column = {
            "payments": "payment_id",
            "customers": "customer_id",
            "products": "product_id",
            "stores": "store_id",
        }[name]

        # Identify conflicting duplicate keys without arbitrarily
        # choosing between different customer/product versions.
        duplicate_keys = set(
            df.loc[
                df[key_column].ne("")
                & df[key_column].duplicated(keep=False),
                key_column,
            ]
        )

        allowed_payment_statuses = {"SUCCESS", "REFUNDED", "FAILED"}
        allowed_payment_modes = set(PAYMENT_MODE_MAP.values())
        allowed_tiers = {"Bronze", "Silver", "Gold", "Platinum"}
        allowed_zones = {"North", "South", "East", "West"}

        order_ids = set()
        if name == "payments":
            orders_path = DATA_DIR / "orders.csv"
            increment_path = DATA_DIR / "orders_2026-10-01_increment.csv"

            for source_path in (orders_path, increment_path):
                if source_path.exists():
                    order_data = pd.read_csv(
                        source_path, dtype=str, keep_default_na=False
                    )
                    order_ids.update(
                        order_data["order_id"].astype(str).str.strip()
                    )
            order_ids.discard("")

        failure_reasons = []
        warning_reasons = []

        for _, row in df.iterrows():
            errors = []
            warnings = []
            key = str(row.get(key_column, "")).strip()

            if not key:
                errors.append(f"missing_{key_column}")
            elif key in duplicate_keys:
                errors.append(f"conflicting_duplicate_{key_column}")

            if name == "payments":
                if row["order_id"].strip() not in order_ids:
                    errors.append("unknown_order_id")

                if row["payment_status"].strip().upper() not in allowed_payment_statuses:
                    errors.append("invalid_payment_status")

                if row["payment_mode"] not in allowed_payment_modes:
                    errors.append("invalid_payment_mode")

                amount = pd.to_numeric(row["amount_paid"], errors="coerce")
                if pd.isna(amount) or amount <= 0:
                    errors.append("invalid_amount_paid")

                if pd.isna(pd.to_datetime(row["payment_ts"], errors="coerce")):
                    errors.append("invalid_payment_ts")

                if not row["gateway_ref"].strip():
                    warnings.append("missing_gateway_ref")

            elif name == "customers":
                if not row["full_name"].strip():
                    errors.append("missing_full_name")

                email = row["email"].strip()
                if email and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
                    warnings.append("invalid_email")
                elif not email:
                    warnings.append("missing_email")

                phone = row["phone"].strip()
                if phone and not re.fullmatch(r"\d{10}", phone):
                    warnings.append("invalid_phone")
                elif not phone:
                    warnings.append("missing_phone")

                if not re.fullmatch(r"\d{6}", row["pincode"].strip()):
                    errors.append("invalid_pincode")

                if pd.isna(pd.to_datetime(row["signup_date"], errors="coerce")):
                    errors.append("invalid_signup_date")

                if row["loyalty_tier"] not in allowed_tiers:
                    errors.append("invalid_loyalty_tier")

                if row["is_active"].strip().upper() not in {"Y", "N"}:
                    errors.append("invalid_is_active")

            elif name == "products":
                category = row["category"].strip()
                if not category:
                    errors.append("missing_category")

                mrp = pd.to_numeric(row["mrp"], errors="coerce")
                cost = pd.to_numeric(row["cost_price"], errors="coerce")

                if pd.isna(mrp) or mrp <= 0:
                    errors.append("invalid_mrp")
                if pd.isna(cost) or cost < 0:
                    errors.append("invalid_cost_price")
                if not pd.isna(mrp) and not pd.isna(cost) and cost > mrp:
                    errors.append("cost_price_exceeds_mrp")

                if pd.isna(pd.to_datetime(row["launch_date"], errors="coerce")):
                    errors.append("invalid_launch_date")

                if not row["product_name"].strip():
                    errors.append("missing_product_name")

            elif name == "stores":
                if row["zone"] not in allowed_zones:
                    errors.append("missing_or_invalid_zone")

                if not re.fullmatch(r"\d{6}", row["pincode"].strip()):
                    errors.append("invalid_pincode")

                capacity = pd.to_numeric(
                    row["capacity_orders_per_day"], errors="coerce"
                )
                if pd.isna(capacity) or capacity <= 0:
                    errors.append("invalid_capacity")

                if pd.isna(pd.to_datetime(row["open_date"], errors="coerce")):
                    errors.append("invalid_open_date")

            failure_reasons.append(";".join(errors))
            warning_reasons.append(";".join(warnings))

        df["failure_reason"] = failure_reasons
        df["warning_reason"] = warning_reasons

        failed = df[df["failure_reason"] != ""].copy()
        passed = df[df["failure_reason"] == ""].copy()

        context = get_current_context()
        run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])

        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        failed_path = QUARANTINE_DIR / f"{name}_failed_{run_id}.csv"
        passed_path = STAGING_DIR / f"{name}_validated_{run_id}.csv"

        # Preserve source columns and include reasons for auditability.
        failed.to_csv(failed_path, index=False)
        passed.to_csv(passed_path, index=False)

        result = {
            "source_name": name,
            "source_rows": int(len(df)),
            "passed_rows": int(len(passed)),
            "failed_rows": int(len(failed)),
            "passed_file": str(passed_path),
            "quarantine_file": str(failed_path),
        }

        print(f"=== {name.upper()} VALIDATION ===")
        print(result)
        print("Top failure reasons:")
        if len(failed):
            print(
                failed["failure_reason"].str.split(";").explode()
                .value_counts().head(10).to_string()
            )

        return result

    @task
    def extract_source(source_name: str, filename: str) -> dict:
        """Read one source file and return lightweight metadata."""
        path = DATA_DIR / filename

        if not path.exists():
            raise FileNotFoundError(f"Required source file not found: {path}")

        df = pd.read_csv(path, dtype=str, keep_default_na=False)

        result = {
            "source_name": source_name,
            "filename": filename,
            "rows": int(len(df)),
            "columns": list(df.columns),
            "status": "EXTRACTED",
        }

        print(f"Source: {filename}")
        print(f"Rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        return result

    @task
    def validate_orders(source_metadata: dict) -> dict:
        """Validate and standardize orders; quarantine failed rows."""
        # The metadata argument creates an Airflow dependency on extraction.
        print(f"Validating source: {source_metadata['filename']}")

        orders_path = DATA_DIR / "orders.csv"
        customers_path = DATA_DIR / "customers.csv"
        products_path = DATA_DIR / "products.csv"
        stores_path = DATA_DIR / "stores.csv"

        for path in (
            orders_path,
            customers_path,
            products_path,
            stores_path,
        ):
            if not path.exists():
                raise FileNotFoundError(f"Required source file not found: {path}")

        
        orders = pd.read_csv(
            orders_path, dtype=str, keep_default_na=False
        ).fillna("")

        increment_path = DATA_DIR / "orders_2026-10-01_increment.csv"
        if increment_path.exists():
            increment = pd.read_csv(
                increment_path, dtype=str, keep_default_na=False
            ).fillna("")

            # Remove exact duplicate source rows first.
            orders = orders.drop_duplicates()
            increment = increment.drop_duplicates()

            # Latest incremental version takes precedence for an order ID.
            corrected_ids = set(increment["order_id"].astype(str).str.strip())
            orders = orders[
                ~orders["order_id"].astype(str).str.strip().isin(corrected_ids)
            ]

            # Append all incremental records after removing superseded versions.
            orders = pd.concat([orders, increment], ignore_index=True)

            print(
                f"Incremental processing: {len(increment)} rows read; "
                f"{len(corrected_ids)} order IDs updated or inserted."
            )


        customers = pd.read_csv(
            customers_path, dtype=str, keep_default_na=False
        ).fillna("")
        products = pd.read_csv(
            products_path, dtype=str, keep_default_na=False
        ).fillna("")
        stores = pd.read_csv(
            stores_path, dtype=str, keep_default_na=False
        ).fillna("")

        # Normalize key whitespace before checking relationships.
        for frame, key in (
            (orders, "order_id"),
            (orders, "customer_id"),
            (orders, "product_id"),
            (orders, "store_id"),
            (customers, "customer_id"),
            (products, "product_id"),
            (stores, "store_id"),
        ):
            frame[key] = frame[key].astype(str).str.strip()

        customer_ids = set(customers["customer_id"]) - {""}
        product_ids = set(products["product_id"]) - {""}
        store_ids = set(stores["store_id"]) - {""}

        # Standardize known status and payment-mode variations.
        orders["order_status"] = (
            orders["order_status"].astype(str).str.strip().str.upper()
        )
        orders["payment_mode"] = (
            orders["payment_mode"].astype(str).str.strip().str.upper()
        )
        orders["payment_mode"] = orders["payment_mode"].map(
            PAYMENT_MODE_MAP
        ).fillna(orders["payment_mode"])

        # Convert numeric fields for checks and downstream calculations.
        numeric_columns = [
            "quantity", "unit_price", "discount_pct",
            "order_amount", "delivery_minutes",
        ]
        for column in numeric_columns:
            orders[column + "_num"] = pd.to_numeric(
                orders[column], errors="coerce"
            )

        parsed_dates = pd.to_datetime(
            orders["order_ts"], errors="coerce"
        )

        reasons_by_row = []
        seen_order_ids = set()

        for index, row in orders.iterrows():
            reasons = []

            order_id = row["order_id"]
            customer_id = row["customer_id"]
            product_id = row["product_id"]
            store_id = row["store_id"]

            if not order_id:
                reasons.append("missing_order_id")
            elif order_id in seen_order_ids:
                reasons.append("duplicate_order_id")
            else:
                seen_order_ids.add(order_id)

            if not row["order_ts"] or pd.isna(parsed_dates.iloc[index]):
                reasons.append("missing_or_invalid_order_ts")

            if not customer_id:
                reasons.append("missing_customer_id")
            elif customer_id not in customer_ids:
                reasons.append("unknown_customer_id")

            if not product_id:
                reasons.append("missing_product_id")
            elif product_id not in product_ids:
                reasons.append("unknown_product_id")

            if not store_id:
                reasons.append("missing_store_id")
            elif store_id not in store_ids:
                reasons.append("unknown_store_id")

            quantity = row["quantity_num"]
            unit_price = row["unit_price_num"]
            discount = row["discount_pct_num"]
            amount = row["order_amount_num"]
            delivery = row["delivery_minutes_num"]

            if pd.isna(quantity) or quantity <= 0:
                reasons.append("quantity_missing_or_not_positive")

            if pd.isna(unit_price) or unit_price <= 0:
                reasons.append("unit_price_missing_or_not_positive")

            if pd.isna(discount) or not 0 <= discount <= 100:
                reasons.append("discount_missing_or_out_of_range")

            if pd.isna(amount):
                reasons.append("order_amount_missing_or_invalid")
            elif (
                not pd.isna(quantity)
                and not pd.isna(unit_price)
                and not pd.isna(discount)
            ):
                expected_amount = round(
                    quantity * unit_price * (1 - discount / 100), 2
                )
                if abs(amount - expected_amount) > 0.01:
                    reasons.append("order_amount_formula_mismatch")

            if row["order_status"] not in VALID_STATUSES:
                reasons.append("invalid_order_status")

            if row["payment_mode"] not in set(PAYMENT_MODE_MAP.values()):
                reasons.append("invalid_payment_mode")

            if row["order_status"] == "DELIVERED":
                if pd.isna(delivery):
                    reasons.append("delivered_order_missing_delivery_minutes")
                elif delivery < 0 or delivery >= 120:
                    reasons.append("delivery_minutes_out_of_range")
            elif not pd.isna(delivery) and (
                delivery < 0 or delivery >= 120
            ):
                reasons.append("delivery_minutes_out_of_range")

            reasons_by_row.append(";".join(reasons))

        orders["failure_reason"] = reasons_by_row

        failed_mask = orders["failure_reason"] != ""
        failed = orders.loc[failed_mask].copy()
        passed = orders.loc[~failed_mask].copy()

        # Keep only source columns plus the quarantine explanation.
        source_columns = [
            column for column in orders.columns
            if not column.endswith("_num")
        ]
        failed = failed[source_columns]
        passed = passed[
            [column for column in orders.columns
             if not column.endswith("_num") and column != "failure_reason"]
        ]

        context = get_current_context()
        run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])

        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        failed_path = QUARANTINE_DIR / f"orders_failed_{run_id}.csv"
        passed_path = STAGING_DIR / f"orders_validated_{run_id}.csv"

        failed.to_csv(failed_path, index=False)
        passed.to_csv(passed_path, index=False)

        summary = {
            "source_rows": int(len(orders)),
            "passed_rows": int(len(passed)),
            "failed_rows": int(len(failed)),
            "passed_file": str(passed_path),
            "quarantine_file": str(failed_path),
        }

        print("=== ORDERS VALIDATION SUMMARY ===")
        print(f"Source rows: {summary['source_rows']}")
        print(f"Passed rows: {summary['passed_rows']}")
        print(f"Quarantined rows: {summary['failed_rows']}")
        print(f"Validated output: {passed_path}")
        print(f"Quarantine output: {failed_path}")

        return summary
    @task(pool="nammamart_duckdb_pool")
    def transform_and_load(
        order_validation: dict,
        other_validations: list[dict],
    ) -> dict:
        """Transform validated files and load the DuckDB warehouse."""
        import duckdb

        warehouse_path = BASE_DIR / "warehouse_dpatil.duckdb"
        warehouse_path.parent.mkdir(parents=True, exist_ok=True)

        def read_validated(summary):
            return pd.read_csv(
                summary["passed_file"],
                dtype=str,
                keep_default_na=False,
            )

        # Use the exact output files produced by this DAG run.
        orders = read_validated(order_validation)
        validated = {
            result["source_name"]: read_validated(result)
            for result in other_validations
        }

        customers = validated["customers"]
        products = validated["products"]
        stores = validated["stores"]
        payments = validated["payments"]

        def mask_email(value):
            value = str(value).strip()
            if "@" not in value:
                return "***" if value else ""
            local, domain = value.split("@", 1)
            return local[:2] + "***@" + domain

        def mask_phone(value):
            digits = re.sub(r"\D", "", str(value))
            if not digits:
                return ""
            if len(digits) <= 4:
                return "*" * len(digits)
            return digits[:2] + "*" * (len(digits) - 4) + digits[-2:]

        # Build customer dimension without exposing raw email/phone.
        customers["email_masked"] = customers["email"].map(mask_email)
        customers["phone_masked"] = customers["phone"].map(mask_phone)

        customer_dim = customers[
            [
                "customer_id", "full_name", "email_masked",
                "phone_masked", "city", "area", "pincode",
                "signup_date", "loyalty_tier", "is_active",
            ]
        ].copy()

        customer_dim["signup_date"] = pd.to_datetime(
            customer_dim["signup_date"], errors="coerce"
        ).dt.date

        # Prepare product and store dimensions.
        product_dim = products[
            [
                "product_id", "product_name", "category",
                "sub_category", "brand", "mrp", "cost_price",
                "unit", "is_perishable", "launch_date",
            ]
        ].copy()

        for column in ("mrp", "cost_price"):
            product_dim[column] = pd.to_numeric(
                product_dim[column], errors="coerce"
            )

        product_dim["launch_date"] = pd.to_datetime(
            product_dim["launch_date"], errors="coerce"
        ).dt.date

        product_dim["is_perishable"] = (
            product_dim["is_perishable"].str.strip().str.lower()
            .map({"true": True, "false": False})
        )

        store_dim = stores[
            [
                "store_id", "store_name", "zone", "area",
                "pincode", "open_date", "manager_name",
                "capacity_orders_per_day",
            ]
        ].copy()

        store_dim["open_date"] = pd.to_datetime(
            store_dim["open_date"], errors="coerce"
        ).dt.date
        store_dim["capacity_orders_per_day"] = pd.to_numeric(
            store_dim["capacity_orders_per_day"], errors="coerce"
        )

        # Convert numeric order columns.
        for column in (
            "quantity", "unit_price", "discount_pct",
            "order_amount", "delivery_minutes",
        ):
            orders[column] = pd.to_numeric(
                orders[column], errors="coerce"
            )

        orders["order_ts"] = pd.to_datetime(
            orders["order_ts"], errors="coerce"
        )
        orders["order_date"] = orders["order_ts"].dt.date

        # Orders can reference a source master record that was itself
        # quarantined. Do not create a fact row with a broken dimension key.
        customer_ids = set(customer_dim["customer_id"])
        product_ids = set(product_dim["product_id"])
        store_ids = set(store_dim["store_id"])


        bad_dimension_reasons = []
        for _, row in orders.iterrows():
            reasons = []
            if row["customer_id"] not in customer_ids:
                reasons.append("customer_not_in_validated_dimension")
            if row["product_id"] not in product_ids:
                reasons.append("product_not_in_validated_dimension")
            if row["store_id"] not in store_ids:
                reasons.append("store_not_in_validated_dimension")
            bad_dimension_reasons.append(";".join(reasons))

        orders["failure_reason"] = bad_dimension_reasons
        dimension_failed = orders[orders["failure_reason"] != ""].copy()

        # Preserve every available order column for audit and troubleshooting.
        dimension_rejected = dimension_failed.copy()

        fact_source = orders[orders["failure_reason"] == ""].copy()


        fact_source = orders[orders["failure_reason"] == ""].copy()

        context = get_current_context()
        run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])

        if not dimension_failed.empty:
            QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
            dimension_failed.to_csv(
                QUARANTINE_DIR / f"orders_dimension_failed_{run_id}.csv",
                index=False,
            )

        # Add product cost for margin calculation.
        cost_lookup = product_dim[["product_id", "cost_price"]]
        fact_source = fact_source.merge(
            cost_lookup,
            on="product_id",
            how="left",
            validate="many_to_one",
        )

        fact_source["net_revenue"] = fact_source["order_amount"].where(
            fact_source["order_status"] == "DELIVERED", 0.0
        )
        fact_source["gross_margin"] = (
            fact_source["net_revenue"]
            - fact_source["quantity"] * fact_source["cost_price"]
        ).where(fact_source["order_status"] == "DELIVERED", 0.0)

        fact_source["within_15_min"] = (
            fact_source["delivery_minutes"].notna()
            & fact_source["delivery_minutes"].le(15)
        )

        fact_orders = fact_source[
            [
                "order_id", "order_date", "order_ts",
                "customer_id", "product_id", "store_id",
                "quantity", "unit_price", "discount_pct",
                "order_amount", "order_status", "payment_mode",
                "channel", "delivery_minutes", "net_revenue",
                "gross_margin", "within_15_min",
            ]
        ].copy()

        # Compare successful payments against clean orders.
        payments["payment_status"] = (
            payments["payment_status"].str.strip().str.upper()
        )
        payments["amount_paid"] = pd.to_numeric(
            payments["amount_paid"], errors="coerce"
        )

        order_amounts = dict(
            zip(fact_orders["order_id"], fact_orders["order_amount"])
        )
        successful = payments[
            payments["payment_status"] == "SUCCESS"
        ].copy()

        reconciliation_rows = []
        for _, payment in successful.iterrows():
            order_id = payment["order_id"]
            paid_amount = payment["amount_paid"]

            if order_id not in order_amounts:
                reason = "no_clean_order_for_success_payment"
                expected = None
            else:
                expected = order_amounts[order_id]
                if pd.isna(paid_amount):
                    reason = "invalid_successful_payment_amount"
                elif abs(paid_amount - expected) > 0.01:
                    reason = "successful_payment_amount_mismatch"
                else:
                    continue

            reconciliation_rows.append({
                "payment_id": payment["payment_id"],
                "order_id": order_id,
                "payment_status": payment["payment_status"],
                "amount_paid": paid_amount,
                "expected_order_amount": expected,
                "reason": reason,
            })

        successful_order_ids = set(successful["order_id"])
        for _, order in fact_orders.iterrows():
            if (
                order["order_status"] == "DELIVERED"
                and order["order_id"] not in successful_order_ids
            ):
                reconciliation_rows.append({
                    "payment_id": "",
                    "order_id": order["order_id"],
                    "payment_status": "",
                    "amount_paid": None,
                    "expected_order_amount": order["order_amount"],
                    "reason": "delivered_order_missing_success_payment",
                })

        reconciliation = pd.DataFrame(
            reconciliation_rows,
            columns=[
                "payment_id", "order_id", "payment_status",
                "amount_paid", "expected_order_amount", "reason",
            ],
        )

        # Idempotent table replacement: reruns do not append duplicate facts.
        with duckdb.connect(str(warehouse_path)) as con:
            con.register("customer_dim_df", customer_dim)
            con.register("product_dim_df", product_dim)
            con.register("store_dim_df", store_dim)
            con.register("fact_orders_df", fact_orders)
            con.register("reconciliation_df", reconciliation)

            con.execute("""
                CREATE OR REPLACE TABLE etl_dim_customer AS
                SELECT * FROM customer_dim_df
            """)
            con.execute("""
                CREATE OR REPLACE TABLE etl_dim_product AS
                SELECT * FROM product_dim_df
            """)
            con.execute("""
                CREATE OR REPLACE TABLE etl_dim_store AS
                SELECT * FROM store_dim_df
            """)
            
            con.register("dimension_rejected_df", dimension_rejected)
            con.execute("""
                CREATE OR REPLACE TABLE etl_rejected_orders AS
                SELECT * FROM dimension_rejected_df
            """)

            con.execute("""
                CREATE OR REPLACE TABLE etl_fact_orders AS
                SELECT * FROM fact_orders_df
            """)
            con.execute("""
                CREATE OR REPLACE TABLE etl_payment_reconciliation AS
                SELECT * FROM reconciliation_df
            """)

            con.execute("""
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT
                    f.order_date,
                    s.zone,
                    COUNT(DISTINCT f.order_id) AS order_count,
                    SUM(f.net_revenue) AS net_revenue,
                    AVG(
                        CASE WHEN f.order_status = 'DELIVERED'
                             THEN f.delivery_minutes END
                    ) AS avg_delivery_minutes
                FROM etl_fact_orders f
                JOIN etl_dim_store s USING (store_id)
                GROUP BY f.order_date, s.zone
            """)

            con.execute("""
                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT
                    p.category,
                    COUNT(DISTINCT f.order_id) AS order_count,
                    SUM(f.net_revenue) AS net_revenue,
                    SUM(f.gross_margin) AS gross_margin
                FROM etl_fact_orders f
                JOIN etl_dim_product p USING (product_id)
                GROUP BY p.category
            """)

            table_names = [
                "etl_dim_customer", "etl_dim_product",
                "etl_dim_store", "etl_fact_orders",
                "etl_payment_reconciliation",
                "etl_daily_zone_revenue", "etl_category_margin",
            ]
            counts = {
                table: con.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in table_names
            }

        result = {
            "warehouse": str(warehouse_path),
            "fact_rows": len(fact_orders),
            "orders_rejected_after_dimension_check": len(dimension_failed),
            "table_counts": counts,
        }

        print("=== DUCKDB WAREHOUSE SUMMARY ===")
        print(result)
        return result
    
    @task(pool="nammamart_duckdb_pool")
    def audit_etl_run(
        extraction_results: list[dict],
        orders_validation: dict,
        other_validations: list[dict],
        warehouse_summary: dict,
    ) -> dict:
        """Record one audit entry for this ETL DAG run."""
        from datetime import datetime, timezone
        import duckdb

        context = get_current_context()
        dag_run = context["dag_run"]
        run_id = str(context["run_id"])
        dag_id = str(context["dag"].dag_id)

        start_time = dag_run.start_date or datetime.now(timezone.utc)
        end_time = datetime.now(timezone.utc)

        rows_extracted = sum(
            int(item["rows"]) for item in extraction_results
        )

        table_counts = warehouse_summary["table_counts"]
        core_tables = (
            "etl_dim_customer",
            "etl_dim_product",
            "etl_dim_store",
            "etl_fact_orders",
        )
        rows_loaded = sum(
            int(table_counts.get(table, 0))
            for table in core_tables
        )

        rows_quarantined = int(
            orders_validation.get("failed_rows", 0)
        )
        rows_quarantined += sum(
            int(item.get("failed_rows", 0))
            for item in other_validations
        )
        rows_quarantined += int(
            warehouse_summary.get(
                "orders_rejected_after_dimension_check", 0
            )
        )

        con = duckdb.connect(warehouse_summary["warehouse"])
        try:
            con.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, dag_id, start_time, end_time,
                    rows_extracted, rows_loaded,
                    rows_quarantined, status
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM pipeline_runs
                    WHERE run_id = ? AND dag_id = ?
                )
                """,
                [
                    run_id, dag_id, start_time, end_time,
                    rows_extracted, rows_loaded,
                    rows_quarantined, "SUCCESS",
                    run_id, dag_id,
                ],
            )
        finally:
            con.close()

        result = {
            "run_id": run_id,
            "dag_id": dag_id,
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
            "rows_quarantined": rows_quarantined,
            "status": "SUCCESS",
        }
        print("=== ETL AUDIT SUMMARY ===")
        print(result)
        return result

    @task
    def report_extraction(
        results: list[dict], validation: dict
    ) -> None:
        print("=== NAMMAMART ETL EXTRACTION SUMMARY ===")

        total_rows = 0
        for result in results:
            print(
                f"{result['filename']}: {result['rows']} rows, "
                f"{len(result['columns'])} columns"
            )
            total_rows += result["rows"]

        print(f"Total rows across five source files: {total_rows}")
        print("=== VALIDATION SUMMARY ===")
        print(validation)

   
    orders_extract = extract_source("orders", SOURCE_FILES["orders"])


    other_extracts = {
        name: extract_source(name, filename)
        for name, filename in SOURCE_FILES.items()
        if name != "orders"
    }

    validation_summary = validate_orders(orders_extract)

    other_validation_summaries = [
        validate_other_source(other_extracts[name])
        for name in ("payments", "customers", "products", "stores")
    ]

    warehouse_summary = transform_and_load(
        validation_summary,
        other_validation_summaries,
    )

    report_extraction(
        [orders_extract, *other_extracts.values()],
        [
            validation_summary,
            *other_validation_summaries,
            warehouse_summary,
        ],
    )
    audit_summary = audit_etl_run(
        [orders_extract, *other_extracts.values()],
        validation_summary,
        other_validation_summaries,
        warehouse_summary,
    )

nammamart_etl_dpatil()
