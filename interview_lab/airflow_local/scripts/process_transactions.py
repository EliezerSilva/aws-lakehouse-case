"""PySpark execution callable by the local Airflow DAG.

Process the same source and transformations used in AWS Glue Studio.
All data is synthetic. Output is isolated by Airflow run timestamp.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, "/opt/airflow/lab-src")

from pyspark.sql import SparkSession
from banking_lab.pipeline import (
    account_daily_totals, latest_by_transaction, normalize_and_validate,
    read_raw_csv, split_valid_invalid, validate_reconciliation,
)

SAMPLE_COUNTS = {
    "raw_count": 10,
    "valid_event_count": 7,
    "quarantine_count": 3,
    "silver_count": 5,
    "deduplicated_events": 2,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    out_dir = Path(args.output)
    if not out_dir.is_absolute() or not str(out_dir).startswith("/opt/airflow/output/"):
        raise ValueError("Output must stay within /opt/airflow/output/<run>")

    spark = (
        SparkSession.builder
        .appName("banking-transactions-data-quality")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = read_raw_csv(spark, args.input)
        valid, quarantine = split_valid_invalid(normalize_and_validate(bronze))
        silver = latest_by_transaction(valid)
        gold = account_daily_totals(silver)

        report = validate_reconciliation(valid, silver)
        report["raw_count"] = bronze.count()
        report["quarantine_count"] = quarantine.count()

        if report["raw_count"] != report["valid_event_count"] + report["quarantine_count"]:
            raise RuntimeError("BRONZE_RECONCILIATION_FAILED")
        for key, expected in SAMPLE_COUNTS.items():
            if report[key] != expected:
                raise RuntimeError(f"INVALID_SAMPLE_COUNT {key}: {report[key]} != {expected}")

        gold_totals = {
            row["account_id"]: str(row["total_amount"])
            for row in gold.collect()
        }
        if gold_totals != {"A100": "150.00", "A300": "40.00"}:
            raise RuntimeError(f"GOLD_VALIDATION_FAILED: {gold_totals}")

        # Retry on same DAG run replaces only its own outputs.
        # Demonstration only: not an atomic multi-table transaction.
        bronze.write.mode("overwrite").parquet(str(out_dir / "bronze"))
        silver.write.mode("overwrite").parquet(str(out_dir / "silver"))
        quarantine.write.mode("overwrite").parquet(str(out_dir / "quarantine"))
        gold.write.mode("overwrite").partitionBy("business_date").parquet(
            str(out_dir / "gold")
        )

        complete = {
            "status": "PASS",
            "environment": "LOCAL_AIRFLOW_PYSPARK",
            "spark_version": spark.version,
            **report,
            "gold_totals": gold_totals,
        }
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "validation_report.json").write_text(
            json.dumps(complete, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print("VALIDATION_REPORT " + json.dumps(complete, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
