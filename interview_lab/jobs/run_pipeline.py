"""spark-submit --master local[2] jobs/run_pipeline.py --input ... --output ..."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession
from banking_lab.pipeline import (
    account_daily_totals, latest_by_transaction, normalize_and_validate,
    read_raw_csv, split_valid_invalid, validate_reconciliation,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("synthetic-banking-pyspark-lab")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        raw = read_raw_csv(spark, args.input)
        valid, rejected = split_valid_invalid(normalize_and_validate(raw))
        silver = latest_by_transaction(valid)
        metrics = validate_reconciliation(valid, silver)
        metrics["raw_count"] = raw.count()
        metrics["quarantine_count"] = rejected.count()
        if metrics["raw_count"] != metrics["valid_event_count"] + metrics["quarantine_count"]:
            raise RuntimeError("RAW_RECONCILIATION_FAILED")
        gold = account_daily_totals(silver)
        # Apenas demonstracao isolada: overwrite Parquet NAO equivale a MERGE atomico.
        silver.write.mode("overwrite").parquet(args.output + "/silver")
        rejected.write.mode("overwrite").parquet(args.output + "/quarantine")
        gold.write.mode("overwrite").partitionBy("business_date").parquet(args.output + "/gold")
        print(json.dumps({"status": "PASS", **metrics}, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
