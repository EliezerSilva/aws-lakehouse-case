"""Independent check of Kafka -> curated Parquet artifacts."""
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def verify(spark, directory: Path):
    report_path = directory / "validation_report.json"
    if not report_path.is_file():
        raise FileNotFoundError(f"No successful publication report: {report_path}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("status") != "PASS" or report.get("runtime") != "LOCAL_KAFKA_BRONZE_BATCH":
        raise RuntimeError("UNEXPECTED_CURATED_REPORT")

    layers = ("silver", "quarantine", "gold")
    for name in layers:
        if not (directory / name / "_SUCCESS").is_file():
            raise FileNotFoundError(f"Missing committed {name}/_SUCCESS")

    silver = spark.read.parquet(str(directory / "silver"))
    quarantine = spark.read.parquet(str(directory / "quarantine"))
    gold = spark.read.parquet(str(directory / "gold"))

    if silver.count() != report["silver_count"]:
        raise RuntimeError("SILVER_COUNT_MISMATCH")
    if quarantine.count() != report["quarantine_count"]:
        raise RuntimeError("QUARANTINE_COUNT_MISMATCH")
    if quarantine.filter(F.col("error_reasons") == "").limit(1).count():
        raise RuntimeError("QUARANTINE_HAS_NO_ERROR_REASON")
    if silver.groupBy("transaction_id").count().filter(F.col("count") > 1).limit(1).count():
        raise RuntimeError("SILVER_TRANSACTION_DUPLICATE")
    if report["raw_count"] != report["unique_event_count"] + report["replayed_event_count"]:
        raise RuntimeError("REPLAY_RECONCILIATION_MISMATCH")
    if report["unique_event_count"] != report["valid_event_count"] + report["quarantine_count"]:
        raise RuntimeError("QUALITY_RECONCILIATION_MISMATCH")
    if report["valid_event_count"] != report["silver_count"] + report["deduplicated_events"]:
        raise RuntimeError("SILVER_RECONCILIATION_MISMATCH")

    rows = gold.groupBy("account_id").agg(F.sum("total_amount").alias("total")).collect()
    totals = {r["account_id"]: str(r["total"]) for r in rows}
    if totals != report["gold_totals"]:
        raise RuntimeError(f"GOLD_RECONCILIATION_MISMATCH: {totals}")
    for name in ("silver", "quarantine"):
        cols = spark.read.parquet(str(directory / name)).columns
        if not {"topic", "partition", "offset", "kafka_key", "raw_payload"}.issubset(cols):
            raise RuntimeError(f"KAFKA_LINEAGE_MISSING_{name.upper()}")
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("verify-kafka-curated")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        report = verify(spark, Path(args.output))
        print(
            "KAFKA_CURATED_VERIFICATION_PASS | Raw={raw_count} | "
            "Quarantine={quarantine_count} | Silver={silver_count}".format(**report)
        )
        print("GOLD_TOTALS " + json.dumps(report["gold_totals"], sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
