"""Kafka Bronze Parquet -> quality quarantine, Silver and Gold (batch).

Reuses the PySpark business rules validated in AWS Glue / local Airflow.
The source Bronze and Kafka topic are strictly read-only. Each run writes
to a *new* destination; overwrite and destructive cleanup are forbidden.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession, Window, functions as F
from banking_lab.pipeline import (
    account_daily_totals,
    latest_by_transaction,
    normalize_and_validate,
    split_valid_invalid,
    validate_reconciliation,
)

REQUIRED = {
    "topic", "partition", "offset", "kafka_timestamp", "kafka_key",
    "raw_payload", "event_id", "transaction_id", "account_id",
    "amount_str", "event_ts_str", "ingested_at_str", "status",
}
DEMO_EXPECTED = {
    "unique_event_count": 10,
    "valid_event_count": 7,
    "quarantine_count": 3,
    "silver_count": 5,
    "deduplicated_events": 2,
}
DEMO_TOTALS = {"A100": "150.00", "A300": "40.00"}
DEMO_TOPIC = "synthetic_transactions_raw"


def build_curated(bronze):
    """Return curated DataFrames and reconciled metrics; no writes.

    Replayed events with the same event_id + identical raw payload collapse
    deterministically by Kafka partition and offset. Conflicting payloads
    with the same event_id fail closed instead of silently winning.
    Events without event_id remain separate so quality rules quarantine them.
    """
    missing = REQUIRED.difference(bronze.columns)
    if missing:
        raise ValueError("KAFKA_BRONZE_MISSING_COLUMNS: " + ", ".join(sorted(missing)))

    input_count = bronze.count()
    id_is_present = F.col("event_id").isNotNull() & (F.trim("event_id") != "")
    keyed = bronze.filter(id_is_present)
    no_id = bronze.filter(~id_is_present)

    conflicting = (
        keyed.groupBy("event_id")
        .agg(F.countDistinct(F.coalesce(F.col("raw_payload"), F.lit("<NULL>"))).alias("payload_versions"))
        .filter(F.col("payload_versions") > 1)
        .limit(1)
        .count()
    )
    if conflicting:
        raise ValueError("CONFLICTING_EVENT_ID_PAYLOAD: manual review required")

    kafka_order = Window.partitionBy("event_id").orderBy(
        F.col("partition").asc(),
        F.col("offset").asc(),
    )
    deduplicated = (
        keyed.withColumn("_event_rank", F.row_number().over(kafka_order))
        .filter(F.col("_event_rank") == 1)
        .drop("_event_rank")
        .unionByName(no_id)
    )
    unique_count = deduplicated.count()
    if unique_count > input_count:
        raise RuntimeError("KAFKA_EVENT_RECONCILIATION_FAILED")

    # The shared batch contract expects a CSV parse diagnostic field.
    # JSON parse failures already produce null fields and are quarantined.
    classified = normalize_and_validate(
        deduplicated.withColumn("_corrupt_record", F.lit(None).cast("string"))
    )
    valid, quarantine = split_valid_invalid(classified)
    silver = latest_by_transaction(valid)
    gold = account_daily_totals(silver)
    metrics = validate_reconciliation(valid, silver)
    metrics.update({
        "raw_count": input_count,
        "unique_event_count": unique_count,
        "replayed_event_count": input_count - unique_count,
        "quarantine_count": quarantine.count(),
    })
    if unique_count != metrics["valid_event_count"] + metrics["quarantine_count"]:
        raise RuntimeError("KAFKA_QUALITY_RECONCILIATION_FAILED")
    if metrics["valid_event_count"] != metrics["silver_count"] + metrics["deduplicated_events"]:
        raise RuntimeError("KAFKA_SILVER_RECONCILIATION_FAILED")
    return {
        "silver": silver,
        "quarantine": quarantine,
        "gold": gold,
        "metrics": metrics,
    }


def gold_totals(gold):
    rows = gold.groupBy("account_id").agg(F.sum("total_amount").alias("total")).collect()
    return {row["account_id"]: str(row["total"]) for row in rows}


def verify_demo(metrics, totals):
    for key, expected in DEMO_EXPECTED.items():
        if metrics[key] != expected:
            raise RuntimeError(f"DEMO_CONTRACT_FAILED {key}: {metrics[key]} != {expected}")
    if totals != DEMO_TOTALS:
        raise RuntimeError(f"DEMO_GOLD_TOTALS_FAILED: {totals}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Existing Kafka Bronze Parquet directory (read-only)")
    parser.add_argument("--output", required=True, help="NEW isolated output run directory")
    parser.add_argument("--expect-demo", action="store_true", help="Assert synthetic 10/7/3/5 counts and Gold totals")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    if not input_path.is_dir():
        raise FileNotFoundError(f"Kafka Bronze directory not found: {input_path}")
    if input_path == output_path or input_path in output_path.parents or output_path in input_path.parents:
        raise ValueError("Input and output must be separate, non-nested directories")
    if output_path.exists():
        raise FileExistsError(f"Output already exists; select a new run path: {output_path}")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("kafka-bronze-silver-gold-data-quality")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.parquet(str(input_path))
        topics = {row["topic"] for row in raw.select("topic").distinct().collect()}
        if topics != {DEMO_TOPIC}:
            raise ValueError(f"UNEXPECTED_KAFKA_TOPICS: {topics}")

        result = build_curated(raw)
        totals = gold_totals(result["gold"])
        if args.expect_demo:
            verify_demo(result["metrics"], totals)

        # Output is isolated: no overwrite, no mutation of Kafka or Bronze.
        for layer in ("silver", "quarantine"):
            result[layer].write.mode("errorifexists").parquet(str(output_path / layer))
        result["gold"].write.mode("errorifexists").partitionBy("business_date").parquet(
            str(output_path / "gold")
        )

        # Verify persisted data, not only the lazy in-memory DataFrames.
        for layer, expected in (
            ("silver", result["metrics"]["silver_count"]),
            ("quarantine", result["metrics"]["quarantine_count"]),
        ):
            actual = spark.read.parquet(str(output_path / layer)).count()
            if actual != expected or not (output_path / layer / "_SUCCESS").is_file():
                raise RuntimeError(f"PERSISTED_LAYER_CHECK_FAILED: {layer}={actual}/{expected}")
        persisted_gold = gold_totals(spark.read.parquet(str(output_path / "gold")))
        if persisted_gold != totals or not (output_path / "gold" / "_SUCCESS").is_file():
            raise RuntimeError("PERSISTED_GOLD_CHECK_FAILED")

        report = {
            "status": "PASS",
            "runtime": "LOCAL_KAFKA_BRONZE_BATCH",
            "topic": DEMO_TOPIC,
            "bronze_source": str(input_path),
            "spark_version": spark.version,
            **result["metrics"],
            "gold_totals": totals,
        }
        # Last artifact is the publication signal. A partial run has no PASS report.
        (output_path / "validation_report.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(
            "KAFKA_CURATED_PASS | Raw={raw_count} | Valid={valid_event_count} | "
            "Quarantine={quarantine_count} | Silver={silver_count} | Replay={replayed_event_count}".format(**report)
        )
        print("GOLD_TOTALS " + json.dumps(totals, sort_keys=True))
        print("EVIDENCE " + str(output_path / "validation_report.json"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
