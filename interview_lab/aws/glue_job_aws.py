"""AWS Glue 5.0 job for the synthetic banking interview lab.

Requires --extra-py-files pointing at the uploaded banking_lab.zip.
No hardcoded credentials. Does not operate on any real customer data.
"""
import json
import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from banking_lab.pipeline import (
    read_raw_csv, normalize_and_validate, split_valid_invalid,
    latest_by_transaction, account_daily_totals, validate_reconciliation,
)


def main():
    args = getResolvedOptions(
        sys.argv, ["INPUT_URI", "BUCKET", "RUN_ID"]
    )
    source = args["INPUT_URI"]
    bucket = args["BUCKET"]
    run_id = args["RUN_ID"]
    if "/" in run_id or ".." in run_id or not run_id:
        raise ValueError("Invalid RUN_ID")

    prefix = f"interview-lab/runs/{run_id}"
    output = f"s3://{bucket}/{prefix}"
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    bronze = read_raw_csv(spark, source)
    valid, quarantine = split_valid_invalid(normalize_and_validate(bronze))
    silver = latest_by_transaction(valid)
    metrics = validate_reconciliation(valid, silver)
    metrics["raw_count"] = bronze.count()
    metrics["quarantine_count"] = quarantine.count()

    if metrics["raw_count"] != metrics["valid_event_count"] + metrics["quarantine_count"]:
        raise RuntimeError("RAW_RECONCILIATION_FAILED")

    # Contrato de aceite EXCLUSIVO do arquivo sintetico de 10 eventos.
    if (
        metrics["raw_count"], metrics["valid_event_count"],
        metrics["quarantine_count"], metrics["silver_count"],
        metrics["deduplicated_events"]
    ) != (10, 7, 3, 5, 2):
        raise RuntimeError(f"SAMPLE_CONTRACT_FAILED: {metrics}")

    gold = account_daily_totals(silver)
    totals = {
        row["account_id"]: str(row["total_amount"])
        for row in gold.collect()
    }
    if totals != {"A100": "150.00", "A300": "40.00"}:
        raise RuntimeError(f"GOLD_CONTRACT_FAILED: {totals}")

    # Isolamento por RUN_ID. Retries usam o mesmo prefixo de run,
    # e sobrescrevem somente os artefatos desse run. Nao e transacao atomica.
    bronze.write.mode("overwrite").parquet(f"{output}/bronze")
    silver.write.mode("overwrite").parquet(f"{output}/silver")
    quarantine.write.mode("overwrite").parquet(f"{output}/quarantine")
    gold.write.mode("overwrite").partitionBy("business_date").parquet(f"{output}/gold")

    report = {
        "run_id": run_id,
        "status": "PASS",
        "runtime": "AWS_GLUE_JOB",
        "spark_version": spark.version,
        **metrics,
        "gold_totals": totals,
    }
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=f"{prefix}/validation_report.json",
        Body=json.dumps(report, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
