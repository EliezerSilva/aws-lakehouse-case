"""Materializacao local do dataset publico NYC TLC."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs" / "nyc_tlc"))

from transform_silver import transform_trips

SOURCE = (
    ROOT / "data_public" / "nyc_tlc" / "raw"
    / "yellow_tripdata_2026-01.parquet"
)


def main():
    run_id = datetime.now(timezone.utc).strftime(
        "run_%Y%m%dT%H%M%S%fZ"
    )
    target = ROOT / "data_public" / "nyc_tlc" / "output" / run_id

    if not SOURCE.is_file():
        raise FileNotFoundError(SOURCE)

    target.mkdir(parents=True, exist_ok=False)

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("nyc-tlc-lakehouse")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        source = spark.read.parquet(str(SOURCE))

        bronze = (
            source
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_name", F.lit(SOURCE.name))
        )

        bronze.write.parquet(str(target / "bronze" / "trips"))

        bronze_saved = spark.read.parquet(
            str(target / "bronze" / "trips")
        )

        silver, quarantine = transform_trips(bronze_saved)

        silver.write.parquet(str(target / "silver" / "trips"))
        quarantine.write.parquet(
            str(target / "quarantine" / "trips")
        )

        silver_saved = spark.read.parquet(
            str(target / "silver" / "trips")
        )
        quarantine_saved = spark.read.parquet(
            str(target / "quarantine" / "trips")
        )

        gold = (
            silver_saved
            .withColumn(
                "trip_date",
                F.to_date("tpep_pickup_datetime")
            )
            .groupBy("trip_date", "PULocationID")
            .agg(
                F.count("*").alias("trip_records"),
                F.sum("trip_distance").alias("total_distance"),
                F.sum("total_amount").alias("net_total_amount"),
                F.sum(
                    F.when(F.col("total_amount") < 0, 1)
                    .otherwise(0)
                ).alias("negative_total_records")
            )
        )

        gold.write.parquet(
            str(target / "gold" / "daily_pickup_zone")
        )

        gold_saved = spark.read.parquet(
            str(target / "gold" / "daily_pickup_zone")
        )

        counts = {
            "bronze": bronze_saved.count(),
            "silver": silver_saved.count(),
            "quarantine": quarantine_saved.count(),
            "gold": gold_saved.count(),
        }

        if counts["bronze"] != (
            counts["silver"] + counts["quarantine"]
        ):
            raise ValueError("Falha na reconciliacao Bronze/Silver")

        if counts["bronze"] != 3724889:
            raise ValueError("Quantidade Bronze inesperada")

        if counts["quarantine"] != 1:
            raise ValueError("Quarentena diferente do discovery")

        gold_count = gold_saved.agg(
            F.sum("trip_records").alias("records")
        ).first()["records"]

        if gold_count != counts["silver"]:
            raise ValueError("Gold nao reconcilia com Silver")

        flags = silver_saved.agg(
            F.sum(
                F.col("flag_zero_duration_positive_distance")
                .cast("int")
            ).alias("zero_duration_positive_distance"),
            F.sum(
                F.col("flag_long_duration")
                .cast("int")
            ).alias("long_duration"),
            F.sum(
                F.col("flag_negative_total")
                .cast("int")
            ).alias("negative_total"),
            F.sum(
                F.col("flag_outside_reference_month")
                .cast("int")
            ).alias("outside_reference_month"),
        ).first().asDict()

        report = {
            "run_id": run_id,
            "status": "PASSED",
            "dataset": "NYC TLC Yellow Taxi - 2026-01",
            "source_type": "public_data",
            "execution_environment": "local_pyspark",
            "spark_version": spark.version,
            "counts": counts,
            "gold_trip_records": gold_count,
            "silver_quality_flags": flags,
            "aws_glue_executed": False,
        }

        (target / "validation_report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        print("\n=== MATERIALIZACAO NYC TLC APROVADA ===")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        print("\nRESULTADOS:", target)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
