"""Materializa o laboratorio localmente, sem utilizar AWS Glue."""

import importlib.util
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "sources" / "erp_locacoes_cabecalho_2026.csv"
SILVER_JOB = ROOT / "jobs" / "02_silver_glue.py"

spec = importlib.util.spec_from_file_location("silver_job", SILVER_JOB)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def main():
    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S%fZ")
    target = ROOT / "output" / run_id

    if target.exists():
        raise FileExistsError(target)

    target.mkdir(parents=True, exist_ok=False)

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("aws-lakehouse-case-materialization")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        source = (
            spark.read.option("header", "true")
            .option("sep", ";")
            .csv(str(SOURCE))
        )

        bronze = (
            source.withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        bronze.write.parquet(str(target / "bronze" / "locacoes"))
        bronze_saved = spark.read.parquet(str(target / "bronze" / "locacoes"))

        silver, quarantine = module.transform_locacoes(bronze_saved)

        silver.write.parquet(str(target / "silver" / "locacoes"))
        quarantine.write.parquet(str(target / "quarantine" / "locacoes"))

        silver_saved = spark.read.parquet(str(target / "silver" / "locacoes"))
        quarantine_saved = spark.read.parquet(
            str(target / "quarantine" / "locacoes")
        )

        gold = (
            silver_saved.groupBy("id_loja", "data_inicio_date")
            .agg(
                F.count("id_locacao").alias("total_locacoes"),
                F.sum("valor_total_num").alias("receita_total")
            )
        )

        gold.write.parquet(str(target / "gold" / "receita_diaria_loja"))
        gold_saved = spark.read.parquet(
            str(target / "gold" / "receita_diaria_loja")
        )

        counts = {
            "bronze": bronze_saved.count(),
            "silver": silver_saved.count(),
            "quarantine": quarantine_saved.count(),
            "gold": gold_saved.count(),
        }

        metrics = gold_saved.agg(
            F.sum("total_locacoes").alias("locacoes"),
            F.sum("receita_total").alias("receita"),
            F.countDistinct("id_loja").alias("lojas"),
        ).first()

        reasons = {
            row["rejection_reason"]: row["count"]
            for row in quarantine_saved.groupBy("rejection_reason")
            .count().collect()
        }

        assert counts == {
            "bronze": 202,
            "silver": 199,
            "quarantine": 3,
            "gold": 144,
        }, counts

        assert metrics["locacoes"] == 199
        assert metrics["lojas"] == 6
        assert metrics["receita"] == Decimal("167767.51")

        assert reasons == {
            "VALOR_NEGATIVO": 1,
            "DUPLICIDADE_CONFLITANTE": 2,
        }, reasons

        report = {
            "run_id": run_id,
            "status": "PASSED",
            "execution_environment": "local_pyspark",
            "spark_version": spark.version,
            "source": SOURCE.name,
            "counts": counts,
            "gold_locacoes": metrics["locacoes"],
            "gold_lojas": metrics["lojas"],
            "gold_receita": str(metrics["receita"]),
            "rejection_reasons": reasons,
            "aws_glue_executed": False,
        }

        (target / "validation_report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        print("\n=== MATERIALIZACAO LOCAL APROVADA ===")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        print("\nRESULTADOS:", target)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
