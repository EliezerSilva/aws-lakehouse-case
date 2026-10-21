"""Testes da transformacao Silver com PySpark."""

import importlib.util
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "jobs" / "02_silver_glue.py"
DATASET = ROOT / "sources" / "erp_locacoes_cabecalho_2026.csv"

spec = importlib.util.spec_from_file_location("silver_glue", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("aws-lakehouse-silver-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_silver_dataset(spark):
    bronze = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .csv(str(DATASET))
    )

    silver, quarantine = module.transform_locacoes(bronze)

    assert bronze.count() == 202
    assert silver.count() == 199
    assert quarantine.count() == 3

    loc5 = silver.filter(
        F.col("id_locacao") == "LOC00005"
    ).collect()

    assert len(loc5) == 1
    assert loc5[0]["valor_total"] == "850.25"

    assert silver.filter(
        F.col("id_locacao") == "LOC00009"
    ).count() == 0

    reasons = {
        (r["id_locacao"], r["id_cliente"], r["rejection_reason"])
        for r in quarantine.select(
            "id_locacao", "id_cliente", "rejection_reason"
        ).collect()
    }

    assert reasons == {
        ("LOC00005", "C0034", "VALOR_NEGATIVO"),
        ("LOC00009", "C0024", "DUPLICIDADE_CONFLITANTE"),
        ("LOC00009", "C9999", "DUPLICIDADE_CONFLITANTE"),
    }

    assert (
        bronze.count() == silver.count() + quarantine.count()
    )


def test_identical_duplicates(spark):
    rows = [
        ("LOC001", "R001", "C001", "V001", "L001",
         "2026-10-01", "100.00", "FINALIZADA"),
        ("LOC001", "R001", "C001", "V001", "L001",
         "2026-10-01", "100.00", "FINALIZADA"),
    ]

    df = spark.createDataFrame(rows, module.BUSINESS_COLUMNS)

    silver, quarantine = module.transform_locacoes(df)

    assert silver.count() == 1
    assert quarantine.count() == 0


def test_invalid_date(spark):
    rows = [
        ("LOC002", "R002", "C001", "V001", "L001",
         "data-invalida", "100.00", "FINALIZADA"),
    ]

    df = spark.createDataFrame(rows, module.BUSINESS_COLUMNS)

    silver, quarantine = module.transform_locacoes(df)

    assert silver.count() == 0
    assert quarantine.count() == 1

    assert (
        quarantine.first()["rejection_reason"] == "DATA_INVALIDA"
    )
