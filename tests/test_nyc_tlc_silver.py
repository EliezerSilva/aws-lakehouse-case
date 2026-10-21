"""Testes das regras de qualidade NYC TLC."""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs" / "nyc_tlc"))

from transform_silver import transform_trips


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("nyc-tlc-silver-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_quality_classification(spark):
    rows = [
        ("2026-01-10 10:00:00", "2026-01-10 10:15:00", 3.0, 20.0),
        ("2026-01-10 10:00:00", "2026-01-10 09:59:00", 1.0, 10.0),
        ("2026-01-10 11:00:00", "2026-01-10 11:00:00", 2.0, 12.0),
        ("2026-01-10 12:00:00", "2026-01-10 12:10:00", 1.0, -8.0),
        ("2025-12-31 23:58:00", "2026-01-01 00:10:00", 2.0, 15.0),
        (None, "2026-01-10 10:00:00", 1.0, 10.0),
    ]

    schema = """
        tpep_pickup_datetime STRING,
        tpep_dropoff_datetime STRING,
        trip_distance DOUBLE,
        total_amount DOUBLE
    """

    from pyspark.sql import functions as F

    df = spark.createDataFrame(rows, schema=schema)

    for column in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        df = df.withColumn(column, F.to_timestamp_ntz(column))

    silver, quarantine = transform_trips(df)

    assert silver.count() == 4
    assert quarantine.count() == 2

    reasons = {
        r["quality_rejection_reason"]
        for r in quarantine.select("quality_rejection_reason").collect()
    }
    assert reasons == {"DURACAO_NEGATIVA", "PICKUP_NULO"}

    assert silver.filter("flag_zero_duration_positive_distance").count() == 1
    assert silver.filter("flag_negative_total").count() == 1
    assert silver.filter("flag_outside_reference_month").count() == 1

    assert df.count() == silver.count() + quarantine.count()


def test_long_duration_alert(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [("2026-01-10 10:00:00", "2026-01-11 11:00:01", 10.0, 100.0)],
        """tpep_pickup_datetime STRING,
           tpep_dropoff_datetime STRING,
           trip_distance DOUBLE,
           total_amount DOUBLE"""
    )

    for column in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        df = df.withColumn(column, F.to_timestamp_ntz(column))

    silver, quarantine = transform_trips(df)

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.first()["flag_long_duration"] is True
