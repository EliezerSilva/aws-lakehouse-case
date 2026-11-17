"""Kafka Bronze -> Quality/Silver/Gold unit tests, no running broker required."""
import importlib.util
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
CURATOR = ROOT / "kafka" / "curate_bronze.py"
spec = importlib.util.spec_from_file_location("banking_kafka_curator", CURATOR)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def synthetic_bronze(spark):
    raw = spark.read.option("header", True).csv(str(ROOT / "data" / "transactions.csv"))
    data_cols = [
        "event_id", "transaction_id", "account_id", "amount_str",
        "event_ts_str", "ingested_at_str", "status",
    ]
    return (
        raw.withColumn("topic", F.lit("synthetic_transactions_raw"))
        .withColumn("partition", (F.length("account_id") % 3).cast("int"))
        .withColumn("offset", F.monotonically_increasing_id().cast("long"))
        .withColumn("kafka_timestamp", F.current_timestamp())
        .withColumn("kafka_key", F.col("transaction_id"))
        .withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in data_cols])))
    )


def test_kafka_to_curated_contract(spark):
    result = module.build_curated(synthetic_bronze(spark))
    metrics = result["metrics"]
    module.verify_demo(metrics, module.gold_totals(result["gold"]))
    assert metrics["raw_count"] == 10
    assert metrics["replayed_event_count"] == 0
    assert result["quarantine"].count() == 3
    assert result["silver"].count() == 5
    assert {"partition", "offset", "kafka_key", "raw_payload"}.issubset(result["silver"].columns)
    reasons = {
        x["event_id"]: x["error_reasons"]
        for x in result["quarantine"].select("event_id", "error_reasons").collect()
    }
    assert "INVALID_AMOUNT" in reasons["E004"]
    assert "INVALID_AMOUNT" in reasons["E005"]
    assert "INVALID_EVENT_TIME" in reasons["E006"]


def test_kafka_replay_same_event_id_is_idempotent(spark):
    bronze = synthetic_bronze(spark)
    replay = bronze.filter(F.col("event_id") == "E001").withColumn("offset", F.lit(999).cast("long"))
    repeated = bronze.unionByName(replay)
    result = module.build_curated(repeated)
    assert result["metrics"]["raw_count"] == 11
    assert result["metrics"]["unique_event_count"] == 10
    assert result["metrics"]["replayed_event_count"] == 1
    module.verify_demo(result["metrics"], module.gold_totals(result["gold"]))


def test_kafka_replay_conflicting_payload_fails(spark):
    bronze = synthetic_bronze(spark)
    mismatch = (
        bronze.filter(F.col("event_id") == "E001")
        .withColumn("raw_payload", F.lit(json.dumps({"event_id": "E001", "amount_str": "999.00"})))
        .withColumn("offset", F.lit(999).cast("long"))
    )
    with pytest.raises(ValueError, match="CONFLICTING_EVENT_ID_PAYLOAD"):
        module.build_curated(bronze.unionByName(mismatch))


def test_kafka_missing_offset_is_rejected(spark):
    with pytest.raises(ValueError, match="KAFKA_BRONZE_MISSING_COLUMNS"):
        module.build_curated(synthetic_bronze(spark).drop("offset"))
