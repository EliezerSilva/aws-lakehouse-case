from banking_lab.pipeline import (
    RAW_SCHEMA, account_daily_totals, join_account_dimension,
    latest_by_transaction, normalize_and_validate,
    split_valid_invalid, validate_reconciliation,
)
import pytest


def event(event_id, transaction_id, account_id, amount, when,
          ingested="2026-11-17 10:00:00", status="APPROVED"):
    return event_id, transaction_id, account_id, amount, when, ingested, status, None


def raw(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)


def test_validation_quarantine(spark):
    rows = [
        event("E1", "T1", "A1", "100.00", "2026-11-17 08:00:00"),
        event("E2", "T2", "A1", "-5.00", "2026-11-17 08:00:00"),
        event("E3", "T3", "A1", "15.999", "2026-11-17 08:00:00"),
        event("E4", "T4", "A1", "20.00", "invalid"),
        event("E5", "T5", "A1", "20.00", "2026-11-17 08:00:00", status="INVALID"),
    ]
    valid, invalid = split_valid_invalid(normalize_and_validate(raw(spark, rows)))
    assert valid.count() == 1
    reasons = {x["event_id"]: x["error_reasons"] for x in invalid.collect()}
    assert invalid.count() == 4
    assert "INVALID_AMOUNT" in reasons["E2"]
    assert "INVALID_AMOUNT" in reasons["E3"]
    assert "INVALID_EVENT_TIME" in reasons["E4"]
    assert "INVALID_STATUS" in reasons["E5"]


def test_latest_deterministic_on_ties(spark):
    rows = [
        event("E1", "T1", "A1", "100.00", "2026-11-17 08:00:00"),
        event("E2", "T1", "A1", "120.00", "2026-11-17 08:05:00"),
        event("E3", "T1", "A1", "130.00", "2026-11-17 08:05:00"),
        event("E4", "T2", "A1", "40.00", "2026-11-17 08:00:00"),
    ]
    valid, _ = split_valid_invalid(normalize_and_validate(raw(spark, rows)))
    silver = latest_by_transaction(valid)
    assert {x["transaction_id"]: str(x["amount"]) for x in silver.collect()} == {
        "T1": "130.00", "T2": "40.00"
    }
    assert validate_reconciliation(valid, silver)["deduplicated_events"] == 2


def test_final_status_cancelled_not_summed(spark):
    rows = [
        event("E1", "T1", "A1", "200.00", "2026-11-17 08:00:00"),
        event("E2", "T1", "A1", "200.00", "2026-11-17 09:00:00", status="CANCELLED"),
        event("E3", "T2", "A1", "30.00", "2026-11-17 09:00:00"),
    ]
    valid, _ = split_valid_invalid(normalize_and_validate(raw(spark, rows)))
    gold = account_daily_totals(latest_by_transaction(valid)).collect()
    assert len(gold) == 1
    assert gold[0]["transaction_count"] == 1
    assert str(gold[0]["total_amount"]) == "30.00"


def test_join_rejects_duplicate_dimension_keys(spark):
    rows = [event("E1", "T1", "A1", "100.00", "2026-11-17 08:00:00")]
    valid, _ = split_valid_invalid(normalize_and_validate(raw(spark, rows)))
    silver = latest_by_transaction(valid)
    accounts = spark.createDataFrame([("A1", "C1"), ("A1", "C2")], ["account_id", "customer_id"])
    with pytest.raises(ValueError, match="DUPLICATE_ACCOUNT_ID"):
        join_account_dimension(silver, accounts)
    unique = accounts.dropDuplicates(["account_id"])
    assert join_account_dimension(silver, unique).count() == 1
