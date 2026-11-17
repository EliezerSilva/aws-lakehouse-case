"""Verificacao ponta a ponta do arquivo sintetico de treinamento."""
from pathlib import Path

from banking_lab.pipeline import (
    account_daily_totals,
    latest_by_transaction,
    normalize_and_validate,
    read_raw_csv,
    split_valid_invalid,
    validate_reconciliation,
)

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "transactions.csv"


def test_sample_dataset_contract(spark):
    bronze = read_raw_csv(spark, str(SAMPLE))
    valid, quarantine = split_valid_invalid(normalize_and_validate(bronze))
    silver = latest_by_transaction(valid)
    gold = account_daily_totals(silver)

    assert bronze.count() == 10
    assert valid.count() == 7
    assert quarantine.count() == 3
    assert silver.count() == 5

    assert validate_reconciliation(valid, silver) == {
        "valid_event_count": 7,
        "unique_transaction_count": 5,
        "silver_count": 5,
        "deduplicated_events": 2,
    }

    reasons = {
        row["event_id"]: row["error_reasons"]
        for row in quarantine.select("event_id", "error_reasons").collect()
    }
    assert "INVALID_AMOUNT" in reasons["E004"]
    assert "INVALID_AMOUNT" in reasons["E005"]
    assert "INVALID_EVENT_TIME" in reasons["E006"]

    latest = {row["transaction_id"]: row
              for row in silver.select(
                  "transaction_id", "amount", "status"
              ).collect()}
    assert str(latest["T001"]["amount"]) == "120.00"
    assert latest["T006"]["status"] == "CANCELLED"
    assert latest["T007"]["status"] == "PENDING"

    totals = {
        row["account_id"]: str(row["total_amount"])
        for row in gold.select("account_id", "total_amount").collect()
    }
    assert totals == {"A100": "150.00", "A300": "40.00"}


def test_reprocessing_same_input_is_deterministic(spark):
    events = normalize_and_validate(read_raw_csv(spark, str(SAMPLE)))
    valid, _ = split_valid_invalid(events)
    original = {
        row["transaction_id"]: (str(row["amount"]), row["status"])
        for row in latest_by_transaction(valid).collect()
    }
    # Repetir os mesmos eventos na entrada nao altera a visao mais recente.
    replayed = valid.unionByName(valid)
    rebuilt = {
        row["transaction_id"]: (str(row["amount"]), row["status"])
        for row in latest_by_transaction(replayed).collect()
    }
    assert original == rebuilt
