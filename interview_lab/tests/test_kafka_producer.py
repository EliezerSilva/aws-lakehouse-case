"""Unitarios de produtor Kafka sem broker e sem dependencia instalada."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "kafka" / "producer.py"
spec = importlib.util.spec_from_file_location("banking_kafka_producer", SCRIPT)
producer_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(producer_module)


class FakeProducer:
    def __init__(self, fail=False):
        self.fail = fail
        self.messages = []

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))
        on_delivery("delivery failure" if self.fail else None, SimpleNamespace())

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


def test_synthetic_csv_delivery_ack():
    fake = FakeProducer()
    count = producer_module.publish_csv(
        ROOT / "data" / "transactions.csv",
        "synthetic_transactions_raw",
        fake,
    )
    assert count == 10
    assert len(fake.messages) == 10
    assert fake.messages[0][1] == b"T001"


def test_delivery_error_does_not_report_success():
    with pytest.raises(RuntimeError, match="Falhas de entrega"):
        producer_module.publish_csv(
            ROOT / "data" / "transactions.csv",
            "synthetic_transactions_raw",
            FakeProducer(fail=True),
        )


def test_missing_transaction_id_is_rejected(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("event_id,transaction_id\nE1,\n", encoding="utf-8")
    with pytest.raises(ValueError, match="transaction_id ausente"):
        producer_module.publish_csv(path, "t", FakeProducer())
