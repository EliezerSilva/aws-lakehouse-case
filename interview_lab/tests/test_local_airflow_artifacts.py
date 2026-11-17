"""Tests for local Airflow delivery without requiring Docker/Airflow on CI."""
import ast
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
AIRFLOW = ROOT / "airflow_local"


def test_local_airflow_python_syntax():
    for relative in (
        "dags/banking_transactions_pipeline.py",
        "scripts/process_transactions.py",
        "scripts/verify_results.py",
    ):
        code = (AIRFLOW / relative).read_text(encoding="utf-8")
        ast.parse(code, filename=relative)


def test_local_airflow_dag_dependencies_and_name():
    code = (AIRFLOW / "dags/banking_transactions_pipeline.py").read_text()
    assert 'dag_id="banking_transactions_pipeline"' in code
    assert "check_input >> process_pyspark >> verify_results" in code
    assert "process_transactions.py" in code
    assert "verify_results.py" in code


def test_local_airflow_verify_results_with_fixture(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "verify_results_local",
        AIRFLOW / "scripts/verify_results.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for layer in ("bronze", "silver", "gold", "quarantine"):
        folder = tmp_path / layer
        folder.mkdir()
        (folder / "_SUCCESS").touch()
    report = {
        "status": "PASS",
        "environment": "LOCAL_AIRFLOW_PYSPARK",
        "raw_count": 10, "valid_event_count": 7,
        "quarantine_count": 3, "silver_count": 5,
        "deduplicated_events": 2,
        "gold_totals": {"A100": "150.00", "A300": "40.00"},
    }
    (tmp_path / "validation_report.json").write_text(
        json.dumps(report), encoding="utf-8"
    )
    assert module.verify(tmp_path)["status"] == "PASS"
    report["quarantine_count"] = 4
    (tmp_path / "validation_report.json").write_text(json.dumps(report))
    with pytest.raises(ValueError, match="quarantine_count"):
        module.verify(tmp_path)
