"""Verify local Airflow pipeline outputs and the published run manifest."""
import argparse
import json
from pathlib import Path


EXPECTED_COUNTS = {
    "raw_count": 10,
    "valid_event_count": 7,
    "quarantine_count": 3,
    "silver_count": 5,
    "deduplicated_events": 2,
}


def verify(base: Path):
    report_path = base / "validation_report.json"
    if not report_path.is_file():
        raise FileNotFoundError(report_path)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("status") != "PASS" or report.get("environment") != "LOCAL_AIRFLOW_PYSPARK":
        raise ValueError("Missing PASS status or unexpected runtime")

    for key, expected in EXPECTED_COUNTS.items():
        if report.get(key) != expected:
            raise ValueError(f"Unexpected {key}: {report.get(key)} != {expected}")

    if report.get("gold_totals") != {"A100": "150.00", "A300": "40.00"}:
        raise ValueError("Unexpected financial totals")

    for name in ("bronze", "silver", "gold", "quarantine"):
        success_marker = base / name / "_SUCCESS"
        if not success_marker.is_file():
            raise FileNotFoundError(success_marker)

    if report["raw_count"] != report["valid_event_count"] + report["quarantine_count"]:
        raise ValueError("Bronze reconciliation failed")
    if report["valid_event_count"] != report["silver_count"] + report["deduplicated_events"]:
        raise ValueError("Silver reconciliation failed")
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    report = verify(Path(args.output))
    print(f"VERIFICATION PASS | Bronze={report['raw_count']} | "
          f"Silver={report['silver_count']} | Quarantine={report['quarantine_count']}")


if __name__ == "__main__":
    main()
