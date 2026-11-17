"""Optional AWS MWAA demo: S3 input -> Glue PySpark -> S3 validation report.

Requires: existing Glue job, uploaded synthetic input/code, suitable AWS IAM.
Never deploy automatically from this repository; MWAA incurs recurring costs.
"""
import json
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


BUCKET = os.environ.get("BANKING_LAB_BUCKET", "aws-lakehouse-case-lab-2026")
JOB_NAME = os.environ.get("BANKING_LAB_GLUE_JOB", "reply-synthetic-banking-pyspark-lab")
ROLE_ARN = os.environ.get("BANKING_LAB_GLUE_ROLE_ARN")  # Optional if Glue job pre-created.
INPUT_KEY = "interview-lab/input/transactions.csv"
SCRIPT_URI = f"s3://{BUCKET}/interview-lab/scripts/glue_job_aws.py"
REPORT_KEY = "interview-lab/runs/{{ ts_nodash }}/validation_report.json"


def verify_report(run_id, **_):
    raw = S3Hook(aws_conn_id="aws_default").read_key(
        key=f"interview-lab/runs/{run_id}/validation_report.json",
        bucket_name=BUCKET,
    )
    report = json.loads(raw)
    expected = {
        "raw_count": 10,
        "valid_event_count": 7,
        "quarantine_count": 3,
        "silver_count": 5,
        "deduplicated_events": 2,
    }
    if report.get("status") != "PASS" or report.get("run_id") != run_id:
        raise ValueError(f"Invalid report status/identity: {report}")
    for key, value in expected.items():
        if report.get(key) != value:
            raise ValueError(f"Bad {key}: {report.get(key)} != {value}")


with DAG(
    dag_id="reply_synthetic_banking_glue_demo",
    start_date=datetime(2026, 11, 17, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering-lab",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["pyspark", "aws-glue", "synthetic", "demo"],
) as dag:
    source_ready = S3KeySensor(
        task_id="source_ready",
        bucket_name=BUCKET,
        bucket_key=INPUT_KEY,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )
    process_glue = GlueJobOperator(
        task_id="process_glue",
        job_name=JOB_NAME,
        script_location=SCRIPT_URI,
        iam_role_arn=ROLE_ARN,
        script_args={
            "--INPUT_URI": f"s3://{BUCKET}/{INPUT_KEY}",
            "--BUCKET": BUCKET,
            "--RUN_ID": "{{ ts_nodash }}",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        verbose=True,
    )
    report_ready = S3KeySensor(
        task_id="report_ready",
        bucket_name=BUCKET,
        bucket_key=REPORT_KEY,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )
    validate = PythonOperator(
        task_id="verify_report",
        python_callable=verify_report,
        op_kwargs={"run_id": "{{ ts_nodash }}"},
    )
    source_ready >> process_glue >> report_ready >> validate
