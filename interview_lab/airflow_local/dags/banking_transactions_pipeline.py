"""Local Airflow DAG: source check -> PySpark -> evidence verification.

Runs only against synthetic data from the repository. No AWS services required.
Compatible with Apache Airflow 2.10.x for an isolated local presentation.
"""
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="banking_transactions_pipeline",
    description="PySpark data quality, latest transaction version, financial aggregation and reconciliation",
    start_date=datetime(2026, 11, 17, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["data-engineering", "pyspark", "data-quality"],
) as dag:
    check_input = BashOperator(
        task_id="check_input",
        bash_command="test -s /opt/airflow/input/transactions.csv",
        execution_timeout=timedelta(minutes=1),
    )
    process_pyspark = BashOperator(
        task_id="process_pyspark",
        bash_command=(
            "spark-submit --master 'local[2]' "
            "/opt/airflow/lab-scripts/process_transactions.py "
            "--input /opt/airflow/input/transactions.csv "
            "--output /opt/airflow/output/{{ ts_nodash }}"
        ),
        execution_timeout=timedelta(minutes=8),
    )
    verify_results = BashOperator(
        task_id="verify_results",
        bash_command=(
            "python /opt/airflow/lab-scripts/verify_results.py "
            "--output /opt/airflow/output/{{ ts_nodash }}"
        ),
        execution_timeout=timedelta(minutes=2),
    )
    check_input >> process_pyspark >> verify_results
