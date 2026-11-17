"""Exemplo de DAG Airflow 2.10.x; exige Spark e lab montado no worker."""
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="synthetic_banking_pyspark_lab",
    start_date=datetime(2026, 11, 17, tzinfo=timezone.utc),
    schedule=None,  # execucao manual, sem infraestrutura de cloud
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["pyspark", "training"],
) as dag:
    source_exists = BashOperator(
        task_id="check_source",
        bash_command="test -s /opt/airflow/lab/data/transactions.csv",
    )
    transform = BashOperator(
        task_id="transform",
        execution_timeout=timedelta(minutes=30),
        bash_command=(
            "spark-submit --master 'local[2]' "
            "/opt/airflow/lab/jobs/run_pipeline.py "
            "--input /opt/airflow/lab/data/transactions.csv "
            "--output /opt/airflow/lab/output/batch"
        ),
    )
    verify = BashOperator(
        task_id="verify_output",
        bash_command="test -e /opt/airflow/lab/output/batch/silver/_SUCCESS",
    )
    source_exists >> transform >> verify
