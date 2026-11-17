# AWS demonstration: Glue Studio notebook + Airflow/MWAA

**All records are synthetic. Neither Reply nor Banco Inter is involved.** This directory extends the *existing local* lab. The notebook and the MWAA DAG are code assets; neither service is deployed automatically.

## Confirmed versus pending

- Confirmed on user's Mac: local PySpark tests (6 passed) and local Spark pipeline (PASS).
- New: AWS Glue Studio notebook, Glue job script, S3 upload helper, optional MWAA DAG (static validation required).
- Pending: successful Glue interactive session, remote Glue job and MWAA runtime in the user's AWS account. Previous account limits may block Glue or MWAA.
- Cost: Glue sessions and jobs are billed; MWAA can have significant ongoing cost. Do not provision MWAA for a short interview demo unless already available.

## Direct links

- AWS Glue Studio Console Ohio: https://console.aws.amazon.com/gluestudio/home?region=us-east-2
- AWS Glue jobs Ohio: https://console.aws.amazon.com/glue/home?region=us-east-2
- Amazon MWAA Console Ohio: https://console.aws.amazon.com/mwaa/home?region=us-east-2
- Official AWS import instructions: https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions-convert.html

## 1. Before using AWS: credentials and S3

From repository root on the Mac, after selecting an authorized AWS profile (if any):

    export AWS_REGION=us-east-2
    export AWS_LAB_BUCKET=aws-lakehouse-case-lab-2026
    aws sts get-caller-identity
    aws s3api head-bucket --bucket "$AWS_LAB_BUCKET" --region "$AWS_REGION"

Prepare the upload; the first command does not upload:

    bash interview_lab/aws/upload_aws_demo.sh
    bash interview_lab/aws/upload_aws_demo.sh --apply

Uploads only these **synthetic lab artifacts**:

    s3://aws-lakehouse-case-lab-2026/interview-lab/input/transactions.csv
    s3://aws-lakehouse-case-lab-2026/interview-lab/scripts/glue_job_aws.py
    s3://aws-lakehouse-case-lab-2026/interview-lab/libs/banking_lab.zip

No login tokens, credentials or customer data should be committed.

## 2. Notebook in AWS Glue Studio

1. Download `interview_lab/notebooks/aws_glue_banking_demo.ipynb` from GitHub to the Mac.
2. Sign in to the authorized AWS account and select **US East (Ohio) us-east-2**.
3. Open **AWS Glue Studio → Jobs → Jupyter Notebook**.
4. Choose **Upload and edit an existing notebook**, select the `.ipynb`, give it a name, select an appropriate IAM execution role and create the notebook.
5. Configure Glue **5.0** if allowed, worker type G.1X, minimal permitted workers and a short idle timeout. IAM requires ability to pass a role plus S3 read/write and Glue session access. Apply changes *before* starting a session.
6. Execute one cell at a time: config → Bronze → validation/quarantine → Silver → Gold → reconciliation → S3 evidence → Spark explain.
7. Verify printed `PASS` and see `s3://<bucket>/interview-lab/runs/<RUN_ID>/validation_report.json`.
8. Stop the interactive session immediately after use (`%stop_session` in a fresh cell).

Notebook outputs are run-isolated. A second write with the same RUN_ID intentionally fails (errorifexists); rerun the config cell to generate a new ID.

### If Glue session cannot start

Do **not** change IAM or request access beyond your authorization. Use the already proven local Spark demo and show code/test evidence. This is especially important because past attempts in this account encountered Glue restrictions.

## 3. Airflow: existing local sample and AWS MWAA option

Existing local example:

    interview_lab/dags/banking_daily_dag.py

New AWS integration:

    interview_lab/dags/mwaa_glue_banking_dag.py

Logical flow:

    S3KeySensor(input) -> GlueJobOperator -> S3KeySensor(validation_report) -> PythonOperator(assert PASS)

Requires *an existing, configured MWAA environment* and a registered AWS Glue job. The AWS IAM role for Airflow must have permission to access the S3 input/report, run and inspect the Glue job; the Glue job role must read/write the lab S3 prefix. Confirm the Airflow and amazon-provider versions supported in your MWAA environment.

Register Glue job definition, **without starting it**, only if permitted:

    export AWS_GLUE_ROLE_ARN=arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<YOUR_GLUE_ROLE>
    bash interview_lab/aws/create_glue_job.sh
    bash interview_lab/aws/create_glue_job.sh --apply

The AWS job uses Glue 5.0, 2 G.1X workers and the uploaded `banking_lab.zip` via `--extra-py-files`. A registered job by itself does not prove runtime permissions.

To deploy the DAG to an **existing MWAA**, find its own environment S3 bucket and DAG path in the MWAA console. Upload `mwaa_glue_banking_dag.py` to that DAG path (not automatically the lab bucket). The environment must contain a compatible `apache-airflow-providers-amazon` package and the appropriate AWS credentials/role. Trigger `reply_synthetic_banking_glue_demo` **manually** only after checking costs and IAM.

An MWAA environment can be expensive even if the DAG has schedule=None. Nothing in this commit creates MWAA or runs a cloud job.

## 4. How to demo with evidence

- Notebook: show the Bronze rows and `_corrupt_record` metadata, 3 quarantined records with reasons, 5 final Silver transactions and Gold A100=150.00 / A300=40.00.
- Run report: show persisted S3 JSON with run ID, metrics and `status: PASS`.
- Airflow: show the DAG and explain dependencies/retries; mark it **not executed on MWAA** unless there are actual remote logs.
- Spark: explain why window + row_number is used instead of dropDuplicates; discuss shuffle, skew and idempotency, and emphasize that a Parquet overwrite is not an atomic MERGE.

## 5. Backout / cleanup

Stop the Glue interactive session; stop the Glue job if running; do not delete or alter existing mobility datasets or S3 files. Demo artifacts are isolated under `interview-lab/`. If you decide to delete demo output later, inspect the prefix explicitly before issuing a recursive delete.

No AWS services are created or started by merely browsing the GitHub repository.
