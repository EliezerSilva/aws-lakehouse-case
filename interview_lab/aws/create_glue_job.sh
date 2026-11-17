#!/usr/bin/env bash
set -euo pipefail

# Creates a billable-when-executed Glue job definition; requires explicit --apply.
BUCKET="${AWS_LAB_BUCKET:-aws-lakehouse-case-lab-2026}"
REGION="${AWS_REGION:-us-east-2}"
JOB_NAME="reply-synthetic-banking-pyspark-lab"

if [[ "${1:-}" != "--apply" ]]; then
  echo "DRY-RUN: register AWS Glue 5.0 Spark job ${JOB_NAME} in ${REGION}"
  echo "Requires AWS_GLUE_ROLE_ARN and uploaded code (see upload_aws_demo.sh)."
  exit 0
fi
: "${AWS_GLUE_ROLE_ARN:?Set AWS_GLUE_ROLE_ARN to authorized execution role ARN}"
aws sts get-caller-identity >/dev/null
if aws glue get-job --job-name "${JOB_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  echo "Glue job already exists: ${JOB_NAME}. No configuration overwritten."
  exit 0
fi
DEFAULT_ARGS="$(python3 -c 'import json,sys; b=sys.argv[1]; print(json.dumps({"--job-language":"python","--extra-py-files":f"s3://{b}/interview-lab/libs/banking_lab.zip"}))' "${BUCKET}")"
aws glue create-job \
  --name "${JOB_NAME}" \
  --role "${AWS_GLUE_ROLE_ARN}" \
  --command "Name=glueetl,ScriptLocation=s3://${BUCKET}/interview-lab/scripts/glue_job_aws.py,PythonVersion=3" \
  --glue-version "5.0" \
  --worker-type "G.1X" \
  --number-of-workers 2 \
  --timeout 10 \
  --max-retries 0 \
  --default-arguments "${DEFAULT_ARGS}" \
  --region "${REGION}"
echo "REGISTERED: job not executed. Running it incurs Glue charges."
