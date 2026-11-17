#!/usr/bin/env bash
set -euo pipefail

# Explicit opt-in to small S3 writes, never triggers billable Glue or MWAA compute.
LAB_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REGION="${AWS_REGION:-us-east-2}"
BUCKET="${AWS_LAB_BUCKET:-aws-lakehouse-case-lab-2026}"
PREFIX="interview-lab"

if [[ "${1:-}" != "--apply" ]]; then
  echo "DRY-RUN: would upload synthetic CSV, Python module ZIP, and Glue job to:"
  echo "s3://${BUCKET}/${PREFIX}/ (region ${REGION})"
  echo "Run with --apply to upload; S3 requests/storage may incur charges."
  exit 0
fi

aws sts get-caller-identity >/dev/null
aws s3api head-bucket --bucket "${BUCKET}" --region "${REGION}" >/dev/null
TMPDIR_WORK="$(mktemp -d)"
trap 'rm -rf "${TMPDIR_WORK}"' EXIT
(
  cd "${LAB_ROOT}/src"
  zip -q -r "${TMPDIR_WORK}/banking_lab.zip" banking_lab -x '*/__pycache__/*' '*.pyc'
)
aws s3 cp "${LAB_ROOT}/data/transactions.csv" "s3://${BUCKET}/${PREFIX}/input/transactions.csv" --region "${REGION}"
aws s3 cp "${LAB_ROOT}/aws/glue_job_aws.py" "s3://${BUCKET}/${PREFIX}/scripts/glue_job_aws.py" --region "${REGION}"
aws s3 cp "${TMPDIR_WORK}/banking_lab.zip" "s3://${BUCKET}/${PREFIX}/libs/banking_lab.zip" --region "${REGION}"
echo "SUCCESS: inputs and code uploaded; AWS Glue/MWAA NOT started."
