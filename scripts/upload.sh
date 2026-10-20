#!/usr/bin/env bash
set -euo pipefail
: "${AWS_BUCKET:?Defina AWS_BUCKET com o nome do bucket criado na AWS}"
aws sts get-caller-identity
aws s3 sync sources/ "s3://$AWS_BUCKET/sources/"
aws s3 sync jobs/ "s3://$AWS_BUCKET/scripts/"
echo "Arquivos enviados ao s3://$AWS_BUCKET/"
