# Arquitetura do laboratorio
Fontes locais sintéticas -> S3/sources -> Glue Bronze Parquet -> Glue Silver Parquet/quarantine -> Glue Gold Parquet -> Glue Catalog/Athena.

Evolucoes: schema explicito, qualidade referencial, ingestion_log, Iceberg incremental, CDC/DMS, Step Functions, dbt, Lake Formation, CI/CD, IAM minimo.
