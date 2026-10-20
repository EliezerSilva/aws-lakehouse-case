# aws-lakehouse-case

Laboratorio AWS de Engenharia de Dados, com dataset 100% sintetico de mobilidade. Inspirado na organizacao Bronze / Silver / Gold / Control do repositorio `databricks-lakehouse-case`. **Nao e infraestrutura ou dado real da Localiza.**

## Fontes
10 arquivos de origem (CSV com separadores diferentes, TXT pipe, JSON array, JSONL); veja `sources/`. Somente `erp_locacoes_cabecalho_2026.csv` e processado na **primeira execucao** do pipeline. Os outros arquivos ficam preparados para etapas seguintes.

## Requisitos
Conta AWS com permissoes adequadas, S3, IAM role de execucao Glue, Glue Spark jobs e Athena. **Atencao: Glue, Athena e S3 podem gerar custos.** Defina AWS Budgets antes de executar.

## Passo a passo (console + Mac)
1. Escolha uma regiao e crie bucket S3 unico `SEU_BUCKET_DADOS` (Block Public Access habilitado).
2. Crie prefixos `sources/`, `scripts/`, `bronze/`, `silver/`, `gold/`, `quarantine/`, `athena-results/`.
3. AWS CLI configurada com credenciais temporarias/perfil. Confira `aws sts get-caller-identity`.
4. Da raiz do projeto: `aws s3 sync sources/ s3://SEU_BUCKET_DADOS/sources/`.
5. Envie scripts: `aws s3 sync jobs/ s3://SEU_BUCKET_DADOS/scripts/`.
6. Configure IAM role do Glue com confianca em `glue.amazonaws.com`, permissao de leitura/escrita nos prefixos S3 necessarios e logs em CloudWatch; confirme `AWSGlueServiceRole` ou politica customizada minima. Nao utilize chaves hardcoded.
7. Glue Console > ETL jobs > Script editor (Spark / Python) e use `jobs/01_bronze_glue.py`. Argumentos: `--SOURCE_BUCKET=SEU_BUCKET_DADOS` e `--TARGET_BUCKET=SEU_BUCKET_DADOS`. Defina Glue version compativel, quantidade minima de workers e timeout curto. Execute e confira `bronze/locacoes/`.
8. Crie e execute `02_silver_glue.py` com `--TARGET_BUCKET=SEU_BUCKET_DADOS`, depois `03_gold_glue.py` com mesmo argumento.
9. Athena > Query editor: configure local de resultados `s3://SEU_BUCKET_DADOS/athena-results/`; abra `sql/athena.sql`, substitua `SEU_BUCKET_DADOS`, execute os `CREATE DATABASE`, a tabela e a consulta.
10. Confira contagens/qualidade em `tests/expected_counts.md` e depois pare os jobs e revise custos. Nao crie crawlers ou orquestracao ate validar o primeiro fluxo.

## Custos e seguranca
- Ative budget e alertas. Desligue jobs agendados ao terminar.
- Nao desative Block Public Access ou torne bucket publico.
- Nao grave credenciais no Git; use IAM/STS e principio do menor privilegio.
- `overwrite` foi usado apenas para o laboratorio; para producao, planeje execucao idempotente e publicacao atomica.
- Nao confunda sucesso tecnico do laboratorio com arquitetura pronta para producao.
