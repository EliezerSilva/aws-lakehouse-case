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

## Execucao validada - Plano B

Em 21/10/2026, o pipeline foi executado localmente com
Python 3.11.15 e PySpark 3.5.6.

Os resultados foram materializados em arquivos Parquet
e publicados no Amazon S3, na regiao us-east-2.

Esta execucao nao utilizou AWS Glue nem Athena.

### Resultados

| Camada | Registros |
|---|---:|
| Bronze | 202 |
| Silver | 199 |
| Quarentena | 3 |
| Gold | 144 |

Indicadores Gold:

- 199 locacoes consolidadas
- 6 lojas
- Receita sintetica de R$ 167.767,51

Qualidade dos dados:

- 1 registro rejeitado por valor negativo
- 2 registros rejeitados por duplicidade conflitante

### Evidencias

Identificador da execucao:

`run_20261021T122829095198Z`

Bucket:

`aws-lakehouse-case-lab-2026`

Prefixo S3:

`runs/run_20261021T122829095198Z/`

Foram publicados cinco arquivos Parquet e um
`validation_report.json`.

O SHA-256 do relatorio local foi comparado com
o relatorio recuperado do S3, sem divergencias.

### Execucao local

Ativar o ambiente virtual e executar:

    source .venv/bin/activate
    export SPARK_LOCAL_IP=127.0.0.1
    python scripts/materialize_local.py

Cada execucao cria um diretorio proprio em `output/`.

### Testes automatizados

Comando:

    python -m pytest tests/test_silver.py -v

Resultado em 21/10/2026:

**3 testes aprovados em 23,72 segundos.**

Os testes cobrem regras da transformacao Silver.
Nao representam cobertura completa das demais camadas.

### Estado dos servicos AWS

- Amazon S3: publicacao confirmada.
- AWS Glue: scripts preparados; execucao pendente.
- Amazon Athena: consultas nao executadas.
- IAM: role Glue configurada, ainda sem teste de runtime.

As restricoes da conta AWS impedem atualmente
a execucao gerenciada pretendida.

### Limites desta demonstracao

O projeto utiliza dez fontes sinteticas, mas o
pipeline funcional processa somente o arquivo
`erp_locacoes_cabecalho_2026.csv`.

Nao existem dados ou integracoes reais da Localiza&Co.

A solucao ainda nao possui orquestracao em producao,
validacao referencial completa ou escrita transacional
atomica entre as camadas.

## Estudo com dados publicos - NYC TLC

O repositorio tambem inclui um estudo independente
de mobilidade urbana com dados publicos da NYC Taxi
and Limousine Commission.

Fonte: Yellow Taxi Trip Records, janeiro de 2026.

O processamento ocorreu localmente com PySpark 3.5.6.
Os resultados foram armazenados em Parquet e publicados
posteriormente no Amazon S3.

### Resultados validados

| Camada | Registros |
|---|---:|
| Bronze | 3.724.889 |
| Silver | 3.724.888 |
| Quarentena | 1 |
| Gold | 7.551 |

Foram aprovados dois testes especificos do NYC TLC,
alem dos tres testes do laboratorio sintetico.

### Documentacao

- `docs/nyc_tlc/quality_contract.md`
- `docs/nyc_tlc/execution_report.md`

### Codigo

- `jobs/nyc_tlc/transform_silver.py`
- `scripts/materialize_nyc_tlc.py`
- `tests/test_nyc_tlc_silver.py`

### Execucao local

    source .venv/bin/activate
    export SPARK_LOCAL_IP=127.0.0.1
    python scripts/materialize_nyc_tlc.py

O arquivo publico deve ser baixado previamente
para `data_public/nyc_tlc/raw/`.

### Limites

A execucao comprovada utilizou PySpark local.
O Amazon S3 foi usado como armazenamento.

AWS Glue, Athena e orquestracao gerenciada
permanecem como evolucoes previstas.

Este estudo nao possui vinculo com empresa cliente.
