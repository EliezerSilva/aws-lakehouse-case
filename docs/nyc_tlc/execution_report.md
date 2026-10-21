# NYC TLC - Relatorio de execucao

## Fonte

NYC Taxi and Limousine Commission - Yellow Taxi.

Periodo nominal: janeiro de 2026.

Fonte oficial:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Dados publicos, sem vinculacao com empresa cliente.

## Ambiente

- Execucao: `run_20261021T133150801713Z`
- Processamento: PySpark local
- Spark: 3.5.6
- Formato de persistencia: Parquet
- Armazenamento publicado: Amazon S3, us-east-2
- AWS Glue executado: False

## Resultados

| Camada | Registros |
|---|---:|
| Bronze | 3,724,889 |
| Silver | 3,724,888 |
| Quarentena | 1 |
| Gold | 7,551 |

Total de registros representados na Gold:
3,724,888.

## Indicadores de qualidade na Silver

| Indicador | Ocorrencias |
|---|---:|
| Duracao zero com distancia positiva | 43,905 |
| Duracao acima de 24 horas | 35 |
| Total financeiro negativo | 39,984 |
| Fora do mes de referencia | 7 |

Os alertas nao representam necessariamente registros
invalidos e podem coexistir na mesma viagem.

## Reconciliacao

Bronze = Silver + Quarentena.

Total representado na Gold = quantidade Silver.

Ambas as conciliacoes foram aprovadas na execucao.

## Publicacao AWS

Bucket: `aws-lakehouse-case-lab-2026`

Prefixo:
`public-data/nyc-tlc/runs/run_20261021T133150801713Z/`

Publicados seis arquivos Parquet e um relatorio JSON.

A integridade do relatorio local e remoto foi
confirmada por SHA-256.

## Limitacoes

- O processamento ocorreu localmente.
- Amazon S3 foi usado como armazenamento.
- AWS Glue, Athena e orquestracao nao foram executados.
- Os dados nao possuem uma chave unica de viagem validada.
- Nenhuma deduplicacao automatica foi aplicada.
- Valores negativos foram preservados com sinalizacao.
- Nao foram medidas melhorias de desempenho ou economia.
- O estudo nao constitui ambiente de producao.
