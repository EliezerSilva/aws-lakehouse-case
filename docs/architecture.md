# Arquitetura do AWS Lakehouse Case

Laboratorio de Engenharia de Dados com dados 100% sinteticos
de mobilidade e locacao de veiculos.

## Arquitetura implementada e validada

Fontes CSV locais
    |
    v
PySpark local
    |
    +-- Bronze: ingestao e metadados
    |
    +-- Silver: validacao e tratamento
    |       |
    |       +-- Quarentena: registros rejeitados
    |
    +-- Gold: receita diaria por loja
    |
    v
Arquivos Parquet
    |
    v
Amazon S3 / runs / identificador da execucao

Execucao comprovada: run_20261021T122829095198Z

Resultados:
- Bronze: 202 registros
- Silver: 199 registros
- Quarentena: 3 registros
- Gold: 144 agregacoes
- Receita sintetica: R$ 167.767,51

Os resultados foram validados localmente, publicados no S3
e acompanhados de um relatorio de execucao.

## Arquitetura AWS planejada

Fontes -> S3 -> Glue Bronze -> Glue Silver/Quarentena
-> Glue Gold -> Glue Data Catalog -> Athena

Os scripts Glue estao implementados, mas os jobs ainda
nao foram executados devido a restricoes da conta AWS.

Nao ha execucao comprovada do Glue Data Catalog ou Athena.

## Evolucoes previstas

- Validacao referencial entre entidades
- Schema explicito e controle de evolucao
- Logs de ingestao e auditoria
- Orquestracao com Step Functions
- Processamento incremental e CDC
- Apache Iceberg
- Governanca com Lake Formation
- CI/CD e monitoramento
- Revisao de permissoes IAM pelo menor privilegio

## Limites

Somente uma das dez fontes sinteticas integra a primeira
versao funcional do pipeline.

A escrita em Parquet nao implementa transacoes atomicas
entre as camadas.

Este laboratorio nao utiliza dados nem integracoes reais
da Localiza&Co.
