# Contrato de validacao - dataset sintetico

## Fontes
- Clientes: 50
- Lojas: 6
- Veiculos: 80
- Reservas: 201, incluindo 1 duplicata

## Locacoes
- Bronze: 202
- IDs distintos: 200
- Silver esperada: 199
- Quarentena esperada: 3
- Duplicatas exatas consolidadas: 0

## Rejeicoes esperadas
- LOC00005: VALOR_NEGATIVO (1 ocorrencia)
- LOC00009: DUPLICIDADE_CONFLITANTE (2 ocorrencias)

## Regras
- Valor negativo deve ser rejeitado.
- Registros com campos obrigatorios invalidos sao rejeitados.
- Versoes conflitantes do mesmo ID sao segregadas.
- Duplicatas identicas podem ser consolidadas.
- Metadados de ingestao nao definem conflito.
- A validacao referencial de clientes sera ampliada depois.

## Limites da versao
- Processamento batch com overwrite.
- Sem CDC, MERGE, Iceberg ou publicacao atomica.
- Contagens precisam ser verificadas na execucao AWS Glue.
