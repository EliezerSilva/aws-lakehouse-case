# Contrato de qualidade - NYC TLC Yellow Taxi

## Escopo

Fonte publica: NYC Taxi and Limousine Commission.
Dataset: Yellow Taxi Trip Records, janeiro de 2026.
Formato: Parquet.
Registros observados: 3.724.889.
Colunas: 20.

Este contrato se aplica exclusivamente ao estudo de
mobilidade urbana com dados publicos.

## Regras propostas

| Codigo | Condicao | Tratamento |
|---|---|---|
| Q001 | Pickup nulo | Quarentena |
| Q002 | Dropoff nulo | Quarentena |
| Q003 | Dropoff anterior ao pickup | Quarentena |
| A001 | Duracao zero com distancia positiva | Alerta |
| A002 | Duracao superior a 24 horas | Alerta |
| A003 | Total financeiro negativo | Classificacao |
| A004 | Data fora do mes nominal do arquivo | Alerta |

Registros com alertas permanecem na Silver.

## Evidencias do discovery

- Duracoes negativas: 1
- Duracoes zero: 45.069
- Duracoes zero com distancia positiva: 43.905
- Duracoes superiores a 24 horas: 35
- Totais financeiros negativos: 39.984
- Tarifas negativas: 39.463
- Embarques fora de janeiro: 7

## Cuidados

- Nao excluir valores financeiros negativos sem justificativa.
- Nao deduplicar apenas por data, valor e distancia.
- Preservar os dados originais na Bronze.
- Manter rastreabilidade dos motivos de rejeicao.
- Validar posteriormente os codigos de localizacao.
- Confirmar a semantica financeira no dicionario oficial.

## Criterios preliminares de aceite

Bronze esperada: 3.724.889 registros.

Silver prevista: 3.724.888 registros.
Quarentena prevista: 1 registro.

As contagens Silver e Quarentena foram confirmadas
na execucao run_20261021T133150801713Z.

A reconciliacao deve considerar:
Bronze = Silver + Quarentena.

## Estado

Discovery concluido.
Transformacao Silver implementada e testada.
Materializacao Bronze, Silver, Quarentena e Gold concluida.
Resultados publicados no Amazon S3.
AWS Glue e Athena ainda nao executados.
