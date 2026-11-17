# Preparacao para entrevista tecnica: engenharia de dados e logica PySpark

Data de referencia: 17/11/2026. Cenário ficticio, sem qualquer relacao com a arquitetura ou dados do Banco Inter.

## 1. Problema e contrato de entrada

Recebemos eventos de transacoes. Uma transacao pode chegar repetida, ser atualizada ou cancelada. Precisamos gerar o estado mais recente por transacao, detectar dados invalidos, agregar somente transacoes aprovadas e reconciliar a origem com os destinos.

Campos reais do CSV sintetico em data/transactions.csv:

- event_id: identificador do evento
- transaction_id: chave de negocio da transacao
- account_id: conta
- amount_str: valor textual (conversao explicita para decimal(18,2))
- event_ts_str: horario do evento
- ingested_at_str: horario de ingestao
- status: APPROVED, PENDING ou CANCELLED

Regras **didaticas**: ids nao vazios; valor > 0; timestamp parseavel; status na lista permitida. Valores negativos e status fora da lista ficam em quarentena. Valores negativos podem ser perfeitamente legitimos em operacoes reais; nunca transportar essa regra para producao sem validacao de dominio.

## 2. Modelo logico

    Eventos CSV (atual) ---                           +--> Bronze: eventos completos
    Kafka (demonstracao) --/           |
                                       v
                             Validate/Normalize PySpark
                               /                   \
                       Quarentena                Validos
                       (motivo)                     |
                                                   v
                                Silver: 1 estado final por transaction_id
                                                   |
                                                   v
                                Gold: totais por account_id/data
                                                   |
                                                   v
                                 Auditoria e reconciliacao

Airflow (demonstracao) define dependencias check_source -> transform -> verify_output.
Kafka e CSV sao alternativas de origem. O consumidor Kafka escreve Bronze Parquet, mas ainda NAO esta ligado ao job batch Silver/Gold. A DAG atual usa CSV local e execucao manual. Nao foi implantado um orquestrador gerenciado.

## 3. Implementacao: passos e justificativas

### Passo A: schema explicito

Arquivo: src/banking_lab/pipeline.py, constante RAW_SCHEMA e funcao read_raw_csv.

Por que? Evitar inferencia que muda quando chegam novos arquivos; preservar campos textuais para controlar a conversao; capturar registros corrompidos; permitir governanca e evolucao de contrato.

### Passo B: conversao e validacao

Funcao: normalize_and_validate(raw).
- trim nos ids; upper no status.
- decimal(18,2) por try_cast, em vez de float para dinheiro.
- timestamps convertidos com formato explicito.
- lista de motivos por registro, sem apagar a evidencia.
- split_valid_invalid separa linhas aprovadas e quarentena.

Discussao tecnica: conversoes devem evitar arredondamento silencioso. O regex antes do cast bloqueia tres casas decimais. O pipeline usa UTC no spark-submit local.

### Passo C: deduplicacao deterministica

Funcao: latest_by_transaction(valid).
- Window.partitionBy(transaction_id) cria grupos da chave de negocio.
- orderBy(event_ts desc, ingested_at desc, event_id desc, hash desc) explicita prioridade.
- row_number=1 seleciona uma linha por chave.

Codigo essencial:

    w = Window.partitionBy("transaction_id").orderBy(
        F.col("event_ts").desc(),
        F.col("ingested_at").desc(),
        F.col("event_id").desc(),
    )
    latest = (
        valid.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

No codigo real ha mais um desempate pelo hash de payload. Esse hash e desempate tecnico, NAO prova precedencia de negocio quando timestamp e identificadores sao inconsistentes. Em producao, preferir versao de evento fornecida pela origem.

Nao usar apenas dropDuplicates(["transaction_id"]): preserva uma linha arbitraria e nao garante a mais recente.

### Passo D: evitar multiplicacao em joins

Funcao: join_account_dimension(latest, accounts).
- Verifica se account_id e unico na dimensao.
- Rejeita dimensao duplicada.
- Usa left join para preservar as transacoes.
- Confere que a quantidade apos join e igual a quantidade antes.

Discussao: quando a dimensao e SCD2, a chave simples deixa de ser unica; precisamos de join temporal pelos periodos de validade, sem sobreposicao.

### Passo E: Gold de negocio

Funcao: account_daily_totals(latest).
- Considera somente o ultimo status APPROVED.
- Deriva business_date de event_ts.
- Agrupa por account_id e business_date.
- Conta transacoes e soma valor decimal.

Limite intencional: business_date e um campo derivado; processamento financeiro real costuma exigir data efetiva, calendario e fuso definidos no contrato.

### Passo F: reconciliacao

Funcao: validate_reconciliation(valid, silver).

Invariantes:
    bronze_count = valid_event_count + quarantine_count
    valid_event_count = silver_count + deduplicated_events
    silver_count = count_distinct(transaction_id dos validos)

Se qualquer invariante falhar, a execucao deve gerar erro e nao declarar sucesso.

## 4. Resultado esperado para o CSV do laboratorio

Estes sao os valores esperados calculados com base nas 10 linhas sinteticas; sao verificados pelo teste test_end_to_end_sample.py, sujeito a confirmacao do workflow.

    Bronze: 10 eventos
    Validos: 7 eventos
    Quarentena: 3 eventos (E004, E005, E006)
    Silver: 5 transacoes finais
    Eventos deduplicados na Silver: 2
    Gold: A100 = 150.00; A300 = 40.00

T001 passa de 100.00 para 120.00 (ultima versao). T006 termina CANCELLED e nao entra no Gold. T007 permanece PENDING e nao entra no Gold. Valores monetarios sao apenas didaticos.

## 5. Performance e escolhas tecnicas

- Window/row_number provoca distribuicao por transaction_id e ordenacao por chave: pode causar shuffle.
- Skew: poucas chaves quentes concentram muitos eventos; analisar Spark UI e plano com df.explain("formatted").
- Broadcast join: candidato quando a dimensao e suficientemente pequena, depois de validar cardinalidade; nunca usar broadcast sem evidencias.
- Cache: apenas se o mesmo dataframe caro for reutilizado em varias actions, medindo beneficio e memoria.
- Evitar collect para dados volumosos. Os testes usam collect sobre amostras pequenas.
- Escrita Parquet particionada por data pode reduzir leituras, mas excesso de particoes gera muitos arquivos pequenos.
- O codigo atual executa counts repetidos: desempenho deve ser medido e otimizado no contexto real, nao presumido.

## 6. Reprocessamento e idempotencia

A funcao de deduplicacao produz o mesmo estado final quando a entrada recebe o mesmo evento duas vezes, conforme teste. Isso nao significa que o pipeline completo e idempotente em producao.

- mode("overwrite") em Parquet demonstrativo NAO e MERGE transacional.
- Para reprocessamento robusto, definir chave de negocio, versao, checkpoint, replay e estrategia de publicacao atomica.
- Um pipeline em producao deve proteger Gold contra dupla contabilizacao e preservar auditoria entre retries.

## 7. Airflow (conceito e codigo atual)

DAG dags/banking_daily_dag.py:
    check_source >> transform >> verify_output

O Airflow orquestra e tenta novamente em caso de falha. Nao transforma dados sozinho. retries nao devem duplicar resultado: um job precisa ser seguro para reexecucao. schedule=None representa disparo manual, e catchup=False desabilita criacao automatica de execucoes antigas. Uma DAG futura pode chamar spark-submit remoto ou operadores de servicos AWS, mas nao presumimos qual servico e usado pelo cliente.

## 8. Kafka (conceito e codigo atual)

Kafka e barramento de eventos; producer publica JSON com chave transaction_id no topico synthetic_transactions_raw. Uma chave estavel normalmente mantem eventos de mesma transacao em uma particao enquanto a estrategia de particionamento e o numero de particoes nao mudarem. A ordenacao e por particao, nunca global.

O Spark Structured Streaming le topic, partition, offset e payload, escreve Parquet Bronze e mantem checkpoint. Isso nao equivale a exactly-once de negocio. Duplicidade, replay e reconciliacao precisam ser enderecados no consumidor e na camada Silver.

## 9. Discurso tecnico de ate 90 segundos

"Primeiro defino chave de negocio e contrato de qualidade. Leio o dado com schema explicito e converto valores monetarios para decimal. Separo registros invalidos em quarentena com o motivo da rejeicao, sem perde-los. Na Silver ordeno as versoes de cada transacao por data de evento e desempates, usando Window e row_number para escolher o estado mais recente. Nao uso dropDuplicates isolado porque ele nao garante precedencia. Na Gold somo somente estados finais aprovados e valido a reconciliacao das contagens. Antes de defender performance, consulto explain e Spark UI para identificar shuffles e skew. Para reprocessamento, diferencio deduplicar eventos de garantir escrita idempotente e transacional."

## 10. Agenda de preparacao

- 17/11: desenhar fluxo, explicar todas as funcoes, rodar testes e justificar regras.
- 18/11: resolver codigo sem consulta; exercicios com empates, joins multiplicadores, agregacoes e reprocessamento; fundamentos Airflow/Kafka.
- 19/11: simular entrevista com tempo limitado e respostas objetivas, respeitando regras do avaliador.

## 11. Perguntas que podem aparecer

1. Por que Window + row_number e nao dropDuplicates?
2. Como resolver empates em timestamps? E se a fonte mandar event_version?
3. Como impedir que join com dimensao duplique registros?
4. Por que decimal em vez de double para dinheiro?
5. Como diferenciar dado invalido de um evento financeiro negativo legitimo?
6. Como explicar shuffle e skew? Quando broadcast e adequado?
7. Como lidar com um job que falha depois de gravar Silver e antes de gravar Gold?
8. Como garantir repetibilidade apos retries em Airflow?
9. Se o Kafka reenviar um evento, o que garante que ele nao seja contabilizado duas vezes?
10. O que voce avaliaria antes de otimizar performance de um processamento de 40 para 2 horas?

Referencias no codigo:
- src/banking_lab/pipeline.py
- jobs/run_pipeline.py
- tests/test_pipeline.py
- tests/test_end_to_end_sample.py
- dags/banking_daily_dag.py
- kafka/producer.py
- kafka/stream_to_bronze.py
