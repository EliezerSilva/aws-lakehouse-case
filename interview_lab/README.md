# Laboratorio independente: PySpark, Airflow e Kafka

Este modulo contem somente **transacoes bancarias sinteticas**, criadas como exercicio de logica, qualidade e engenharia de dados. **Nao representa nenhum banco, cliente ou projeto real.** Ele nao modifica o fluxo de mobilidade ja documentado na raiz.

## Prioridade: PySpark

Compatibilidade-alvo: Python 3.11, Spark 3.5.6 e Java 17. Depois de ativar um ambiente virtual:

    pip install -r interview_lab/requirements-core.txt
    python -m pytest -q interview_lab/tests
    spark-submit --master 'local[2]' interview_lab/jobs/run_pipeline.py --input interview_lab/data/transactions.csv --output output/interview_lab

O conjunto de exemplo possui 10 eventos: 7 eventos validos, 3 para quarentena e 5 transacoes finais distintas. Verifique as contagens em execucao real antes de reporta-las como evidencias.

Contrato didatico: chaves obrigatorias, valor monetario decimal(18,2) estritamente positivo, timestamp valido, status APPROVED/PENDING/CANCELLED, ultima versao por transaction_id com desempate total. As regras nao correspondem a politica real de nenhuma instituicao. Ingestao bronze preserva eventos; Silver escolhe o estado final; Gold soma apenas estados finais APPROVED. Quarentena preserva motivos. A funcao join_account_dimension impede duplicidade de chaves na dimensao.

## Airflow (exemplo, nao implantado)

A DAG em dags/banking_daily_dag.py usa Airflow 2.10.x, retries, dependencia entre tarefas e disparo manual. O worker precisa ter o codigo montado em /opt/airflow/lab e spark-submit disponivel. Nao foi implantada em MWAA nem em outro servico.

## Kafka (exemplo, nao implantado)

    docker compose -f interview_lab/docker-compose.kafka.yml up -d
    pip install -r interview_lab/requirements-kafka.txt
    python interview_lab/kafka/producer.py

Para Spark 3.5.6 e Scala 2.12 (ajuste conforme instalacao):

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 interview_lab/kafka/stream_to_bronze.py --output output/interview_kafka/bronze --checkpoint output/interview_kafka/checkpoints

O consumidor Kafka grava Bronze separadamente. **A ligacao Kafka Bronze -> Silver batch ainda nao foi feita.** Nao presume exactly-once de ponta a ponta. Chaves iguais tendem a mesma particao com particionamento estavel; so ha ordenacao dentro da particao, nao global.

## Limites e seguranca

- Parquet overwrite e somente didatico. Nao ha MERGE transacional, isolamento concorrente ou publicacao atomica entre Silver/Gold/Quarentena.
- O codigo e os testes foram preparados para estudo, sem alegacao de execucao comprovada nesta contribuicao. Confira CI e execute localmente.
- Nao contem credenciais nem dados pessoais de clientes.
- No streaming, controle de schema evolutivo, contratos de eventos e deduplicacao idempotente fim a fim sao evolucoes adicionais.
- Os exercicios sao independentes da pilha AWS especifica do restante do repositorio.

## Roteiro de avaliacao

1. Explique por que dropDuplicates(transaction_id) nao garante a versao mais recente.
2. Valide tipos, nulos, quantias invalidas e timestamps com quarentena e reconciliacao.
3. Adicione join com contas provando que nao multiplicou as transacoes.
4. Explique Window/row_number e desempate deterministico.
5. Justifique shuffle, skew, broadcast e inspecao de plano com explain.
6. Desenhe upsert incremental/replay; diferencie overwrite Parquet de MERGE.
7. Airflow: retries, dependencias, catchup e idempotencia.
8. Kafka: topicos, particoes, chave, offsets, consumer groups e replay.
