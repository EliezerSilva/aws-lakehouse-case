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

    docker compose -f interview_lab/docker-compose.kafka.yml up -d kafka-ui kafka-ui-proxy

A interface visual do Kafka (Provectus Kafka UI) fica em http://localhost:8089 e usa o cluster `banking-lab`. Na interface, abra Topics -> synthetic_transactions_raw -> Messages para mostrar os 10 eventos sinteticos e as 3 particoes. O acesso via navegador e restrito a 127.0.0.1, sem autenticacao (somente desenvolvimento). A UI compartilha a rede do broker e recebe acesso HTTP via proxy Nginx, evitando alterar/recriar o broker e perder as mensagens de teste existentes. Se esta for uma instalacao nova, inicie com `docker compose -f interview_lab/docker-compose.kafka.yml up -d`.

    pip install -r interview_lab/requirements-kafka.txt
    python interview_lab/kafka/producer.py

O produtor usa confluent-kafka/librdkafka e imprime `PRODUCER PASS | Published=10` somente apos receber as 10 confirmacoes de entrega do broker. Use localhost IPv4 (127.0.0.1), pois o broker Docker publica apenas nessa interface. O topico e criado previamente com 3 particoes e fator de replicacao 1. `enable.idempotence` nao impede novos eventos duplicados caso o script inteiro seja disparado novamente.

Para Spark 3.5.6 e Scala 2.12 (ajuste conforme instalacao):

    mkdir -p output/interview_kafka
    spark-submit --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 interview_lab/kafka/stream_to_bronze.py --bootstrap 127.0.0.1:9092 --output output/interview_kafka/bronze --checkpoint output/interview_kafka/checkpoints

Por padrao, a streaming query executa com `availableNow=True` e encerra depois de consumir os eventos disponiveis. Um checkpoint preserva o progresso; para uma demonstracao nova, use um par de caminhos de output/checkpoint novos. Nao reutilize caminhos de uma execucao com outro formato ou topico.

### Kafka Bronze -> qualidade, quarentena, Silver e Gold

O consumidor de Kafka grava **apenas a Bronze**; a curadoria e um **job batch PySpark separado** que le o Parquet ja persistido e reutiliza o contrato de validacao e a deduplicacao deterministica do laboratorio. Nao publica no Kafka nem altera a Bronze, o Airflow ou o notebook AWS.

Execute apos a validacao do streaming, **uma unica vez por diretorio de saida**:

    export SPARK_LOCAL_IP=127.0.0.1
    spark-submit --master 'local[2]' interview_lab/kafka/curate_bronze.py \
      --input output/interview_kafka/bronze \
      --output output/interview_kafka/curated-demo-01 \
      --expect-demo

Verifique os arquivos realmente gravados:

    python interview_lab/kafka/verify_curated.py --output output/interview_kafka/curated-demo-01

Esperado: \`KAFKA_CURATED_PASS | Raw=10 | Valid=7 | Quarantine=3 | Silver=5 | Replay=0\`, Gold A100=150.00 / A300=40.00 e \`KAFKA_CURATED_VERIFICATION_PASS\`. As camadas curadas incluem rastreabilidade por \`topic\`, \`partition\`, \`offset\` e \`raw_payload\`. O relatorio \`validation_report.json\` e publicado por ultimo, depois de verificar os arquivos Parquet.

O job trata **replay exato de event_id + payload igual**, mantendo um so evento para a regra de negocio; se o mesmo event_id tiver payload diferente, falha para revisao. Eventos sem ID ficam na quarentena. Os offsets sao usados para rastreabilidade e desempate do replay, nao como versao de negocio. A deduplicacao entre transacoes utiliza o horario de evento e desempates documentados. O sample obrigatorio de 10 eventos so e cobrado com \`--expect-demo\`.

Para nova demonstracao, escolha **outro diretorio de saida**, como \`curated-demo-02\`. Reutilizar \`curated-demo-01\` falha propositalmente em vez de sobrescrever resultados. O processo NAO e um pipeline unico de streaming ate Gold e nao garante exactly-once fim a fim. Para producao, seriam necessarios tabelas transacionais, politica de publicacao atomica, gestao de reprocesso e deduplicacao por contrato de origem. Chaves iguais tendem a mesma particao Kafka com particionamento estavel; a ordenacao do Kafka e garantida somente dentro de cada particao.

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

## AWS Glue Studio e Airflow gerenciado (opcional)

- Notebook Glue Studio, autoexplicativo: [notebooks/aws_glue_banking_demo.ipynb](notebooks/aws_glue_banking_demo.ipynb)
- DAG Airflow/MWAA -> Glue: [dags/mwaa_glue_banking_dag.py](dags/mwaa_glue_banking_dag.py)
- Guia de implantacao passo a passo, permissões, riscos e custos: [docs/AWS_DEMO.md](docs/AWS_DEMO.md)

`bash interview_lab/aws/upload_aws_demo.sh` opera em modo de conferencia. Para copiar os tres pequenos artefatos sinteticos ao S3, use `--apply` **somente apos confirmar conta, IAM, bucket e custos**. Nada inicia Glue ou MWAA automaticamente.

O notebook pode ser importado no AWS Glue Studio; uma execucao remota real requer que o usuario configure a role de sessao e execute as celulas na conta autorizada. A DAG MWAA precisa de ambiente existente e Glue job registrado, e nao foi implantada nem executada na AWS nesta entrega.
