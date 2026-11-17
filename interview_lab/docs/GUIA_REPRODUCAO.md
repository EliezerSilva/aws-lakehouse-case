# Guia de reprodução integral — laboratório bancário sintético

Este guia permite reproduzir **sem acesso à conta AWS do autor** o laboratório local, os testes, o Airflow, Kafka e a curadoria. O AWS Glue é **opcional** e exige conta própria autorizada. O laboratório é didático, não usa dados nem infraestrutura de Reply/Banco Inter.

## 0. Ferramentas e versões

- macOS/Linux, Git, Docker Desktop/Engine + Docker Compose v2; internet para baixar imagens.
- Python 3.11, Java 17, Apache Spark/PySpark 3.5.6; AWS CLI v2 **apenas para AWS**.
- Airflow 2.10.5 em Docker com PySpark 3.5.6, Kafka 3.9.0 em Docker.
- Mínimo prático: 4 GB livres para Airflow e recursos adicionais para Kafka/Spark.
- Portas locais: Airflow `127.0.0.1:8088`; Kafka UI `127.0.0.1:8089`; broker `127.0.0.1:9092`; Spark UI transitória `127.0.0.1:4040`.

## 1. Clonar e preparar o Mac

```bash
git clone https://github.com/EliezerSilva/aws-lakehouse-case.git
cd aws-lakehouse-case
git switch feature/banking-pyspark-airflow-kafka-lab
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r interview_lab/requirements-core.txt
python -m pip install -r interview_lab/requirements-kafka.txt
java -version
docker compose version
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=python
```

No macOS, instale Java 17 via distribuição confiável e configure `JAVA_HOME` para o JDK 17. Verifique `spark-submit --version` (deve usar Spark 3.5.6). Se `python3.11` não estiver no PATH, instale-o antes de prosseguir.

## 2. Testes e pipeline CSV local (sem AWS, Kafka ou Docker)

```bash
python -m pytest -q interview_lab/tests
spark-submit --master 'local[2]' interview_lab/jobs/run_pipeline.py \
  --input interview_lab/data/transactions.csv \
  --output output/interview_lab
```

Esperado: Bronze=10; válidos=7; quarentena=3; Silver=5; Gold A100=150.00 e A300=40.00. Atenção: o job CSV utiliza overwrite no diretório `output/interview_lab` (somente laboratório). **Não use diretórios com dados importantes.**

## 3. Kafka local: broker e painel

```bash
docker compose -f interview_lab/docker-compose.kafka.yml up -d
docker compose -f interview_lab/docker-compose.kafka.yml ps
docker compose -f interview_lab/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic synthetic_transactions_raw \
  --partitions 3 --replication-factor 1
python interview_lab/kafka/producer.py
```

Esperado do producer: `PRODUCER PASS | Published=10 | Topic=synthetic_transactions_raw`. **Publicar novamente gera mais mensagens**; o produtor idempotente não deduplica execuções inteiras. Faça a primeira publicação uma vez. Kafka UI: [http://localhost:8089](http://localhost:8089) → Clusters → banking-lab → Topics → synthetic_transactions_raw → Messages. O painel pode demorar dezenas de segundos a subir e responder 502 durante a inicialização.

Verificar o tópico sem modificar os dados:

```bash
docker compose -f interview_lab/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic synthetic_transactions_raw
```

## 4. Spark Structured Streaming → Bronze Parquet

```bash
mkdir -p output/interview_kafka
spark-submit --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  interview_lab/kafka/stream_to_bronze.py \
  --bootstrap 127.0.0.1:9092 \
  --topic synthetic_transactions_raw \
  --output output/interview_kafka/bronze \
  --checkpoint output/interview_kafka/checkpoints
```

O job usa `availableNow=True`, consome o que estiver disponível e termina. Esperado `KAFKA_BRONZE_STREAM: SUCCESS`. O pacote Spark Kafka é baixado do Maven na primeira execução. **Checkpoint + output formam um par inseparável**; não reutilize um checkpoint com outro destino.

## 5. Bronze → quarentena, Silver e Gold (batch)

```bash
spark-submit --master 'local[2]' interview_lab/kafka/curate_bronze.py \
  --input output/interview_kafka/bronze \
  --output output/interview_kafka/curated-demo-01 \
  --expect-demo

python interview_lab/kafka/verify_curated.py \
  --output output/interview_kafka/curated-demo-01
cat output/interview_kafka/curated-demo-01/validation_report.json
```

Saída comprovada em 17/11/2026:

```text
KAFKA_CURATED_PASS | Raw=10 | Valid=7 | Quarantine=3 | Silver=5 | Replay=0
GOLD_TOTALS {"A100": "150.00", "A300": "40.00"}
KAFKA_CURATED_VERIFICATION_PASS | Raw=10 | Quarantine=3 | Silver=5
```

`--expect-demo` exige os dados sintéticos exatamente esperados. Se você já publicou mais eventos, não force contagens: use `--output` novo e retire `--expect-demo` para explorar os resultados reais. Nunca remova ou sobrescreva a Bronze validada para "fazer passar". A curadoria recusa saída já existente.

## 6. Airflow local (fluxo CSV separado)

```bash
mkdir -p output/airflow-local
docker compose -f interview_lab/airflow_local/docker-compose.yml up -d --build
docker compose -f interview_lab/airflow_local/docker-compose.yml ps
```

Abra [http://localhost:8088](http://localhost:8088). O usuário é `admin`; obtenha a senha localmente (não publique):

```bash
docker compose -f interview_lab/airflow_local/docker-compose.yml exec airflow \
  cat /opt/airflow/standalone_admin_password.txt
```

No painel, dispare manualmente `banking_transactions_pipeline`. Confirme `check_input → process_pyspark → verify_results` e o log `VERIFICATION PASS`. O Airflow **não consome Kafka** e **não executa Glue** nesta demonstração. [Manual detalhado](../airflow_local/README.md).

## 7. AWS Glue Studio e Amazon S3 (opcional, conta própria)

Links oficiais de console, região **us-east-2 (Ohio)**:
- [AWS Glue Studio](https://console.aws.amazon.com/gluestudio/home?region=us-east-2)
- [AWS Glue](https://console.aws.amazon.com/glue/home?region=us-east-2)
- [Amazon S3](https://s3.console.aws.amazon.com/s3/home?region=us-east-2)
- [CloudWatch](https://console.aws.amazon.com/cloudwatch/home?region=us-east-2)
- [AWS MWAA](https://console.aws.amazon.com/mwaa/home?region=us-east-2)

**Custos e segurança:** use AWS Budgets, IAM de menor privilégio e bucket privado; sessões Glue são cobradas. Nunca publique credenciais. A conta AWS do autor não é compartilhada com leitores.

```bash
export AWS_REGION=us-east-2
export AWS_LAB_BUCKET=SEU_BUCKET_UNICO
aws sts get-caller-identity
aws s3api head-bucket --bucket "$AWS_LAB_BUCKET" --region "$AWS_REGION"
bash interview_lab/aws/upload_aws_demo.sh
bash interview_lab/aws/upload_aws_demo.sh --apply
```

O `--apply` faz upload de três artefatos sintéticos para o bucket configurado. Importe [notebook Glue](../notebooks/aws_glue_banking_demo.ipynb) em Glue Studio → Jobs → Jupyter Notebook, escolha role autorizada, Spark/Glue compatível e execute célula a célula. Confirme `validation_report.json` em `s3://SEU_BUCKET/interview-lab/runs/<RUN_ID>/`. **Pare a sessão após validar**.

Referência de execução histórica do autor (não pública):
`s3://aws-lakehouse-case-lab-2026/interview-lab/runs/notebook-20261117T171758580425Z/`.

Para IAM, registro de Glue Job, opções MWAA e troubleshooting: [AWS_DEMO.md](AWS_DEMO.md). A DAG MWAA é **exemplo não executado**; não crie MWAA apenas para testar este repositório.

## 8. Diagrama editável e evidências

- [Jornada AWS editável (.excalidraw)](architecture_aws_interview.excalidraw): abra em [excalidraw.com](https://excalidraw.com) → Open → selecione o arquivo.
- [Matriz de evidências, prints e limites](EVIDENCIAS.md).
- [Modelo lógico e decisões de PySpark](01_modelo_logico_e_preparacao.md).

## 9. Encerramento seguro

```bash
docker compose -f interview_lab/airflow_local/docker-compose.yml down
docker compose -f interview_lab/docker-compose.kafka.yml stop
```

Não execute `docker compose down -v` nem exclua `output/` ou checkpoints sem backup. **O Kafka deste Compose não possui volume persistente dedicado**: recriar o broker pode perder as mensagens. Se quiser preservar a demonstração, mantenha o contêiner atual e evite recriação.

## 10. Arquitetura: o que foi e não foi integrado

- Kafka → Bronze: streaming real validado.
- Bronze → Quarentena/Silver/Gold: job batch real validado e reconciliado.
- Airflow local → CSV → camadas: fluxo independente validado.
- AWS Glue Studio → S3: execução independente validada na AWS.
- MWAA → Glue Job: código de referência; sem execução remota comprovada.
- Agentes de IA / arena-skill: **governança conceitual**, não executada neste laboratório. A aprovação humana e gates estão desenhados como evolução, não implementação.

Este repositório não demonstra disponibilidade, segurança, observabilidade ou ACID de produção.
