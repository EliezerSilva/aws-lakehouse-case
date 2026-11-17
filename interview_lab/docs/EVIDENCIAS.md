# Evidências auditáveis — AWS / PySpark / Kafka / Airflow

> Dados exclusivamente sintéticos. Referência: 17/11/2026. Este arquivo distingue **resultado observado**, **reprodução por terceiros** e **captura de tela ainda não publicada**. Nenhum print foi fabricado.

## Matriz de evidências observadas

| Componente | Evidência observada no laboratório | Artefato / verificação | Situação |
|---|---|---|---|
| AWS Glue Studio, Spark 3.5.4-amzn-0 | Notebook executado: 10 Bronze, 7 válidos, 3 quarentena, 5 Silver | S3 `interview-lab/runs/notebook-20261117T171758580425Z/validation_report.json` | PASS informado e validado no terminal |
| Gold AWS Glue | A100=150.00; A300=40.00 | Mesmo relatório de execução S3 | PASS |
| Airflow local 2.10.5 | `check_input → process_pyspark → verify_results` | Log `VERIFICATION PASS | Bronze=10 | Silver=5 | Quarantine=3` | PASS |
| Kafka 3.9.0 | 10 mensagens no tópico, 3 partições | Kafka UI local e console consumer | PASS |
| Spark Streaming 3.5.6 | 10 eventos consumidos e Bronze Parquet | `output/interview_kafka/bronze`, checkpoint | PASS |
| Kafka Bronze → Quarentena/Silver/Gold | `KAFKA_CURATED_PASS | Raw=10 | Valid=7 | Quarantine=3 | Silver=5 | Replay=0` | `output/interview_kafka/curated-demo-01/validation_report.json` | PASS |
| Verificação independente de arquivos | `KAFKA_CURATED_VERIFICATION_PASS | Raw=10 | Quarantine=3 | Silver=5` | `kafka/verify_curated.py` | PASS |
| Testes CI | GitHub Actions em 17/11/2026 | [Run 44171328979](https://github.com/EliezerSilva/aws-lakehouse-case/actions/runs/44171328979) e [run 44171317787](https://github.com/EliezerSilva/aws-lakehouse-case/actions/runs/44171317787) | SUCCESS |
| MWAA → Glue | DAG e script preparados, sem execução remota | `dags/mwaa_glue_banking_dag.py` | NÃO EXECUTADO |
| Agentes IA / arena-skill | Discussão de arquitetura, sem integração neste repositório | Diagrama | PROPOSTA |

## Links públicos e privados

- [AWS Glue Studio — Ohio](https://console.aws.amazon.com/gluestudio/home?region=us-east-2) — requer login e permissões próprias.
- [AWS Glue Jobs — Ohio](https://console.aws.amazon.com/glue/home?region=us-east-2) — requer login.
- [Amazon S3 — Ohio](https://s3.console.aws.amazon.com/s3/home?region=us-east-2) — requer login.
- [CloudWatch — Ohio](https://console.aws.amazon.com/cloudwatch/home?region=us-east-2) — requer login.
- [Amazon MWAA — Ohio](https://console.aws.amazon.com/mwaa/home?region=us-east-2) — recurso não implantado.
- [Kafka UI local](http://localhost:8089) e [Airflow local](http://localhost:8088) — só funcionam na máquina que executa os contêineres; não são links públicos.
- S3 `s3://aws-lakehouse-case-lab-2026/interview-lab/runs/notebook-20261117T171758580425Z/` — caminho de evidência em conta privada, não download público.

## Capturas de tela — checklist para publicação

Os prints apresentados durante a preparação **não estão armazenados no Git**. Para não criar evidências falsas, capture novamente e revise antes de commitar:

1. `interview_lab/docs/screenshots/01-kafka-cluster.png`: cluster `banking-lab`, broker online.
2. `02-kafka-topic.png`: tópico `synthetic_transactions_raw`, 3 partições, 10 mensagens.
3. `03-kafka-messages.png`: mensagens JSON e offsets, sem tokens.
4. `04-airflow-grid.png`: três tarefas verdes da DAG `banking_transactions_pipeline`.
5. `05-airflow-verification.png`: log `VERIFICATION PASS`, ocultando credenciais.
6. `06-aws-glue-notebook.png`: execução real e contagens; ocultar ID de conta/ARNs quando apropriado.
7. `07-aws-s3-report.png`: JSON `status: PASS` e prefixo de execução, sem credenciais.
8. `08-local-curated-report.png`: relatório local `validation_report.json` com contagens e Gold.

**Nunca incluir** senhas do Airflow, tokens, cookies, chaves AWS, identificadores pessoais ou URLs de reunião. Revise todas as abas e barras de endereço antes da captura.

Se os prints forem incluídos no Git, substitua este checklist pelos links relativos correspondentes. Até lá, as evidências reproduzíveis são código, relatórios gerados e CI.

## Verificação reproduzível no Mac

```bash
python interview_lab/kafka/verify_curated.py --output output/interview_kafka/curated-demo-01
cat output/interview_kafka/curated-demo-01/validation_report.json
```

O arquivo de saída é gerado localmente e pode estar ignorado pelo `.gitignore`. Não versionar diretórios de dados Parquet ou checkpoints. Publique apenas capturas revisadas e relatórios sanitizados, se desejar.

## Limites

- O processamento Kafka → Bronze é streaming; Bronze → Quarentena/Silver/Gold é **batch**.
- Airflow local processa CSV independente; não chama Glue AWS.
- Glue Studio foi executado na AWS; MWAA e Glue job agendado não foram demonstrados.
- Parquet isolado não garante atomicidade multi-tabela nem exactly-once fim a fim.
