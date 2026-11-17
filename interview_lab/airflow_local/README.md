# Apache Airflow local — Banking Transactions Data Pipeline

Demonstração técnica isolada com dados sintéticos. A instalação local **não acessa a AWS** nem exige credenciais em contêineres.

## Requisitos

- macOS com Docker Desktop e Docker Engine em execução.
- Docker Compose v2.
- Pelo menos 4 GB livres de RAM para o contêiner; esta configuração limita o uso a 4 GB.
- Acesso à internet somente para baixar e construir a imagem inicial.

O Apache Airflow 2.10.5 usa Python 3.11 e PySpark 3.5.6 com OpenJDK 17. O Compose usa um único contêiner em modo `standalone`, SQLite e SequentialExecutor: **apenas desenvolvimento e apresentação**, não produção.

## Preparar e executar

A partir da raiz do repositório, na branch `feature/banking-pyspark-airflow-kafka-lab`:

    git pull --ff-only
    mkdir -p output/airflow-local
    docker info --format '{{.ServerVersion}}'
    docker compose -f interview_lab/airflow_local/docker-compose.yml config -q
    docker compose -f interview_lab/airflow_local/docker-compose.yml up -d --build

O primeiro build baixa a imagem e instala Java + PySpark e pode consumir alguns GB de disco.

Para consultar a inicialização e credenciais do usuário admin:

    docker compose -f interview_lab/airflow_local/docker-compose.yml logs --tail=80 airflow
    docker compose -f interview_lab/airflow_local/docker-compose.yml exec airflow cat /opt/airflow/standalone_admin_password.txt

Acesso **somente no próprio Mac**: http://localhost:8088

Usuário padrão: `admin`. Senha: gerada pelo Airflow e disponível somente localmente no arquivo indicado. **Não publique a senha em GitHub, vídeos ou apresentações.**

## Executar a DAG

1. Na interface, procure `banking_transactions_pipeline`.
2. Se estiver pausada, habilite a DAG.
3. Use **Trigger DAG** para executar manualmente.
4. Verifique as tarefas em ordem:

       check_input -> process_pyspark -> verify_results

5. Confirme que as três ficaram verdes (success), abra os logs de `verify_results` e procure `VERIFICATION PASS`.
6. Os resultados da execução ficam em `output/airflow-local/<logical_timestamp>/`, com:

       bronze/
       silver/
       quarantine/
       gold/
       validation_report.json

O relatório contém contagens de entrada e Silver, quarentena e totais Gold. A execução materializa arquivos Parquet localmente; o pipeline AWS Glue já executado anteriormente é **um fluxo independente**.

## Diagnóstico

    docker compose -f interview_lab/airflow_local/docker-compose.yml ps
    docker compose -f interview_lab/airflow_local/docker-compose.yml logs --tail=100 airflow
    docker compose -f interview_lab/airflow_local/docker-compose.yml exec airflow airflow dags list
    docker compose -f interview_lab/airflow_local/docker-compose.yml exec airflow java -version
    docker compose -f interview_lab/airflow_local/docker-compose.yml exec airflow spark-submit --version

Se a porta 8088 estiver ocupada, altere **apenas** o lado esquerdo da publicação em `docker-compose.yml`, por exemplo para `127.0.0.1:8090:8080`, e use a nova URL.

## Encerrar sem apagar os resultados

    docker compose -f interview_lab/airflow_local/docker-compose.yml down

A configuração preserva o volume de metadados Airflow. **Não execute `down -v`** se desejar preservar histórico.

## Limitações

- O dataset e as regras financeiras são didáticos e não representam clientes reais.
- Single container, SQLite, SequentialExecutor: não atende exigências de alta disponibilidade ou produção.
- O processamento local não prova execução do Airflow em AWS MWAA.
- Repetir um mesmo DAG run sobrescreve apenas o prefixo daquela execução; não equivale a transação ACID.
- A chave `transaction_id`, a quarentena, o reprocessamento e a reconciliação são discutidos no guia `../docs/01_modelo_logico_e_preparacao.md`.
