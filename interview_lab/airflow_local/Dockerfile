# Local-only engineering demonstration. No AWS keys, no remote services.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/airflow/local_state /opt/airflow/output \
    && chown -R airflow:root /opt/airflow/local_state /opt/airflow/output

USER airflow
RUN pip install --no-cache-dir "pyspark==3.5.6"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
