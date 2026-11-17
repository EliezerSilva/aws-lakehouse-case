"""Publica eventos bancarios sinteticos no Kafka com confirmacao de entrega.

Usa confluent-kafka/librdkafka para evitar problemas de conexao do cliente
kafka-python 2.x em alguns ambientes macOS. Nao faz acesso a nuvem.
"""
import argparse
import csv
import json
from pathlib import Path


DEFAULT_CSV = Path(__file__).resolve().parents[1] / "data" / "transactions.csv"


def publish_csv(csv_file, topic, producer):
    """Publica linhas CSV como JSON, validando o ACK de cada mensagem.

    Retorna a quantidade de mensagens confirmadas pelo broker. Producao
    idempotente cobre retries internos deste produtor, nao novos reenvios
    executados separadamente.
    """
    submitted = 0
    acknowledged = 0
    failures = []

    def on_delivery(error, message):
        nonlocal acknowledged
        if error is not None:
            failures.append(str(error))
        else:
            acknowledged += 1

    with open(csv_file, encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or "transaction_id" not in reader.fieldnames:
            raise ValueError("CSV precisa conter transaction_id")

        for row in reader:
            transaction_id = (row.get("transaction_id") or "").strip()
            if not transaction_id:
                raise ValueError("transaction_id ausente no CSV")

            producer.produce(
                topic,
                key=transaction_id.encode("utf-8"),
                value=json.dumps(row, ensure_ascii=False).encode("utf-8"),
                on_delivery=on_delivery,
            )
            submitted += 1
            producer.poll(0)

    pending = producer.flush(30)
    if pending:
        raise RuntimeError(f"{pending} mensagem(ns) nao confirmada(s) no timeout")
    if failures:
        raise RuntimeError("Falhas de entrega Kafka: " + "; ".join(failures[:3]))
    if acknowledged != submitted:
        raise RuntimeError(
            f"Confirmacoes divergentes: enviados={submitted}, confirmados={acknowledged}"
        )
    return acknowledged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="synthetic_transactions_raw")
    parser.add_argument("--csv", default=str(DEFAULT_CSV))
    args = parser.parse_args()

    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "broker.address.family": "v4",
        "client.id": "banking-transactions-synthetic-producer",
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 30000,
    })
    count = publish_csv(args.csv, args.topic, producer)
    print(f"PRODUCER PASS | Published={count} | Topic={args.topic}")


if __name__ == "__main__":
    main()
