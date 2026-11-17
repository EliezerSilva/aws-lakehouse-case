"""Publicacao de eventos sinteticos. Requer kafka-python e broker local."""
import argparse
import csv
import json
from pathlib import Path
from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="synthetic_transactions_raw")
    parser.add_argument("--csv", default=str(Path(__file__).resolve().parents[1] / "data/transactions.csv"))
    args = parser.parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap, acks="all", retries=3,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda row: json.dumps(row).encode("utf-8"),
    )
    try:
        with open(args.csv, encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                producer.send(args.topic, key=row["transaction_id"], value=row).get(timeout=20)
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
