"""Consume events from Kafka and index them into MongoDB + Elasticsearch.

    python run_consumer.py                 # both sinks
    python run_consumer.py --sinks mongo   # only MongoDB

Requires Kafka + MongoDB + Elasticsearch running (see docker-compose.yml).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import config
from src.streaming import EventConsumer

BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "logpulse.events")


def build_sinks(names: list[str]) -> list:
    sinks = []
    if "mongo" in names:
        from src.mongo_store import MongoStore
        sinks.append(MongoStore(config.MONGO_URI, config.MONGO_DB, config.MONGO_COLLECTION))
    if "es" in names:
        from src.es_indexer import ESIndexer
        sinks.append(ESIndexer(config.ES_HOSTS, config.ES_INDEX))
    return sinks


def main() -> None:
    p = argparse.ArgumentParser(description="LogPulse Kafka consumer")
    p.add_argument("--sinks", default="mongo,es")
    p.add_argument("--max", type=int, default=None, help="stop after N messages")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    try:
        sinks = build_sinks([s.strip() for s in args.sinks.split(",") if s.strip()])
    except Exception as exc:
        print(f"ERROR: could not connect to a datastore: {exc}", file=sys.stderr)
        sys.exit(1)

    consumer = EventConsumer(BROKERS, sinks, TOPIC)
    n = consumer.run(max_messages=args.max)
    print(f"Consumer done: {n} messages -> {[s.name for s in sinks]}")


if __name__ == "__main__":
    main()
