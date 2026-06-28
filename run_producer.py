"""Stream synthetic events into Kafka.

    python run_producer.py --rounds 10 --batch 25 --interval 1
    python run_producer.py            # stream forever (Ctrl-C to stop)

Requires Kafka running (see docker-compose.yml).
"""

from __future__ import annotations

import argparse
import logging
import os

from src.streaming import EventProducer

BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "logpulse.events")


def main() -> None:
    p = argparse.ArgumentParser(description="LogPulse Kafka producer")
    p.add_argument("--rounds", type=int, default=None, help="number of batches (default: forever)")
    p.add_argument("--batch", type=int, default=25)
    p.add_argument("--interval", type=float, default=1.0)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    producer = EventProducer(BROKERS, TOPIC)
    try:
        producer.stream(batch_size=args.batch, interval_s=args.interval, rounds=args.rounds)
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
