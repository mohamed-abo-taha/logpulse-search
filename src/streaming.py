"""Real-time streaming with Kafka.

This turns LogPulse from a *batch* pipeline into a *streaming* one. Events flow
through a Kafka topic and are processed the moment they arrive:

    EventProducer  ──►  Kafka topic  ──►  EventConsumer  ──►  MongoDB + Elasticsearch
    (emits events)     (logpulse.events)   (transform + fan-out to the same Sinks)

The consumer reuses the exact ``Sink`` classes the batch pipeline uses, so the
storage logic is written once and works for both batch and streaming.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Iterable

from .base import Sink
from .generator import SyntheticSource
from .transform import EventTransformer

logger = logging.getLogger("logpulse")


class EventProducer:
    """Publish events to a Kafka topic as a continuous stream."""

    def __init__(self, brokers: str, topic: str = "logpulse.events") -> None:
        from kafka import KafkaProducer

        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",                 # wait for the broker to confirm — no silent loss
            retries=3,
        )

    def emit(self, events: Iterable[dict]) -> int:
        n = 0
        for event in events:
            self.producer.send(self.topic, key=event.get("event_id"), value=event)
            n += 1
        self.producer.flush()
        logger.info("Produced %d events to topic %s", n, self.topic)
        return n

    def stream(self, batch_size: int = 25, interval_s: float = 1.0,
               rounds: int | None = None) -> None:
        """Emit a fresh batch every ``interval_s`` seconds (simulates live traffic)."""
        round_no = 0
        while rounds is None or round_no < rounds:
            seed = 1000 + round_no
            events = SyntheticSource(count=batch_size, seed=seed).fetch()
            # make event ids unique per round so each batch is genuinely new
            for e in events:
                e["event_id"] = f"{e['event_id']}-r{round_no}"
            self.emit(events)
            round_no += 1
            if rounds is None or round_no < rounds:
                time.sleep(interval_s)

    def close(self) -> None:
        self.producer.close()


class EventConsumer:
    """Consume the topic, transform each message, fan out to the sinks."""

    def __init__(self, brokers: str, sinks: list[Sink],
                 topic: str = "logpulse.events", group_id: str = "logpulse-indexer") -> None:
        from kafka import KafkaConsumer

        self.sinks = sinks
        self.transformer = EventTransformer()
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=10000,   # stop after 10s idle so the demo can exit
        )

    def run(self, max_messages: int | None = None) -> int:
        processed = 0
        for message in self.consumer:
            events = self.transformer.transform([message.value])
            for sink in self.sinks:
                sink.write(events)
            processed += 1
            if processed % 25 == 0:
                logger.info("Consumed %d messages", processed)
            if max_messages and processed >= max_messages:
                break
        logger.info("Consumer finished — %d messages processed", processed)
        return processed
