"""Elasticsearch sink — indexes events for full-text search and aggregations.

Where MongoDB is the document system-of-record, Elasticsearch is the *query*
layer: it gives us fast full-text search over the ``message`` field and cheap
aggregations (error rate per service, latency percentiles, etc.).

An explicit index mapping is created so fields get the right types — ``message``
is analysed text (searchable), while ``service``/``level``/``region`` are
keywords (aggregatable/filterable).
"""

from __future__ import annotations

import logging
from typing import Iterable

from .base import Sink
from .models import Event

logger = logging.getLogger("logpulse")

INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "event_id":    {"type": "keyword"},
            "timestamp":   {"type": "date"},
            "service":     {"type": "keyword"},
            "level":       {"type": "keyword"},
            "message":     {"type": "text"},
            "status_code": {"type": "integer"},
            "latency_ms":  {"type": "float"},
            "region":      {"type": "keyword"},
            "user_id":     {"type": "keyword"},
            "ingested_at": {"type": "date"},
        }
    }
}


class ESIndexer(Sink):
    name = "elasticsearch"

    def __init__(self, hosts: str | list[str], index: str = "logpulse-events") -> None:
        from elasticsearch import Elasticsearch

        self.client = Elasticsearch(hosts, request_timeout=10)
        self.index = index
        self._ensure_index()

    def _ensure_index(self) -> None:
        if not self.client.indices.exists(index=self.index):
            self.client.indices.create(index=self.index, body=INDEX_MAPPING)
            logger.info("Created ES index %s with explicit mapping", self.index)

    def healthcheck(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception as exc:  # pragma: no cover - depends on live server
            logger.error("Elasticsearch ping failed: %s", exc)
            return False

    def write(self, events: Iterable[Event]) -> int:
        from elasticsearch.helpers import bulk

        actions = []
        for e in events:
            doc = e.to_document()
            doc.pop("_id", None)
            actions.append({
                "_index": self.index,
                "_id": e.event_id,
                "_source": doc,
            })
        if not actions:
            return 0
        success, _ = bulk(self.client, actions, refresh="wait_for")
        logger.info("Elasticsearch indexed %d documents", success)
        return success
