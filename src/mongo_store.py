"""MongoDB sink — stores raw events as documents (the system of record).

MongoDB is the landing zone: every event is persisted as a JSON document keyed
by ``event_id``. Bulk **upserts** make re-runs idempotent, and indexes keep the
common access patterns (by service, by level, by time) fast.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from .base import Sink
from .models import Event

logger = logging.getLogger("logpulse")


class MongoStore(Sink):
    name = "mongodb"

    def __init__(
        self,
        uri: str,
        db_name: str = "logpulse",
        collection: str = "events",
    ) -> None:
        from pymongo import MongoClient

        self.client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        self.db = self.client[db_name]
        self.collection = self.db[collection]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self.collection.create_index("service")
        self.collection.create_index("level")
        self.collection.create_index("timestamp")
        logger.info("MongoDB indexes ensured on %s", self.collection.name)

    def healthcheck(self) -> bool:
        try:
            self.client.admin.command("ping")
            return True
        except Exception as exc:  # pragma: no cover - depends on live server
            logger.error("MongoDB ping failed: %s", exc)
            return False

    def write(self, events: Iterable[Event]) -> int:
        from pymongo import ReplaceOne

        ops = [
            ReplaceOne({"_id": e.event_id}, e.to_document(), upsert=True)
            for e in events
        ]
        if not ops:
            return 0
        result = self.collection.bulk_write(ops, ordered=False)
        written = result.upserted_count + result.modified_count
        logger.info("MongoDB wrote %d documents", written)
        return written

    # --- read helpers used by the analytics / verification layer ---

    def count(self) -> int:
        return self.collection.count_documents({})

    def aggregate(self, pipeline: list[dict]) -> list[dict[str, Any]]:
        return list(self.collection.aggregate(pipeline))
