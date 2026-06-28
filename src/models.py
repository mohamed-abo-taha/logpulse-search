"""Domain model for an application log/telemetry event.

Same OOP philosophy as Project 1: the messy raw payload is normalised behind a
clean, validated object before it ever touches a datastore.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Any

VALID_LEVELS = {"DEBUG", "INFO", "WARN", "ERROR"}


@dataclass
class Event:
    """A single service event (one log line / telemetry record)."""

    event_id: str
    timestamp: str          # ISO-8601 UTC
    service: str
    level: str
    message: str
    status_code: int
    latency_ms: float
    region: str
    user_id: str
    ingested_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @classmethod
    def from_raw(cls, payload: dict[str, Any]) -> "Event":
        """Normalise a raw event dict (e.g. from a REST source or NiFi)."""
        level = str(payload.get("level", "INFO")).upper()
        if level not in VALID_LEVELS:
            level = "INFO"

        def num(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        return cls(
            event_id=str(payload.get("event_id", "")).strip(),
            timestamp=str(payload.get("timestamp", "")).strip(),
            service=str(payload.get("service", "unknown")).strip(),
            level=level,
            message=str(payload.get("message", "")).strip(),
            status_code=int(num(payload.get("status_code"), 0)),
            latency_ms=round(num(payload.get("latency_ms")), 2),
            region=str(payload.get("region", "unknown")).strip(),
            user_id=str(payload.get("user_id", "")).strip(),
        )

    def is_valid(self) -> bool:
        return bool(self.event_id) and bool(self.timestamp)

    def to_document(self) -> dict[str, Any]:
        """Mongo/Elasticsearch friendly dict. ``event_id`` doubles as the _id."""
        doc = asdict(self)
        doc["_id"] = self.event_id
        return doc
