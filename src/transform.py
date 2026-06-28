"""Transform raw event dicts into validated ``Event`` objects."""

from __future__ import annotations

import logging

from .models import Event

logger = logging.getLogger("logpulse")


class EventTransformer:
    """Normalise, validate and deduplicate raw events."""

    def transform(self, raw_events: list[dict]) -> list[Event]:
        seen: set[str] = set()
        out: list[Event] = []
        dropped = 0
        for raw in raw_events:
            event = Event.from_raw(raw)
            if not event.is_valid():
                dropped += 1
                continue
            if event.event_id in seen:
                continue
            seen.add(event.event_id)
            out.append(event)
        logger.info(
            "Transformed %d raw -> %d valid events (%d dropped, %d dup)",
            len(raw_events), len(out), dropped,
            len(raw_events) - len(out) - dropped,
        )
        return out
