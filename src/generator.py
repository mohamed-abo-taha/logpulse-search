"""Event sources.

* ``SyntheticSource`` — deterministically generates realistic service events so
  the pipeline can be demoed without any upstream system.
* ``FileSource``      — replays events from a JSON file (e.g. produced by NiFi).
* ``RestSource``      — pulls a batch of events from an HTTP/JSON endpoint,
  illustrating "data flows between systems via REST APIs".
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .base import Source

logger = logging.getLogger("logpulse")

SERVICES = ["auth", "payments", "search", "catalog", "checkout", "notifications"]
REGIONS = ["us-east", "us-west", "eu-central", "me-south", "ap-south"]
LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"]  # weighted
MESSAGES = {
    "DEBUG": ["cache hit", "cache miss", "config reloaded"],
    "INFO": ["request handled", "user logged in", "order placed", "item indexed"],
    "WARN": ["slow query detected", "retrying upstream call", "rate limit near"],
    "ERROR": ["upstream timeout", "db connection refused", "payment declined",
              "null pointer in handler"],
}


class SyntheticSource(Source):
    """Generate ``count`` plausible events. Seeded for reproducibility."""

    def __init__(self, count: int = 500, seed: int = 42) -> None:
        self.count = count
        self.rng = random.Random(seed)

    def fetch(self) -> list[dict]:
        base = datetime(2026, 6, 1, tzinfo=timezone.utc)
        events: list[dict] = []
        for i in range(self.count):
            level = self.rng.choice(LEVELS)
            service = self.rng.choice(SERVICES)
            ts = base + timedelta(seconds=self.rng.randint(0, 7 * 24 * 3600))
            # ERROR events get higher latency + 5xx, the rest are mostly fast/2xx.
            if level == "ERROR":
                status = self.rng.choice([500, 502, 503])
                latency = self.rng.uniform(800, 4000)
            elif level == "WARN":
                status = self.rng.choice([200, 200, 429])
                latency = self.rng.uniform(300, 1200)
            else:
                status = self.rng.choice([200, 200, 200, 201, 204])
                latency = self.rng.uniform(10, 400)
            events.append({
                "event_id": f"evt-{i:06d}",
                "timestamp": ts.isoformat(),
                "service": service,
                "level": level,
                "message": self.rng.choice(MESSAGES[level]),
                "status_code": status,
                "latency_ms": round(latency, 2),
                "region": self.rng.choice(REGIONS),
                "user_id": f"user-{self.rng.randint(1, 200):03d}",
            })
        logger.info("Generated %d synthetic events", len(events))
        return events


class FileSource(Source):
    """Replay events from a JSON array file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def fetch(self) -> list[dict]:
        with self.path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        logger.info("Loaded %d events from %s", len(data), self.path)
        return data


class RestSource(Source):
    """Fetch a batch of events from an HTTP/JSON endpoint."""

    def __init__(self, url: str, timeout: int = 20) -> None:
        self.url = url
        self.timeout = timeout

    def fetch(self) -> list[dict]:
        import requests

        resp = requests.get(self.url, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        # Accept either a bare array or {"events": [...]}.
        events = data["events"] if isinstance(data, dict) else data
        logger.info("Fetched %d events from %s", len(events), self.url)
        return events


def write_sample_file(path: str | Path, count: int = 200) -> int:
    """Helper used by tooling to materialise a sample JSON file."""
    events = SyntheticSource(count=count).fetch()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as fh:
        json.dump(events, fh, indent=2)
    return len(events)
