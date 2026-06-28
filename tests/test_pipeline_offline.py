"""Offline tests — generation, model normalisation and transform.

These run with no MongoDB/Elasticsearch (CI-friendly). The datastore sinks are
exercised manually with `docker compose up` + `run_pipeline.py`.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generator import SyntheticSource
from src.models import Event
from src.transform import EventTransformer


def test_synthetic_source_is_deterministic():
    a = SyntheticSource(count=50, seed=7).fetch()
    b = SyntheticSource(count=50, seed=7).fetch()
    assert a == b                      # same seed -> same data
    assert len(a) == 50


def test_event_normalisation():
    e = Event.from_raw({
        "event_id": "evt-1", "timestamp": "2026-06-01T00:00:00+00:00",
        "service": "auth", "level": "error", "message": "boom",
        "status_code": "500", "latency_ms": "1234.5", "region": "eu",
        "user_id": "u1",
    })
    assert e.level == "ERROR"          # upper-cased
    assert e.status_code == 500        # coerced from str
    assert e.latency_ms == 1234.5
    assert e.is_valid()
    assert e.to_document()["_id"] == "evt-1"


def test_event_invalid_when_no_id():
    assert not Event.from_raw({"timestamp": "x"}).is_valid()
    assert not Event.from_raw({"event_id": "y"}).is_valid()   # no timestamp


def test_transform_dedupes_and_validates():
    raw = [
        {"event_id": "a", "timestamp": "t"},
        {"event_id": "a", "timestamp": "t"},     # dup
        {"event_id": "", "timestamp": "t"},       # invalid
        {"level": "INFO"},                         # invalid (no id/ts)
    ]
    out = EventTransformer().transform(raw)
    assert [e.event_id for e in out] == ["a"]


def test_unknown_level_defaults_to_info():
    assert Event.from_raw({"event_id": "1", "timestamp": "t", "level": "weird"}).level == "INFO"
