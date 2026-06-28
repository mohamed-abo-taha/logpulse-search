"""Tests for the event data-quality framework."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generator import SyntheticSource
from src.quality import AllowedValuesCheck, RangeCheck, UniqueCheck, default_event_suite
from src.transform import EventTransformer


def _clean_rows(n=100):
    raw = SyntheticSource(count=n, seed=3).fetch()
    return [e.to_document() for e in EventTransformer().transform(raw)]


def test_generated_events_pass_quality():
    report = default_event_suite().run(_clean_rows())
    assert report.passed, report.pretty()


def test_bad_level_flagged():
    rows = [{"event_id": "1", "timestamp": "t", "service": "a", "level": "NOPE",
             "status_code": 200, "latency_ms": 10}]
    assert AllowedValuesCheck("level", ["INFO", "ERROR"]).run(rows).failed_rows == 1


def test_status_code_range():
    rows = [{"status_code": 999}, {"status_code": 200}]
    assert RangeCheck("status_code", min_value=100, max_value=599).run(rows).failed_rows == 1


def test_duplicate_event_ids():
    rows = [{"event_id": "x"}, {"event_id": "x"}, {"event_id": "y"}]
    assert UniqueCheck("event_id").run(rows).failed_rows == 1


def test_suite_blocks_on_error():
    rows = [{"event_id": "", "timestamp": "", "service": "", "level": "BAD",
             "status_code": 700, "latency_ms": -1}]
    assert not default_event_suite().run(rows).passed
