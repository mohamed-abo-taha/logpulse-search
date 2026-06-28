"""LogPulse orchestrator: Source -> transform -> fan out to multiple Sinks.

The same validated ``Event`` batch is written to every configured sink (MongoDB
as the document store, Elasticsearch as the search index). Because both stores
implement the ``Sink`` interface, adding a third destination is a one-liner.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from .base import Sink, Source
from .transform import EventTransformer

logger = logging.getLogger("logpulse")


@dataclass
class PipelineResult:
    fetched: int
    transformed: int
    written: dict[str, int] = field(default_factory=dict)
    duration_s: float = 0.0


class Pipeline:
    def __init__(self, source: Source, sinks: list[Sink]) -> None:
        self.source = source
        self.sinks = sinks
        self.transformer = EventTransformer()

    def run(self) -> PipelineResult:
        start = time.perf_counter()
        logger.info("=== LogPulse run started ===")

        raw = self.source.fetch()
        events = self.transformer.transform(raw)

        written: dict[str, int] = {}
        for sink in self.sinks:
            written[sink.name] = sink.write(events)

        result = PipelineResult(
            fetched=len(raw),
            transformed=len(events),
            written=written,
            duration_s=round(time.perf_counter() - start, 3),
        )
        logger.info("=== LogPulse finished: %s ===", result)
        return result
