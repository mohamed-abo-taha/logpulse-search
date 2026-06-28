"""Abstract contracts for the LogPulse pipeline (OOP backbone).

A ``Source`` produces raw events; a ``Sink`` consumes ``Event`` objects. MongoDB
and Elasticsearch are both sinks, so the pipeline can fan out to both behind the
same interface.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Iterable

from .models import Event

logger = logging.getLogger("logpulse")


class Source(ABC):
    """Produces raw event dictionaries from some upstream system."""

    @abstractmethod
    def fetch(self) -> list[dict]:
        raise NotImplementedError


class Sink(ABC):
    """Persists ``Event`` objects into a destination datastore."""

    name: str = "sink"

    @abstractmethod
    def write(self, events: Iterable[Event]) -> int:
        """Persist events, return the number written."""
        raise NotImplementedError

    def healthcheck(self) -> bool:  # optional override
        return True
