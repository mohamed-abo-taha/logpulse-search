"""Configuration for LogPulse (env-overridable)."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "logpulse")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "events")

ES_HOSTS = os.getenv("ES_HOSTS", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "logpulse-events")

SAMPLE_PATH = BASE_DIR / "sample_data" / "events_sample.json"
EVENT_COUNT = int(os.getenv("EVENT_COUNT", "500"))
