"""Run the LogPulse ingestion pipeline.

    python run_pipeline.py                      # synthetic -> Mongo + ES
    python run_pipeline.py --source file        # replay sample_data file
    python run_pipeline.py --sinks mongo        # only MongoDB
    python run_pipeline.py --dry-run            # no datastores: just generate+transform

Requires MongoDB + Elasticsearch running (see docker-compose.yml) unless --dry-run.
"""

from __future__ import annotations

import argparse
import logging
import sys

import config
from src.generator import FileSource, SyntheticSource
from src.pipeline import Pipeline
from src.transform import EventTransformer


def build_source(name: str):
    if name == "file":
        return FileSource(config.SAMPLE_PATH)
    return SyntheticSource(count=config.EVENT_COUNT)


def build_sinks(names: list[str]) -> list:
    sinks = []
    if "mongo" in names:
        from src.mongo_store import MongoStore
        sinks.append(MongoStore(config.MONGO_URI, config.MONGO_DB, config.MONGO_COLLECTION))
    if "es" in names:
        from src.es_indexer import ESIndexer
        sinks.append(ESIndexer(config.ES_HOSTS, config.ES_INDEX))
    return sinks


def main() -> None:
    parser = argparse.ArgumentParser(description="LogPulse ingestion pipeline")
    parser.add_argument("--source", choices=["synthetic", "file"], default="synthetic")
    parser.add_argument("--sinks", default="mongo,es",
                        help="comma list: mongo,es (default both)")
    parser.add_argument("--dry-run", action="store_true",
                        help="generate + transform only, write to no datastore")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )

    source = build_source(args.source)

    if args.dry_run:
        raw = source.fetch()
        events = EventTransformer().transform(raw)
        levels: dict[str, int] = {}
        for e in events:
            levels[e.level] = levels.get(e.level, 0) + 1
        print(f"[dry-run] fetched={len(raw)} transformed={len(events)} "
              f"by_level={levels}")
        return

    sink_names = [s.strip() for s in args.sinks.split(",") if s.strip()]
    try:
        sinks = build_sinks(sink_names)
    except Exception as exc:
        print(f"ERROR: could not connect to a datastore: {exc}", file=sys.stderr)
        print("Hint: start the stack with `docker compose up -d`, "
              "or use --dry-run.", file=sys.stderr)
        sys.exit(1)

    result = Pipeline(source, sinks).run()
    print(f"LogPulse OK: fetched={result.fetched} transformed={result.transformed} "
          f"written={result.written} in {result.duration_s}s")


if __name__ == "__main__":
    main()
