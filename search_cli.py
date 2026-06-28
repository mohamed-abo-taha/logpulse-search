"""Query the indexed events from the command line (Elasticsearch).

    python search_cli.py text "timeout"          # full-text search on messages
    python search_cli.py errors --service payments
    python search_cli.py report                   # error-rate + region analytics
"""

from __future__ import annotations

import argparse
import json

import config
from src.search import EventSearch


def main() -> None:
    parser = argparse.ArgumentParser(description="LogPulse search CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_text = sub.add_parser("text", help="full-text search on message")
    p_text.add_argument("query")
    p_text.add_argument("--size", type=int, default=10)

    p_err = sub.add_parser("errors", help="list ERROR events")
    p_err.add_argument("--service", default=None)
    p_err.add_argument("--size", type=int, default=10)

    sub.add_parser("report", help="aggregation analytics")

    args = parser.parse_args()
    search = EventSearch(config.ES_HOSTS, config.ES_INDEX)

    if args.cmd == "text":
        hits = search.full_text(args.query, size=args.size)
        for h in hits:
            print(f"  [{h['level']:<5}] {h['service']:<14} {h['message']}")
        print(f"\n{len(hits)} hit(s) for '{args.query}'")

    elif args.cmd == "errors":
        hits = search.filter_errors(service=args.service, size=args.size)
        for h in hits:
            print(f"  {h['timestamp']}  {h['service']:<14} {h['status_code']}  {h['message']}")
        print(f"\n{len(hits)} error(s)")

    elif args.cmd == "report":
        print("\n# Error rate & p95 latency by service")
        for r in search.error_rate_by_service():
            print(f"  {r['service']:<14} events={r['events']:<5} "
                  f"errors={r['errors']:<4} rate={r['error_rate_pct']:>5}%  "
                  f"p95={r['p95_latency_ms']}ms")
        print("\n# Events by region")
        for r in search.events_by_region():
            print(f"  {r['region']:<12} {r['events']}")


if __name__ == "__main__":
    main()
