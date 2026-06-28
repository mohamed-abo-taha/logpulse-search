"""Search & analytics over the Elasticsearch index.

This is the "search + analytical thinking" payoff: full-text queries plus
aggregations that answer operational questions (which service is erroring, what
are the latency percentiles, how do events break down by region).
"""

from __future__ import annotations

from typing import Any


class EventSearch:
    def __init__(self, hosts: str | list[str], index: str = "logpulse-events") -> None:
        from elasticsearch import Elasticsearch

        self.client = Elasticsearch(hosts, request_timeout=10)
        self.index = index

    def full_text(self, text: str, size: int = 10) -> list[dict[str, Any]]:
        """Full-text search on the message field."""
        body = {
            "size": size,
            "query": {"match": {"message": text}},
            "sort": [{"timestamp": "desc"}],
        }
        resp = self.client.search(index=self.index, body=body)
        return [h["_source"] for h in resp["hits"]["hits"]]

    def filter_errors(self, service: str | None = None, size: int = 10) -> list[dict]:
        """Boolean filter query: ERROR events, optionally for one service."""
        must = [{"term": {"level": "ERROR"}}]
        if service:
            must.append({"term": {"service": service}})
        body = {"size": size, "query": {"bool": {"must": must}},
                "sort": [{"timestamp": "desc"}]}
        resp = self.client.search(index=self.index, body=body)
        return [h["_source"] for h in resp["hits"]["hits"]]

    def error_rate_by_service(self) -> list[dict[str, Any]]:
        """Aggregation: error ratio per service (analytics)."""
        body = {
            "size": 0,
            "aggs": {
                "by_service": {
                    "terms": {"field": "service", "size": 20},
                    "aggs": {
                        "errors": {"filter": {"term": {"level": "ERROR"}}},
                        "p95_latency": {"percentiles": {"field": "latency_ms", "percents": [95]}},
                    },
                }
            },
        }
        resp = self.client.search(index=self.index, body=body)
        rows = []
        for bucket in resp["aggregations"]["by_service"]["buckets"]:
            total = bucket["doc_count"]
            errors = bucket["errors"]["doc_count"]
            p95 = bucket["p95_latency"]["values"].get("95.0")
            rows.append({
                "service": bucket["key"],
                "events": total,
                "errors": errors,
                "error_rate_pct": round(100 * errors / total, 2) if total else 0.0,
                "p95_latency_ms": round(p95, 1) if p95 else None,
            })
        return sorted(rows, key=lambda r: r["error_rate_pct"], reverse=True)

    def events_by_region(self) -> list[dict[str, Any]]:
        body = {
            "size": 0,
            "aggs": {"by_region": {"terms": {"field": "region", "size": 20}}},
        }
        resp = self.client.search(index=self.index, body=body)
        return [
            {"region": b["key"], "events": b["doc_count"]}
            for b in resp["aggregations"]["by_region"]["buckets"]
        ]
