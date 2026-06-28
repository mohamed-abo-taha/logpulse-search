# Apache NiFi flow — LogPulse ingestion & orchestration

This folder documents the **Apache NiFi** flow that orchestrates the same
pipeline the Python code implements. NiFi is the "nice to have" in the job spec
(*pipeline building and orchestration*); this shows the flow visually/declaratively
as a complement to the code-first pipeline.

## The flow

```
 InvokeHTTP  ──►  SplitJson  ──►  ValidateRecord  ──►  RouteOnAttribute
 (poll REST       (1 FlowFile     (schema check,        (by level)
  every 30s)       per event)      dead-letter bad)        │   │
                                                     error  │   │ other
                                                            ▼   ▼
                                                        PutMongo  (upsert by event_id)
                                                            │ success
                                                            ▼
                                                   PutElasticsearchHttp (index)
```

| Processor              | Role in the pipeline                                       |
| ---------------------- | ---------------------------------------------------------- |
| `InvokeHTTP`           | **Ingest** — pull a JSON batch from a REST endpoint        |
| `SplitJson`            | Fan the batch array into one FlowFile per event            |
| `ValidateRecord`       | **Transform/validate** against the `event` Avro schema; bad records go to a dead-letter relationship |
| `RouteOnAttribute`     | Route by `level` so ERRORs can be alerted on separately    |
| `PutMongo`             | **Store** the raw document in MongoDB (upsert by `event_id`) |
| `PutElasticsearchHttp` | **Index** the event into Elasticsearch for search          |

`LogPulse_Flow.json` captures the processors, properties, connections and the
record schema in a readable form (a documentation/templating artifact — not a
binary NiFi template).

## How to run it

1. Start the stack (NiFi is included):

   ```bash
   docker compose up -d
   ```

2. Open the NiFi UI at **https://localhost:8443/nifi**
   (accept the self-signed cert; login `admin` / the password from
   `docker-compose.yml`).

3. Rebuild the flow above on the canvas using `LogPulse_Flow.json` as the spec,
   wire `logpulse-mongo` / `logpulse-es` as the destinations, and start the
   processor group.

> Either drive ingestion with **NiFi** (this flow) **or** with the Python
> pipeline (`run_pipeline.py`). Both write to the same MongoDB collection and
> Elasticsearch index, so the search CLI works regardless of which one you use.
