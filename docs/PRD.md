# PRD: Dataflow Streaming Pipeline for Clickstream Aggregation

Version 1.0 · Teams T015, T115, T215 · Google Cloud Dataflow, Apache Beam, Pub/Sub, BigQuery

This repository implements the PRD. The requirement-to-code map is in the README.

## Summary of requirements

| ID | Requirement | Where |
|---|---|---|
| FR-1 | Ingest JSON events (8 required fields) from Pub/Sub | `clickstream/pipeline.py`, `producer/` |
| FR-2 | Validate; dead-letter bad events with raw payload + reason | `clickstream/parsing.py` |
| FR-3 | Event-time timestamps from `event_time` | `pipeline.py` (`AssignEventTime`, Pub/Sub `timestamp_attribute`) |
| FR-4 | 5-min fixed windows; page_views, clicks, active_users, sessions, purchases, conversion_rate, events_per_second | `clickstream/aggregation.py` |
| FR-5 | Early firing ~60s; final at watermark | `pipeline.py` triggers |
| FR-6 | 10 min allowed lateness, ACCUMULATING, too-late events to table | `pipeline.py`, `clickstream/late.py` |
| FR-7 | Append to BigQuery | `clickstream/schemas.py`, `infra/bigquery/` |
| FR-8 | In-flight updates, snapshot/drain fallback | `scripts/`, `docs/RUNBOOK.md` |
| FR-9 | Monitoring | `infra/monitoring/`, custom counters |

Non-goals, personas, NFR targets, success metrics and acceptance criteria are as in the original PRD.

## Team split

| Team | Scope | Primary code |
|---|---|---|
| T015 | Pub/Sub, schema, producer, validation, timestamps, dedup, dead letters | `producer/`, `parsing.py`, `scripts/setup_pubsub.sh` |
| T115 | Windowing, triggers, watermark, late policy, aggregation, unit tests | `aggregation.py`, `late.py`, `sketch.py`, `tests/` |
| T215 | Deployment, autoscaling, monitoring, BigQuery, dashboards, runbook | `scripts/`, `infra/`, `docs/RUNBOOK.md` |
