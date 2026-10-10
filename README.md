# Clickstream Aggregation on Dataflow

Streaming Apache Beam pipeline: Pub/Sub clickstream events → 5-minute engagement metrics in BigQuery,
with late-data handling, deduplication, dead-lettering and safe updates. Spec: [docs/PRD.md](docs/PRD.md).

```
Pub/Sub clickstream-sub
  → ParseValidate ──(invalid)──────────────→ BigQuery dead_letters
  → AssignEventTime → Deduplicate(event_id)
  → RouteTooLate ──(>10 min late)──────────→ BigQuery late_events
  → FixedWindows(5m) + triggers + CombinePerKey(page_url)
  → BigQuery page_metrics_5m → view page_metrics_5m_latest → Looker Studio
```

## Layout

| Path | Purpose |
|---|---|
| `clickstream/` | Pipeline package (parsing, sketch, aggregation, late routing, pipeline entry point) |
| `frontend/` | React 19 + Vite dashboard (Overview, Data quality, How it works) using bundled sample data |
| `scripts/generate_sample_data.py` | Runs the real pipeline transforms over a simulated hour of traffic and writes `frontend/public/sample-data.json` and `sample_data/events.jsonl` |
| `producer/publisher.py` | Simulated traffic incl. late, duplicate and malformed events |
| `tests/` | Unit tests + `TestStream` tests for early/on-time/late panes, dedup, too-late and dead letters |
| `infra/bigquery/` | Table schemas and `create_tables.sh` (also creates the `_latest` view) |
| `infra/monitoring/` | Alert policies (backlog, lag, watermark, dead letters, errors) |
| `scripts/` | Pub/Sub setup, job launch/update, snapshot + drain |
| `docs/RUNBOOK.md` | Deploy, update, replay, troubleshooting |

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                   # no GCP needed

export PROJECT=my-project REGION=us-central1
scripts/setup_pubsub.sh
infra/bigquery/create_tables.sh
gsutil mb -l $REGION gs://$PROJECT-dataflow
scripts/run_dataflow.sh                  # start streaming job
python -m producer.publisher --project $PROJECT --rate 20 --duration 600 \
  --late-fraction 0.05 --duplicate-fraction 0.02 --malformed-fraction 0.01
infra/monitoring/create_alerts.sh
```

## Dashboard (sample data)

```bash
python -m scripts.generate_sample_data   # optional: regenerate the sample data
cd frontend && npm install && npm run dev
```

Plain CSS on design tokens, "Cotton candy skies" palette (light/dark via `data-theme`), status always shown as colour + icon + word,
44px touch targets, reduced-motion respected. It reads `public/sample-data.json`; for live data, serve
`page_metrics_5m_latest` through a small read-only API and point `src/data.js` at it. Looker Studio
remains the PRD's official dashboard.

## Design decisions

- **Event time.** The producer sets an `event_time` message attribute and the source uses it as
  `timestamp_attribute`, so the *watermark* follows event time. The pipeline also re-stamps each
  element from the JSON body, which is authoritative.
- **Panes.** Early every 60 s of processing time, on-time at the watermark, then one pane per late
  element (≤10 min). `ACCUMULATING` means each pane holds the full-to-date totals, so the table is
  append-only with several rows per `(window, page)`. `is_final=false` marks early panes only; a late
  pane is a correction and has `is_final=true`. Dashboards must read **`page_metrics_5m_latest`**,
  which keeps the newest row per `(window_start, page_url)`.
- **Distinct counts.** `active_users`/`sessions` use a mergeable KMV sketch (k=1024, ~3% error, exact
  below 1024 distinct values) with bounded state per key.
- **Deduplication.** `DeduplicatePerKey` on `event_id` with a 1 h processing-time horizon, ahead of
  windowing. Duplicates arriving after the horizon are not caught.
- **Too-late events.** Beam drops data past allowed lateness silently, so `RouteTooLate` diverts it first.
  A stateless DoFn cannot see the watermark, so it compares wall-clock time to
  `window_end + allowed_lateness`. This is an approximation: an event can pass this check yet be
  dropped by Beam if the watermark is ahead of wall-clock, or if the watermark lags it could be kept
  longer. Dropped-late counts appear in Dataflow's job counters.
- **Hot keys.** Not manually salted. Salting a key *and* using `ACCUMULATING` early panes double counts
  unless the first stage is `DISCARDING`; Dataflow's automatic combiner lifting already pre-aggregates
  per worker. If one page dominates, see the runbook for the two-stage approach.
- **Sink.** Streaming inserts for lowest latency (tables must pre-exist: `CREATE_NEVER`). Switch to the
  Storage Write API for higher volume.

## Status and caveats

Unit and `TestStream` tests pass locally on the DirectRunner. **Not yet run against real GCP** (no
credentials here): the Dataflow launch, BigQuery writes, alert policies and the custom-counter metric
name used by the dead-letter alert should be verified in a staging project. The Looker Studio dashboard
is a manual step: connect it to `page_metrics_5m_latest`.
