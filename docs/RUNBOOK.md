# Runbook

Variables: `PROJECT`, `REGION`, `JOB_NAME` (default `clickstream-aggregation`).

## 1. First deployment
1. `scripts/setup_pubsub.sh`, `infra/bigquery/create_tables.sh`, create bucket `gs://$PROJECT-dataflow`.
2. `scripts/run_dataflow.sh`.
3. Start the producer and confirm rows in `clickstream.page_metrics_5m` within ~1–2 minutes.
4. `infra/monitoring/create_alerts.sh` (set `CHANNEL` to a notification channel).

## 2. Compatible update (in-flight, no downtime)
Use for non-stateful logic changes, new nullable output fields, monitoring changes.

1. Deploy to a staging project first and compare output.
2. Keep `JOB_NAME` identical and **do not rename transform labels**
   (`ParseValidate`, `Deduplicate`, `Window5Min`, `AggregateEngagement`, …). If you must,
   pass `--transform_name_mapping='{"old":"new"}'`.
3. `scripts/run_dataflow.sh --update`
4. Watch `data_watermark_age`, `system_lag` and BigQuery insert errors for 15 minutes.

Changing windowing, triggers, the aggregation key, or the accumulator/coder (`_Acc`) is **incompatible**.

## 3. Incompatible update
1. `scripts/drain_and_restart.sh` (snapshots job + subscription, then drains; drain closes open
   windows and fires remaining triggers).
2. When the job is `Drained`, launch the new version with `scripts/run_dataflow.sh`
   (optionally `--create_from_snapshot=<id>`; use a new job name if the old one is still shutting down).
3. Unacked messages stay in Pub/Sub (7-day retention). To replay already-acked data, seek the
   subscription to a timestamp or snapshot: `gcloud pubsub subscriptions seek clickstream-sub --time=<RFC3339>`.
   Replayed events are deduplicated only within the 1 h horizon, so seek back no further than that, or
   rebuild affected windows via batch.
4. Validate continuity: compare `page_metrics_5m_latest` before/after the cutover window.

## 4. Operations
| Symptom | Check |
|---|---|
| Backlog / oldest unacked age rising | Autoscaling ceiling (`MAX_WORKERS`), worker errors, stuck stage in Dataflow UI |
| Watermark age rising, no final rows | Stuck stage or a hot key; producer clocks far off; missing `event_time` attribute |
| Many dead letters | `SELECT failure_type, COUNT(*) FROM clickstream.dead_letters GROUP BY 1` — likely schema drift (`unsupported_schema`) |
| Many late events | `late_events.lateness_seconds`; consider raising `--allowed_lateness_seconds` (state grows) |
| Hot page overloading one worker | Move to a two-stage aggregation: salt `(page, shard)` with `DISCARDING` first stage, merge in a second stage |
| BigQuery insert errors | Schema mismatch with table; quota; table missing (`CREATE_NEVER`) |

## 5. Correcting from late events
`late_events` holds events beyond 10 minutes. A batch job can aggregate them per `(window, page)` and
merge into the metrics, or replay them to the topic with a longer-lateness job.
