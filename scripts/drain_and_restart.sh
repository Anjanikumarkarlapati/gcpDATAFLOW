#!/usr/bin/env bash
# Incompatible-update path: snapshot -> drain -> relaunch. See docs/RUNBOOK.md.
set -euo pipefail
: "${PROJECT:?set PROJECT}"; : "${REGION:?set REGION}"
JOB_NAME="${JOB_NAME:-clickstream-aggregation}"
JOB_ID=$(gcloud dataflow jobs list --project="$PROJECT" --region="$REGION" \
  --status=active --filter="name=$JOB_NAME" --format='value(JOB_ID)' | head -1)
[ -n "$JOB_ID" ] || { echo "no active job named $JOB_NAME"; exit 1; }

# --snapshot-sources also snapshots the Pub/Sub subscription so unacked data can be replayed.
gcloud dataflow snapshots create --job-id="$JOB_ID" --region="$REGION" --project="$PROJECT" \
  --snapshot-sources=true
gcloud dataflow jobs drain "$JOB_ID" --region="$REGION" --project="$PROJECT"
echo "Draining $JOB_ID. Wait for state=Drained, then run scripts/run_dataflow.sh"
echo "(add: --create_from_snapshot=<SNAPSHOT_ID>) with a NEW job name suffix if needed."
