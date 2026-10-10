#!/usr/bin/env bash
# Launch or update the streaming job.
#   PROJECT=p REGION=us-central1 scripts/run_dataflow.sh            # new job
#   PROJECT=p REGION=us-central1 scripts/run_dataflow.sh --update   # compatible in-flight update
set -euo pipefail
: "${PROJECT:?set PROJECT}"; : "${REGION:?set REGION}"
DATASET="${DATASET:-clickstream}"
JOB_NAME="${JOB_NAME:-clickstream-aggregation}"   # keep stable: required for --update
BUCKET="${BUCKET:-$PROJECT-dataflow}"
cd "$(dirname "$0")/.."

python -m clickstream.pipeline \
  --runner=DataflowRunner \
  --project="$PROJECT" --region="$REGION" \
  --job_name="$JOB_NAME" \
  --temp_location="gs://$BUCKET/temp" --staging_location="gs://$BUCKET/staging" \
  --setup_file=./setup.py \
  --streaming \
  --enable_streaming_engine \
  --max_num_workers="${MAX_WORKERS:-10}" \
  --autoscaling_algorithm=THROUGHPUT_BASED \
  --subscription="projects/$PROJECT/subscriptions/clickstream-sub" \
  --metrics_table="$PROJECT:$DATASET.page_metrics_5m" \
  --dead_letter_table="$PROJECT:$DATASET.dead_letters" \
  --late_events_table="$PROJECT:$DATASET.late_events" \
  "$@"
