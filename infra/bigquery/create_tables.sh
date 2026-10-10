#!/usr/bin/env bash
# Creates the dataset, the three tables and the "latest" view.
# Usage: PROJECT=my-proj [DATASET=clickstream] [LOCATION=US] infra/bigquery/create_tables.sh
set -euo pipefail
: "${PROJECT:?set PROJECT}"
DATASET="${DATASET:-clickstream}"
LOCATION="${LOCATION:-US}"
DIR="$(cd "$(dirname "$0")" && pwd)"

bq --project_id="$PROJECT" --location="$LOCATION" mk --dataset --force "$PROJECT:$DATASET" 2>/dev/null || true

# Metrics: partitioned by window_start day, clustered by page for dashboards.
bq --project_id="$PROJECT" mk --table \
  --time_partitioning_field=window_start --time_partitioning_type=DAY \
  --clustering_fields=page_url \
  "$PROJECT:$DATASET.page_metrics_5m" "$DIR/metrics_schema.json"

bq --project_id="$PROJECT" mk --table \
  --time_partitioning_field=processing_time --time_partitioning_type=DAY \
  "$PROJECT:$DATASET.dead_letters" "$DIR/dead_letter_schema.json"

bq --project_id="$PROJECT" mk --table \
  --time_partitioning_field=processing_time --time_partitioning_type=DAY \
  "$PROJECT:$DATASET.late_events" "$DIR/late_events_schema.json"

# Looker Studio source: one row per (window, page) = the most recent pane.
bq --project_id="$PROJECT" query --use_legacy_sql=false \
"CREATE OR REPLACE VIEW \`$PROJECT.$DATASET.page_metrics_5m_latest\` AS
 SELECT * EXCEPT (rn) FROM (
   SELECT *, ROW_NUMBER() OVER (
     PARTITION BY window_start, page_url ORDER BY processing_time DESC) AS rn
   FROM \`$PROJECT.$DATASET.page_metrics_5m\`)
 WHERE rn = 1"
