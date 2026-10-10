#!/usr/bin/env bash
# Usage: PROJECT=p [CHANNEL=projects/p/notificationChannels/123] infra/monitoring/create_alerts.sh
set -euo pipefail
: "${PROJECT:?set PROJECT}"
DIR="$(cd "$(dirname "$0")" && pwd)"
for f in "$DIR"/alert_*.json; do
  gcloud alpha monitoring policies create --project="$PROJECT" \
    --policy-from-file="$f" ${CHANNEL:+--notification-channels="$CHANNEL"}
done
