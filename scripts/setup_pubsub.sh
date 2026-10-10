#!/usr/bin/env bash
# Usage: PROJECT=my-proj scripts/setup_pubsub.sh
set -euo pipefail
: "${PROJECT:?set PROJECT}"
gcloud pubsub topics create clickstream-events --project="$PROJECT"
# Dead-letter topic for messages Dataflow repeatedly fails to ack (belt and braces).
gcloud pubsub topics create clickstream-events-dlq --project="$PROJECT"
gcloud pubsub subscriptions create clickstream-sub --project="$PROJECT" \
  --topic=clickstream-events \
  --ack-deadline=60 \
  --message-retention-duration=7d      # enables replay after incompatible updates
gcloud pubsub subscriptions create clickstream-dlq-sub --project="$PROJECT" \
  --topic=clickstream-events-dlq
