"""Pipeline-wide constants and tunables (PRD sections 6 and 7)."""
from dataclasses import dataclass

REQUIRED_FIELDS = (
    "event_id",
    "user_id",
    "session_id",
    "event_type",
    "page_url",
    "device",
    "event_time",
    "schema_version",
)

SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0"})

# Event types that feed a dedicated metric; every valid event counts toward
# events_per_second regardless of type.
PAGE_VIEW = "page_view"
CLICK = "click"
PURCHASE = "purchase"

# Pub/Sub message attribute carrying event_time so the Dataflow source can
# derive its watermark from event time instead of publish time.
EVENT_TIME_ATTRIBUTE = "event_time"


@dataclass(frozen=True)
class WindowConfig:
    window_seconds: int = 300          # FR-4: 5-minute fixed windows
    early_firing_seconds: int = 60     # FR-5: preliminary result every ~60s
    allowed_lateness_seconds: int = 600  # FR-6: 10 minutes
    dedup_retention_seconds: int = 3600  # FR-2/Late-data policy: dedup horizon
    sketch_size: int = 1024            # KMV k: ~3% error on distinct counts
