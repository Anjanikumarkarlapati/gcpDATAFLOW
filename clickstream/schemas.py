"""BigQuery schemas for the three output tables (FR-7, FR-2, FR-6)."""

METRICS_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "page_url", "type": "STRING", "mode": "REQUIRED"},
        {"name": "page_views", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "clicks", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "active_users", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "sessions", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "purchases", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "conversion_rate", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "events_per_second", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "is_final", "type": "BOOLEAN", "mode": "REQUIRED"},
    ]
}

DEAD_LETTER_SCHEMA = {
    "fields": [
        {"name": "raw_payload", "type": "STRING", "mode": "NULLABLE"},
        {"name": "failure_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "failure_reason", "type": "STRING", "mode": "REQUIRED"},
        {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

LATE_EVENTS_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "page_url", "type": "STRING", "mode": "REQUIRED"},
        {"name": "device", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "schema_version", "type": "STRING", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "lateness_seconds", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}
