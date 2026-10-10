import json
from datetime import datetime, timezone

# Aligned to a 5-minute boundary so window math is easy to read.
BASE = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp()


def iso(offset_s: float) -> str:
    return datetime.fromtimestamp(BASE + offset_s, tz=timezone.utc).isoformat()


def event(offset_s, event_type="page_view", user="u1", session="s1", page="/home",
          event_id=None, **overrides):
    e = {
        "event_id": event_id or f"e-{offset_s}-{event_type}-{user}-{session}",
        "user_id": user,
        "session_id": session,
        "event_type": event_type,
        "page_url": page,
        "device": "web",
        "event_time": iso(offset_s),
        "schema_version": "1.0",
    }
    e.update(overrides)
    return e


def raw(e) -> bytes:
    return json.dumps(e).encode()
