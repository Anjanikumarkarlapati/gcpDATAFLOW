"""Parsing and validation of raw clickstream payloads (FR-1, FR-2, FR-3)."""
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics import Metrics

from clickstream.config import REQUIRED_FIELDS, SUPPORTED_SCHEMA_VERSIONS

VALID = "valid"
DEAD_LETTER = "dead_letter"


class ValidationError(ValueError):
    """Raised when a payload is rejected; `failure_type` is stored with it."""

    def __init__(self, failure_type: str, reason: str):
        super().__init__(reason)
        self.failure_type = failure_type
        self.reason = reason


def utc_str(epoch_seconds: float) -> str:
    """Format epoch seconds as a BigQuery-friendly UTC timestamp string."""
    dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC")


def parse_event(raw) -> dict:
    """Parse and validate one payload.

    Returns the event dict with an added float `event_ts` (epoch seconds).
    Raises ValidationError for anything malformed, incomplete or unsupported.
    """
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        obj = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValidationError("malformed_json", f"cannot parse JSON: {e}")
    if not isinstance(obj, dict):
        raise ValidationError("malformed_json", "payload is not a JSON object")

    missing = [f for f in REQUIRED_FIELDS if obj.get(f) in (None, "")]
    if missing:
        raise ValidationError("missing_field", f"missing fields: {', '.join(missing)}")

    bad_type = [f for f in REQUIRED_FIELDS if not isinstance(obj[f], str)]
    if bad_type:
        raise ValidationError("invalid_type", f"fields must be strings: {', '.join(bad_type)}")

    if obj["schema_version"] not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValidationError(
            "unsupported_schema", f"unsupported schema_version {obj['schema_version']!r}"
        )

    try:
        ts = datetime.fromisoformat(obj["event_time"])
    except ValueError:
        raise ValidationError("invalid_event_time", f"bad event_time {obj['event_time']!r}")
    if ts.tzinfo is None:
        raise ValidationError("invalid_event_time", "event_time must include a UTC offset")

    event = {f: obj[f] for f in REQUIRED_FIELDS}
    event["event_ts"] = ts.timestamp()
    return event


class ParseAndValidate(beam.DoFn):
    """Splits raw payloads into VALID events and DEAD_LETTER rows."""

    def __init__(self, clock=None):
        self._clock = clock
        self.valid_counter = Metrics.counter("clickstream", "valid_events")
        self.dead_letter_counter = Metrics.counter("clickstream", "dead_letter_events")

    def process(self, raw):
        import time

        try:
            event = parse_event(raw)
        except ValidationError as e:
            self.dead_letter_counter.inc()
            payload = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
            now = (self._clock or time.time)()
            yield pvalue.TaggedOutput(
                DEAD_LETTER,
                {
                    "raw_payload": payload,
                    "failure_type": e.failure_type,
                    "failure_reason": e.reason,
                    "processing_time": utc_str(now),
                },
            )
            return
        self.valid_counter.inc()
        yield pvalue.TaggedOutput(VALID, event)
