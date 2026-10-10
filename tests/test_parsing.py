import json

import pytest

from clickstream.parsing import ValidationError, parse_event
from tests.helpers import BASE, event, raw


def test_valid_event_gets_epoch_timestamp():
    e = parse_event(raw(event(10)))
    assert e["event_ts"] == BASE + 10
    assert e["event_type"] == "page_view"


@pytest.mark.parametrize(
    "payload,failure_type",
    [
        (b"{not json", "malformed_json"),
        (b"\xff\xfe", "malformed_json"),
        (b"[1, 2]", "malformed_json"),
        (json.dumps({k: v for k, v in event(1).items() if k != "user_id"}).encode(), "missing_field"),
        (raw(event(1, user="")), "missing_field"),
        (raw(event(1, device=7)), "invalid_type"),
        (raw(event(1, schema_version="9.9")), "unsupported_schema"),
        (raw(event(1, event_time="yesterday")), "invalid_event_time"),
        (raw(event(1, event_time="2026-01-01T12:00:00")), "invalid_event_time"),
    ],
)
def test_rejections(payload, failure_type):
    with pytest.raises(ValidationError) as exc:
        parse_event(payload)
    assert exc.value.failure_type == failure_type
