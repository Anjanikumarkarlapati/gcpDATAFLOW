import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from clickstream.config import WindowConfig
from clickstream.parsing import utc_str
from clickstream.pipeline import process_clickstream
from tests.helpers import BASE, event, raw

CFG = WindowConfig(sketch_size=64)


def _pipeline():
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    return p


def _row(page_views, clicks, users, sessions, purchases, total, final, page="/home", start=0):
    return {
        "window_start": utc_str(BASE + start),
        "window_end": utc_str(BASE + start + 300),
        "page_url": page,
        "page_views": page_views, "clicks": clicks, "active_users": users,
        "sessions": sessions, "purchases": purchases,
        "conversion_rate": purchases / sessions if sessions else None,
        "events_per_second": total / 300,
        "is_final": final,
    }


def _strip(row):
    return {k: v for k, v in row.items() if k != "processing_time"}


def _stamp(e):
    return TimestampedValue(raw(e), BASE + 600)  # source timestamp is irrelevant


def test_early_ontime_and_late_panes_accumulate():
    clock = lambda: BASE + 400  # noqa: E731  (window closed, still within lateness)
    stream = (
        TestStream()
        .advance_watermark_to(BASE)
        .add_elements([
            _stamp(event(10, "page_view", "u1", "s1")),
            _stamp(event(20, "click", "u1", "s1")),
            _stamp(event(30, "purchase", "u2", "s2")),
            _stamp(event(30, "purchase", "u2", "s2", event_id="dup")),  # distinct id
        ])
        .advance_processing_time(61)                 # early pane
        .advance_watermark_to(BASE + 301)            # on-time pane
        .add_elements([_stamp(event(40, "page_view", "u3", "s3"))])  # late, in lateness
        .advance_watermark_to_infinity()
    )
    with _pipeline() as p:
        metrics, late, dead = process_clickstream(p | stream, CFG, clock)
        assert_that(metrics | "S" >> beam.Map(_strip), equal_to([
            _row(1, 1, 2, 2, 2, 4, final=False),
            _row(1, 1, 2, 2, 2, 4, final=True),
            _row(2, 1, 3, 3, 2, 5, final=True),
        ]), label="metrics")
        assert_that(late, equal_to([]), label="late")
        assert_that(dead, equal_to([]), label="dead")


def test_duplicates_are_ignored():
    clock = lambda: BASE + 100  # noqa: E731
    same = event(10, event_id="same")
    stream = (
        TestStream()
        .advance_watermark_to(BASE)
        .add_elements([_stamp(same), _stamp(same), _stamp(same)])
        .advance_watermark_to_infinity()
    )
    with _pipeline() as p:
        metrics, _, _ = process_clickstream(p | stream, CFG, clock)
        assert_that(metrics | beam.Map(_strip), equal_to([_row(1, 0, 1, 1, 0, 1, final=True)]))


def test_too_late_and_invalid_events_are_preserved():
    clock = lambda: BASE + 300 + 601 + 10  # noqa: E731  10s past the lateness deadline
    bad = b"{oops"
    stream = (
        TestStream()
        .advance_watermark_to(BASE)
        .add_elements([_stamp(event(10, event_id="old")), TimestampedValue(bad, BASE)])
        .advance_watermark_to_infinity()
    )
    with _pipeline() as p:
        metrics, late, dead = process_clickstream(p | stream, CFG, clock)
        assert_that(metrics, equal_to([]), label="metrics")
        assert_that(
            late | "L" >> beam.Map(lambda r: (r["event_id"], r["lateness_seconds"])),
            equal_to([("old", 611)]), label="late")
        assert_that(
            dead | "D" >> beam.Map(lambda r: (r["raw_payload"], r["failure_type"])),
            equal_to([("{oops", "malformed_json")]), label="dead")


def test_separate_pages_and_windows():
    clock = lambda: BASE + 100  # noqa: E731
    stream = (
        TestStream()
        .advance_watermark_to(BASE)
        .add_elements([
            _stamp(event(10, page="/a")),
            _stamp(event(20, page="/b", user="u2", session="s2")),
            _stamp(event(310, page="/a", user="u3", session="s3")),  # next window
        ])
        .advance_watermark_to_infinity()
    )
    with _pipeline() as p:
        metrics, _, _ = process_clickstream(p | stream, CFG, clock)
        assert_that(metrics | beam.Map(_strip), equal_to([
            _row(1, 0, 1, 1, 0, 1, True, page="/a"),
            _row(1, 0, 1, 1, 0, 1, True, page="/b"),
            _row(1, 0, 1, 1, 0, 1, True, page="/a", start=300),
        ]))
