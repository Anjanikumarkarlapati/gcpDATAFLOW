"""Routing of events that are beyond the allowed lateness (FR-6)."""
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics import Metrics

from clickstream.config import WindowConfig
from clickstream.parsing import utc_str

ON_TIME = "on_time"
TOO_LATE = "too_late"


class RouteTooLate(beam.DoFn):
    """Diverts events whose window can no longer accept data.

    Beam silently drops data older than a window's allowed lateness, so we detect
    it first and preserve the event. A stateless DoFn cannot read the pipeline
    watermark, so this uses wall-clock time as a proxy: an event is too late when
    now > window_end + allowed_lateness. The watermark normally trails wall-clock
    time, so this errs on the side of keeping events in the live path; anything
    that still slips past the true watermark is dropped by Beam and is visible
    via the `dropped due to lateness` Dataflow counter.
    """

    def __init__(self, cfg: WindowConfig = WindowConfig(), clock=None):
        self.cfg = cfg
        self._clock = clock
        self.late_counter = Metrics.counter("clickstream", "late_events")

    def process(self, event):
        import math
        import time

        now = (self._clock or time.time)()
        w = self.cfg.window_seconds
        window_end = (math.floor(event["event_ts"] / w) + 1) * w
        deadline = window_end + self.cfg.allowed_lateness_seconds
        if now > deadline:
            self.late_counter.inc()
            yield pvalue.TaggedOutput(
                TOO_LATE,
                {
                    **{k: v for k, v in event.items() if k != "event_ts"},
                    "event_time": utc_str(event["event_ts"]),
                    "window_end": utc_str(window_end),
                    "lateness_seconds": int(now - window_end),
                    "processing_time": utc_str(now),
                },
            )
        else:
            yield pvalue.TaggedOutput(ON_TIME, event)
