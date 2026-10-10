"""Per-page engagement aggregation (FR-4) and output formatting (FR-5, FR-7)."""
import apache_beam as beam
from apache_beam.utils.windowed_value import PaneInfoTiming

from clickstream.config import CLICK, PAGE_VIEW, PURCHASE, WindowConfig
from clickstream.parsing import utc_str
from clickstream.sketch import KMVSketch


class _Acc:
    __slots__ = ("page_views", "clicks", "purchases", "total", "users", "sessions")

    def __init__(self, k):
        self.page_views = self.clicks = self.purchases = self.total = 0
        self.users = KMVSketch(k)
        self.sessions = KMVSketch(k)


class EngagementCombineFn(beam.CombineFn):
    """Counts plus approximate distinct users/sessions in one combine pass."""

    def __init__(self, sketch_size: int = WindowConfig.sketch_size):
        self.k = sketch_size

    def create_accumulator(self):
        return _Acc(self.k)

    def add_input(self, acc, event):
        acc.total += 1
        etype = event["event_type"]
        if etype == PAGE_VIEW:
            acc.page_views += 1
        elif etype == CLICK:
            acc.clicks += 1
        elif etype == PURCHASE:
            acc.purchases += 1
        acc.users.add(event["user_id"])
        acc.sessions.add(event["session_id"])
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for a in accumulators:
            merged.page_views += a.page_views
            merged.clicks += a.clicks
            merged.purchases += a.purchases
            merged.total += a.total
            merged.users.merge(a.users)
            merged.sessions.merge(a.sessions)
        return merged

    def extract_output(self, acc):
        return {
            "page_views": acc.page_views,
            "clicks": acc.clicks,
            "purchases": acc.purchases,
            "total": acc.total,
            "active_users": acc.users.estimate(),
            "sessions": acc.sessions.estimate(),
        }


class FormatMetric(beam.DoFn):
    """Turns (page_url, aggregate) into the BigQuery metrics row.

    is_final is False only for EARLY (preliminary) panes. ON_TIME and LATE panes
    are both "final-grade"; a LATE pane is a correction of an earlier row for the
    same (window, page), so consumers keep the row with the latest processing_time.
    """

    def __init__(self, window_seconds: int = WindowConfig.window_seconds, clock=None):
        self.window_seconds = window_seconds
        self._clock = clock

    def process(self, element, window=beam.DoFn.WindowParam, pane_info=beam.DoFn.PaneInfoParam):
        import time

        page_url, agg = element
        sessions = agg["sessions"]
        now = (self._clock or time.time)()
        yield {
            "window_start": utc_str(float(window.start)),
            "window_end": utc_str(float(window.end)),
            "page_url": page_url,
            "page_views": agg["page_views"],
            "clicks": agg["clicks"],
            "active_users": agg["active_users"],
            "sessions": sessions,
            "purchases": agg["purchases"],
            "conversion_rate": (agg["purchases"] / sessions) if sessions else None,
            "events_per_second": agg["total"] / self.window_seconds,
            "processing_time": utc_str(now),
            "is_final": pane_info.timing != PaneInfoTiming.EARLY,
        }
