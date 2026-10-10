"""Generate deterministic sample data by running the real pipeline transforms.

Writes frontend/public/sample-data.json (metrics, late events, dead letters) and
sample_data/events.jsonl (raw events for use with the producer/tests).

  python -m scripts.generate_sample_data
"""
import json
import os
import tempfile
import random
from datetime import datetime, timedelta, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from clickstream.config import WindowConfig
from clickstream.pipeline import process_clickstream
from producer.publisher import make_event

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
START = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
HOUR = 3600


def build_events(rng):
    """~1h of traffic with a mid-hour spike, plus duplicates and malformed payloads."""
    normal, dupes = [], []
    for sec in range(HOUR):
        rate = 4 + (6 if 1500 <= sec < 2100 else 0) + rng.randint(0, 2)  # events/sec
        for _ in range(rate):
            e = make_event(START + timedelta(seconds=sec + rng.random()))
            normal.append(e)
            if rng.random() < 0.02:
                dupes.append(e)
    malformed = [b'{"event_id": "broken", ' for _ in range(40)] + [
        json.dumps({**make_event(START), "schema_version": "9.9"}).encode() for _ in range(12)
    ] + [json.dumps({k: v for k, v in make_event(START).items() if k != "user_id"}).encode()
         for _ in range(8)]
    stale = [make_event(START + timedelta(seconds=rng.randint(0, 1500))) for _ in range(60)]
    return normal, dupes, malformed, stale




def _append(row, path):
    # Appending one small line per write is atomic enough across local workers.
    with open(path, "a") as f:
        f.write(json.dumps(row) + "\n")


def collect(events_raw, clock_epoch):
    results = {}
    cfg = WindowConfig()
    p = beam.Pipeline()
    p.options.view_as(StandardOptions).streaming = True
    # Element timestamps are re-assigned from event_time inside the pipeline.
    stream = TestStream().advance_watermark_to(0)
    for i in range(0, len(events_raw), 1000):
        stream = stream.add_elements([TimestampedValue(r, 1) for r in events_raw[i:i + 1000]])
    raw = p | stream.advance_watermark_to_infinity()
    metrics, late, dead = process_clickstream(raw, cfg, clock=lambda: clock_epoch)
    tmp = tempfile.mkdtemp()
    for name, pc in (("metrics", metrics), ("late", late), ("dead", dead)):
        pc | f"Save{name}" >> beam.Map(_append, os.path.join(tmp, name + ".jsonl"))
    p.run().wait_until_finish()
    for name in ("metrics", "late", "dead"):
        fn = os.path.join(tmp, name + ".jsonl")
        results[name] = [json.loads(line) for line in open(fn)] if os.path.exists(fn) else []
    return results


def main():
    rng = random.Random(7)
    normal, dupes, malformed, stale = build_events(rng)
    # The "too late" check compares wall-clock to window_end + lateness, so run one
    # pass per window with arrival time shortly after that window closes.
    by_window = {}
    for e in normal + dupes:
        w = int(datetime.fromisoformat(e["event_time"]).timestamp() - START.timestamp()) // 300
        by_window.setdefault(w, []).append(json.dumps(e).encode())
    by_window.setdefault(0, []).extend(malformed)
    a = {"metrics": [], "late": [], "dead": []}
    ordered = []
    for w, raws in sorted(by_window.items()):
        rng.shuffle(raws)
        ordered += raws
        res = collect(raws, START.timestamp() + (w + 1) * 300 + 30)
        for k in a:
            a[k] += res[k]
    # Stale events arriving an hour later land in the late-events table.
    b = collect([json.dumps(e).encode() for e in stale], (START + timedelta(seconds=2 * HOUR)).timestamp())
    rng.shuffle(ordered)

    metrics = sorted(a.get("metrics", []), key=lambda r: (r["window_start"], r["page_url"]))
    data = {
        "generated_for": "sample",
        "window_seconds": 300,
        "metrics": metrics,
        "late_events": sorted(b.get("late", []), key=lambda r: r["event_time"])[:25],
        "late_total": len(b.get("late", [])),
        "dead_letters": a.get("dead", [])[:25],
        "dead_total": len(a.get("dead", [])),
        "totals": {
            "valid_events": len(normal),
            "duplicates_sent": len(dupes),
            "raw_events_sent": len(ordered),
        },
    }
    os.makedirs(os.path.join(ROOT, "frontend/public"), exist_ok=True)
    with open(os.path.join(ROOT, "frontend/public/sample-data.json"), "w") as f:
        json.dump(data, f, indent=1)
    os.makedirs(os.path.join(ROOT, "sample_data"), exist_ok=True)
    with open(os.path.join(ROOT, "sample_data/events.jsonl"), "w") as f:
        for line in ordered[:500]:
            f.write(line.decode() + "\n")
    print(len(metrics), "metric rows;", data["late_total"], "late;", data["dead_total"], "dead")


if __name__ == "__main__":
    main()
