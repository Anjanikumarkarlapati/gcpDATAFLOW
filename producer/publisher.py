"""Sample clickstream producer: publishes simulated events to Pub/Sub.

Includes knobs to exercise every path the pipeline handles: late events,
duplicates and malformed payloads.

  python -m producer.publisher --project P --topic clickstream-events \
      --rate 20 --duration 600 --late-fraction 0.05 --duplicate-fraction 0.02 \
      --malformed-fraction 0.01
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from clickstream.config import EVENT_TIME_ATTRIBUTE

PAGES = ["/home", "/search", "/product/42", "/product/7", "/cart", "/checkout"]
DEVICES = ["web", "ios", "android"]
# Weighted so that purchases are rare and /home is a "hot" page.
EVENT_TYPES = ["page_view"] * 6 + ["click"] * 3 + ["purchase"]
PAGE_WEIGHTS = [5, 2, 2, 1, 1, 1]


def make_event(now: datetime, late_seconds: float = 0.0, users: int = 500) -> dict:
    user = f"u{random.randrange(users)}"
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user,
        "session_id": f"{user}-s{random.randrange(3)}",
        "event_type": random.choice(EVENT_TYPES),
        "page_url": random.choices(PAGES, PAGE_WEIGHTS)[0],
        "device": random.choice(DEVICES),
        "event_time": (now - timedelta(seconds=late_seconds)).isoformat(),
        "schema_version": "1.0",
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", default="clickstream-events")
    ap.add_argument("--rate", type=float, default=10, help="events per second")
    ap.add_argument("--duration", type=float, default=300, help="seconds; 0 = forever")
    ap.add_argument("--late-fraction", type=float, default=0.0)
    ap.add_argument("--late-seconds", type=float, default=420,
                    help="how far behind late events are (>300 usually crosses a window)")
    ap.add_argument("--very-late-seconds", type=float, default=900,
                    help="used for the too-late share of late events")
    ap.add_argument("--duplicate-fraction", type=float, default=0.0)
    ap.add_argument("--malformed-fraction", type=float, default=0.0)
    args = ap.parse_args(argv)

    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(args.project, args.topic)

    def send(payload: bytes, event_time: str = None):
        attrs = {EVENT_TIME_ATTRIBUTE: event_time} if event_time else {}
        publisher.publish(topic, payload, **attrs)

    end = time.time() + args.duration if args.duration else float("inf")
    sent = 0
    while time.time() < end:
        now = datetime.now(timezone.utc)
        r = random.random()
        if r < args.malformed_fraction:
            send(b'{"event_id": "broken", ')
        else:
            late = 0.0
            if random.random() < args.late_fraction:
                late = random.choice([args.late_seconds, args.very_late_seconds])
            e = make_event(now, late)
            send(json.dumps(e).encode(), e["event_time"])
            if random.random() < args.duplicate_fraction:
                send(json.dumps(e).encode(), e["event_time"])  # same event_id
        sent += 1
        time.sleep(1.0 / args.rate)
    print(f"published ~{sent} events")


if __name__ == "__main__":
    main()
