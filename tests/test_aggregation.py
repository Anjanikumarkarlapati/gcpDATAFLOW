from clickstream.aggregation import EngagementCombineFn
from tests.helpers import event


def test_combine_counts_and_merge():
    fn = EngagementCombineFn(sketch_size=64)
    evs = [
        event(1, "page_view", "u1", "s1"),
        event(2, "page_view", "u2", "s2"),
        event(3, "click", "u1", "s1"),
        event(4, "purchase", "u1", "s1"),
        event(5, "scroll", "u3", "s3"),  # counted as an event, not a named metric
    ]
    a, b = fn.create_accumulator(), fn.create_accumulator()
    for e in evs[:2]:
        fn.add_input(a, e)
    for e in evs[2:]:
        fn.add_input(b, e)
    out = fn.extract_output(fn.merge_accumulators([a, b]))
    assert out == {
        "page_views": 2, "clicks": 1, "purchases": 1, "total": 5,
        "active_users": 3, "sessions": 3,
    }
