from clickstream.sketch import KMVSketch


def test_exact_below_k():
    s = KMVSketch(k=64)
    for i in range(50):
        s.add(f"u{i}")
        s.add(f"u{i}")  # duplicates ignored
    assert s.estimate() == 50


def test_approximate_above_k():
    s = KMVSketch(k=1024)
    for i in range(50_000):
        s.add(f"user-{i}")
    assert abs(s.estimate() - 50_000) / 50_000 < 0.10
    assert len(s.hashes) == 1024  # bounded state


def test_merge_equals_union():
    a, b = KMVSketch(k=64), KMVSketch(k=64)
    for i in range(30):
        a.add(f"u{i}")
    for i in range(20, 50):
        b.add(f"u{i}")
    a.merge(b)
    assert a.estimate() == 50
