"""End-to-end tests over the real trace samples bundled with this repository.

These pin the property the tooling exists for: filtering a trace through an L1
cache yields a trace with measurably lower locality.
"""

import os
import unittest

import _path  # noqa: F401
from _path import SAMPLES_DIR  # noqa: E402
from l1_filter import FilterStats, L1Cache, filter_requests  # noqa: E402
from reuse_distance import analyze, build_report  # noqa: E402
from trace_io import format_request, open_trace, parse_line, read_trace  # noqa: E402

# Kept small so the suite stays fast in CI; large enough to be representative.
SAMPLE_REQUESTS = 30_000


def working_set_bytes(requests):
    """Bytes needed to hold every distinct object in *requests* at once."""
    sizes = {r.key: r.object_size for r in requests}
    return sum(sizes.values())


def load_sample(name, limit=SAMPLE_REQUESTS):
    path = os.path.join(SAMPLES_DIR, name)
    with open_trace(path) as handle:
        return list(read_trace(handle, max_requests=limit))


@unittest.skipUnless(
    os.path.isdir(SAMPLES_DIR), "trace samples are not present in this checkout"
)
class SampleTraceTest(unittest.TestCase):
    def test_samples_parse_cleanly(self):
        for name in ("cluster001", "cluster052"):
            with self.subTest(sample=name):
                requests = load_sample(name, limit=5_000)
                self.assertEqual(len(requests), 5_000)
                self.assertTrue(all(r.object_size >= 0 for r in requests))

    def test_roundtrip_preserves_sample_bytes(self):
        path = os.path.join(SAMPLES_DIR, "cluster001")
        with open_trace(path) as handle:
            for _, line in zip(range(2_000), handle):
                line = line.rstrip("\n")
                self.assertEqual(format_request(parse_line(line)), line)

    def test_filtering_lowers_locality(self):
        """The whole point of the tool: the L2 trace has lower locality."""
        requests = load_sample("cluster001")
        before, before_counters = analyze(iter(requests))
        before_report = build_report(before, before_counters)

        capacity = working_set_bytes(requests) // 20  # L1 well below the WSS
        cache = L1Cache(policy="lru", capacity_bytes=capacity)
        stats = FilterStats()
        filtered = list(filter_requests(iter(requests), cache, stats=stats))
        after, after_counters = analyze(iter(filtered))
        after_report = build_report(after, after_counters)

        self.assertGreater(stats.read_hits, 0, "L1 absorbed nothing")
        self.assertLess(len(filtered), len(requests))

        # Short-reuse traffic is gone, so what remains is spread further apart.
        self.assertGreater(after.reuse_mean, before.reuse_mean)
        self.assertGreater(
            after.percentiles("reuse")["p50"], before.percentiles("reuse")["p50"]
        )
        self.assertGreater(
            after_report["cold_miss_ratio"], before_report["cold_miss_ratio"]
        )
        self.assertGreater(
            after_report["one_hit_wonder_ratio"],
            before_report["one_hit_wonder_ratio"],
        )

    def test_cold_miss_ratio_rises_monotonically_with_l1_size(self):
        """Cold-miss and one-hit-wonder ratios are the robust locality signals.

        Unlike the mean reuse distance they hold at every L1 size, including
        one large enough to absorb the whole working set.
        """
        requests = load_sample("cluster001")
        wss = working_set_bytes(requests)
        ratios = []
        for divisor in (50, 20, 5, 1):
            cache = L1Cache(policy="lru", capacity_bytes=max(1, wss // divisor))
            filtered = list(filter_requests(iter(requests), cache))
            stats, counters = analyze(iter(filtered))
            report = build_report(stats, counters)
            ratios.append((report["cold_miss_ratio"], report["one_hit_wonder_ratio"]))
        cold = [r[0] for r in ratios]
        wonders = [r[1] for r in ratios]
        self.assertEqual(cold, sorted(cold))
        self.assertEqual(wonders, sorted(wonders))

    def test_l1_larger_than_working_set_yields_a_pure_cold_miss_stream(self):
        """An oversized L1 is a degenerate filter: no reuse survives at all.

        This is the trap users should know about -- the resulting trace has no
        reuse to study, so the L1 must be sized below the working set.
        """
        requests = load_sample("cluster001")
        capacity = working_set_bytes(requests) * 4
        cache = L1Cache(policy="lru", capacity_bytes=capacity)
        filtered = list(filter_requests(iter(requests), cache, write_policy="drop"))
        stats, counters = analyze(iter(filtered))
        self.assertEqual(stats.count, 0, "an oversized L1 should leave no reuses")
        self.assertEqual(counters["requests_analyzed"], counters["distinct_objects"])
        self.assertEqual(build_report(stats, counters)["cold_miss_ratio"], 1.0)

    def test_bigger_l1_filters_harder(self):
        requests = load_sample("cluster001")
        wss = working_set_bytes(requests)
        forwarded = {}
        for capacity in (wss // 50, wss // 10, wss // 2):
            cache = L1Cache(policy="lru", capacity_bytes=capacity)
            forwarded[capacity] = len(list(filter_requests(iter(requests), cache)))
        sizes = sorted(forwarded)
        counts = [forwarded[size] for size in sizes]
        self.assertEqual(counts, sorted(counts, reverse=True))

    def test_output_remains_a_valid_trace(self):
        requests = load_sample("cluster052", limit=10_000)
        cache = L1Cache(policy="fifo", capacity_objects=500)
        for request in filter_requests(iter(requests), cache):
            # Re-parsing the rendered line must give back an identical request.
            self.assertEqual(parse_line(format_request(request)), request)

    def test_all_policies_produce_a_usable_l2_trace(self):
        requests = load_sample("cluster001", limit=10_000)
        capacity = working_set_bytes(requests) // 20
        for policy in ("lru", "fifo", "clock", "random"):
            with self.subTest(policy=policy):
                cache = L1Cache(policy=policy, capacity_bytes=capacity)
                filtered = list(filter_requests(iter(requests), cache))
                self.assertTrue(filtered)
                after, _ = analyze(iter(filtered))
                before, _ = analyze(iter(requests))
                self.assertGreater(after.reuse_mean, before.reuse_mean)

    def test_ttl_aware_filtering_forwards_at_least_as_much(self):
        requests = load_sample("cluster052", limit=20_000)
        capacity = working_set_bytes(requests) // 10
        plain = L1Cache(policy="lru", capacity_bytes=capacity, ttl_aware=False)
        ttl = L1Cache(policy="lru", capacity_bytes=capacity, ttl_aware=True)
        plain_count = len(list(filter_requests(iter(requests), plain)))
        ttl_count = len(list(filter_requests(iter(requests), ttl)))
        # Expiry can only remove objects early, never add hits.
        self.assertGreaterEqual(ttl_count, plain_count)


if __name__ == "__main__":
    unittest.main()
