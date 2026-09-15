"""Tests for scripts/reuse_distance.py."""

import io
import json
import os
import random
import tempfile
import unittest
from contextlib import redirect_stdout

import _path  # noqa: F401
from reuse_distance import (  # noqa: E402
    DistanceStats,
    Fenwick,
    analyze,
    build_report,
    main,
)
from trace_io import parse_line  # noqa: E402


def trace_from_keys(keys, op="get"):
    return [parse_line(f"{i},{key},8,100,1,{op},0") for i, key in enumerate(keys)]


def brute_force_reuse_distances(keys):
    """Reference implementation: distinct keys between consecutive accesses."""
    distances = []
    last = {}
    for index, key in enumerate(keys):
        if key in last:
            distances.append(len(set(keys[last[key] + 1 : index])))
        last[key] = index
    return distances


class FenwickTest(unittest.TestCase):
    def test_prefix_sum_matches_naive(self):
        rng = random.Random(1234)
        size = 300
        tree = Fenwick(size=16)  # start small to exercise growth
        naive = [0] * size
        for _ in range(2000):
            index = rng.randrange(size)
            delta = rng.choice((1, -1))
            tree.add(index, delta)
            naive[index] += delta
            probe = rng.randrange(size)
            self.assertEqual(tree.prefix_sum(probe), sum(naive[: probe + 1]))

    def test_grows_on_demand(self):
        tree = Fenwick(size=4)
        tree.add(1000, 1)
        self.assertGreater(len(tree), 1000)
        self.assertEqual(tree.prefix_sum(1000), 1)
        self.assertEqual(tree.prefix_sum(999), 0)

    def test_growth_preserves_existing_values(self):
        tree = Fenwick(size=4)
        for index in range(4):
            tree.add(index, 1)
        tree.add(500, 1)  # forces a rebuild
        self.assertEqual(tree.prefix_sum(3), 4)
        self.assertEqual(tree.prefix_sum(500), 5)

    def test_negative_index_is_zero(self):
        tree = Fenwick(size=8)
        tree.add(0, 5)
        self.assertEqual(tree.prefix_sum(-1), 0)

    def test_index_beyond_size_clamps(self):
        tree = Fenwick(size=8)
        tree.add(0, 3)
        self.assertEqual(tree.prefix_sum(10_000), 3)


class BucketTest(unittest.TestCase):
    def test_bucket_boundaries(self):
        self.assertEqual(DistanceStats.bucket_of(0), 0)
        self.assertEqual(DistanceStats.bucket_bounds(0), (0, 0))
        self.assertEqual(DistanceStats.bucket_of(1), 1)
        self.assertEqual(DistanceStats.bucket_bounds(1), (1, 1))
        self.assertEqual(DistanceStats.bucket_of(3), 2)
        self.assertEqual(DistanceStats.bucket_bounds(2), (2, 3))
        self.assertEqual(DistanceStats.bucket_of(4), 3)
        self.assertEqual(DistanceStats.bucket_bounds(3), (4, 7))

    def test_every_distance_falls_inside_its_bucket(self):
        for distance in range(0, 5000):
            low, high = DistanceStats.bucket_bounds(DistanceStats.bucket_of(distance))
            self.assertTrue(low <= distance <= high, distance)


class AnalyzeTest(unittest.TestCase):
    def test_known_sequence(self):
        stats, counters = analyze(iter(trace_from_keys("ABCA")))
        self.assertEqual(counters["requests_analyzed"], 4)
        self.assertEqual(counters["distinct_objects"], 3)
        self.assertEqual(counters["reuses"], 1)
        self.assertEqual(list(stats.reuse), [2])
        self.assertEqual(list(stats.reference), [2])

    def test_immediate_reuse_has_distance_zero(self):
        stats, _ = analyze(iter(trace_from_keys("AA")))
        self.assertEqual(list(stats.reuse), [0])
        self.assertEqual(list(stats.reference), [0])

    def test_repeated_intervening_key_counted_once(self):
        # A B B A -> only one *distinct* object (B) sits between the two A
        # accesses, but two requests do, so the reference distance is larger.
        stats, _ = analyze(iter(trace_from_keys("ABBA")))
        self.assertEqual(list(stats.reuse), [0, 1])
        self.assertEqual(list(stats.reference), [0, 2])

    def test_matches_brute_force_on_random_traces(self):
        rng = random.Random(99)
        for trial in range(20):
            keys = [f"k{rng.randrange(12)}" for _ in range(200)]
            stats, _ = analyze(iter(trace_from_keys(keys)))
            with self.subTest(trial=trial):
                self.assertEqual(list(stats.reuse), brute_force_reuse_distances(keys))

    def test_matches_brute_force_with_skewed_popularity(self):
        rng = random.Random(7)
        # Zipf-ish: a few hot keys and a long tail, like a real cache workload.
        keys = [f"k{min(int(rng.paretovariate(1.2)), 60)}" for _ in range(500)]
        stats, _ = analyze(iter(trace_from_keys(keys)))
        self.assertEqual(list(stats.reuse), brute_force_reuse_distances(keys))

    def test_one_hit_wonders(self):
        stats, counters = analyze(iter(trace_from_keys("AABC")))
        self.assertEqual(counters["one_hit_wonders"], 2)  # B and C
        self.assertEqual(counters["distinct_objects"], 3)

    def test_reads_only_filters_writes(self):
        requests = [
            parse_line("0,a,8,100,1,get,0"),
            parse_line("1,b,8,100,1,set,60"),
            parse_line("2,a,8,100,1,get,0"),
        ]
        _, counters = analyze(iter(requests), reads_only=True)
        self.assertEqual(counters["requests_read"], 3)
        self.assertEqual(counters["requests_analyzed"], 2)
        self.assertEqual(counters["distinct_objects"], 1)

    def test_empty_trace(self):
        stats, counters = analyze(iter([]))
        self.assertEqual(counters["requests_analyzed"], 0)
        self.assertEqual(stats.reuse_mean, 0.0)
        report = build_report(stats, counters)
        self.assertEqual(report["cold_miss_ratio"], 0.0)


class PercentileTest(unittest.TestCase):
    def test_exact_percentiles(self):
        stats = DistanceStats(keep_samples=True)
        for value in range(1, 101):
            stats.record(value, value)
        percentiles = stats.percentiles("reuse")
        self.assertEqual(percentiles["p50"], 50)
        self.assertEqual(percentiles["p99"], 99)
        self.assertEqual(percentiles["p10"], 10)

    def test_approximate_percentiles_land_in_the_right_bucket(self):
        exact = DistanceStats(keep_samples=True)
        approx = DistanceStats(keep_samples=False)
        rng = random.Random(3)
        for _ in range(5000):
            value = rng.randrange(0, 4096)
            exact.record(value, value)
            approx.record(value, value)
        exact_p50 = exact.percentiles("reuse")["p50"]
        approx_p50 = approx.percentiles("reuse")["p50"]
        # The approximation reports a bucket's upper bound, so it is never
        # smaller than the exact value and never more than 2x away.
        self.assertGreaterEqual(approx_p50, exact_p50)
        self.assertLessEqual(approx_p50, max(1, exact_p50 * 2 + 1))

    def test_approximate_mode_keeps_no_samples(self):
        stats = DistanceStats(keep_samples=False)
        for value in range(100):
            stats.record(value, value)
        self.assertEqual(len(stats.reuse), 0)
        self.assertEqual(stats.count, 100)
        self.assertAlmostEqual(stats.reuse_mean, 49.5)


class HistogramTest(unittest.TestCase):
    def test_fractions_sum_to_one(self):
        stats, _ = analyze(iter(trace_from_keys([f"k{i % 20}" for i in range(400)])))
        rows = stats.histogram_rows("reuse")
        self.assertAlmostEqual(sum(row["fraction"] for row in rows), 1.0, places=9)
        self.assertAlmostEqual(rows[-1]["cumulative_fraction"], 1.0, places=9)

    def test_cdf_is_monotonic(self):
        stats, _ = analyze(iter(trace_from_keys([f"k{i % 37}" for i in range(900)])))
        rows = stats.histogram_rows("reuse")
        cdf = [row["cumulative_fraction"] for row in rows]
        self.assertEqual(cdf, sorted(cdf))


class LocalityOrderingTest(unittest.TestCase):
    """A sequential scan has far worse locality than a hot-key loop."""

    def test_scan_has_larger_reuse_distance_than_loop(self):
        loop_stats, _ = analyze(
            iter(trace_from_keys([f"k{i % 5}" for i in range(500)]))
        )
        scan_stats, _ = analyze(
            iter(trace_from_keys([f"k{i % 250}" for i in range(500)]))
        )
        self.assertLess(loop_stats.reuse_mean, scan_stats.reuse_mean)


class CommandLineTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.trace = os.path.join(self.tmpdir, "in.trace")
        with open(self.trace, "w") as fh:
            for i in range(300):
                fh.write(f"{i},k{i % 25},8,100,1,get,0\n")

    def tearDown(self):
        for name in os.listdir(self.tmpdir):
            os.unlink(os.path.join(self.tmpdir, name))
        os.rmdir(self.tmpdir)

    def _run(self, *args):
        out = io.StringIO()
        with redirect_stdout(out):
            code = main([self.trace, *args])
        return code, out.getvalue()

    def test_prints_a_report(self):
        code, out = self._run()
        self.assertEqual(code, 0)
        self.assertIn("reuse distance", out)
        self.assertIn("histogram", out)

    def test_json_output(self):
        path = os.path.join(self.tmpdir, "report.json")
        code, _ = self._run("--json", path)
        self.assertEqual(code, 0)
        with open(path) as fh:
            report = json.load(fh)
        self.assertEqual(report["requests_analyzed"], 300)
        self.assertEqual(report["distinct_objects"], 25)
        self.assertEqual(report["reuse_distance"]["mean"], 24.0)

    def test_histogram_csv_output(self):
        path = os.path.join(self.tmpdir, "hist.csv")
        self._run("--histogram-csv", path)
        with open(path) as fh:
            lines = fh.read().strip().splitlines()
        self.assertEqual(
            lines[0], "bucket_low,bucket_high,count,fraction,cumulative_fraction"
        )
        self.assertGreater(len(lines), 1)

    def test_max_requests(self):
        path = os.path.join(self.tmpdir, "report.json")
        self._run("--max-requests", "50", "--json", path)
        with open(path) as fh:
            self.assertEqual(json.load(fh)["requests_analyzed"], 50)

    def test_approximate_flag_runs(self):
        code, out = self._run("--approximate")
        self.assertEqual(code, 0)
        self.assertIn("reuse distance", out)

    def test_reference_metric_histogram(self):
        code, out = self._run("--metric", "reference")
        self.assertEqual(code, 0)
        self.assertIn("reference distance histogram", out)


if __name__ == "__main__":
    unittest.main()
