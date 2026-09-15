"""Tests for scripts/l1_filter.py."""

import argparse
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

import _path  # noqa: F401
from l1_filter import (  # noqa: E402
    POLICIES,
    FilterStats,
    L1Cache,
    filter_requests,
    main,
    parse_size,
)
from trace_io import parse_line  # noqa: E402


def req(key, op="get", ts=0, key_size=1, value_size=0, ttl=0, client="1"):
    """Build a Request tersely."""
    return parse_line(f"{ts},{key},{key_size},{value_size},{client},{op},{ttl}")


def misses(sequence, **cache_kwargs):
    """Return the keys forwarded to L2 for a sequence of (key, op) pairs."""
    cache = L1Cache(**cache_kwargs)
    requests = (req(k, op) if isinstance(k, str) else req(*k) for k, op in sequence)
    return [r.key for r in filter_requests(requests, cache)]


class ParseSizeTest(unittest.TestCase):
    def test_plain_bytes(self):
        self.assertEqual(parse_size("1024"), 1024)

    def test_decimal_suffixes(self):
        self.assertEqual(parse_size("4MB"), 4_000_000)
        self.assertEqual(parse_size("2g"), 2_000_000_000)

    def test_binary_suffixes(self):
        self.assertEqual(parse_size("1KiB"), 1024)
        self.assertEqual(parse_size("1GiB"), 1024**3)

    def test_fractional(self):
        self.assertEqual(parse_size("1.5KB"), 1500)

    def test_rejects_garbage(self):
        for bad in ("", "abc", "-5", "5 parsecs", "0"):
            with self.assertRaises(argparse.ArgumentTypeError):
                parse_size(bad)


class CacheConstructionTest(unittest.TestCase):
    def test_requires_exactly_one_capacity(self):
        with self.assertRaises(ValueError):
            L1Cache(capacity_bytes=10, capacity_objects=10)
        with self.assertRaises(ValueError):
            L1Cache()

    def test_rejects_unknown_policy(self):
        with self.assertRaises(ValueError):
            L1Cache(policy="mru", capacity_objects=1)


class EvictionPolicyTest(unittest.TestCase):
    """Behavioural tests that distinguish the policies from one another."""

    def test_lru_keeps_recently_used(self):
        # A B A C A: LRU evicts B for C, so the final A still hits.
        seq = [(k, "get") for k in "ABACA"]
        self.assertEqual(misses(seq, policy="lru", capacity_objects=2), list("ABC"))

    def test_fifo_ignores_recency(self):
        # Same sequence: FIFO evicts A (inserted first) for C, so A misses again.
        seq = [(k, "get") for k in "ABACA"]
        self.assertEqual(misses(seq, policy="fifo", capacity_objects=2), list("ABCA"))

    def test_clock_gives_a_second_chance(self):
        # A's reference bit is set by the hit, so CLOCK evicts B instead.
        seq = [(k, "get") for k in "ABACA"]
        self.assertEqual(misses(seq, policy="clock", capacity_objects=2), list("ABC"))

    def test_random_is_deterministic_for_a_seed(self):
        seq = [(k, "get") for k in "ABCABCABCABC"]
        first = misses(seq, policy="random", capacity_objects=2, seed=7)
        second = misses(seq, policy="random", capacity_objects=2, seed=7)
        self.assertEqual(first, second)

    def test_every_policy_respects_capacity(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                cache = L1Cache(policy=policy, capacity_objects=3)
                for i in range(100):
                    cache.admit(f"k{i % 20}", 1, now=i)
                    self.assertLessEqual(len(cache), 3)

    def test_infinite_capacity_forwards_each_key_once(self):
        seq = [(k, "get") for k in "ABCABCABC"]
        self.assertEqual(misses(seq, policy="lru", capacity_objects=1000), list("ABC"))

    def test_tiny_capacity_forwards_everything(self):
        # Capacity of one object: alternating keys never hit.
        seq = [(k, "get") for k in "ABABAB"]
        self.assertEqual(misses(seq, policy="lru", capacity_objects=1), list("ABABAB"))


class SizeAccountingTest(unittest.TestCase):
    def test_evicts_until_within_byte_budget(self):
        cache = L1Cache(policy="fifo", capacity_bytes=100)
        cache.admit("a", 60, now=0)
        cache.admit("b", 60, now=1)
        self.assertFalse(cache.contains("a", 2))
        self.assertTrue(cache.contains("b", 2))
        self.assertEqual(cache.used_bytes, 60)

    def test_one_large_object_can_evict_several_small_ones(self):
        cache = L1Cache(policy="fifo", capacity_bytes=100)
        for i in range(10):
            cache.admit(f"k{i}", 10, now=i)
        self.assertEqual(len(cache), 10)
        cache.admit("big", 95, now=11)
        self.assertLessEqual(cache.used_bytes, 100)
        self.assertTrue(cache.contains("big", 12))

    def test_object_larger_than_cache_is_rejected(self):
        cache = L1Cache(policy="lru", capacity_bytes=100)
        cache.admit("huge", 500, now=0)
        self.assertEqual(len(cache), 0)
        self.assertEqual(cache.rejected_too_large, 1)

    def test_resize_on_update_is_accounted(self):
        cache = L1Cache(policy="lru", capacity_bytes=1000)
        cache.admit("a", 10, now=0)
        cache.admit("a", 30, now=1)
        self.assertEqual(cache.used_bytes, 30)
        self.assertEqual(len(cache), 1)

    def test_invalidate_frees_space(self):
        cache = L1Cache(policy="lru", capacity_bytes=1000)
        cache.admit("a", 40, now=0)
        self.assertTrue(cache.invalidate("a"))
        self.assertEqual(cache.used_bytes, 0)
        self.assertFalse(cache.invalidate("a"))


class TtlTest(unittest.TestCase):
    def test_object_expires_after_its_ttl(self):
        cache = L1Cache(policy="lru", capacity_objects=10, ttl_aware=True)
        cache.admit("a", 1, now=0, ttl=10)
        self.assertTrue(cache.contains("a", 5))
        self.assertFalse(cache.contains("a", 10))
        self.assertEqual(cache.expirations, 1)

    def test_ttl_ignored_when_not_ttl_aware(self):
        cache = L1Cache(policy="lru", capacity_objects=10, ttl_aware=False)
        cache.admit("a", 1, now=0, ttl=10)
        self.assertTrue(cache.contains("a", 10_000))

    def test_ttl_zero_never_expires(self):
        cache = L1Cache(policy="lru", capacity_objects=10, ttl_aware=True)
        cache.admit("a", 1, now=0, ttl=0)
        self.assertTrue(cache.contains("a", 10_000))

    def test_read_after_expiry_is_forwarded(self):
        cache = L1Cache(policy="lru", capacity_objects=10, ttl_aware=True)
        requests = [
            req("a", "set", ts=0, ttl=10),
            req("a", "get", ts=5),
            req("a", "get", ts=50),
        ]
        forwarded = list(filter_requests(requests, cache))
        # the set is written through, the t=5 get hits, the t=50 get expired
        self.assertEqual(
            [(r.operation, r.timestamp) for r in forwarded], [("set", 0), ("get", 50)]
        )


class WritePolicyTest(unittest.TestCase):
    def test_write_through_caches_and_forwards(self):
        cache = L1Cache(policy="lru", capacity_objects=10)
        requests = [req("a", "set"), req("a", "get")]
        forwarded = list(filter_requests(requests, cache, write_policy="through"))
        self.assertEqual([r.operation for r in forwarded], ["set"])

    def test_write_around_invalidates_and_forwards(self):
        cache = L1Cache(policy="lru", capacity_objects=10)
        requests = [req("a", "get"), req("a", "set"), req("a", "get")]
        forwarded = list(filter_requests(requests, cache, write_policy="around"))
        self.assertEqual([r.operation for r in forwarded], ["get", "set", "get"])

    def test_write_drop_emits_only_read_misses(self):
        cache = L1Cache(policy="lru", capacity_objects=10)
        requests = [req("a", "set"), req("a", "get"), req("b", "get")]
        forwarded = list(filter_requests(requests, cache, write_policy="drop"))
        self.assertEqual([r.key for r in forwarded], ["b"])

    def test_rejects_unknown_write_policy(self):
        cache = L1Cache(policy="lru", capacity_objects=10)
        with self.assertRaises(ValueError):
            list(filter_requests(iter([]), cache, write_policy="sideways"))

    def test_delete_invalidates_and_is_forwarded(self):
        cache = L1Cache(policy="lru", capacity_objects=10)
        requests = [req("a", "get"), req("a", "delete"), req("a", "get")]
        forwarded = list(filter_requests(requests, cache))
        self.assertEqual([r.operation for r in forwarded], ["get", "delete", "get"])

    def test_unknown_operation_passes_through(self):
        cache = L1Cache(policy="lru", capacity_objects=10)
        stats = FilterStats()
        forwarded = list(filter_requests([req("a", "quit")], cache, stats=stats))
        self.assertEqual(len(forwarded), 1)
        self.assertEqual(stats.other, 1)


class StatsTest(unittest.TestCase):
    def test_counters_add_up(self):
        cache = L1Cache(policy="lru", capacity_objects=2)
        stats = FilterStats()
        requests = [req(k, "get") for k in "ABABAB"] + [req("a", "set")]
        list(filter_requests(requests, cache, stats=stats))
        self.assertEqual(stats.requests, 7)
        self.assertEqual(stats.reads, 6)
        self.assertEqual(stats.writes, 1)
        self.assertEqual(stats.read_hits + stats.read_misses, stats.reads)
        self.assertAlmostEqual(stats.read_hit_ratio + stats.read_miss_ratio, 1.0)

    def test_ratios_are_zero_for_empty_input(self):
        stats = FilterStats()
        self.assertEqual(stats.read_hit_ratio, 0.0)
        summary = stats.to_dict(L1Cache(policy="lru", capacity_objects=1))
        self.assertEqual(summary["compression_ratio"], 0.0)


class OutputInvariantTest(unittest.TestCase):
    """The L2 trace must remain a valid, order-preserving subset of the input."""

    def _input_requests(self):
        keys = [f"k{i % 37}" for i in range(500)]
        ops = ["get"] * 9 + ["set"]
        return [
            req(key, ops[i % len(ops)], ts=i, key_size=8, value_size=100 + (i % 50))
            for i, key in enumerate(keys)
        ]

    def test_output_is_an_ordered_subsequence_of_the_input(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                requests = self._input_requests()
                cache = L1Cache(policy=policy, capacity_bytes=2000)
                forwarded = list(filter_requests(iter(requests), cache))
                self.assertLessEqual(len(forwarded), len(requests))
                iterator = iter(requests)
                for item in forwarded:
                    self.assertTrue(
                        any(item is candidate for candidate in iterator),
                        "forwarded request is out of order or not from the input",
                    )

    def test_filtering_never_invents_keys(self):
        requests = self._input_requests()
        cache = L1Cache(policy="lru", capacity_bytes=2000)
        forwarded = list(filter_requests(iter(requests), cache))
        self.assertTrue({r.key for r in forwarded} <= {r.key for r in requests})

    def test_every_key_appears_at_least_once_downstream(self):
        # A cold miss can never be absorbed, so each key must reach L2 once.
        requests = self._input_requests()
        cache = L1Cache(policy="lru", capacity_bytes=2000)
        forwarded = list(filter_requests(iter(requests), cache))
        self.assertEqual({r.key for r in forwarded}, {r.key for r in requests})


class CommandLineTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.trace = os.path.join(self.tmpdir, "in.trace")
        with open(self.trace, "w") as fh:
            for i in range(200):
                fh.write(f"{i},k{i % 10},8,100,1,get,0\n")

    def tearDown(self):
        for name in os.listdir(self.tmpdir):
            os.unlink(os.path.join(self.tmpdir, name))
        os.rmdir(self.tmpdir)

    def _run(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main([self.trace, *args])
        return code, out.getvalue(), err.getvalue()

    def test_writes_valid_trace_to_stdout(self):
        code, out, _ = self._run("--cache-objects", "5", "--quiet")
        self.assertEqual(code, 0)
        lines = out.strip().splitlines()
        self.assertTrue(lines)
        for line in lines:
            parse_line(line)  # raises if the output is not a valid trace

    def test_reports_stats_json(self):
        stats_path = os.path.join(self.tmpdir, "stats.json")
        code, _, _ = self._run(
            "--cache-objects", "5", "--quiet", "--stats-json", stats_path
        )
        self.assertEqual(code, 0)
        with open(stats_path) as fh:
            summary = json.load(fh)
        self.assertEqual(summary["requests"], 200)
        self.assertEqual(summary["policy"], "lru")
        self.assertEqual(summary["capacity_objects"], 5)

    def test_stats_go_to_stderr_not_stdout(self):
        _, out, err = self._run("--cache-objects", "5")
        self.assertIn("read_hit_ratio", err)
        self.assertNotIn("read_hit_ratio", out)

    def test_output_file_option(self):
        dest = os.path.join(self.tmpdir, "out.trace")
        code, _, _ = self._run("--cache-objects", "5", "--quiet", "-o", dest)
        self.assertEqual(code, 0)
        with open(dest) as fh:
            self.assertTrue(fh.read().strip())

    def test_max_requests_is_honoured(self):
        stats_path = os.path.join(self.tmpdir, "stats.json")
        self._run(
            "--cache-objects",
            "5",
            "--quiet",
            "--max-requests",
            "20",
            "--stats-json",
            stats_path,
        )
        with open(stats_path) as fh:
            self.assertEqual(json.load(fh)["requests"], 20)

    def test_capacity_is_required(self):
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            main([self.trace])

    def test_capacity_and_objects_are_mutually_exclusive(self):
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            main([self.trace, "--cache-size", "1MB", "--cache-objects", "5"])

    def test_larger_l1_forwards_fewer_requests(self):
        _, small, _ = self._run("--cache-objects", "2", "--quiet")
        _, large, _ = self._run("--cache-objects", "50", "--quiet")
        self.assertGreater(len(small.splitlines()), len(large.splitlines()))


if __name__ == "__main__":
    unittest.main()
