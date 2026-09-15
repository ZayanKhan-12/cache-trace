#!/usr/bin/env python3
"""Measure the locality of a trace via its reuse-distance distribution.

Two distances are reported for every re-access of an object:

``reuse distance`` (also called stack distance)
    the number of *distinct* objects referenced since the previous access to
    this object. This is the quantity that determines the miss ratio of an LRU
    cache: a request hits in an LRU cache of N objects exactly when its reuse
    distance is below N. "Low locality" means this distribution is shifted
    towards large values.

``reference distance``
    the number of requests since the previous access to this object, i.e. how
    far apart the two accesses are in wall-clock request order.

Use this to confirm that ``l1_filter.py`` did what you expect: the L2 trace it
produces should have a visibly larger reuse distance than its input.

Example
-------
::

    ./scripts/reuse_distance.py samples/2020Mar/cluster001 --max-requests 200000
"""

from __future__ import annotations

import argparse
import json
import sys
from array import array
from typing import Dict, Iterator, List, Optional, Tuple

try:  # when run as a script
    from trace_io import Request, open_trace, read_trace
except ImportError:  # when imported as part of a package
    from .trace_io import Request, open_trace, read_trace

DEFAULT_PERCENTILES = (10.0, 25.0, 50.0, 75.0, 90.0, 99.0, 99.9)


class Fenwick:
    """Binary indexed tree over 0/1 markers, one per request position.

    The tree grows by doubling. Because a Fenwick tree's internal nodes cover
    ranges that depend on its size, growth rebuilds the tree from the raw
    markers in linear time, which keeps insertion amortised O(log n).
    """

    __slots__ = ("_tree", "_raw", "_size")

    def __init__(self, size: int = 1024) -> None:
        self._size = max(1, size)
        self._tree = array("q", [0]) * (self._size + 1)
        self._raw = array("b", [0]) * self._size

    def __len__(self) -> int:
        return self._size

    def _grow(self, needed: int) -> None:
        new_size = self._size
        while new_size < needed:
            new_size *= 2
        raw = self._raw
        raw.extend(array("b", [0]) * (new_size - self._size))
        tree = array("q", [0]) * (new_size + 1)
        for i in range(new_size):
            tree[i + 1] = raw[i]
        for i in range(1, new_size + 1):
            parent = i + (i & -i)
            if parent <= new_size:
                tree[parent] += tree[i]
        self._tree = tree
        self._raw = raw
        self._size = new_size

    def add(self, index: int, delta: int) -> None:
        """Add *delta* to position *index* (0-based)."""
        if index >= self._size:
            self._grow(index + 1)
        self._raw[index] += delta
        i = index + 1
        tree = self._tree
        while i <= self._size:
            tree[i] += delta
            i += i & -i

    def prefix_sum(self, index: int) -> int:
        """Sum of positions ``0..index`` inclusive (0-based)."""
        if index < 0:
            return 0
        if index >= self._size:
            index = self._size - 1
        i = index + 1
        total = 0
        tree = self._tree
        while i > 0:
            total += tree[i]
            i -= i & -i
        return total


class DistanceStats:
    """Accumulates reuse- and reference-distance samples."""

    def __init__(self, keep_samples: bool = True) -> None:
        self.keep_samples = keep_samples
        self.reuse = array("q")
        self.reference = array("q")
        self.count = 0
        self.reuse_total = 0
        self.reference_total = 0
        self.reuse_max = 0
        self.reference_max = 0
        # log2 histogram: bucket i holds distances in [2^(i-1), 2^i - 1],
        # bucket 0 holds distance 0.
        self.reuse_histogram: Dict[int, int] = {}
        self.reference_histogram: Dict[int, int] = {}

    @staticmethod
    def bucket_of(distance: int) -> int:
        return 0 if distance <= 0 else distance.bit_length()

    @staticmethod
    def bucket_bounds(bucket: int) -> Tuple[int, int]:
        if bucket == 0:
            return (0, 0)
        return (1 << (bucket - 1), (1 << bucket) - 1)

    def record(self, reuse: int, reference: int) -> None:
        self.count += 1
        self.reuse_total += reuse
        self.reference_total += reference
        if reuse > self.reuse_max:
            self.reuse_max = reuse
        if reference > self.reference_max:
            self.reference_max = reference
        b = self.bucket_of(reuse)
        self.reuse_histogram[b] = self.reuse_histogram.get(b, 0) + 1
        b = self.bucket_of(reference)
        self.reference_histogram[b] = self.reference_histogram.get(b, 0) + 1
        if self.keep_samples:
            self.reuse.append(reuse)
            self.reference.append(reference)

    @property
    def reuse_mean(self) -> float:
        return self.reuse_total / self.count if self.count else 0.0

    @property
    def reference_mean(self) -> float:
        return self.reference_total / self.count if self.count else 0.0

    def percentiles(
        self, which: str, percentiles: Tuple[float, ...] = DEFAULT_PERCENTILES
    ) -> Dict[str, int]:
        samples = self.reuse if which == "reuse" else self.reference
        if not self.keep_samples:
            return self._percentiles_from_histogram(which, percentiles)
        if not samples:
            return {f"p{p:g}": 0 for p in percentiles}
        ordered = sorted(samples)
        result = {}
        for p in percentiles:
            # Nearest-rank: the smallest value at or above the p-th percentile.
            rank = max(1, min(len(ordered), int(-(-p * len(ordered) // 100))))
            result[f"p{p:g}"] = ordered[rank - 1]
        return result

    def _percentiles_from_histogram(
        self, which: str, percentiles: Tuple[float, ...]
    ) -> Dict[str, int]:
        histogram = (
            self.reuse_histogram if which == "reuse" else self.reference_histogram
        )
        result = {}
        for p in percentiles:
            target = p * self.count / 100.0
            seen = 0
            value = 0
            for bucket in sorted(histogram):
                seen += histogram[bucket]
                if seen >= target:
                    value = self.bucket_bounds(bucket)[1]
                    break
            result[f"p{p:g}"] = value
        return result

    def histogram_rows(self, which: str) -> List[Dict[str, object]]:
        histogram = (
            self.reuse_histogram if which == "reuse" else self.reference_histogram
        )
        rows: List[Dict[str, object]] = []
        cumulative = 0
        for bucket in sorted(histogram):
            count = histogram[bucket]
            cumulative += count
            low, high = self.bucket_bounds(bucket)
            rows.append(
                {
                    "bucket_low": low,
                    "bucket_high": high,
                    "count": count,
                    "fraction": count / self.count if self.count else 0.0,
                    "cumulative_fraction": cumulative / self.count
                    if self.count
                    else 0.0,
                }
            )
        return rows


def analyze(
    requests: Iterator[Request],
    keep_samples: bool = True,
    reads_only: bool = False,
) -> Tuple[DistanceStats, Dict[str, int]]:
    """Compute reuse and reference distances over *requests*.

    Returns the distance statistics plus overall trace counters. The first
    access to an object has no predecessor and is counted as a cold miss
    rather than contributing a distance sample.
    """
    stats = DistanceStats(keep_samples=keep_samples)
    tree = Fenwick()
    last_position: Dict[str, int] = {}
    frequency: Dict[str, int] = {}

    position = 0
    total = 0
    for request in requests:
        total += 1
        if reads_only and not request.is_read:
            continue
        key = request.key
        frequency[key] = frequency.get(key, 0) + 1
        previous = last_position.get(key)
        if previous is not None:
            # Every distinct key contributes exactly one marker, placed at its
            # most recent access. Markers strictly after `previous` therefore
            # count the distinct objects touched since then.
            distinct_after = len(last_position) - tree.prefix_sum(previous)
            stats.record(distinct_after, position - previous - 1)
            tree.add(previous, -1)
        tree.add(position, 1)
        last_position[key] = position
        position += 1

    one_hit_wonders = sum(1 for count in frequency.values() if count == 1)
    counters = {
        "requests_read": total,
        "requests_analyzed": position,
        "distinct_objects": len(last_position),
        "cold_misses": len(last_position),
        "one_hit_wonders": one_hit_wonders,
        "reuses": stats.count,
    }
    return stats, counters


def build_report(stats: DistanceStats, counters: Dict[str, int]) -> Dict[str, object]:
    analyzed = counters["requests_analyzed"]
    distinct = counters["distinct_objects"]
    return {
        **counters,
        "one_hit_wonder_ratio": round(
            counters["one_hit_wonders"] / distinct if distinct else 0.0, 6
        ),
        "cold_miss_ratio": round(distinct / analyzed if analyzed else 0.0, 6),
        "reuse_distance": {
            "mean": round(stats.reuse_mean, 3),
            "max": stats.reuse_max,
            **stats.percentiles("reuse"),
        },
        "reference_distance": {
            "mean": round(stats.reference_mean, 3),
            "max": stats.reference_max,
            **stats.percentiles("reference"),
        },
    }


def render_text(report: Dict[str, object], stats: DistanceStats, metric: str) -> str:
    lines = [
        f"requests analyzed : {report['requests_analyzed']:,}",
        f"distinct objects  : {report['distinct_objects']:,}",
        f"reuses            : {report['reuses']:,}",
        f"cold miss ratio   : {report['cold_miss_ratio']:.4f}",
        f"one-hit wonders   : {report['one_hit_wonders']:,} "
        f"({report['one_hit_wonder_ratio']:.4f} of distinct objects)",
        "",
    ]
    for name in ("reuse_distance", "reference_distance"):
        summary = report[name]
        assert isinstance(summary, dict)
        lines.append(f"{name.replace('_', ' ')}:")
        ordered = ["mean"] + [k for k in summary if k.startswith("p")] + ["max"]
        lines.append(
            "  " + "  ".join(f"{k}={summary[k]:,}" for k in ordered if k in summary)
        )
        lines.append("")

    rows = stats.histogram_rows(metric)
    if rows:
        lines.append(f"{metric} distance histogram (log2 buckets):")
        lines.append(f"  {'range':>20}  {'count':>12}  {'frac':>8}  {'cdf':>8}")
        for row in rows:
            label = (
                f"[{row['bucket_low']:,}, {row['bucket_high']:,}]"
                if row["bucket_low"] != row["bucket_high"]
                else f"{row['bucket_low']:,}"
            )
            lines.append(
                f"  {label:>20}  {row['count']:>12,}  "
                f"{row['fraction']:>8.4f}  {row['cumulative_fraction']:>8.4f}"
            )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reuse_distance.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("trace", help="input trace ('-' for stdin; .zst/.gz/.bz2/.xz)")
    parser.add_argument(
        "--max-requests",
        type=int,
        default=None,
        help="analyze only the first N requests (recommended on full traces)",
    )
    parser.add_argument(
        "--reads-only",
        action="store_true",
        help="consider only get/gets requests",
    )
    parser.add_argument(
        "--metric",
        choices=("reuse", "reference"),
        default="reuse",
        help="which distance to show the histogram for (default: reuse)",
    )
    parser.add_argument(
        "--approximate",
        action="store_true",
        help="derive percentiles from the histogram instead of keeping every "
        "sample; uses constant memory at bucket resolution",
    )
    parser.add_argument("--json", metavar="PATH", help="write the report as JSON")
    parser.add_argument(
        "--histogram-csv", metavar="PATH", help="write the histogram as CSV"
    )
    parser.add_argument(
        "--skip-malformed", action="store_true", help="skip unparseable lines"
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    with open_trace(args.trace) as handle:
        requests = read_trace(
            handle,
            skip_malformed=args.skip_malformed,
            max_requests=args.max_requests,
        )
        stats, counters = analyze(
            requests,
            keep_samples=not args.approximate,
            reads_only=args.reads_only,
        )

    report = build_report(stats, counters)
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(report, fh, indent=2, sort_keys=True)
            fh.write("\n")
    if args.histogram_csv:
        with open(args.histogram_csv, "w") as fh:
            fh.write("bucket_low,bucket_high,count,fraction,cumulative_fraction\n")
            for row in stats.histogram_rows(args.metric):
                fh.write(
                    "{bucket_low},{bucket_high},{count},"
                    "{fraction:.8f},{cumulative_fraction:.8f}\n".format(**row)
                )
    print(render_text(report, stats, args.metric))
    return 0


if __name__ == "__main__":
    sys.exit(main())
