#!/usr/bin/env python3
"""Derive an L2 (low-locality) trace by filtering a trace through an L1 cache.

A request trace collected in front of a cache contains a lot of short-reuse-
distance traffic. Replaying it through a small L1 cache absorbs exactly that
traffic: what comes out the other side is the *miss stream*, which is what an
L2 cache behind the L1 would actually see. The miss stream has a markedly
larger reuse distance than the original trace, which makes it a good workload
for studying caching under low locality.

The output uses the same seven-column format as the input, so it can be fed
straight back into any tool that consumes these traces -- including
``reuse_distance.py`` in this directory, which will show the locality drop.

Examples
--------
Filter a sample through a 4 MB LRU L1 and write the resulting L2 trace::

    ./scripts/l1_filter.py samples/2020Mar/cluster001 \\
        --policy lru --cache-size 4MB -o cluster001.l2

Use an object-counting FIFO L1 and keep only the read-miss stream::

    ./scripts/l1_filter.py samples/2020Mar/cluster052 \\
        --policy fifo --cache-objects 10000 --write-policy drop
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import OrderedDict, deque
from typing import Deque, Dict, Iterator, List, Optional, Tuple

try:  # when run as a script
    from trace_io import Request, format_request, open_trace, read_trace
except ImportError:  # when imported as part of a package
    from .trace_io import Request, format_request, open_trace, read_trace

POLICIES = ("lru", "fifo", "clock", "random")
WRITE_POLICIES = ("through", "around", "drop")

_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?i?b?)\s*$", re.IGNORECASE)
_SIZE_MULTIPLIERS = {
    "": 1,
    "b": 1,
    "k": 1000,
    "kb": 1000,
    "ki": 1024,
    "kib": 1024,
    "m": 1000**2,
    "mb": 1000**2,
    "mi": 1024**2,
    "mib": 1024**2,
    "g": 1000**3,
    "gb": 1000**3,
    "gi": 1024**3,
    "gib": 1024**3,
    "t": 1000**4,
    "tb": 1000**4,
    "ti": 1024**4,
    "tib": 1024**4,
}


def parse_size(text: str) -> int:
    """Parse a human-readable byte size such as ``512``, ``4MB`` or ``1GiB``."""
    match = _SIZE_RE.match(text)
    if match is None:
        raise argparse.ArgumentTypeError(f"invalid size: {text!r}")
    number, suffix = match.groups()
    multiplier = _SIZE_MULTIPLIERS.get(suffix.lower())
    if multiplier is None:
        raise argparse.ArgumentTypeError(f"unknown size suffix in {text!r}")
    value = int(float(number) * multiplier)
    if value <= 0:
        raise argparse.ArgumentTypeError(f"size must be positive: {text!r}")
    return value


class L1Cache:
    """A single-tier cache used purely as a trace filter.

    Capacity is expressed either in bytes (``capacity_bytes``, where an object
    costs ``key size + value size``) or in objects (``capacity_objects``).
    Exactly one of the two must be given.
    """

    def __init__(
        self,
        policy: str = "lru",
        capacity_bytes: Optional[int] = None,
        capacity_objects: Optional[int] = None,
        ttl_aware: bool = False,
        seed: int = 0,
    ) -> None:
        if policy not in POLICIES:
            raise ValueError(f"unknown policy {policy!r}; expected one of {POLICIES}")
        if (capacity_bytes is None) == (capacity_objects is None):
            raise ValueError("specify exactly one of capacity_bytes, capacity_objects")

        self.policy = policy
        self.capacity_bytes = capacity_bytes
        self.capacity_objects = capacity_objects
        self.ttl_aware = ttl_aware
        self._rng = random.Random(seed)

        # key -> (size in bytes, absolute expiry time or None)
        self._entries: OrderedDict[str, Tuple[int, Optional[int]]] = OrderedDict()
        self.used_bytes = 0

        # CLOCK bookkeeping
        self._ring: Deque[str] = deque()
        self._ref: Dict[str, bool] = {}
        self._in_ring: Dict[str, bool] = {}

        # RANDOM bookkeeping: a dense key list plus its reverse index
        self._keys: List[str] = []
        self._key_index: Dict[str, int] = {}

        self.evictions = 0
        self.expirations = 0
        self.rejected_too_large = 0
        self.peak_bytes = 0

    # ------------------------------------------------------------------ size

    def __len__(self) -> int:
        return len(self._entries)

    def _is_full(self) -> bool:
        if self.capacity_bytes is not None:
            return self.used_bytes > self.capacity_bytes
        assert self.capacity_objects is not None
        return len(self._entries) > self.capacity_objects

    def _fits(self, size: int) -> bool:
        if self.capacity_bytes is not None:
            return size <= self.capacity_bytes
        return True

    # ---------------------------------------------------------------- policy

    def _track_insert(self, key: str) -> None:
        if self.policy == "clock":
            self._ref[key] = False
            if not self._in_ring.get(key):
                self._ring.append(key)
                self._in_ring[key] = True
        elif self.policy == "random":
            if key not in self._key_index:
                self._key_index[key] = len(self._keys)
                self._keys.append(key)

    def _track_hit(self, key: str) -> None:
        if self.policy == "lru":
            self._entries.move_to_end(key)
        elif self.policy == "clock":
            self._ref[key] = True

    def _track_remove(self, key: str) -> None:
        if self.policy == "clock":
            self._ref.pop(key, None)
            # The ring slot is reclaimed lazily by _select_victim.
        elif self.policy == "random":
            index = self._key_index.pop(key, None)
            if index is not None:
                last = self._keys.pop()
                if last != key:
                    self._keys[index] = last
                    self._key_index[last] = index

    def _select_victim(self) -> str:
        if self.policy in ("lru", "fifo"):
            # OrderedDict is in insertion order; LRU additionally moves an entry
            # to the end on every hit, so the head is the right victim for both.
            return next(iter(self._entries))
        if self.policy == "random":
            return self._keys[self._rng.randrange(len(self._keys))]

        # CLOCK: sweep the ring, clearing reference bits until one is already 0.
        while self._ring:
            key = self._ring[0]
            if key not in self._entries:  # stale slot left by an invalidation
                self._ring.popleft()
                self._in_ring.pop(key, None)
                continue
            if self._ref.get(key, False):
                self._ref[key] = False
                self._ring.rotate(-1)
                continue
            self._ring.popleft()
            self._in_ring.pop(key, None)
            return key
        # Ring desynchronised (should not happen); fall back to insertion order.
        return next(iter(self._entries))

    # ------------------------------------------------------------------- api

    def _remove(self, key: str) -> None:
        size, _ = self._entries.pop(key)
        self.used_bytes -= size
        self._track_remove(key)

    def contains(self, key: str, now: int) -> bool:
        """Return whether *key* is resident and unexpired, recording a hit."""
        entry = self._entries.get(key)
        if entry is None:
            return False
        _, expire_at = entry
        if self.ttl_aware and expire_at is not None and now >= expire_at:
            self._remove(key)
            self.expirations += 1
            return False
        self._track_hit(key)
        return True

    def admit(self, key: str, size: int, now: int, ttl: int = 0) -> None:
        """Insert or update *key*, evicting as needed to stay within capacity."""
        if not self._fits(size):
            # An object larger than the whole cache can never be held.
            self.rejected_too_large += 1
            if key in self._entries:
                self._remove(key)
            return

        expire_at = now + ttl if (self.ttl_aware and ttl > 0) else None
        if key in self._entries:
            old_size, _ = self._entries[key]
            self.used_bytes += size - old_size
            self._entries[key] = (size, expire_at)
            if self.policy == "lru":
                self._entries.move_to_end(key)
            if self.policy == "clock":
                self._ref[key] = False
        else:
            self._entries[key] = (size, expire_at)
            self.used_bytes += size
            self._track_insert(key)

        while self._entries and self._is_full():
            victim = self._select_victim()
            self._remove(victim)
            self.evictions += 1

        self.peak_bytes = max(self.peak_bytes, self.used_bytes)

    def invalidate(self, key: str) -> bool:
        """Drop *key* if present. Returns whether anything was removed."""
        if key in self._entries:
            self._remove(key)
            return True
        return False


class FilterStats:
    """Counters describing one filtering run."""

    def __init__(self) -> None:
        self.requests = 0
        self.reads = 0
        self.writes = 0
        self.deletes = 0
        self.other = 0
        self.read_hits = 0
        self.read_misses = 0
        self.emitted = 0
        self.malformed = 0

    @property
    def read_hit_ratio(self) -> float:
        return self.read_hits / self.reads if self.reads else 0.0

    @property
    def read_miss_ratio(self) -> float:
        return self.read_misses / self.reads if self.reads else 0.0

    def to_dict(self, cache: L1Cache) -> Dict[str, object]:
        return {
            "policy": cache.policy,
            "capacity_bytes": cache.capacity_bytes,
            "capacity_objects": cache.capacity_objects,
            "ttl_aware": cache.ttl_aware,
            "requests": self.requests,
            "reads": self.reads,
            "writes": self.writes,
            "deletes": self.deletes,
            "other_ops": self.other,
            "read_hits": self.read_hits,
            "read_misses": self.read_misses,
            "read_hit_ratio": round(self.read_hit_ratio, 6),
            "read_miss_ratio": round(self.read_miss_ratio, 6),
            "emitted_requests": self.emitted,
            "compression_ratio": round(self.emitted / self.requests, 6)
            if self.requests
            else 0.0,
            "evictions": cache.evictions,
            "expirations": cache.expirations,
            "rejected_too_large": cache.rejected_too_large,
            "objects_resident_at_end": len(cache),
            "peak_bytes": cache.peak_bytes,
            "malformed_lines": self.malformed,
        }


def filter_requests(
    requests: Iterator[Request],
    cache: L1Cache,
    write_policy: str = "through",
    stats: Optional[FilterStats] = None,
) -> Iterator[Request]:
    """Yield the L1 miss stream for *requests*.

    Read hits are absorbed. Read misses are forwarded and admitted into L1.
    Writes follow *write_policy*:

    ``through``
        update L1 and forward the write (default; models a write-through L1).
    ``around``
        invalidate L1 and forward the write (models a write-around L1).
    ``drop``
        update L1 but do not forward, leaving a pure read-miss stream.

    Deletes always invalidate L1 and are forwarded, so the L2 trace stays
    consistent with the L1's view of the keyspace.
    """
    if write_policy not in WRITE_POLICIES:
        raise ValueError(
            f"unknown write policy {write_policy!r}; expected one of {WRITE_POLICIES}"
        )
    if stats is None:
        stats = FilterStats()

    # The TTL column is 0 on reads, so remember the TTL each key was last
    # written with in order to expire read-admitted objects correctly.
    last_ttl: Dict[str, int] = {}

    for request in requests:
        stats.requests += 1
        key = request.key

        if request.is_read:
            stats.reads += 1
            if cache.contains(key, request.timestamp):
                stats.read_hits += 1
                continue
            stats.read_misses += 1
            cache.admit(
                key,
                request.object_size,
                request.timestamp,
                last_ttl.get(key, 0) if cache.ttl_aware else 0,
            )
            stats.emitted += 1
            yield request
        elif request.is_write:
            stats.writes += 1
            if cache.ttl_aware and request.ttl > 0:
                last_ttl[key] = request.ttl
            if write_policy == "around":
                cache.invalidate(key)
            else:
                cache.admit(key, request.object_size, request.timestamp, request.ttl)
            if write_policy == "drop":
                continue
            stats.emitted += 1
            yield request
        elif request.is_delete:
            stats.deletes += 1
            cache.invalidate(key)
            last_ttl.pop(key, None)
            stats.emitted += 1
            yield request
        else:
            # Unrecognised operation: pass it through untouched rather than
            # silently changing the shape of the workload.
            stats.other += 1
            stats.emitted += 1
            yield request


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="l1_filter.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("trace", help="input trace ('-' for stdin; .zst/.gz/.bz2/.xz)")
    capacity = parser.add_mutually_exclusive_group(required=True)
    capacity.add_argument(
        "--cache-size",
        type=parse_size,
        metavar="SIZE",
        help="L1 capacity in bytes, e.g. 4MB, 1GiB, 1048576",
    )
    capacity.add_argument(
        "--cache-objects",
        type=int,
        metavar="N",
        help="L1 capacity as a number of objects",
    )
    parser.add_argument(
        "--policy", choices=POLICIES, default="lru", help="L1 eviction policy"
    )
    parser.add_argument(
        "--write-policy",
        choices=WRITE_POLICIES,
        default="through",
        help="how writes are handled (default: through)",
    )
    parser.add_argument(
        "--ttl",
        action="store_true",
        help="honour the TTL column and expire objects (costs extra memory)",
    )
    parser.add_argument(
        "-o", "--output", default="-", help="output trace path ('-' for stdout)"
    )
    parser.add_argument(
        "--max-requests", type=int, default=None, help="stop after N input requests"
    )
    parser.add_argument(
        "--seed", type=int, default=0, help="RNG seed for --policy random"
    )
    parser.add_argument(
        "--skip-malformed", action="store_true", help="skip unparseable lines"
    )
    parser.add_argument(
        "--stats-json", metavar="PATH", help="also write run statistics as JSON"
    )
    parser.add_argument(
        "--quiet", action="store_true", help="do not print statistics to stderr"
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if args.cache_objects is not None and args.cache_objects <= 0:
        build_parser().error("--cache-objects must be positive")

    cache = L1Cache(
        policy=args.policy,
        capacity_bytes=args.cache_size,
        capacity_objects=args.cache_objects,
        ttl_aware=args.ttl,
        seed=args.seed,
    )
    stats = FilterStats()
    errors: List[str] = []

    # Closed in the finally block below; a with-statement would need to span
    # the whole streaming loop without covering the stdout case.
    out = sys.stdout if args.output == "-" else open(args.output, "w")  # noqa: SIM115
    try:
        with open_trace(args.trace) as handle:
            requests = read_trace(
                handle,
                skip_malformed=args.skip_malformed,
                max_requests=args.max_requests,
                errors=errors,
            )
            for request in filter_requests(
                requests, cache, write_policy=args.write_policy, stats=stats
            ):
                out.write(format_request(request))
                out.write("\n")
    except BrokenPipeError:  # e.g. piped into `head`
        return 0
    finally:
        if out is not sys.stdout:
            out.close()

    stats.malformed = len(errors)
    summary = stats.to_dict(cache)
    if args.stats_json:
        with open(args.stats_json, "w") as fh:
            json.dump(summary, fh, indent=2, sort_keys=True)
            fh.write("\n")
    if not args.quiet:
        print(json.dumps(summary, indent=2, sort_keys=True), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
