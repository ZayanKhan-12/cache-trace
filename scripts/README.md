## Trace tooling

Two small, dependency-free tools for working with the traces in this
repository. They need nothing but a Python 3.9+ interpreter — no PyMimircache,
no libCacheSim, no `pip install`.

| Script | Purpose |
| --- | --- |
| [`l1_filter.py`](l1_filter.py) | Replay a trace through an L1 cache and emit the **miss stream** as a new trace (an "L2 trace"). |
| [`reuse_distance.py`](reuse_distance.py) | Report the **reuse-distance distribution** of a trace, i.e. how much locality it has. |
| [`trace_io.py`](trace_io.py) | Shared parsing/formatting helpers, importable from your own scripts. |

Both tools read plain traces as well as `.zst`, `.gz`, `.bz2` and `.xz`, and
accept `-` for stdin. Output uses the same seven-column format as the input,
so the tools compose with each other and with anything else that consumes
these traces.

---

### Why filter a trace through an L1 cache?

A trace recorded in front of a cache is dominated by short-reuse-distance
traffic: a small set of hot keys accounts for most requests. If you want to
study caching under **low locality** — large reuse distances, high one-hit-wonder
ratios — that traffic is exactly what you need to remove.

Sending the trace through a small L1 cache does this for you. The L1 absorbs
the hot, closely-spaced re-references; what falls out the other side is the
miss stream that a second-tier cache would actually see, and it has markedly
lower locality than the original.

---

### Quick start

Measure the locality of a trace as it ships:

```sh
./scripts/reuse_distance.py samples/2020Mar/cluster001 --max-requests 200000
```

Filter it through a 1 MB LRU L1 to produce an L2 trace:

```sh
./scripts/l1_filter.py samples/2020Mar/cluster001 \
    --max-requests 200000 --policy lru --cache-size 1MB \
    -o cluster001.l2
```

Measure the result:

```sh
./scripts/reuse_distance.py cluster001.l2
```

On the bundled `cluster001` sample (first 200,000 requests, 1 MB LRU L1, which
absorbs 95.8% of reads and leaves 9,522 of the 200,000 requests) that gives:

| Metric | Original trace | L2 trace |
| --- | ---: | ---: |
| mean reuse distance | 433 | **1,866** |
| median (p50) reuse distance | 203 | **1,973** |
| p90 reuse distance | 1,134 | **3,567** |
| cold-miss ratio | 0.027 | **0.564** |
| one-hit-wonder ratio | 0.178 | **0.497** |

The reuse distance grows by roughly 4x at the mean and 10x at the median,
and half of the objects in the L2 trace are now never reused at all.

---

### Choosing the L1 size

**This is the setting that matters.** The L1 must be meaningfully *smaller*
than the trace's working set. If it is larger it absorbs every re-reference,
and the miss stream collapses towards one cold miss per object — a trace with
almost no reuse left to study.

Sweeping the L1 size over the first 200,000 requests of `cluster001` (working
set 1.92 MB, LRU, default write policy) shows the effect:

| L1 size, as % of working set | requests out | reuses left | mean reuse distance |
| ---: | ---: | ---: | ---: |
| 1% | 149,524 | 144,151 | 577 |
| 5% | 97,911 | 92,538 | 836 |
| 10% | 64,021 | 58,648 | 1,120 |
| 50% | 9,888 | 4,515 | **1,886** |
| 100% | 6,501 | 1,128 | 399 |
| 400% | 6,501 | 1,128 | 399 |

Locality keeps dropping as the L1 grows until the L1 approaches the working
set; past that point the only re-references still reaching L2 are write
traffic, and the distribution collapses. (With `--write-policy drop` and an L1
larger than the working set, exactly zero reuses survive.)

A good rule of thumb: sweep a few sizes between 10% and 50% of the working
set and watch the `reuses` count reported by `reuse_distance.py`. If it falls
close to zero, the L1 is too big to be a useful filter. Per-cluster working-set
sizes are listed in [`../stat/2020Mar.md`](../stat/2020Mar.md).

---

### `l1_filter.py`

```
./scripts/l1_filter.py TRACE (--cache-size SIZE | --cache-objects N) [options]
```

| Option | Meaning |
| --- | --- |
| `--cache-size SIZE` | L1 capacity in bytes; accepts `4MB`, `1GiB`, `1048576`. An object costs `key size + value size`. |
| `--cache-objects N` | L1 capacity as a fixed number of objects instead. |
| `--policy {lru,fifo,clock,random}` | Eviction policy (default `lru`). |
| `--write-policy {through,around,drop}` | How writes are handled (default `through`). |
| `--ttl` | Honour the TTL column and expire objects using trace timestamps. |
| `--max-requests N` | Only process the first N requests. |
| `-o PATH` | Output trace (default stdout). |
| `--stats-json PATH` | Write run statistics as JSON. |
| `--quiet` | Suppress the statistics normally printed to stderr. |

Statistics go to **stderr** and the trace to **stdout**, so piping is safe:

```sh
./scripts/l1_filter.py trace.zst --cache-size 4MB | ./scripts/reuse_distance.py -
```

#### Write policies

The traces contain writes as well as reads, and how you propagate them changes
the shape of the L2 trace:

* `through` (default) — writes update L1 *and* are forwarded. This models a
  write-through L1 and keeps the L2 trace's writes intact. Note that hot keys
  rewritten frequently still appear at short distances in the output.
* `around` — writes invalidate L1 and are forwarded, modelling a write-around L1.
* `drop` — writes update L1 but are not forwarded, leaving a **pure read-miss
  stream**. Use this when you want the lowest-locality trace possible and do not
  need the write traffic.

Deletes always invalidate the L1 entry and are forwarded, so the L2 trace stays
consistent with the L1's view of the keyspace.

---

### `reuse_distance.py`

```
./scripts/reuse_distance.py TRACE [options]
```

Reports two distances for every re-access of an object:

* **reuse distance** (stack distance) — the number of *distinct* objects seen
  since the previous access to this object. A request hits in an LRU cache of
  N objects exactly when its reuse distance is below N, which is what makes
  this the standard measure of locality.
* **reference distance** — the number of requests since the previous access.

Useful options: `--max-requests N` (recommended on full traces), `--reads-only`,
`--metric {reuse,reference}` to pick which histogram to print, `--json PATH`,
`--histogram-csv PATH`, and `--approximate` to derive percentiles from the
histogram in constant memory instead of keeping every sample.

Distances are computed exactly, in O(n log n), with a Fenwick tree. Throughput
is roughly 300k requests/second; memory is about 8 bytes per request unless you
pass `--approximate`, plus one entry per distinct key. For multi-billion-request
traces, use `--max-requests` to analyze a prefix.

---

### Using the modules from your own code

```python
import sys
sys.path.insert(0, "scripts")

from l1_filter import L1Cache, filter_requests
from reuse_distance import analyze
from trace_io import open_trace, read_trace

with open_trace("samples/2020Mar/cluster001") as handle:
    requests = list(read_trace(handle, max_requests=100_000))

cache = L1Cache(policy="lru", capacity_bytes=1_000_000)
l2_trace = list(filter_requests(iter(requests), cache))

stats, counters = analyze(iter(l2_trace))
print(stats.reuse_mean, counters["reuses"])
```

---

### Tests

The tools are covered by a dependency-free test suite, including end-to-end
tests that run against the real samples in this repository:

```sh
python -m unittest discover -s tests -t tests -v
```
