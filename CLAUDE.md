# CLAUDE.md

Guidance for Claude Code (and other AI coding agents) working in this repository.

## What this repository is

`cache-trace` publishes **anonymized cache request traces from Twitter production**
(54 Twemcache/Pelikan clusters, one week, March 2020). It is primarily a **data and
documentation repository**, not an application. The full traces (2.8 TB compressed)
live on external hosts; this repo holds the documentation, per-cluster statistics,
small samples, and the tooling for working with them.

The traces back the OSDI '20 paper *"A large scale analysis of hundreds of in-memory
cache clusters at Twitter"*, and are widely cited. Treat the documentation as
research-facing: accuracy matters more than volume.

## Layout

```
README.md              Primary documentation: format, download mirrors, per-cluster advice
samples/2020Mar/       One-million-request plain-text samples, clusterNNN, real data
stat/2020Mar.md        Per-cluster computed statistics (working set size, Zipf alpha, ...)
scripts/               Dependency-free Python tooling (see scripts/README.md)
tests/                 unittest suite covering scripts/, including end-to-end sample tests
bibliography.bib       Papers that use or analyze the traces
storj, storj_wget.sh   Download helpers for the (now-retired) Storj mirror
```

## Trace format

Plain text, comma-separated, one request per line, seven columns:

```
timestamp,anonymized key,key size,value size,client id,operation,TTL
0,z44uy84y444444zkuMdF44i444444svfX484u84444CF44Cgv_-CyLli48d4y84444wIVo44,72,455,1,get,0
```

* `timestamp` is in seconds; `key size` and `value size` are bytes.
* `operation` is one of get/gets/set/add/replace/cas/append/prepend/delete/incr/decr.
* `TTL` is 0 for any request that is not a write.
* Keys never contain commas, so splitting on `,` is safe. Keys **do** contain
  `:`, `=`, `-` and `_`; do not assume a fixed number of namespace fields.
* Full traces are zstd-compressed and split into 1,000,000,000-line files named
  `clusterN.M.zst`.

Always use `scripts/trace_io.py` rather than re-implementing parsing.

## Conventions

* **No third-party dependencies.** The tooling must run on a bare Python 3.9+
  interpreter. Researchers clone this repo on shared clusters where they cannot
  `pip install`. If you need zstd, degrade gracefully (see `trace_io._open_zstd`).
* **Python 3.9 compatibility.** Use `from __future__ import annotations` and
  `typing.Dict`/`List`/`Tuple` spellings. CI tests 3.9 through 3.13.
* **Streaming over buffering.** Traces run to billions of lines. Prefer
  generators and `__slots__`; never load a full trace into memory in library code.
  Offer `--max-requests` on anything that must accumulate.
* **stdout is data, stderr is commentary.** Tools write traces to stdout and
  statistics to stderr so they can be piped.
* **Tools compose.** Anything that emits a trace emits the same seven columns,
  so it can be fed back into the other tools.

## Working here

Run the tests (no dependencies needed):

```sh
python -m unittest discover -s tests -t tests -v
```

Lint and format (pinned in CI, `pip install ruff==0.6.9`):

```sh
ruff check scripts tests
ruff format --check scripts tests
```

CI (`.github/workflows/ci.yml`) runs lint, the test matrix, and an end-to-end
smoke test that asserts filtering a real sample actually lowers its locality.

## Things to be careful about

* **Do not commit trace data.** The samples already in `samples/` are ~150 MB of
  real production data and are intentionally the only traces in the repo. Never add
  generated traces, filtered output, or downloaded trace files. Write them to a
  temporary directory instead.
* **Do not edit `samples/` or `stat/`.** They are published data; changing them
  would invalidate results others have already published against them.
* **Do not "fix" the anonymized keys.** They look like noise because they are
  anonymized. That is correct.
* **Licensing.** Data and documentation are CC-BY 4.0 (see `LICENSE`).
* **Claims in docs must be measured.** The README quotes specific numbers
  (hit ratios, reuse distances). If you change tooling behaviour, re-run the
  commands and update the numbers rather than estimating them.

## Domain notes

* **Reuse distance** (stack distance) is the number of *distinct* objects
  referenced between two accesses to the same object. A request hits in an LRU
  cache of N objects exactly when its reuse distance is below N. "Low locality"
  means this distribution is shifted toward large values.
* **L1 filtering** produces a low-locality trace: replay a trace through a small
  cache and keep the miss stream. The L1 must be *smaller* than the working set,
  otherwise it absorbs every re-reference and the output degenerates into one
  cold miss per object, with no reuse left to study. See `scripts/README.md`.
* Mean reuse distance is not monotonic in L1 size, so it is not a safe invariant
  to assert on its own. Cold-miss ratio and one-hit-wonder ratio are.
