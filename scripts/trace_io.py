"""Reading and writing Twitter production cache traces.

The traces are plain-text, comma-separated, one request per line::

    timestamp,anonymized key,key size,value size,client id,operation,TTL

See the repository README for the full description of each column. This module
is intentionally dependency-free so that it runs anywhere Python 3.8+ does.
"""

from __future__ import annotations

import bz2
import gzip
import lzma
import shutil
import subprocess
import sys
from contextlib import contextmanager
from typing import IO, Iterable, Iterator, List, Optional

__all__ = [
    "FIELD_NAMES",
    "READ_OPS",
    "WRITE_OPS",
    "DELETE_OPS",
    "Request",
    "TraceFormatError",
    "format_request",
    "open_trace",
    "parse_line",
    "read_trace",
]

FIELD_NAMES = (
    "timestamp",
    "key",
    "key_size",
    "value_size",
    "client_id",
    "operation",
    "ttl",
)

#: Operations that read an object and can therefore be absorbed by an L1 cache.
READ_OPS = frozenset({"get", "gets"})

#: Operations that create or modify an object's value.
WRITE_OPS = frozenset(
    {"set", "add", "replace", "cas", "append", "prepend", "incr", "decr"}
)

#: Operations that remove an object.
DELETE_OPS = frozenset({"delete"})


class TraceFormatError(ValueError):
    """Raised when a trace line cannot be parsed."""


class Request:
    """A single cache request.

    ``__slots__`` keeps the per-request footprint small; traces routinely run to
    billions of lines and the parsed objects dominate memory otherwise.
    """

    __slots__ = (
        "timestamp",
        "key",
        "key_size",
        "value_size",
        "client_id",
        "operation",
        "ttl",
    )

    def __init__(
        self,
        timestamp: int,
        key: str,
        key_size: int,
        value_size: int,
        client_id: str,
        operation: str,
        ttl: int,
    ) -> None:
        self.timestamp = timestamp
        self.key = key
        self.key_size = key_size
        self.value_size = value_size
        self.client_id = client_id
        self.operation = operation
        self.ttl = ttl

    @property
    def object_size(self) -> int:
        """Bytes the object occupies in a cache (key plus value)."""
        return self.key_size + self.value_size

    @property
    def is_read(self) -> bool:
        return self.operation in READ_OPS

    @property
    def is_write(self) -> bool:
        return self.operation in WRITE_OPS

    @property
    def is_delete(self) -> bool:
        return self.operation in DELETE_OPS

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Request):
            return NotImplemented
        return all(getattr(self, f) == getattr(other, f) for f in FIELD_NAMES)

    def __repr__(self) -> str:
        args = ", ".join(f"{f}={getattr(self, f)!r}" for f in FIELD_NAMES)
        return f"Request({args})"


def parse_line(line: str) -> Request:
    """Parse one trace line into a :class:`Request`.

    Raises :class:`TraceFormatError` when the line does not have the expected
    seven columns or the numeric columns are not integers.
    """
    parts = line.rstrip("\r\n").split(",")
    if len(parts) != len(FIELD_NAMES):
        raise TraceFormatError(
            f"expected {len(FIELD_NAMES)} columns, got {len(parts)}: {line!r}"
        )
    timestamp, key, key_size, value_size, client_id, operation, ttl = parts
    try:
        return Request(
            timestamp=int(timestamp),
            key=key,
            key_size=int(key_size),
            value_size=int(value_size),
            client_id=client_id,
            operation=operation,
            ttl=int(ttl),
        )
    except ValueError as exc:  # non-integer numeric column
        raise TraceFormatError(f"malformed numeric column: {line!r}") from exc


def format_request(request: Request) -> str:
    """Render a :class:`Request` back into a trace line (no trailing newline)."""
    return (
        f"{request.timestamp},{request.key},{request.key_size},"
        f"{request.value_size},{request.client_id},{request.operation},"
        f"{request.ttl}"
    )


def _open_zstd(path: str) -> IO[str]:
    """Open a zstd-compressed trace, preferring the pure-Python decoders."""
    try:  # Python 3.14+
        from compression import zstd  # type: ignore[import-not-found]

        return zstd.open(path, "rt")  # type: ignore[no-any-return]
    except ImportError:
        pass
    try:  # widely available third-party module
        import zstandard  # type: ignore[import-not-found]

        return zstandard.open(path, "rt")  # type: ignore[no-any-return]
    except ImportError:
        pass

    zstd_bin = shutil.which("zstd")
    if zstd_bin is None:
        raise RuntimeError(
            f"cannot read {path!r}: install the 'zstandard' package or the "
            "'zstd' command-line tool, or decompress the trace first "
            "with `zstd -d`"
        )
    proc = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        [zstd_bin, "-dcq", path], stdout=subprocess.PIPE, text=True
    )
    assert proc.stdout is not None
    return proc.stdout


@contextmanager
def open_trace(path: str) -> Iterator[IO[str]]:
    """Open a trace for reading, transparently decompressing by extension.

    ``-`` reads standard input. ``.zst``, ``.gz``, ``.bz2`` and ``.xz`` are
    decompressed on the fly; anything else is read as plain text.
    """
    if path == "-":
        yield sys.stdin
        return

    lowered = path.lower()
    handle: IO[str]
    if lowered.endswith(".zst"):
        handle = _open_zstd(path)
    elif lowered.endswith(".gz"):
        handle = gzip.open(path, "rt")
    elif lowered.endswith(".bz2"):
        handle = bz2.open(path, "rt")
    elif lowered.endswith(".xz"):
        handle = lzma.open(path, "rt")
    else:
        handle = open(path)  # noqa: SIM115 - closed by this contextmanager

    try:
        yield handle
    finally:
        handle.close()


def read_trace(
    lines: Iterable[str],
    *,
    skip_malformed: bool = False,
    max_requests: Optional[int] = None,
    errors: Optional[List[str]] = None,
) -> Iterator[Request]:
    """Parse an iterable of trace lines into :class:`Request` objects.

    :param skip_malformed: skip unparseable lines instead of raising.
    :param max_requests: stop after this many successfully parsed requests.
    :param errors: if given, malformed line descriptions are appended here.
    """
    emitted = 0
    for lineno, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            request = parse_line(line)
        except TraceFormatError as exc:
            if not skip_malformed:
                raise TraceFormatError(f"line {lineno}: {exc}") from exc
            if errors is not None:
                errors.append(f"line {lineno}: {exc}")
            continue
        yield request
        emitted += 1
        if max_requests is not None and emitted >= max_requests:
            return
