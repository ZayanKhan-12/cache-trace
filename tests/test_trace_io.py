"""Tests for scripts/trace_io.py."""

import gzip
import io
import os
import tempfile
import unittest

import _path  # noqa: F401  (side effect: puts scripts/ on sys.path)
from trace_io import (  # noqa: E402
    Request,
    TraceFormatError,
    format_request,
    open_trace,
    parse_line,
    read_trace,
)

SAMPLE_LINE = "0,z44uy84y444444zku,72,455,1,get,0"


class ParseLineTest(unittest.TestCase):
    def test_parses_all_columns(self):
        request = parse_line(SAMPLE_LINE)
        self.assertEqual(request.timestamp, 0)
        self.assertEqual(request.key, "z44uy84y444444zku")
        self.assertEqual(request.key_size, 72)
        self.assertEqual(request.value_size, 455)
        self.assertEqual(request.client_id, "1")
        self.assertEqual(request.operation, "get")
        self.assertEqual(request.ttl, 0)

    def test_object_size_is_key_plus_value(self):
        self.assertEqual(parse_line(SAMPLE_LINE).object_size, 72 + 455)

    def test_tolerates_trailing_newlines(self):
        self.assertEqual(parse_line(SAMPLE_LINE + "\r\n"), parse_line(SAMPLE_LINE))

    def test_operation_classification(self):
        self.assertTrue(parse_line("1,k,1,1,c,get,0").is_read)
        self.assertTrue(parse_line("1,k,1,1,c,gets,0").is_read)
        self.assertTrue(parse_line("1,k,1,1,c,set,0").is_write)
        self.assertTrue(parse_line("1,k,1,1,c,incr,0").is_write)
        self.assertTrue(parse_line("1,k,1,1,c,delete,0").is_delete)
        other = parse_line("1,k,1,1,c,quit,0")
        self.assertFalse(other.is_read or other.is_write or other.is_delete)

    def test_rejects_wrong_column_count(self):
        with self.assertRaises(TraceFormatError):
            parse_line("0,key,72,455,1,get")

    def test_rejects_non_integer_column(self):
        with self.assertRaises(TraceFormatError):
            parse_line("0,key,notanint,455,1,get,0")

    def test_keys_containing_no_comma_are_preserved(self):
        # Keys use ':' and '=' but never ',', so a plain split is sufficient.
        request = parse_line("5,q:q:1:8WTwl7huJeQ==,17,249,4,get,0")
        self.assertEqual(request.key, "q:q:1:8WTwl7huJeQ==")


class FormatRequestTest(unittest.TestCase):
    def test_roundtrip_is_lossless(self):
        for line in (
            SAMPLE_LINE,
            "1583020800,q:q:1:MYY8VvHze8,16,0,2,set,3600",
            "12,yDqF:gY:1AJrn9G9,27,27,1005,delete,0",
        ):
            self.assertEqual(format_request(parse_line(line)), line)


class ReadTraceTest(unittest.TestCase):
    def test_skips_blank_lines(self):
        lines = [SAMPLE_LINE, "", "   \n", SAMPLE_LINE]
        self.assertEqual(len(list(read_trace(lines))), 2)

    def test_raises_on_malformed_by_default(self):
        with self.assertRaises(TraceFormatError) as ctx:
            list(read_trace([SAMPLE_LINE, "bad line"]))
        self.assertIn("line 2", str(ctx.exception))

    def test_skip_malformed_collects_errors(self):
        errors = []
        requests = list(
            read_trace(
                [SAMPLE_LINE, "bad", SAMPLE_LINE],
                skip_malformed=True,
                errors=errors,
            )
        )
        self.assertEqual(len(requests), 2)
        self.assertEqual(len(errors), 1)

    def test_max_requests_stops_early(self):
        lines = [SAMPLE_LINE] * 10
        self.assertEqual(len(list(read_trace(lines, max_requests=3))), 3)

    def test_is_lazy(self):
        def exploding():
            yield SAMPLE_LINE
            raise AssertionError("should not be reached")

        first = next(read_trace(exploding()))
        self.assertIsInstance(first, Request)


class OpenTraceTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        for name in os.listdir(self.tmpdir):
            os.unlink(os.path.join(self.tmpdir, name))
        os.rmdir(self.tmpdir)

    def test_reads_plain_text(self):
        path = os.path.join(self.tmpdir, "trace")
        with open(path, "w") as fh:
            fh.write(SAMPLE_LINE + "\n")
        with open_trace(path) as handle:
            self.assertEqual(len(list(read_trace(handle))), 1)

    def test_reads_gzip(self):
        path = os.path.join(self.tmpdir, "trace.gz")
        with gzip.open(path, "wt") as fh:
            fh.write(SAMPLE_LINE + "\n")
        with open_trace(path) as handle:
            self.assertEqual(len(list(read_trace(handle))), 1)

    def test_dash_reads_stdin(self):
        import sys

        original = sys.stdin
        sys.stdin = io.StringIO(SAMPLE_LINE + "\n")
        try:
            with open_trace("-") as handle:
                self.assertEqual(len(list(read_trace(handle))), 1)
        finally:
            sys.stdin = original


if __name__ == "__main__":
    unittest.main()
