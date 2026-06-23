"""Shared pytest fixtures and path setup.

The package is installed *non-editable* and its modules import each other
flat (``from waybackup_to_warc import ...``), so the tests run against the
source of truth in ``src/`` by putting that directory on ``sys.path``.
"""

import os
import sys

import pytest

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


@pytest.fixture
def warc_bytes_factory():
    """Return a helper that builds an in-memory ``.warc.gz`` byte string.

    The helper accepts a list of ``(url, warc_date, content_type, body)``
    tuples and writes one WARC ``response`` record per tuple.
    """
    import gzip
    from io import BytesIO

    from warcio.warcwriter import WARCWriter
    from warcio.statusandheaders import StatusAndHeaders

    def _make(records):
        buf = BytesIO()
        # WARCWriter(gzip=True) gzips each record individually, which is the
        # same on-disk shape the production code produces.
        writer = WARCWriter(buf, gzip=True)
        for url, warc_date, content_type, body in records:
            http_headers = StatusAndHeaders(
                "200 OK", [("Content-Type", content_type)], protocol="HTTP/1.0"
            )
            record = writer.create_warc_record(
                url,
                "response",
                payload=BytesIO(body),
                length=len(body),
                http_headers=http_headers,
                warc_headers_dict={"WARC-Date": warc_date} if warc_date else None,
            )
            writer.write_record(record)
        return buf.getvalue()

    return _make


@pytest.fixture(autouse=True)
def _restore_std_streams():
    """Guard against tests that swap ``sys.stdout`` / ``sys.stderr`` and fail
    to restore them (e.g. the stdout-redirect context manager)."""
    saved_out, saved_err = sys.stdout, sys.stderr
    try:
        yield
    finally:
        sys.stdout, sys.stderr = saved_out, saved_err
