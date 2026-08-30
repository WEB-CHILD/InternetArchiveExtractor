"""Tests for warc_outlinks.py (outgoing-link archiving)."""

from io import BytesIO

import pytest
import requests
from warcio.archiveiterator import ArchiveIterator

import warc_outlinks as o


# --------------------------------------------------------------------------- #
# extract_outgoing_links
# --------------------------------------------------------------------------- #
def test_extract_resolves_relative_links():
    """Relative href values are resolved to absolute URLs using the page's base URL."""
    html = b'<a href="page2.html">x</a>'
    links = o.extract_outgoing_links(html, "http://a.com/dir/page1.html")
    assert links == ["http://a.com/dir/page2.html"]


def test_extract_collects_embedded_resources():
    """img, script, link, and iframe tags are all collected alongside anchor hrefs."""
    html = (
        b'<img src="img.png">'
        b'<script src="app.js"></script>'
        b'<link href="style.css">'
        b'<iframe src="frame.html"></iframe>'
    )
    links = o.extract_outgoing_links(html, "http://a.com/")
    assert set(links) == {
        "http://a.com/img.png",
        "http://a.com/app.js",
        "http://a.com/style.css",
        "http://a.com/frame.html",
    }


def test_extract_video_has_two_url_attrs():
    """Video tags yield both the src and poster attributes as separate links."""
    html = b'<video src="v.mp4" poster="p.jpg"></video>'
    links = o.extract_outgoing_links(html, "http://a.com/")
    assert set(links) == {"http://a.com/v.mp4", "http://a.com/p.jpg"}


def test_extract_deduplicates_preserving_order():
    """Duplicate URLs appear only once, with the first occurrence's position preserved."""
    html = b'<a href="b.html">1</a><a href="c.html">2</a><a href="b.html">3</a>'
    links = o.extract_outgoing_links(html, "http://a.com/")
    assert links == ["http://a.com/b.html", "http://a.com/c.html"]


def test_extract_skips_fragment_and_nonfetchable_schemes():
    """Fragment-only links and non-HTTP schemes (mailto, javascript, tel, ftp) are discarded."""
    html = (
        b'<a href="#top">x</a>'
        b'<a href="mailto:me@a.com">x</a>'
        b'<a href="javascript:void(0)">x</a>'
        b'<a href="tel:123">x</a>'
        b'<a href="ftp://a.com/f">x</a>'
        b'<a href="http://a.com/keep">x</a>'
    )
    links = o.extract_outgoing_links(html, "http://a.com/")
    assert links == ["http://a.com/keep"]


def test_extract_strips_fragment_from_otherwise_valid_link():
    """Fragment identifiers are stripped from otherwise valid HTTP links."""
    html = b'<a href="http://a.com/page#section">x</a>'
    assert o.extract_outgoing_links(html, "http://a.com/") == ["http://a.com/page"]


def test_extract_ignores_empty_href():
    """Anchor tags with empty or missing href attributes produce no links."""
    html = b'<a href="">x</a><a>y</a>'
    assert o.extract_outgoing_links(html, "http://a.com/") == []


# --------------------------------------------------------------------------- #
# timestamp conversion helpers
# --------------------------------------------------------------------------- #
def test_warc_date_to_timestamp_roundtrip():
    """A Wayback timestamp converts to a WARC-Date string and back without loss."""
    ts = "20000302202605"
    warc_date = o._timestamp_to_warc_date(ts)
    assert warc_date == "2000-03-02T20:26:05Z"
    assert o._warc_date_to_timestamp(warc_date) == ts


@pytest.mark.parametrize("bad", [None, "", "garbage", "2000-03-02"])
def test_warc_date_to_timestamp_invalid_returns_none(bad):
    """_warc_date_to_timestamp returns None for any value that is not a valid WARC-Date."""
    assert o._warc_date_to_timestamp(bad) is None


@pytest.mark.parametrize("bad", [None, "", "garbage", "2000"])
def test_timestamp_to_warc_date_invalid_returns_none(bad):
    """_timestamp_to_warc_date returns None for any value that is not a 14-digit Wayback timestamp."""
    assert o._timestamp_to_warc_date(bad) is None


# --------------------------------------------------------------------------- #
# _actual_capture_timestamp
# --------------------------------------------------------------------------- #
class _Resp:
    def __init__(self, url="", status_code=200, reason="OK", headers=None, content=b""):
        self.url = url
        self.status_code = status_code
        self.reason = reason
        self.headers = headers or {}
        self.content = content


def test_actual_capture_timestamp_prefers_redirected_url():
    """The timestamp embedded in the final redirected URL takes priority over the requested one."""
    resp = _Resp(url="https://web.archive.org/web/19991231120000id_/http://a.com")
    assert o._actual_capture_timestamp(resp, "20000101000000") == "19991231120000"


def test_actual_capture_timestamp_falls_back_to_requested():
    """When no timestamp is found in the response URL, the originally requested one is returned."""
    resp = _Resp(url="https://example.com/no-timestamp")
    assert o._actual_capture_timestamp(resp, "20000101000000") == "20000101000000"


# --------------------------------------------------------------------------- #
# collect_outlinks_from_warcs
# --------------------------------------------------------------------------- #
def test_collect_outlinks_from_warcs(tmp_path, warc_bytes_factory):
    """Outgoing links are extracted from HTML records; self-links already in the WARC are excluded."""
    warc_bytes = warc_bytes_factory(
        [
            (
                "http://a.com/index.html",
                "2000-03-02T20:26:05Z",
                "text/html",
                b'<a href="http://a.com/out.html">x</a><a href="http://a.com/index.html">self</a>',
            )
        ]
    )
    warc_path = tmp_path / "src-0001.warc.gz"
    warc_path.write_bytes(warc_bytes)

    outlinks = o.collect_outlinks_from_warcs([str(warc_path)])
    # The self-link (already captured) is excluded; the outgoing link keeps the page date.
    assert outlinks == {"http://a.com/out.html": "20000302202605"}


def test_collect_outlinks_skips_non_html(tmp_path, warc_bytes_factory):
    """Non-HTML WARC records (e.g. images) are not scanned for outgoing links."""
    warc_bytes = warc_bytes_factory(
        [("http://a.com/pic.png", "2000-03-02T20:26:05Z", "image/png", b"\x89PNG")]
    )
    warc_path = tmp_path / "src-0001.warc.gz"
    warc_path.write_bytes(warc_bytes)
    assert o.collect_outlinks_from_warcs([str(warc_path)]) == {}


def test_collect_outlinks_keeps_earliest_timestamp(tmp_path, warc_bytes_factory):
    """When a link appears on multiple pages, the earliest capture timestamp is kept."""
    warc_bytes = warc_bytes_factory(
        [
            ("http://a.com/p1.html", "2000-01-01T00:00:00Z", "text/html",
             b'<a href="http://a.com/shared.html">x</a>'),
            ("http://a.com/p2.html", "2001-01-01T00:00:00Z", "text/html",
             b'<a href="http://a.com/shared.html">x</a>'),
        ]
    )
    warc_path = tmp_path / "src-0001.warc.gz"
    warc_path.write_bytes(warc_bytes)
    outlinks = o.collect_outlinks_from_warcs([str(warc_path)])
    assert outlinks["http://a.com/shared.html"] == "20000101000000"


def test_collect_outlinks_parallel_matches_serial(tmp_path, warc_bytes_factory):
    """A multi-process scan returns exactly the same result as a single-process scan."""
    paths = []
    for i in range(4):
        warc_bytes = warc_bytes_factory(
            [
                (f"http://a.com/p{i}.html", "2000-03-02T20:26:05Z", "text/html",
                 f'<a href="http://a.com/out{i}.html">x</a>'
                 f'<a href="http://a.com/p{(i + 1) % 4}.html">next</a>'.encode()),
            ]
        )
        path = tmp_path / f"src-{i:04d}.warc.gz"
        path.write_bytes(warc_bytes)
        paths.append(str(path))

    serial = o.collect_outlinks_from_warcs(paths, scan_workers=1)
    parallel = o.collect_outlinks_from_warcs(paths, scan_workers=4)

    assert parallel == serial
    # Cross-file self-links (p0..p3 are captured in the set) are excluded either way.
    assert set(serial) == {f"http://a.com/out{i}.html" for i in range(4)}


def test_collect_outlinks_parallel_keeps_earliest_timestamp(tmp_path, warc_bytes_factory):
    """Merging worker results in file order preserves the 'earliest wins' timestamp rule."""
    first = tmp_path / "src-0001.warc.gz"
    first.write_bytes(warc_bytes_factory(
        [("http://a.com/p1.html", "2000-01-01T00:00:00Z", "text/html",
          b'<a href="http://a.com/shared.html">x</a>')]
    ))
    second = tmp_path / "src-0002.warc.gz"
    second.write_bytes(warc_bytes_factory(
        [("http://a.com/p2.html", "2001-01-01T00:00:00Z", "text/html",
          b'<a href="http://a.com/shared.html">x</a>')]
    ))
    paths = [str(first), str(second)]

    assert o.collect_outlinks_from_warcs(paths, scan_workers=2) == \
        o.collect_outlinks_from_warcs(paths, scan_workers=1)
    assert o.collect_outlinks_from_warcs(paths, scan_workers=2)[
        "http://a.com/shared.html"] == "20000101000000"


def test_scan_warc_for_outlinks_returns_links_and_known_urls(tmp_path, warc_bytes_factory):
    """The per-file worker reports its own links and every URL captured in that file."""
    path = tmp_path / "src-0001.warc.gz"
    path.write_bytes(warc_bytes_factory(
        [("http://a.com/index.html", "2000-03-02T20:26:05Z", "text/html",
          b'<a href="http://b.com/out.html">x</a>')]
    ))

    outlinks, known = o.scan_warc_for_outlinks(str(path))

    # The worker does NOT apply the self-link exclusion; that happens after the merge.
    assert outlinks == {"http://b.com/out.html": "20000302202605"}
    assert known == {"http://a.com/index.html"}


def test_collect_outlinks_scan_workers_capped_to_file_count(tmp_path, warc_bytes_factory):
    """Asking for more scan workers than there are files does not fail."""
    path = tmp_path / "src-0001.warc.gz"
    path.write_bytes(warc_bytes_factory(
        [("http://a.com/i.html", "2000-03-02T20:26:05Z", "text/html",
          b'<a href="http://b.com/o.html">x</a>')]
    ))
    assert o.collect_outlinks_from_warcs([str(path)], scan_workers=64) == {
        "http://b.com/o.html": "20000302202605"
    }


# --------------------------------------------------------------------------- #
# _download_archived_resource (mocked session)
# --------------------------------------------------------------------------- #
class _FakeSession:
    def __init__(self, responses):
        # responses: list of _Resp or Exception instances, consumed per call.
        self._responses = list(responses)
        self.calls = 0

    def get(self, url, **kwargs):
        self.calls += 1
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def mount(self, prefix, adapter):
        pass

    def close(self):
        pass


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Make retry backoff instantaneous."""
    monkeypatch.setattr(o.time, "sleep", lambda *_: None)


def test_download_resource_success_first_try():
    """A 200 response on the first attempt is returned immediately with no retries."""
    session = _FakeSession([_Resp(status_code=200)])
    resp = o._download_archived_resource("http://a.com", "2000", session, "UA", 30, 2)
    assert resp.status_code == 200
    assert session.calls == 1


def test_download_resource_retries_on_500_then_succeeds():
    """A 500 response triggers a retry, and the subsequent 200 is returned."""
    session = _FakeSession([_Resp(status_code=500), _Resp(status_code=200)])
    resp = o._download_archived_resource("http://a.com", "2000", session, "UA", 30, 2)
    assert resp.status_code == 200
    assert session.calls == 2


def test_download_resource_retries_on_429():
    """A 429 rate-limit response is retried and the next 200 is returned."""
    session = _FakeSession([_Resp(status_code=429), _Resp(status_code=200)])
    resp = o._download_archived_resource("http://a.com", "2000", session, "UA", 30, 2)
    assert resp.status_code == 200


def test_download_resource_returns_none_after_exhausting_retries():
    """When all retry attempts return server errors, the resource is treated as a failure (None)."""
    session = _FakeSession([_Resp(status_code=500), _Resp(status_code=503)])
    resp = o._download_archived_resource("http://a.com", "2000", session, "UA", 30, 1)
    assert resp is None
    assert session.calls == 2


def test_download_resource_returns_none_on_persistent_exception():
    """None is returned when every attempt raises a network-level RequestException."""
    session = _FakeSession([requests.RequestException("x"), requests.RequestException("y")])
    resp = o._download_archived_resource("http://a.com", "2000", session, "UA", 30, 1)
    assert resp is None


def test_download_resource_does_not_retry_on_404():
    """A 404 response is treated as a definitive answer and returned without retrying."""
    session = _FakeSession([_Resp(status_code=404)])
    resp = o._download_archived_resource("http://a.com", "2000", session, "UA", 30, 2)
    assert resp.status_code == 404
    assert session.calls == 1


# --------------------------------------------------------------------------- #
# _OutlinksWarcWriter
# --------------------------------------------------------------------------- #
def _read_outlink_records(path):
    out = []
    with open(path, "rb") as stream:
        for record in ArchiveIterator(stream):
            out.append(
                (
                    record.rec_headers.get_header("WARC-Target-URI"),
                    record.http_headers.get_statuscode(),
                    record.content_stream().read(),
                )
            )
    return out


def test_outlinks_writer_writes_record(tmp_path):
    """_OutlinksWarcWriter writes a WARC record with the correct URL, status, and payload."""
    writer = o._OutlinksWarcWriter(str(tmp_path), "site_outlinks", 1073741824)
    writer.write_resource(
        url="http://a.com/x",
        status_code=200,
        reason="OK",
        content_type="text/html",
        content=b"<html></html>",
        warc_date="2000-03-02T20:26:05Z",
    )
    writer.close()

    records = _read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))
    assert records[0][0] == "http://a.com/x"
    assert records[0][1] == "200"
    assert records[0][2] == b"<html></html>"


def test_outlinks_writer_splits_on_size(tmp_path):
    """_OutlinksWarcWriter opens a new part file when the size threshold is exceeded."""
    writer = o._OutlinksWarcWriter(str(tmp_path), "site_outlinks", max_size_bytes=10)
    for i in range(3):
        writer.write_resource(
            url=f"http://a.com/{i}",
            status_code=200,
            reason="OK",
            content_type="text/html",
            content=b"x" * 20,
            warc_date=None,
        )
    writer.close()
    parts = sorted(tmp_path.glob("site_outlinks-*.warc.gz"))
    assert len(parts) >= 2


# --------------------------------------------------------------------------- #
# fetch_and_archive_outlinks (end-to-end with mocked network)
# --------------------------------------------------------------------------- #
def test_fetch_and_archive_outlinks_no_links_writes_nothing(tmp_path, monkeypatch):
    """fetch_and_archive_outlinks exits early and writes no files when there are no outlinks."""
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: {})
    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site")
    assert list(tmp_path.glob("*.warc.gz")) == []


def test_fetch_and_archive_outlinks_end_to_end(tmp_path, monkeypatch):
    """fetch_and_archive_outlinks downloads each outlink and stores it in a WARC file."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/out.html": "20000302202605"},
    )

    captured = _Resp(
        url="https://web.archive.org/web/20000302202605id_/http://a.com/out.html",
        status_code=200,
        reason="OK",
        headers={"Content-Type": "text/html"},
        content=b"<html>linked</html>",
    )
    monkeypatch.setattr(requests, "Session", lambda: _FakeSession([captured]))

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1)

    records = _read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))
    assert records[0][0] == "http://a.com/out.html"
    assert records[0][2] == b"<html>linked</html>"


def test_fetch_and_archive_outlinks_counts_failures(tmp_path, monkeypatch):
    """Failed downloads are counted and omitted from the WARC; the part file is still created."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/out.html": "20000302202605"},
    )
    # Session always raises -> download returns None -> counted as failure, no record.
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([requests.RequestException("x"), requests.RequestException("y"),
                              requests.RequestException("z")]),
    )
    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1, max_retries=2)
    # An (empty) WARC part file is still created.
    records = _read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))
    assert records == []


# --------------------------------------------------------------------------- #
# progress reporting
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "seconds,expected",
    [(0, "0s"), (5, "5s"), (65, "1m 05s"), (3600, "1h 00m"), (7325, "2h 02m"), (-1, "0s")],
)
def test_format_duration(seconds, expected):
    """Durations render as compact h/m/s strings and never go negative."""
    assert o._format_duration(seconds) == expected


def test_download_archived_resource_logs_progress_prefix(monkeypatch, caplog):
    """The 'n/total' label is prefixed to the per-request debug line."""
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([_Resp(url="http://x", status_code=200)]),
    )
    session = requests.Session()
    with caplog.at_level("DEBUG", logger="warc_outlinks"):
        o._download_archived_resource(
            "http://a.com/x", "20000302202605", session,
            o.DEFAULT_USER_AGENT, 5, 0, progress="7/99",
        )
    assert any("[7/99] Downloading" in r.message for r in caplog.records)


def test_progress_logged_every_n_completed(tmp_path, monkeypatch, caplog):
    """A progress summary is emitted every progress_every downloads and once at the end."""
    links = {f"http://a.com/p{i}.html": "20000302202605" for i in range(5)}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([
            _Resp(url=f"http://a.com/p{i}.html", status_code=200,
                  headers={"Content-Type": "text/html"}, content=b"x")
            for i in range(5)
        ]),
    )

    with caplog.at_level("INFO", logger="warc_outlinks"):
        o.fetch_and_archive_outlinks(
            ["unused"], str(tmp_path), "site", threads=1, progress_every=2,
        )

    progress = [r.message for r in caplog.records if "Outlink progress" in r.message]
    # 2 and 4 hit the interval; 5 is the final total.
    assert [m.split()[4] for m in progress] == ["2/5", "4/5", "5/5"]


def test_progress_counts_failures_as_completed(tmp_path, monkeypatch, caplog):
    """Failed downloads still advance the progress counter so a bad run keeps reporting."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/out.html": "20000302202605"},
    )
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([requests.RequestException("x")]),
    )

    with caplog.at_level("INFO", logger="warc_outlinks"):
        o.fetch_and_archive_outlinks(
            ["unused"], str(tmp_path), "site", threads=1, max_retries=0, progress_every=1,
        )

    progress = [r.message for r in caplog.records if "Outlink progress" in r.message]
    assert len(progress) == 1
    assert "1/1 (100.0%)" in progress[0]
    assert "0 ok, 1 failed" in progress[0]


def test_progress_every_zero_disables_progress_lines(tmp_path, monkeypatch, caplog):
    """progress_every=0 suppresses the periodic progress lines entirely."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/out.html": "20000302202605"},
    )
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([
            _Resp(url="http://a.com/out.html", status_code=200,
                  headers={"Content-Type": "text/html"}, content=b"x")
        ]),
    )

    with caplog.at_level("INFO", logger="warc_outlinks"):
        o.fetch_and_archive_outlinks(
            ["unused"], str(tmp_path), "site", threads=1, progress_every=0,
        )

    assert not [r for r in caplog.records if "Outlink progress" in r.message]


# --------------------------------------------------------------------------- #
# failed-request log
# --------------------------------------------------------------------------- #
def test_failed_requests_written_one_per_line(tmp_path, monkeypatch):
    """Every failed request is written to <basename>_outlinks_failed.txt, one URL per line."""
    links = {f"http://a.com/p{i}.html": "20000302202605" for i in range(3)}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([requests.RequestException("x") for _ in range(3)]),
    )

    o.fetch_and_archive_outlinks(
        ["unused"], str(tmp_path), "site", threads=1, max_retries=0,
    )

    lines = (tmp_path / "site_outlinks_failed.txt").read_text().splitlines()
    assert sorted(lines) == sorted(
        f"https://web.archive.org/web/20000302202605id_/http://a.com/p{i}.html"
        for i in range(3)
    )


def test_no_failures_leaves_no_file(tmp_path, monkeypatch):
    """A run where everything succeeds does not create an empty failures file."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/out.html": "20000302202605"},
    )
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([
            _Resp(url="http://a.com/out.html", status_code=200,
                  headers={"Content-Type": "text/html"}, content=b"x")
        ]),
    )

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1)

    assert not (tmp_path / "site_outlinks_failed.txt").exists()


def test_failures_file_only_lists_failures(tmp_path, monkeypatch):
    """Successful downloads are not recorded in the failures file."""
    links = {"http://a.com/ok.html": "20000302202605", "http://a.com/bad.html": "20000302202605"}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)
    # _FakeSession consumes responses in order, and threads=1 keeps that order stable.
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([
            _Resp(url="http://a.com/ok.html", status_code=200,
                  headers={"Content-Type": "text/html"}, content=b"x"),
            requests.RequestException("boom"),
        ]),
    )

    o.fetch_and_archive_outlinks(
        ["unused"], str(tmp_path), "site", threads=1, max_retries=0,
    )

    lines = (tmp_path / "site_outlinks_failed.txt").read_text().splitlines()
    assert lines == ["https://web.archive.org/web/20000302202605id_/http://a.com/bad.html"]


def test_failures_file_overwritten_on_rerun(tmp_path, monkeypatch):
    """A re-run replaces the previous failures file rather than appending to it."""
    stale = tmp_path / "site_outlinks_failed.txt"
    stale.write_text("https://example.com/stale-entry\n")

    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/bad.html": "20000302202605"},
    )
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([requests.RequestException("x")]),
    )

    o.fetch_and_archive_outlinks(
        ["unused"], str(tmp_path), "site", threads=1, max_retries=0,
    )

    assert stale.read_text().splitlines() == [
        "https://web.archive.org/web/20000302202605id_/http://a.com/bad.html"
    ]

