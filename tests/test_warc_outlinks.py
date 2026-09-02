"""Tests for warc_outlinks.py (outgoing-link archiving)."""

from io import BytesIO

import gc
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
import inspect
import logging
from urllib.parse import unquote

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

def test_extract_resolves_every_relative_form():
    """Same-dir, parent, root-relative, protocol-relative and query-only hrefs all resolve."""
    html = (
        b'<a href="./same.html">1</a>'
        b'<a href="../up.html">2</a>'
        b'<a href="/root.html">3</a>'
        b'<a href="//cdn.example.com/lib.js">4</a>'
        b'<a href="?q=1">5</a>'
        b'<img src="../img/logo.png">'
        b'<video poster="thumbs/p.jpg"></video>'
    )
    links = o.extract_outgoing_links(html, "http://a.com/dir/sub/page.html?old=1")
    assert links == [
        "http://a.com/dir/sub/same.html",
        "http://a.com/dir/up.html",
        "http://a.com/root.html",
        "http://cdn.example.com/lib.js",
        # A query-only href replaces the base's query but keeps its path.
        "http://a.com/dir/sub/page.html?q=1",
        "http://a.com/dir/img/logo.png",
        "http://a.com/dir/sub/thumbs/p.jpg",
    ]


def test_extract_protocol_relative_link_inherits_base_scheme():
    """A "//host/path" href takes its scheme from the page it was found on."""
    html = b'<script src="//cdn.example.com/lib.js"></script>'
    assert o.extract_outgoing_links(html, "https://a.com/dir/p.html") == [
        "https://cdn.example.com/lib.js"
    ]


def test_extract_strips_whitespace_before_resolving():
    """Whitespace padding around a relative href does not leak into the resolved URL."""
    html = b'<a href="  next.html  ">x</a>'
    assert o.extract_outgoing_links(html, "http://a.com/dir/p.html") == [
        "http://a.com/dir/next.html"
    ]



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
# extract_outgoing_links: source-byte fidelity on non-ASCII hrefs
# --------------------------------------------------------------------------- #
def test_extract_preserves_latin1_href_bytes():
    """A latin-1 page's href is percent-encoded from its original bytes, not from UTF-8."""
    html = '<a href="b\xf8ger.html">x</a>'.encode("latin-1")
    assert o.extract_outgoing_links(html, "http://a.com/") == ["http://a.com/b%F8ger.html"]


def test_extract_preserves_utf8_href_bytes():
    """A UTF-8 page's href is percent-encoded from its UTF-8 bytes."""
    html = '<a href="b\xf8ger.html">x</a>'.encode("utf-8")
    assert o.extract_outgoing_links(html, "http://a.com/") == ["http://a.com/b%C3%B8ger.html"]


def test_extract_keeps_distinct_legacy_hrefs_distinct():
    """Different high bytes stay different URLs instead of collapsing onto one U+FFFD key."""
    html = ('<a href="b\xf8ger.html">1</a>'
            '<a href="b\xe6ger.html">2</a>'
            '<a href="b\xe5ger.html">3</a>').encode("latin-1")
    assert o.extract_outgoing_links(html, "http://a.com/") == [
        "http://a.com/b%F8ger.html",
        "http://a.com/b%E6ger.html",
        "http://a.com/b%E5ger.html",
    ]


def test_extract_preserves_multibyte_legacy_href_bytes():
    """A multi-byte legacy encoding (euc-kr) round-trips byte for byte."""
    href = "\ud55c\uae00.html".encode("euc-kr")
    html = b'<a href="' + href + b'">x</a>'
    expected = "http://a.com/" + "".join(f"%{b:02X}" for b in href[:-5]) + ".html"
    assert o.extract_outgoing_links(html, "http://a.com/") == [expected]


def test_extract_resolves_numeric_character_reference_to_its_byte():
    """A character reference inside latin-1's range encodes to that single byte."""
    html = b'<a href="b&#248;ger.html">x</a>'
    assert o.extract_outgoing_links(html, "http://a.com/") == ["http://a.com/b%F8ger.html"]


def test_extract_falls_back_to_utf8_for_unrepresentable_character_reference():
    """A character reference above latin-1 has no source byte, so UTF-8 is used."""
    html = b'<a href="p&#8364;.html">x</a>'
    assert o.extract_outgoing_links(html, "http://a.com/") == ["http://a.com/p%E2%82%AC.html"]


def test_extract_leaves_ascii_url_syntax_untouched():
    """Encoding a non-ASCII href must not escape the query, fragment or path separators."""
    html = '<a href="/a/b?x=1&y=2#f\xf8">x</a>'.encode("latin-1")
    assert o.extract_outgoing_links(html, "http://a.com/") == ["http://a.com/a/b?x=1&y=2"]


def test_extract_encodes_non_ascii_host():
    """A non-ASCII host is percent-encoded rather than replaced, keeping the URL usable."""
    html = '<a href="http://\xf8.com/x">y</a>'.encode("latin-1")
    assert o.extract_outgoing_links(html, "http://a.com/") == ["http://%F8.com/x"]


def test_extract_does_not_alter_already_percent_encoded_hrefs():
    """An href that is already percent-encoded is pure ASCII and passes through as-is."""
    html = b'<a href="b%F8ger.html">x</a>'
    assert o.extract_outgoing_links(html, "http://a.com/") == ["http://a.com/b%F8ger.html"]


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
    def __init__(self, url="", status_code=200, reason="OK", headers=None, content=b"",
                 history=(), memento=True):
        self.url = url
        self.status_code = status_code
        self.reason = reason
        self.headers = dict(headers or {})
        # Wayback sets Memento-Datetime on everything it replays from a real capture,
        # so that is the default here. memento=False models a response Wayback
        # generated itself (a "not archived" page or a replay-level redirect).
        if memento:
            self.headers.setdefault("Memento-Datetime", "Thu, 02 Mar 2000 20:26:05 GMT")
        self.content = content
        # requests.Response always exposes .history; the redirect chain is empty
        # for a response that was not redirected.
        self.history = list(history)


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


# --------------------------------------------------------------------------- #
# TLD exclusion
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("given,expected", [
    ([".dk", ".com"], (".dk", ".com")),
    (["dk", "com"], (".dk", ".com")),
    ([".DK", " .Com "], (".dk", ".com")),
    ([".co.uk"], (".co.uk",)),
    ([".dk", "dk", ".DK."], (".dk",)),
    (["", "  ", "."], ()),
    (None, ()),
])
def test_normalize_excluded_tlds(given, expected):
    """The leading dot is optional, case and whitespace are ignored, duplicates drop out."""
    assert o.normalize_excluded_tlds(given) == expected


@pytest.mark.parametrize("url,excluded", [
    ("http://example.dk/x", True),
    ("http://example.dk:8080/x", True),
    ("http://user:pw@example.dk/x", True),
    ("http://EXAMPLE.DK/x", True),
    ("http://example.dk./x", True),          # root-labelled host
    ("http://sub.example.dk/x", True),
    ("http://example.dk", True),             # no path at all
    ("http://example.com/x", False),
    ("http://example.dk.com/x", False),      # ".dk" only mid-host
    ("http://notdk/x", False),               # suffix must follow a dot
    ("http://dk/x", False),                  # the TLD alone is not a host under it
    ("http://192.168.0.1/x", False),
    ("http://[2001:db8::1]/x", False),       # IPv6 literal has no TLD
    ("not a url", False),
])
def test_host_is_excluded(url, excluded):
    """Ports, userinfo, case and a trailing root dot must not defeat the match."""
    assert o._host_is_excluded(url, (".dk",)) is excluded


def test_host_is_excluded_matches_urlsplit():
    """The fast authority parser must agree with urlsplit on realistic URLs."""
    from urllib.parse import urlsplit
    suffixes = (".dk", ".com", ".co.uk")
    urls = [
        "http://example.dk/a/b?c=1#d", "https://a.b.example.co.uk/x", "http://x.com:81/",
        "http://u@x.com/", "http://u:p@x.com:81/y", "http://EXAMPLE.DK", "http://x.dk.",
        "http://x.org/", "https://[::1]:8080/x", "http://192.168.0.1/", "http://x.comic/",
    ]
    for url in urls:
        host = (urlsplit(url).hostname or "").rstrip(".")
        expected = bool(host) and host.endswith(suffixes)
        assert o._host_is_excluded(url, suffixes) is expected, url


def test_scan_warc_for_outlinks_excludes_tlds(tmp_path, warc_bytes_factory):
    """Excluded links are dropped in the worker and counted, not returned."""
    html = (b'<a href="http://keep.org/a">a</a>'
            b'<a href="http://drop.dk/b">b</a>'
            b'<a href="http://drop.com/c">c</a>')
    path = tmp_path / "s.warc.gz"
    path.write_bytes(warc_bytes_factory(
        [("http://src.org/", "2000-03-02T20:26:05Z", "text/html", html)]))

    outlinks, known, excluded = o.scan_warc_for_outlinks(str(path), (".dk", ".com"))

    assert set(outlinks) == {"http://keep.org/a"}
    assert known == {"http://src.org/"}
    assert excluded == 2


def test_collect_outlinks_from_warcs_excludes_tlds(tmp_path, warc_bytes_factory):
    """Exclusion applies the same way through the parallel and serial scan paths."""
    html = b'<a href="http://keep.org/a">a</a><a href="http://drop.dk/b">b</a>'
    paths = []
    for i in range(3):
        path = tmp_path / f"s{i}.warc.gz"
        path.write_bytes(warc_bytes_factory(
            [(f"http://src{i}.org/", "2000-03-02T20:26:05Z", "text/html", html)]))
        paths.append(str(path))

    serial = o.collect_outlinks_from_warcs(paths, scan_workers=1, excluded_tlds=(".dk",))
    parallel = o.collect_outlinks_from_warcs(paths, scan_workers=3, excluded_tlds=(".dk",))

    assert serial == parallel == {"http://keep.org/a": "20000302202605"}
    # Without the flag the .dk link is kept, so the filter is what removed it.
    assert "http://drop.dk/b" in o.collect_outlinks_from_warcs(paths, scan_workers=1)


def test_excluded_links_are_never_downloaded(tmp_path, monkeypatch, warc_bytes_factory):
    """An excluded link must not reach the download step at all."""
    html = b'<a href="http://keep.org/a">a</a><a href="http://drop.dk/b">b</a>'
    path = tmp_path / "s.warc.gz"
    path.write_bytes(warc_bytes_factory(
        [("http://src.org/", "2000-03-02T20:26:05Z", "text/html", html)]))

    requested = []

    class _RecordingSession:
        def get(self, url, **kwargs):
            requested.append(url)
            return _Resp(url=url, headers={"Content-Type": "text/html"}, content=b"x")

        def mount(self, prefix, adapter):
            pass

        def close(self):
            pass

    monkeypatch.setattr(requests, "Session", _RecordingSession)

    o.fetch_and_archive_outlinks([str(path)], str(tmp_path), "site", threads=1,
                                 scan_workers=1, excluded_tlds=(".dk",))

    assert len(requested) == 1
    assert "http://keep.org/a" in requested[0]
    assert not any("drop.dk" in url for url in requested)


def test_scan_warc_for_outlinks_returns_links_and_known_urls(tmp_path, warc_bytes_factory):
    """The per-file worker reports its own links and every URL captured in that file."""
    path = tmp_path / "src-0001.warc.gz"
    path.write_bytes(warc_bytes_factory(
        [("http://a.com/index.html", "2000-03-02T20:26:05Z", "text/html",
          b'<a href="http://b.com/out.html">x</a>')]
    ))

    outlinks, known, excluded = o.scan_warc_for_outlinks(str(path))
    assert excluded == 0

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


def test_outlinks_writer_leaves_no_file_when_nothing_is_written(tmp_path):
    """A writer that is opened and closed without a single resource creates no part file."""
    writer = o._OutlinksWarcWriter(str(tmp_path), "site_outlinks", 1073741824)
    writer.close()
    assert list(tmp_path.glob("site_outlinks-*.warc.gz")) == []
    assert writer.files_written == 0


def test_outlinks_writer_numbers_the_lazily_opened_first_part(tmp_path):
    """Deferring the open does not shift the numbering: the first part is still -0001."""
    writer = o._OutlinksWarcWriter(str(tmp_path), "site_outlinks", 1073741824)
    writer.write_resource(
        url="http://a.com/x",
        status_code=200,
        reason="OK",
        content_type="text/html",
        content=b"<html></html>",
        warc_date=None,
    )
    writer.close()
    assert (tmp_path / "site_outlinks-0001.warc.gz").exists()
    assert writer.files_written == 1


def test_outlinks_writer_counts_every_part_it_split_into(tmp_path):
    """files_written tracks the parts actually created, not just the last part's number."""
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
    assert writer.files_written == len(list(tmp_path.glob("site_outlinks-*.warc.gz")))


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


def test_fetch_and_archive_outlinks_counts_failures(tmp_path, monkeypatch, caplog):
    """Failed downloads are counted, and a run that archives nothing leaves no WARC behind."""
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
    with caplog.at_level("INFO", logger="warc_outlinks"):
        o.fetch_and_archive_outlinks(
            ["unused"], str(tmp_path), "site", threads=1, max_retries=2)

    # No record was ever written, so no part file should exist -- an empty one is not
    # even a valid gzip member, and the failure is already recorded in the failed list.
    assert list(tmp_path.glob("site_outlinks-*.warc.gz")) == []
    assert (tmp_path / "site_outlinks_failed.txt").exists()

    summary = [r.message for r in caplog.records if "Outlinks WARC summary" in r.message][0]
    assert "Links failed:       1" in summary
    assert "WARC part files:    0" in summary
    assert "Creating outlinks WARC file" not in caplog.text


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


def test_progress_is_reported_while_a_run_is_stalled(tmp_path, monkeypatch, caplog):
    """
    The heartbeat reports even when nothing completes.

    This is the case the count-based line cannot cover: on a stalled archive every
    thread sits in a timeout-and-retry cycle, so a run can go hours without reaching
    the next thousand completions and look identical to a hang.
    """
    release = threading.Event()
    links = {f"http://a.com/p{i}.html": "20000302202605" for i in range(4)}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)

    class _StalledSession:
        def get(self, url, **kwargs):
            # Blocks until the test lets go, standing in for a download that is
            # waiting out its timeout.
            release.wait(timeout=10)
            raise requests.RequestException("stalled")

        def mount(self, *a, **kw):
            pass

        def close(self):
            pass

    monkeypatch.setattr(requests, "Session", lambda: _StalledSession())

    def run():
        with caplog.at_level("INFO", logger="warc_outlinks"):
            o.fetch_and_archive_outlinks(
                ["unused"], str(tmp_path), "site", threads=1, max_retries=0,
                progress_every=1000, progress_interval=0.05,
            )

    worker = threading.Thread(target=run)
    worker.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if [r for r in caplog.records if "Outlink progress" in r.message]:
                break
            time.sleep(0.02)
    finally:
        release.set()
        worker.join(timeout=10)

    progress = [r.message for r in caplog.records if "Outlink progress" in r.message]
    # Reported before a single download finished -- 1000 completions never arrived.
    assert progress, "a stalled run reported nothing"
    assert "0/4" in progress[0]
    assert "in flight" in progress[0]


def test_progress_before_any_completion_does_not_claim_an_eta(tmp_path, monkeypatch, caplog):
    """With no completions there is no rate, so the line says so instead of 'ETA 0s'."""
    release = threading.Event()
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/out.html": "20000302202605"},
    )

    class _StalledSession:
        def get(self, url, **kwargs):
            release.wait(timeout=10)
            raise requests.RequestException("stalled")

        def mount(self, *a, **kw):
            pass

        def close(self):
            pass

    monkeypatch.setattr(requests, "Session", lambda: _StalledSession())

    def run():
        with caplog.at_level("INFO", logger="warc_outlinks"):
            o.fetch_and_archive_outlinks(
                ["unused"], str(tmp_path), "site", threads=1, max_retries=0,
                progress_every=0, progress_interval=0.05,
            )

    worker = threading.Thread(target=run)
    worker.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if [r for r in caplog.records if "Outlink progress" in r.message]:
                break
            time.sleep(0.02)
    finally:
        release.set()
        worker.join(timeout=10)

    progress = [r.message for r in caplog.records if "Outlink progress" in r.message]
    assert progress
    assert "ETA unknown" in progress[0]
    assert "ETA 0s" not in progress[0]


def test_heartbeat_does_not_duplicate_a_fresh_count_based_line(tmp_path, monkeypatch, caplog):
    """A count-based line restarts the heartbeat clock, so the two never double up."""
    links = {f"http://a.com/p{i}.html": "20000302202605" for i in range(4)}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([
            _Resp(url=f"http://a.com/p{i}.html", status_code=200,
                  headers={"Content-Type": "text/html"}, content=b"x")
            for i in range(4)
        ]),
    )

    with caplog.at_level("INFO", logger="warc_outlinks"):
        o.fetch_and_archive_outlinks(
            ["unused"], str(tmp_path), "site", threads=1,
            progress_every=1, progress_interval=30,
        )

    progress = [r.message for r in caplog.records if "Outlink progress" in r.message]
    # One line per completion and no extras: the run finishes far inside the interval.
    assert [m.split()[4] for m in progress] == ["1/4", "2/4", "3/4", "4/4"]


def test_progress_interval_zero_leaves_the_wait_unbounded(tmp_path, monkeypatch, caplog):
    """progress_interval=0 disables the heartbeat and restores the blocking wait."""
    seen = {}
    real_wait = o.wait

    def spy(fs, timeout=None, return_when=None):
        seen["timeout"] = timeout
        return real_wait(fs, timeout=timeout, return_when=return_when)

    monkeypatch.setattr(o, "wait", spy)
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
            ["unused"], str(tmp_path), "site", threads=1,
            progress_every=0, progress_interval=0,
        )

    assert seen["timeout"] is None
    assert not [r for r in caplog.records if "Outlink progress" in r.message]


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


# --------------------------------------------------------------------------- #
# redirect archiving
# --------------------------------------------------------------------------- #
def _wb(ts, url):
    return f"https://web.archive.org/web/{ts}id_/{url}"


def test_split_wayback_url():
    """A Wayback replay URL splits into its timestamp and the archived URL."""
    assert o._split_wayback_url(_wb("20000302202605", "http://a.com/x")) == (
        "20000302202605", "http://a.com/x")


@pytest.mark.parametrize("bad", ["", None, "http://a.com/x", "https://web.archive.org/about"])
def test_split_wayback_url_non_wayback(bad):
    """Anything that is not a Wayback replay URL splits to (None, None)."""
    assert o._split_wayback_url(bad) == (None, None)


def test_redirect_hops_yields_real_redirect():
    """A redirect to a different archived URL is reported as an archivable hop."""
    hop = _Resp(url=_wb("20000302202605", "http://a.com/old"), status_code=301,
                reason="Moved Permanently",
                headers={"Location": _wb("20000302202605", "http://a.com/new")})
    final = _Resp(url=_wb("20000302202605", "http://a.com/new"), history=[hop])

    hops = list(o._redirect_hops(final))
    assert len(hops) == 1
    url, status, reason, location, content, ts = hops[0]
    assert (url, status, location, ts) == (
        "http://a.com/old", 301, "http://a.com/new", "20000302202605")


def test_redirect_hops_skips_temporal_redirect():
    """A redirect to the same URL at a nearer snapshot is Wayback plumbing, not content."""
    hop = _Resp(url=_wb("20000302202605", "http://a.com/x"), status_code=302, reason="Found",
                headers={"Location": _wb("19991231120000", "http://a.com/x")}, memento=False)
    final = _Resp(url=_wb("19991231120000", "http://a.com/x"), history=[hop])

    assert list(o._redirect_hops(final)) == []


def test_redirect_hops_resolves_relative_location():
    """A relative Location header is resolved against the hop's own URL."""
    hop = _Resp(url=_wb("20000302202605", "http://a.com/old"), status_code=302, reason="Found",
                headers={"Location": "/web/20000302202605id_/http://a.com/new"})
    final = _Resp(url=_wb("20000302202605", "http://a.com/new"), history=[hop])

    assert list(o._redirect_hops(final))[0][3] == "http://a.com/new"


def test_redirect_record_written_to_warc(tmp_path, monkeypatch):
    """A followed redirect produces a 3xx record with its Location, plus the target's body."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/old": "20000302202605"},
    )
    hop = _Resp(url=_wb("20000302202605", "http://a.com/old"), status_code=301,
                reason="Moved Permanently",
                headers={"Location": _wb("20000302202605", "http://a.com/new")})
    final = _Resp(url=_wb("20000302202605", "http://a.com/new"), status_code=200, reason="OK",
                  headers={"Content-Type": "text/html"}, content=b"<html>new</html>",
                  history=[hop])
    monkeypatch.setattr(requests, "Session", lambda: _FakeSession([final]))

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1)

    records = _read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))
    assert len(records) == 2
    # The redirect is archived under the URL that issued it...
    assert records[0][0] == "http://a.com/old"
    # ...and the body is archived under the URL it actually came from.
    assert records[1][0] == "http://a.com/new"
    assert records[1][2] == b"<html>new</html>"


def test_record_redirects_false_skips_redirect_records(tmp_path, monkeypatch):
    """With record_redirects=False only the final resource is written."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/old": "20000302202605"},
    )
    hop = _Resp(url=_wb("20000302202605", "http://a.com/old"), status_code=301, reason="Moved",
                headers={"Location": _wb("20000302202605", "http://a.com/new")})
    final = _Resp(url=_wb("20000302202605", "http://a.com/new"), status_code=200, reason="OK",
                  headers={"Content-Type": "text/html"}, content=b"<html>new</html>",
                  history=[hop])
    monkeypatch.setattr(requests, "Session", lambda: _FakeSession([final]))

    o.fetch_and_archive_outlinks(
        ["unused"], str(tmp_path), "site", threads=1, record_redirects=False,
    )

    records = _read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))
    assert [r[0] for r in records] == ["http://a.com/new"]


def test_non_redirected_response_keeps_its_own_url(tmp_path, monkeypatch):
    """A response with no redirect chain is still archived under the requested URL."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/out.html": "20000302202605"},
    )
    final = _Resp(url=_wb("20000302202605", "http://a.com/out.html"), status_code=200,
                  reason="OK", headers={"Content-Type": "text/html"}, content=b"x")
    monkeypatch.setattr(requests, "Session", lambda: _FakeSession([final]))

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1)

    records = _read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))
    assert [r[0] for r in records] == ["http://a.com/out.html"]


# --------------------------------------------------------------------------- #
# archived vs. not-archived responses
# --------------------------------------------------------------------------- #
def test_is_archived_capture():
    """Memento-Datetime marks a response Wayback replayed from a real capture."""
    assert o._is_archived_capture(_Resp(status_code=200)) is True
    assert o._is_archived_capture(_Resp(status_code=200, memento=False)) is False


def test_archived_404_is_written_to_warc(tmp_path, monkeypatch):
    """A 404 the site really served at capture time is archived like any other capture."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/gone.html": "20000302202605"},
    )
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([_Resp(url=_wb("20000302202605", "http://a.com/gone.html"),
                                    status_code=404, reason="Not Found",
                                    headers={"Content-Type": "text/html"},
                                    content=b"<html>gone</html>")]),
    )

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1)

    records = _read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))
    assert [(r[0], r[1]) for r in records] == [("http://a.com/gone.html", "404")]


def test_wayback_miss_is_not_written_to_warc(tmp_path, monkeypatch):
    """A Wayback 'no capture' page is skipped silently -- not archived, not logged."""
    monkeypatch.setattr(
        o, "collect_outlinks_from_warcs",
        lambda paths, **kwargs: {"http://a.com/never.html": "20000302202605"},
    )
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([_Resp(url=_wb("20000302202605", "http://a.com/never.html"),
                                    status_code=404, reason="Not Found",
                                    headers={"Content-Type": "text/html"},
                                    content=b"<html>Wayback Machine</html>",
                                    memento=False)]),
    )

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1)

    # Nothing was archived, so no part file is created at all.
    assert list(tmp_path.glob("site_outlinks-*.warc.gz")) == []
    # An archive miss is not a request failure, and is not worth a line of its own.
    assert not (tmp_path / "site_outlinks_failed.txt").exists()
    assert not (tmp_path / "site_outlinks_not_archived.txt").exists()


# --------------------------------------------------------------------------- #
# Robustness: one bad link must not abort the run
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
# Retry policy
# --------------------------------------------------------------------------- #

def _sleeps(monkeypatch):
    """Record every backoff sleep instead of performing it."""
    recorded = []
    monkeypatch.setattr(o.time, "sleep", lambda seconds: recorded.append(seconds))
    return recorded


@pytest.mark.parametrize("status", [404, 403, 200, 301])
def test_archive_answers_are_never_retried(monkeypatch, status):
    """Anything Wayback actually served is final -- a miss must not cost a backoff."""
    recorded = _sleeps(monkeypatch)
    session = _FakeSession([_Resp(status_code=status, memento=False)])

    response = o._download_archived_resource(
        "http://a.com/x", "20000302202605", session, "ua", 20, 3)

    assert response.status_code == status
    assert session.calls == 1
    assert recorded == []


@pytest.mark.parametrize("status", [429, 500, 503])
def test_throttling_and_server_errors_are_retried(monkeypatch, status):
    """Throttling and server errors are transient and worth another attempt."""
    recorded = _sleeps(monkeypatch)
    session = _FakeSession([_Resp(status_code=status) for _ in range(4)])

    assert o._download_archived_resource(
        "http://a.com/x", "20000302202605", session, "ua", 20, 3) is None
    assert session.calls == 4                 # first attempt plus three retries
    assert recorded == [1, 2, 4]              # exponential backoff


def test_retry_succeeds_after_a_transient_failure(monkeypatch):
    """A capture behind one 503 is kept rather than discarded."""
    _sleeps(monkeypatch)
    session = _FakeSession([
        _Resp(status_code=503),
        _Resp(status_code=200, content=b"real capture"),
    ])

    response = o._download_archived_resource(
        "http://a.com/x", "20000302202605", session, "ua", 20, 3)

    assert response.content == b"real capture"
    assert session.calls == 2


def test_transport_errors_are_retried(monkeypatch):
    """A read timeout is transport failure, not an answer; archive.org's tail is long."""
    _sleeps(monkeypatch)
    session = _FakeSession([
        requests.ReadTimeout("too slow"),
        _Resp(status_code=200, content=b"arrived"),
    ])

    response = o._download_archived_resource(
        "http://a.com/x", "20000302202605", session, "ua", 20, 3)

    assert response.content == b"arrived"


def test_retry_after_seconds_header_is_honoured(monkeypatch):
    """A numeric Retry-After overrides our own backoff when it asks for longer."""
    recorded = _sleeps(monkeypatch)
    session = _FakeSession([
        _Resp(status_code=429, headers={"Retry-After": "30"}) for _ in range(2)
    ])

    o._download_archived_resource("http://a.com/x", "20000302202605", session, "ua", 20, 1)

    assert recorded == [30]


def test_retry_after_http_date_is_honoured(monkeypatch):
    """Retry-After may be an HTTP date rather than a delay."""
    from datetime import datetime, timedelta, timezone
    from email.utils import format_datetime

    when = datetime.now(timezone.utc) + timedelta(seconds=25)
    response = _Resp(status_code=429, headers={"Retry-After": format_datetime(when)})

    assert 20 <= o._retry_after_seconds(response, 1) <= 30


@pytest.mark.parametrize("header,expected", [
    ("2", 4),            # shorter than our own backoff: keep the backoff
    ("9999", 60),        # absurd: capped so one thread cannot stall the run
    ("not-a-date", 4),   # unparseable: fall back
    (None, 4),           # absent
])
def test_retry_after_bounds(header, expected):
    """Retry-After is a floor on our backoff and is capped at MAX_RETRY_WAIT_SECONDS."""
    response = _Resp(status_code=429, headers={"Retry-After": header} if header else None)
    assert o._retry_after_seconds(response, 4) == expected


def test_retry_after_on_a_failed_request():
    """With no response at all the caller's own backoff is used."""
    assert o._retry_after_seconds(None, 8) == 8


def test_retry_is_logged_at_debug_not_info(monkeypatch, caplog):
    """Retries are routine under load and must not bury the INFO progress lines."""
    _sleeps(monkeypatch)
    session = _FakeSession([_Resp(status_code=503) for _ in range(2)])

    with caplog.at_level(logging.DEBUG, logger="warc_outlinks"):
        o._download_archived_resource(
            "http://a.com/x", "20000302202605", session, "ua", 20, 1)

    retry_records = [r for r in caplog.records if "Retrying" in r.getMessage()]
    assert retry_records, "the retry should still be logged"
    assert all(r.levelno == logging.DEBUG for r in retry_records)


def test_download_defaults_are_generous_enough_for_the_archive():
    """The defaults are the point of this policy, so pin them."""
    assert o.DEFAULT_TIMEOUT_SECONDS >= 20
    assert o.DEFAULT_MAX_RETRIES >= 3
    signature = inspect.signature(o.fetch_and_archive_outlinks).parameters
    assert signature["timeout"].default == o.DEFAULT_TIMEOUT_SECONDS
    assert signature["max_retries"].default == o.DEFAULT_MAX_RETRIES


@pytest.fixture
def latin1_redirect_server():
    """A server that answers like Wayback does for a 1990s national-language URL:
    a 302 whose Location carries raw latin-1 bytes (Danish "a-ring" is 0xe5)."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(302 if "redir" in self.path else 200)
            if "redir" in self.path:
                self.send_header("Location", b"/target/l\xe5n.html".decode("latin-1"))
            self.send_header("Content-Type", "text/html")
            self.send_header("Memento-Datetime", "Thu, 02 Mar 2000 20:26:05 GMT")
            self.end_headers()
            self.wfile.write(b"body")

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_plain_requests_dies_on_an_undecodable_redirect(latin1_redirect_server):
    """The failure being guarded against is real and lives inside requests itself."""
    with pytest.raises(UnicodeDecodeError):
        requests.Session().get(latin1_redirect_server + "/redir", timeout=5)


def test_tolerate_undecodable_redirects_follows_the_redirect(latin1_redirect_server):
    """Hardening the session follows the redirect instead of raising."""
    session = requests.Session()
    o._tolerate_undecodable_redirects(session)

    response = session.get(latin1_redirect_server + "/redir", timeout=5)

    assert response.status_code == 200
    # The chain survives, which is what _redirect_hops needs to archive the 302.
    assert [hop.status_code for hop in response.history] == [302]


def test_tolerate_undecodable_redirects_ignores_a_stand_in_session():
    """A session object without get_redirect_target is left alone rather than failing."""
    class Bare:
        pass

    bare = Bare()
    o._tolerate_undecodable_redirects(bare)          # must not raise
    assert not hasattr(bare, "get_redirect_target")


def test_undecodable_redirect_does_not_abort_the_run(tmp_path, monkeypatch,
                                                     latin1_redirect_server):
    """One link with an undecodable redirect must not cost the rest of the run."""
    links = {"http://bad.dk/redir.html": "20000302202605",
             "http://good.org/plain.html": "20000302202605"}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)
    # Route the downloader at the local server; "redir" in the path picks the 302.
    monkeypatch.setattr(
        o, "WAYBACK_RAW_URL",
        latin1_redirect_server + "/{timestamp}/{url}",
    )

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=2)

    # Both links archived: the redirect was followed rather than killing the run.
    # (No 302 hop record here -- _redirect_hops only recognises real Wayback replay
    # URLs, and this fixture serves from localhost.)
    records = _read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))
    assert sorted(r[0] for r in records) == [
        "http://bad.dk/redir.html", "http://good.org/plain.html"]
    assert not (tmp_path / "site_outlinks_failed.txt").exists()


def test_unexpected_error_on_one_link_is_recorded_not_fatal(tmp_path, monkeypatch):
    """An error while handling one result must cost that link, not the whole run."""
    links = {f"http://a.com/{i}.html": "20000302202605" for i in range(5)}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)
    monkeypatch.setattr(
        requests, "Session",
        lambda: _FakeSession([
            _Resp(url=_wb("20000302202605", f"http://a.com/{i}.html"),
                  headers={"Content-Type": "text/html"}, content=b"x")
            for i in range(5)
        ]),
    )

    real_write = o._OutlinksWarcWriter.write_resource
    calls = {"n": 0}

    def _flaky_write(self, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 3:
            raise ValueError("boom")
        return real_write(self, *args, **kwargs)

    monkeypatch.setattr(o._OutlinksWarcWriter, "write_resource", _flaky_write)

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1)

    assert len(_read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))) == 4
    # The link that failed is recorded so it can be retried.
    assert len((tmp_path / "site_outlinks_failed.txt").read_text().splitlines()) == 1


def test_completed_responses_are_released_during_the_run(tmp_path, monkeypatch):
    """Finished downloads must not stay resident -- a multi-million-link run cannot
    hold every response body until the end."""
    links = {f"http://a.com/{i}.html": "20000302202605" for i in range(60)}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)

    handed_out = []          # weakrefs to every response the session returned
    alive_midway = []

    class _WeakSession:
        def get(self, url, **kwargs):
            resp = _Resp(url=url, headers={"Content-Type": "text/html"},
                         content=b"x" * 1000)
            handed_out.append(weakref.ref(resp))
            if len(handed_out) == 50:
                # Everything from the first half of the run should already be
                # collectable by now; only the in-flight window may still be alive.
                gc.collect()
                alive_midway.append(sum(1 for ref in handed_out[:20] if ref() is not None))
            return resp

        def mount(self, prefix, adapter):
            pass

        def close(self):
            pass

    monkeypatch.setattr(requests, "Session", _WeakSession)

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=1)

    assert len(_read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))) == 60
    assert alive_midway == [0]


def test_in_flight_work_is_bounded_by_the_window(tmp_path, monkeypatch):
    """Only a window of downloads is ever submitted, however many links there are."""
    links = {f"http://a.com/{i}.html": "20000302202605" for i in range(200)}
    monkeypatch.setattr(o, "collect_outlinks_from_warcs", lambda paths, **kwargs: links)

    submitted = 0
    max_outstanding = 0
    real_submit = ThreadPoolExecutor.submit

    def _counting_submit(self, fn, *args, **kwargs):
        nonlocal submitted, max_outstanding
        submitted += 1
        max_outstanding = max(max_outstanding, submitted - completed_count[0])
        return real_submit(self, fn, *args, **kwargs)

    completed_count = [0]

    class _CountingSession:
        def get(self, url, **kwargs):
            completed_count[0] += 1
            return _Resp(url=url, headers={"Content-Type": "text/html"}, content=b"x")

        def mount(self, prefix, adapter):
            pass

        def close(self):
            pass

    monkeypatch.setattr(ThreadPoolExecutor, "submit", _counting_submit)
    monkeypatch.setattr(requests, "Session", _CountingSession)

    o.fetch_and_archive_outlinks(["unused"], str(tmp_path), "site", threads=2)

    # threads * 4 is the window; nothing beyond it is ever queued up front.
    assert max_outstanding <= 2 * 4
    assert submitted == 200
    assert len(_read_outlink_records(str(tmp_path / "site_outlinks-0001.warc.gz"))) == 200


def test_replay_level_redirect_hop_is_not_archived():
    """A hop Wayback generated itself (no Memento-Datetime) is not a historical redirect."""
    # Same resource, differing only by the default ":80" Wayback canonicalises away.
    hop = _Resp(url=_wb("20000302202605", "http://a.com:80/x"), status_code=302, reason="Found",
                headers={"Location": _wb("20000302202605", "http://a.com/x")}, memento=False)
    final = _Resp(url=_wb("20000302202605", "http://a.com/x"), history=[hop])

    assert list(o._redirect_hops(final)) == []

