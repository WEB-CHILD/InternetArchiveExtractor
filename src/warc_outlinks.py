"""
Outgoing-link archiving step.

After the primary WARC files have been created, this module walks through those
WARC files, extracts the outgoing links (``<a href>`` targets) and embedded
resources (images, scripts, stylesheets, frames, media, objects) from every
archived HTML page, downloads each one from the Wayback Machine using the *same*
capture date as the page that referenced it, and packages everything into a
separate, properly named WARC file (``<basename>_outlinks-XXXX.warc.gz``).
"""

import os
import re
import time
from io import BytesIO
from datetime import datetime
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from selectolax.lexbor import LexborHTMLParser
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders

from logging_config import get_logger

logger = get_logger(__name__)

# Wayback "id_" modifier returns the raw archived resource without rewriting or toolbar.
WAYBACK_RAW_URL = "https://web.archive.org/web/{timestamp}id_/{url}"
DEFAULT_USER_AGENT = "InternetArchiveExtractor/0.0.11 (+outlinks)"

# Matches the 14-digit capture timestamp in a Wayback URL (e.g. /web/20000302202605/...).
_WAYBACK_TS_RE = re.compile(r"/web/(\d{14})")
# Schemes that are not fetchable resources and should be ignored.
_SKIP_SCHEMES = {"mailto", "javascript", "tel", "data", "ftp", "file"}


# For each HTML tag, the attribute(s) that hold a fetchable URL. Covers both
# outgoing hyperlinks (<a href>) and embedded resources (images, scripts,
# stylesheets, frames, media, objects).
_URL_ATTRS = {
    "a": ("href",),
    "area": ("href",),
    "img": ("src",),
    "script": ("src",),
    "link": ("href",),
    "iframe": ("src",),
    "frame": ("src",),
    "source": ("src",),
    "embed": ("src",),
    "video": ("src", "poster"),
    "audio": ("src",),
    "object": ("data",),
}


# CSS selector matching every tag that can carry a fetchable URL.
_URL_TAG_SELECTOR = ",".join(_URL_ATTRS)

# Leading "scheme:" of a URL. Used instead of a full urlparse() for the two scheme
# checks below, which are the hot path of the scan (see _normalize_links).
_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*):")


def _normalize_links(hrefs, base_url):
    """
    Resolve raw href values against ``base_url`` and filter them down to the
    fetchable HTTP(S) URLs, de-duplicated with document order preserved.

    Cheap regex scheme matching replaces urlparse() here, and the fragment is cut
    with a string slice instead of urldefrag(). Both are equivalent for the inputs
    this sees, and avoid three of the four urlsplit() calls each href would
    otherwise cost -- URL normalization is roughly half of total scan time.
    """
    links = []
    seen = set()
    for href in hrefs:
        href = href.strip()
        if not href or href[0] == "#":
            continue

        match = _SCHEME_RE.match(href)
        if match and match.group(1).lower() in _SKIP_SCHEMES:
            continue

        absolute = urljoin(base_url, href)
        fragment = absolute.find("#")
        if fragment != -1:
            absolute = absolute[:fragment]

        match = _SCHEME_RE.match(absolute)
        if not match or not match.group(1).lower().startswith("http"):
            continue

        if absolute not in seen:
            seen.add(absolute)
            links.append(absolute)
    return links


def extract_outgoing_links(html_bytes, base_url):
    """
    Parse an HTML document and return a de-duplicated, order-preserving list of
    absolute URLs to fetch: outgoing ``<a>`` hyperlinks plus embedded resources
    (images, scripts, stylesheets, frames, media, objects).

    Relative links are resolved against ``base_url`` (the page's original URL).
    Fragments are stripped and non-HTTP(S) schemes are discarded.
    """
    text = html_bytes.decode("utf-8", errors="replace")
    try:
        tree = LexborHTMLParser(text)
    except Exception as e:  # malformed markup should never abort the run
        logger.debug(f"HTML parse error for {base_url}: {e}")
        return []

    hrefs = []
    for node in tree.css(_URL_TAG_SELECTOR):
        for attr in _URL_ATTRS.get(node.tag, ()):
            value = node.attributes.get(attr)
            if value:
                hrefs.append(value)

    return _normalize_links(hrefs, base_url)


def _warc_date_to_timestamp(warc_date):
    """Convert a WARC-Date ('2000-03-02T20:26:05Z') to a Wayback timestamp ('20000302202605')."""
    try:
        return datetime.strptime(warc_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M%S")
    except (TypeError, ValueError):
        return None


def _timestamp_to_warc_date(timestamp):
    """Convert a Wayback timestamp ('20000302202605') to a WARC-Date ('2000-03-02T20:26:05Z')."""
    try:
        return datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return None


def scan_warc_for_outlinks(warc_path):
    """
    Scan a single WARC file for outgoing links.

    This is the unit of work handed to the scan worker processes, so it must stay a
    module-level function (picklable) and must not log through a shared handler.

    Args:
        warc_path (str): The WARC file to scan.

    Returns:
        tuple[dict, set]: ``(outlinks, known_urls)`` where ``outlinks`` maps
        ``absolute_url -> wayback_timestamp`` in document order for this file, and
        ``known_urls`` is every URL captured in this file.
    """
    outlinks = {}
    known_urls = set()

    with open(warc_path, "rb") as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type != "response":
                continue

            target_uri = record.rec_headers.get_header("WARC-Target-URI")
            if target_uri:
                known_urls.add(target_uri)

            content_type = (
                record.http_headers.get_header("Content-Type")
                if record.http_headers else None
            )
            if not content_type or "html" not in content_type.lower():
                continue

            timestamp = _warc_date_to_timestamp(
                record.rec_headers.get_header("WARC-Date")
            )
            if timestamp is None:
                continue

            payload = record.content_stream().read()
            if not payload:
                continue

            for link in extract_outgoing_links(payload, target_uri):
                # Keep the first (earliest-seen) timestamp for each unique URL.
                outlinks.setdefault(link, timestamp)

    return outlinks, known_urls


def collect_outlinks_from_warcs(source_warc_paths, scan_workers=None):
    """
    Walk through the given WARC files and gather every outgoing link found in the
    archived HTML pages.

    Scanning is CPU-bound -- HTML parsing dominates it, with file I/O a rounding
    error -- so the files are scanned in parallel across worker *processes*; threads
    would be serialized by the GIL. Results are merged in source-file order, which
    makes the output identical to a serial scan.

    Args:
        source_warc_paths (list[str]): WARC files to scan.
        scan_workers (int, optional): Number of scan processes. Defaults to one per
            CPU core, capped at the number of files. 1 scans in the current process.

    Returns:
        dict: ``absolute_url -> wayback_timestamp`` (the capture date of the page that
        linked to it). Links pointing at pages already captured in the source WARC
        files are excluded so they are not downloaded twice.
    """
    if scan_workers is None:
        scan_workers = os.cpu_count() or 1
    scan_workers = max(1, min(scan_workers, len(source_warc_paths)))

    outlinks = {}
    known_urls = set()

    if scan_workers == 1:
        results = []
        for warc_path in source_warc_paths:
            logger.debug(f"Scanning for outgoing links: {warc_path}")
            results.append(scan_warc_for_outlinks(warc_path))
    else:
        logger.info(
            f"Scanning {len(source_warc_paths)} WARC file(s) across {scan_workers} process(es)..."
        )
        with ProcessPoolExecutor(max_workers=scan_workers) as executor:
            # Mapping in submission order keeps the merge deterministic and identical
            # to a serial scan, which as_completed() would not.
            futures = [
                executor.submit(scan_warc_for_outlinks, path) for path in source_warc_paths
            ]
            results = []
            for warc_path, future in zip(source_warc_paths, futures):
                results.append(future.result())
                logger.debug(f"Finished scanning: {warc_path}")

    for file_outlinks, file_known_urls in results:
        known_urls |= file_known_urls
        for link, timestamp in file_outlinks.items():
            outlinks.setdefault(link, timestamp)

    # Don't re-fetch pages that are already present in the source archive.
    for url in known_urls:
        outlinks.pop(url, None)

    return outlinks


def _download_archived_resource(url, timestamp, session, user_agent, timeout, max_retries, progress=""):
    """
    Fetch a single resource from the Wayback Machine at the given capture date.

    Returns the ``requests.Response`` (after following redirects to the nearest
    snapshot) or ``None`` if the request could not be completed.

    ``progress`` is an "n/total" label identifying this link's position in the
    submitted work; it is prefixed to every log line so that interleaved output from
    the download threads can still be placed in the overall run.
    """
    request_url = WAYBACK_RAW_URL.format(timestamp=timestamp, url=url)
    prefix = f"[{progress}] " if progress else ""
    logger.debug(f"{prefix}Downloading {request_url}...")
    for attempt in range(max_retries + 1):
        try:
            response = session.get(
                request_url,
                headers={"User-Agent": user_agent},
                timeout=timeout,
                allow_redirects=True,
            )
        except requests.RequestException as e:
            logger.warning(f"{prefix}Request error for {request_url} (attempt {attempt + 1}): {e}")
            response = None

        # Back off and retry on throttling / transient server errors.
        if response is not None and response.status_code not in (429,) and response.status_code < 500:
            return response
        if attempt < max_retries:
            logger.info(f"{prefix}Retrying {request_url} after {2 ** attempt} seconds (attempt {attempt + 1})...")
            time.sleep(2 ** attempt)

    # Retries exhausted: a final throttled/5xx response is a failure, not a
    # resource worth archiving, so don't write it into the outlinks WARC.
    return None


def _format_duration(seconds):
    """Render a number of seconds as a compact 'HHh MMm SSs' string for progress logs."""
    seconds = int(max(0, seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def _actual_capture_timestamp(response, requested_timestamp):
    """Prefer the real snapshot timestamp from the redirected URL, falling back to the requested one."""
    match = _WAYBACK_TS_RE.search(response.url or "")
    return match.group(1) if match else requested_timestamp


def fetch_and_archive_outlinks(
    source_warc_paths,
    output_dir,
    output_basename,
    *,
    threads=10,
    scan_workers=None,
    progress_every=1000,
    timeout=5,
    max_retries=1,
    user_agent=DEFAULT_USER_AGENT,
    max_size_bytes=1073741824,
):
    """
    End-to-end outgoing-link archiving for a set of WARC files.

    Collects outgoing links from ``source_warc_paths``, downloads each one from the
    Wayback Machine at the capture date of the referencing page, and writes the
    results to ``<output_dir>/<output_basename>_outlinks-XXXX.warc.gz``.

    Requests that could not be completed are written, one Wayback request URL per
    line, to ``<output_dir>/<output_basename>_outlinks_failed.txt``. That file is
    only created if there is at least one failure, and is overwritten on a re-run.

    Downloads run concurrently across threads; the WARC writing itself is
    serialized, so the output is written from a single thread.

    Args:
        source_warc_paths (list[str]): WARC files to extract outgoing links from.
        output_dir (str): Directory the downloaded resources are written to.
        output_basename (str): Base name of the source WARC (without the ``-XXXX`` suffix).
        threads (int): Number of concurrent download threads. Defaults to 5 (matching main.py).
        scan_workers (int, optional): Number of processes used to scan the source WARC
            files for outgoing links. Defaults to one per CPU core.
        progress_every (int): Emit an INFO progress summary every N completed downloads.
            Defaults to 1000. Set to 0 to report only at the end.
        timeout (int): Per-request timeout in seconds.
        max_retries (int): Retries on throttling / transient server errors.
        user_agent (str): User-Agent header sent with each request.
        max_size_bytes (int): Size threshold at which a new WARC part file is started.
    """
    outlinks = collect_outlinks_from_warcs(source_warc_paths, scan_workers=scan_workers)
    if not outlinks:
        logger.info(f"No outgoing links found for '{output_basename}'. Skipping outlinks WARC.")
        return

    threads = max(1, threads)
    logger.info(
        f"Found {len(outlinks)} unique outgoing link(s) for '{output_basename}'. "
        f"Downloading with {threads} thread(s)..."
    )

    output_filename = f"{output_basename}_outlinks"
    writer_state = _OutlinksWarcWriter(output_dir, output_filename, max_size_bytes)

    # Opened on the first failure so a clean run leaves no empty file behind, and
    # line-buffered so the list survives a run that is killed part-way through.
    failures_path = os.path.join(output_dir, f"{output_filename}_failed.txt")
    failures_file = None

    session = requests.Session()
    
    # Size the connection pool to the thread count. This ensures it is possible to have all threads concurrently downloading without waiting for a connection.
    adapter = HTTPAdapter(pool_connections=threads, pool_maxsize=threads)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    total = len(outlinks)

    def _download(numbered_item):
        index, (url, timestamp) = numbered_item
        response = _download_archived_resource(
            url, timestamp, session, user_agent, timeout, max_retries,
            progress=f"{index}/{total}",
        )
        return url, timestamp, response

    success = 0
    failed = 0
    completed = 0
    started_at = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            logger.info(f"Submitting {total} outgoing link(s) for download through {threads} thread(s)...")
            futures = [
                executor.submit(_download, item)
                for item in enumerate(outlinks.items(), start=1)
            ]
            # Results are consumed (and written) here in a single thread, so the
            # non-thread-safe writer is only ever touched serially.
            for future in as_completed(futures):
                url, timestamp, response = future.result()
                if response is None:
                    failed += 1
                    if failures_file is None:
                        failures_file = open(failures_path, "w", encoding="utf-8", buffering=1)
                        logger.info(f"Recording failed requests in: {failures_path}")
                    # The full Wayback request URL, so the line is directly re-fetchable
                    # and still carries both the original URL and its capture timestamp.
                    failures_file.write(
                        WAYBACK_RAW_URL.format(timestamp=timestamp, url=url) + "\n"
                    )
                else:
                    capture_ts = _actual_capture_timestamp(response, timestamp)
                    writer_state.write_resource(
                        url=url,
                        status_code=response.status_code,
                        reason=response.reason or "",
                        content_type=response.headers.get("Content-Type", "application/octet-stream"),
                        content=response.content,
                        warc_date=_timestamp_to_warc_date(capture_ts),
                    )
                    success += 1

                completed += 1
                # Completed (not just successful) downloads drive the progress line, so
                # a run with many failures still reports that it is moving.
                if progress_every and (completed % progress_every == 0 or completed == total):
                    elapsed = time.monotonic() - started_at
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total - completed) / rate if rate > 0 else 0
                    logger.info(
                        f"Outlink progress for '{output_basename}': "
                        f"{completed}/{total} ({100 * completed / total:.1f}%) - "
                        f"{success} ok, {failed} failed - "
                        f"{rate:.1f}/s, elapsed {_format_duration(elapsed)}, "
                        f"ETA {_format_duration(eta)}"
                    )
    finally:
        writer_state.close()
        session.close()
        if failures_file is not None:
            failures_file.close()

    logger.info(
        f"\nOutlinks WARC summary for '{output_basename}':\n"
        f"  Links downloaded:   {success}\n"
        f"  Links failed:       {failed}\n"
        f"  Total outgoing:     {len(outlinks)}\n"
        f"  WARC part files:    {writer_state.file_number}"
        + (f"\n  Failed requests in: {failures_path}" if failed else "")
    )


class _OutlinksWarcWriter:
    """Writes downloaded resources to one or more size-bounded ``.warc.gz`` part files."""

    def __init__(self, output_dir, output_filename, max_size_bytes):
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.max_size_bytes = max_size_bytes
        self.file_number = 1
        self.current_size = 0
        os.makedirs(output_dir, exist_ok=True)
        self._open_new_file()

    def _current_path(self):
        return os.path.join(
            self.output_dir, f"{self.output_filename}-{self.file_number:04d}.warc.gz"
        )

    def _open_new_file(self):
        self.warc_path = self._current_path()
        logger.info(f"Creating outlinks WARC file: {self.warc_path}")
        self.stream = open(self.warc_path, "wb")
        self.writer = WARCWriter(self.stream, gzip=True)

    def write_resource(self, url, status_code, reason, content_type, content, warc_date):
        if self.current_size >= self.max_size_bytes:
            self.stream.close()
            logger.info(
                f"Completed outlinks WARC file: {self.warc_path} "
                f"(Size: {self.current_size / (1024 ** 3):.2f} GB)"
            )
            self.file_number += 1
            self.current_size = 0
            self._open_new_file()

        http_headers = StatusAndHeaders(
            f"{status_code} {reason}".strip(),
            [("Content-Type", content_type)],
            protocol="HTTP/1.0",
        )
        record = self.writer.create_warc_record(
            url,
            "response",
            payload=BytesIO(content),
            length=len(content),
            http_headers=http_headers,
            warc_headers_dict={"WARC-Date": warc_date} if warc_date else None,
        )
        self.writer.write_record(record)
        self.current_size += len(content)

    def close(self):
        self.stream.close()
        logger.info(
            f"Completed outlinks WARC file: {self.warc_path} "
            f"(Size: {self.current_size / (1024 ** 3):.2f} GB)"
        )
