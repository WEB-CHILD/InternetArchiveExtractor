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
from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag, urlparse

import requests
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


class _LinkExtractor(HTMLParser):
    """Collects outgoing hyperlinks and embedded-resource URLs from an HTML document."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []

    def handle_starttag(self, tag, attrs):
        url_attrs = _URL_ATTRS.get(tag)
        if not url_attrs:
            return
        for name, value in attrs:
            if name in url_attrs and value:
                self.links.append(value)


def extract_outgoing_links(html_bytes, base_url):
    """
    Parse an HTML document and return a de-duplicated, order-preserving list of
    absolute URLs to fetch: outgoing ``<a>`` hyperlinks plus embedded resources
    (images, scripts, stylesheets, frames, media, objects).

    Relative links are resolved against ``base_url`` (the page's original URL).
    Fragments are stripped and non-HTTP(S) schemes are discarded.
    """
    text = html_bytes.decode("utf-8", errors="replace")
    parser = _LinkExtractor()
    try:
        parser.feed(text)
    except Exception as e:  # malformed markup should never abort the run
        logger.debug(f"HTML parse error for {base_url}: {e}")

    links = []
    seen = set()
    for href in parser.links:
        href = href.strip()
        if not href or href.startswith("#"):
            continue
        if urlparse(href).scheme.lower() in _SKIP_SCHEMES:
            continue
        absolute, _ = urldefrag(urljoin(base_url, href))
        if not urlparse(absolute).scheme.lower().startswith("http"):
            continue
        if absolute not in seen:
            seen.add(absolute)
            links.append(absolute)
    return links


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


def collect_outlinks_from_warcs(source_warc_paths):
    """
    Walk through the given WARC files and gather every outgoing link found in the
    archived HTML pages.

    Returns a dict mapping ``absolute_url -> wayback_timestamp`` (the capture date of
    the page that linked to it). Links that point to pages already captured in the
    source WARC files are excluded so they are not downloaded twice.
    """
    outlinks = {}
    known_urls = set()

    for warc_path in source_warc_paths:
        logger.debug(f"Scanning for outgoing links: {warc_path}")
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
                    logger.debug(f"No usable capture date for {target_uri}; skipping its links.")
                    continue

                payload = record.content_stream().read()
                if not payload:
                    continue

                for link in extract_outgoing_links(payload, target_uri):
                    # Keep the first (earliest-seen) timestamp for each unique URL.
                    outlinks.setdefault(link, timestamp)

    # Don't re-fetch pages that are already present in the source archive.
    for url in known_urls:
        outlinks.pop(url, None)

    return outlinks


def _download_archived_resource(url, timestamp, session, user_agent, timeout, max_retries):
    """
    Fetch a single resource from the Wayback Machine at the given capture date.

    Returns the ``requests.Response`` (after following redirects to the nearest
    snapshot) or ``None`` if the request could not be completed.
    """
    request_url = WAYBACK_RAW_URL.format(timestamp=timestamp, url=url)
    for attempt in range(max_retries + 1):
        try:
            response = session.get(
                request_url,
                headers={"User-Agent": user_agent},
                timeout=timeout,
                allow_redirects=True,
            )
        except requests.RequestException as e:
            logger.debug(f"Request error for {request_url} (attempt {attempt + 1}): {e}")
            response = None

        # Back off and retry on throttling / transient server errors.
        if response is not None and response.status_code not in (429,) and response.status_code < 500:
            return response
        if attempt < max_retries:
            time.sleep(2 ** attempt)

    if response is not None:
        return response
    return None


def _actual_capture_timestamp(response, requested_timestamp):
    """Prefer the real snapshot timestamp from the redirected URL, falling back to the requested one."""
    match = _WAYBACK_TS_RE.search(response.url or "")
    return match.group(1) if match else requested_timestamp


def fetch_and_archive_outlinks(
    source_warc_paths,
    output_dir,
    output_basename,
    *,
    delay=0.1,
    timeout=30,
    max_retries=2,
    user_agent=DEFAULT_USER_AGENT,
    max_size_bytes=1073741824,
):
    """
    End-to-end outgoing-link archiving for one set of source WARC files.

    Collects outgoing links from ``source_warc_paths``, downloads each one from the
    Wayback Machine at the capture date of the referencing page, and writes the
    results to ``<output_dir>/<output_basename>_outlinks-XXXX.warc.gz``.

    Args:
        source_warc_paths (list[str]): WARC files to extract outgoing links from.
        output_dir (str): Directory the outlinks WARC file(s) are written to.
        output_basename (str): Base name of the source WARC (without the ``-XXXX`` suffix).
        delay (float): Politeness delay in seconds between requests to the Wayback Machine.
        timeout (int): Per-request timeout in seconds.
        max_retries (int): Retries on throttling / transient server errors.
        user_agent (str): User-Agent header sent with each request.
        max_size_bytes (int): Size threshold at which a new WARC part file is started.
    """
    outlinks = collect_outlinks_from_warcs(source_warc_paths)
    if not outlinks:
        logger.info(f"No outgoing links found for '{output_basename}'. Skipping outlinks WARC.")
        return

    logger.info(f"Found {len(outlinks)} unique outgoing link(s) for '{output_basename}'. Downloading...")

    output_filename = f"{output_basename}_outlinks"
    writer_state = _OutlinksWarcWriter(output_dir, output_filename, max_size_bytes)

    session = requests.Session()
    success = 0
    failed = 0
    try:
        for index, (url, timestamp) in enumerate(outlinks.items(), start=1):
            if index % 100 == 0:
                logger.debug(f"Outlink progress: {index}/{len(outlinks)}")

            response = _download_archived_resource(
                url, timestamp, session, user_agent, timeout, max_retries
            )
            if response is None:
                failed += 1
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

            if delay:
                time.sleep(delay)
    finally:
        writer_state.close()
        session.close()

    logger.info(
        f"\nOutlinks WARC summary for '{output_basename}':\n"
        f"  Links downloaded:   {success}\n"
        f"  Links failed:       {failed}\n"
        f"  Total outgoing:     {len(outlinks)}\n"
        f"  WARC part files:    {writer_state.file_number}"
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
        logger.debug(f"Completed outlinks WARC file: {self.warc_path}")
