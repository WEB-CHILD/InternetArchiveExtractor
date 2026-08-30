"""Tests for internet_archive_downloader.py.

Network and pywaybackup interactions are stubbed out; these tests cover the
pure logic (URL parsing, filename construction, date-range selection) and the
filesystem/SQLite housekeeping helpers.
"""

import os

import pytest
from sqlalchemy import create_engine, inspect, text

import internet_archive_downloader as d
from constants import Period
from wayback_date_object import WaybackDateObject


# --------------------------------------------------------------------------- #
# get_wayback_date_and_archived_url
# --------------------------------------------------------------------------- #
def test_parses_wayback_url():
    """A Wayback Machine URL is split into a WaybackDateObject and the original archived URL."""
    date, url = d.get_wayback_date_and_archived_url(
        "https://web.archive.org/web/20030409193011/http://www.example.com/page"
    )
    assert isinstance(date, WaybackDateObject)
    assert date.wayback_format() == "20030409193011"
    assert url == "http://www.example.com/page"


def test_non_wayback_url_returns_none_date():
    """A plain URL (not a Wayback URL) returns None for the date and the URL unchanged."""
    date, url = d.get_wayback_date_and_archived_url("http://www.example.com/live")
    assert date is None
    assert url == "http://www.example.com/live"


# --------------------------------------------------------------------------- #
# create_waybackup_filename
# --------------------------------------------------------------------------- #
def test_create_waybackup_filename_basic():
    """A simple URL is converted to the expected 'waybackup_<sanitized>.csv' filename."""
    assert (
        d.create_waybackup_filename("http://www.example.com/page")
        == "waybackup_http.www.example.com.page.csv"
    )


def test_create_waybackup_filename_collapses_repeated_punctuation():
    """Special characters in query strings are sanitized without leaving consecutive dots."""
    name = d.create_waybackup_filename("http://www.example.com/page?x=1")
    assert name.startswith("waybackup_")
    assert name.endswith(".csv")
    assert ".." not in name


# --------------------------------------------------------------------------- #
# cleanup_temporary_files
# --------------------------------------------------------------------------- #
def test_cleanup_temporary_files_removes_contents(tmp_path):
    """cleanup_temporary_files deletes all files and subdirectories but keeps the folder itself."""
    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / "file.txt").write_text("x")
    subdir = snap / "sub"
    subdir.mkdir()
    (subdir / "nested.txt").write_text("y")

    d.cleanup_temporary_files(str(snap))

    assert snap.exists()  # the folder itself is kept
    assert list(snap.iterdir()) == []


def test_cleanup_temporary_files_missing_dir_is_noop(tmp_path):
    """cleanup_temporary_files does not raise when the target directory does not exist."""
    d.cleanup_temporary_files(str(tmp_path / "does-not-exist"))


# --------------------------------------------------------------------------- #
# copy_log_files
# --------------------------------------------------------------------------- #
def test_copy_log_files_copies_logs(tmp_path):
    """copy_log_files copies .log files to the destination and ignores other file types."""
    src = tmp_path / "snap"
    src.mkdir()
    (src / "a.log").write_text("log a")
    (src / "ignore.txt").write_text("nope")
    dest = tmp_path / "logs"

    d.copy_log_files(str(src), str(dest))

    assert (dest / "a.log").read_text() == "log a"
    assert not (dest / "ignore.txt").exists()


def test_copy_log_files_renames_on_conflict(tmp_path):
    """copy_log_files appends a timestamp to the filename when the destination file already exists."""
    src = tmp_path / "snap"
    src.mkdir()
    (src / "a.log").write_text("new")
    dest = tmp_path / "logs"
    dest.mkdir()
    (dest / "a.log").write_text("existing")

    d.copy_log_files(str(src), str(dest))

    # Original is preserved and a timestamped copy is added.
    assert (dest / "a.log").read_text() == "existing"
    log_files = list(dest.glob("a*.log"))
    assert len(log_files) == 2


def test_copy_log_files_missing_source_is_noop(tmp_path):
    """copy_log_files returns silently and does not create the destination when the source is missing."""
    d.copy_log_files(str(tmp_path / "nope"), str(tmp_path / "logs"))
    assert not (tmp_path / "logs").exists()


def test_copy_log_files_no_logs_does_not_error(tmp_path):
    """copy_log_files creates the destination directory but writes nothing when there are no .log files."""
    src = tmp_path / "snap"
    src.mkdir()
    (src / "data.txt").write_text("x")
    dest = tmp_path / "logs"
    d.copy_log_files(str(src), str(dest))
    assert list(dest.glob("*.log")) == []


# --------------------------------------------------------------------------- #
# drop_snapshot_indexes
# --------------------------------------------------------------------------- #
def test_drop_snapshot_indexes_removes_indexes(tmp_path):
    """drop_snapshot_indexes drops all indexes from every SQLite database in the folder."""
    db_path = tmp_path / "snapshot.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER, name TEXT)"))
        conn.execute(text("CREATE INDEX idx_name ON t (name)"))
        conn.commit()
    engine.dispose()

    d.drop_snapshot_indexes(str(tmp_path))

    engine = create_engine(f"sqlite:///{db_path}")
    indexes = inspect(engine).get_indexes("t")
    engine.dispose()
    assert indexes == []


def test_drop_snapshot_indexes_missing_dir_is_noop(tmp_path):
    """drop_snapshot_indexes returns silently when the folder does not exist."""
    d.drop_snapshot_indexes(str(tmp_path / "nope"))


def test_drop_snapshot_indexes_no_db_files_is_noop(tmp_path):
    """drop_snapshot_indexes returns silently when the folder contains no .db files."""
    (tmp_path / "not_a_db.txt").write_text("x")
    d.drop_snapshot_indexes(str(tmp_path))


# --------------------------------------------------------------------------- #
# create_outlinks_warc
# --------------------------------------------------------------------------- #
def test_create_outlinks_warc_no_source_warcs_skips(tmp_path, monkeypatch):
    """create_outlinks_warc does not call fetch_and_archive_outlinks when no WARC files are found."""
    called = []
    monkeypatch.setattr(d, "fetch_and_archive_outlinks", lambda *a, **k: called.append(a))
    d.create_outlinks_warc(str(tmp_path), "site")
    assert called == []


def test_create_outlinks_warc_invokes_fetch(tmp_path, monkeypatch):
    """create_outlinks_warc passes all matching WARC part files to fetch_and_archive_outlinks."""
    (tmp_path / "site-0001.warc.gz").write_bytes(b"")
    (tmp_path / "site-0002.warc.gz").write_bytes(b"")
    captured = {}

    def fake_fetch(source_warcs, warc_output, warcfile_name, **kwargs):
        captured["sources"] = source_warcs
        captured["name"] = warcfile_name

    monkeypatch.setattr(d, "fetch_and_archive_outlinks", fake_fetch)
    d.create_outlinks_warc(str(tmp_path), "site")

    assert len(captured["sources"]) == 2
    assert captured["name"] == "site"


# --------------------------------------------------------------------------- #
# find_existing_warc_basenames
# --------------------------------------------------------------------------- #
def test_find_existing_warc_basenames_groups_part_files(tmp_path):
    """Part files of the same source WARC are collapsed into a single base name."""
    (tmp_path / "site_com-0001.warc.gz").write_bytes(b"")
    (tmp_path / "site_com-0002.warc.gz").write_bytes(b"")
    (tmp_path / "other_com-0001.warc.gz").write_bytes(b"")

    assert d.find_existing_warc_basenames(str(tmp_path)) == ["other_com", "site_com"]


def test_find_existing_warc_basenames_skips_outlinks_warcs(tmp_path):
    """Previously generated outlinks WARCs are not treated as sources for a new run."""
    (tmp_path / "site_com-0001.warc.gz").write_bytes(b"")
    (tmp_path / "site_com_outlinks-0001.warc.gz").write_bytes(b"")

    assert d.find_existing_warc_basenames(str(tmp_path)) == ["site_com"]


def test_find_existing_warc_basenames_ignores_unexpected_names(tmp_path):
    """WARC files without the '-XXXX' part suffix are ignored."""
    (tmp_path / "loose.warc.gz").write_bytes(b"")
    (tmp_path / "notes.txt").write_text("x")

    assert d.find_existing_warc_basenames(str(tmp_path)) == []


# --------------------------------------------------------------------------- #
# fetch_outlinks_for_existing_warcs
# --------------------------------------------------------------------------- #
def test_fetch_outlinks_for_existing_warcs_processes_each_basename(tmp_path, monkeypatch):
    """Every source WARC found on disk gets its outgoing links archived exactly once."""
    (tmp_path / "a_com-0001.warc.gz").write_bytes(b"")
    (tmp_path / "b_com-0001.warc.gz").write_bytes(b"")
    (tmp_path / "b_com-0002.warc.gz").write_bytes(b"")
    calls = []

    monkeypatch.setattr(
        d, "create_outlinks_warc",
        lambda output, name, threads=None: calls.append((name, threads)),
    )
    d.fetch_outlinks_for_existing_warcs(str(tmp_path), threads=3)

    assert calls == [("a_com", 3), ("b_com", 3)]


def test_fetch_outlinks_for_existing_warcs_no_warcs_is_noop(tmp_path, monkeypatch):
    """An output folder with no WARC files does nothing rather than raising."""
    calls = []
    monkeypatch.setattr(d, "create_outlinks_warc", lambda *a, **k: calls.append(a))
    d.fetch_outlinks_for_existing_warcs(str(tmp_path))
    assert calls == []


def test_fetch_outlinks_for_existing_warcs_missing_dir_is_noop(tmp_path, monkeypatch):
    """A non-existent output folder does nothing rather than raising."""
    calls = []
    monkeypatch.setattr(d, "create_outlinks_warc", lambda *a, **k: calls.append(a))
    d.fetch_outlinks_for_existing_warcs(str(tmp_path / "nope"))
    assert calls == []


def test_fetch_outlinks_for_existing_warcs_continues_after_failure(tmp_path, monkeypatch):
    """A failure on one WARC group does not abort the remaining groups."""
    (tmp_path / "a_com-0001.warc.gz").write_bytes(b"")
    (tmp_path / "b_com-0001.warc.gz").write_bytes(b"")
    seen = []

    def flaky(output, name, threads=None):
        seen.append(name)
        if name == "a_com":
            raise RuntimeError("boom")

    monkeypatch.setattr(d, "create_outlinks_warc", flaky)
    d.fetch_outlinks_for_existing_warcs(str(tmp_path))

    assert seen == ["a_com", "b_com"]


# --------------------------------------------------------------------------- #
# download_urls_from_csv (date-range selection, network stubbed)
# --------------------------------------------------------------------------- #
@pytest.fixture
def _stub_downloader(monkeypatch):
    """Stub out the side-effecting pieces of download_urls_from_csv and record
    the (start, end) date range passed to download_single_url."""
    calls = {}

    def fake_download_single_url(url, start, end, *a, **k):
        calls["url"] = url
        calls["start"] = start
        calls["end"] = end

    monkeypatch.setattr(d, "download_single_url", fake_download_single_url)
    monkeypatch.setattr(d, "process_csv_file", lambda *a, **k: None)
    monkeypatch.setattr(d, "create_outlinks_warc", lambda *a, **k: None)
    monkeypatch.setattr(d, "drop_snapshot_indexes", lambda *a, **k: None)
    monkeypatch.setattr(d, "copy_log_files", lambda *a, **k: None)
    return calls


def _write_url_csv(tmp_path, url):
    csv = tmp_path / "urls.csv"
    # utils.read_csv uses ';' as the separator.
    csv.write_text(f"Internet_Archive_URL\n{url}\n")
    return str(csv)


def test_download_day_period_range(tmp_path, _stub_downloader):
    """Period.DAY sets the download window to one day before and after the archived date."""
    csv = _write_url_csv(
        tmp_path, "https://web.archive.org/web/20030409193011/http://a.com"
    )
    d.download_urls_from_csv(csv, "Internet_Archive_URL", download_period=Period.DAY)
    assert _stub_downloader["start"] == "20030408193011"
    assert _stub_downloader["end"] == "20030410193011"


def test_download_week_period_range(tmp_path, _stub_downloader):
    """Period.WEEK sets the download window to one week before and after the archived date."""
    csv = _write_url_csv(
        tmp_path, "https://web.archive.org/web/20030409193011/http://a.com"
    )
    d.download_urls_from_csv(csv, "Internet_Archive_URL", download_period=Period.WEEK)
    assert _stub_downloader["start"] == "20030402193011"
    assert _stub_downloader["end"] == "20030416193011"


def test_download_full_period_uses_fixed_range(tmp_path, _stub_downloader):
    """Period.FULL uses the fixed range 1995-01-01 to 2005-12-31 regardless of the archived date."""
    csv = _write_url_csv(
        tmp_path, "https://web.archive.org/web/20030409193011/http://a.com"
    )
    d.download_urls_from_csv(csv, "Internet_Archive_URL", download_period=Period.FULL)
    assert _stub_downloader["start"] == "19950101000000"
    assert _stub_downloader["end"] == "20051231235959"


def test_download_defaults_to_full_when_period_none(tmp_path, _stub_downloader):
    """Passing download_period=None defaults to the FULL period range."""
    csv = _write_url_csv(
        tmp_path, "https://web.archive.org/web/20030409193011/http://a.com"
    )
    d.download_urls_from_csv(csv, "Internet_Archive_URL", download_period=None)
    assert _stub_downloader["start"] == "19950101000000"


def test_download_custom_period_uses_start_end(tmp_path, _stub_downloader):
    """Period.CUSTOM passes the caller-supplied start_time and end_time directly to the downloader."""
    csv = _write_url_csv(
        tmp_path, "https://web.archive.org/web/20030409193011/http://a.com"
    )
    d.download_urls_from_csv(
        csv,
        "Internet_Archive_URL",
        start_time="20000101000000",
        end_time="20001231235959",
        download_period=Period.CUSTOM,
    )
    assert _stub_downloader["start"] == "20000101000000"
    assert _stub_downloader["end"] == "20001231235959"
