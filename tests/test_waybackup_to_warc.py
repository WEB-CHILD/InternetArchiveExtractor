"""Tests for waybackup_to_warc.py (CSV -> WARC conversion)."""

import gzip
import os

import pandas as pd
import pytest
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter

import waybackup_to_warc as w


def _read_records(warc_path):
    """Return a list of (target_uri, status_line, payload_bytes) from a WARC."""
    out = []
    with open(warc_path, "rb") as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type != "response":
                continue
            uri = record.rec_headers.get_header("WARC-Target-URI")
            status = record.http_headers.get_statuscode() if record.http_headers else None
            payload = record.content_stream().read()
            out.append((uri, status, payload))
    return out


# --------------------------------------------------------------------------- #
# remove_port_80 / process_csv
# --------------------------------------------------------------------------- #
def test_remove_port_80():
    """remove_port_80 strips :80 and leaves URLs without it unchanged."""
    assert w.remove_port_80("http://a.com:80/x") == "http://a.com/x"
    assert w.remove_port_80("http://a.com/x") == "http://a.com/x"


def test_process_csv_strips_ports(tmp_path):
    """process_csv removes :80 from both url_archive and url_origin columns."""
    csv = tmp_path / "in.csv"
    csv.write_text(
        "url_archive,url_origin\n"
        "http://web.archive.org:80/a,http://a.com:80/x\n"
    )
    df = w.process_csv(str(csv))
    assert df.iloc[0]["url_archive"] == "http://web.archive.org/a"
    assert df.iloc[0]["url_origin"] == "http://a.com/x"


# --------------------------------------------------------------------------- #
# write_404 / write_500 helpers
# --------------------------------------------------------------------------- #
def test_write_404_warc_entry(tmp_path):
    """write_404_warc_entry writes a WARC response record with a 404 status."""
    path = tmp_path / "out.warc.gz"
    with open(path, "wb") as stream:
        writer = WARCWriter(stream, gzip=True)
        w.write_404_warc_entry(writer, "http://a.com", "2003-04-09T19:30:11Z")

    records = _read_records(str(path))
    assert len(records) == 1
    uri, status, _ = records[0]
    assert uri == "http://a.com"
    assert status == "404"


def test_write_500_warc_entry(tmp_path):
    """write_500_warc_entry writes a WARC response record with a 500 status."""
    path = tmp_path / "out.warc.gz"
    with open(path, "wb") as stream:
        writer = WARCWriter(stream, gzip=True)
        w.write_500_warc_entry(writer, "http://a.com", "2003-04-09T19:30:11Z")

    uri, status, _ = _read_records(str(path))[0]
    assert status == "500"


# --------------------------------------------------------------------------- #
# create_warc_gz
# --------------------------------------------------------------------------- #
def test_create_warc_gz_empty_data_creates_nothing(tmp_path):
    """create_warc_gz with an empty data list writes no files and returns silently."""
    w.create_warc_gz([], str(tmp_path), "out")
    assert list(tmp_path.glob("*.warc.gz")) == []


def test_create_warc_gz_writes_200_record_with_content(tmp_path):
    """create_warc_gz writes a 200 WARC record whose payload matches the source file."""
    src = tmp_path / "page.html"
    src.write_bytes(b"<html>hi</html>")
    data = [
        {
            "url_origin": "http://a.com",
            "file": str(src),
            "timestamp": "20030409193011",
            "response": "200",
        }
    ]
    w.create_warc_gz(data, str(tmp_path / "out"), "site")

    warcs = sorted((tmp_path / "out").glob("site-*.warc.gz"))
    assert len(warcs) == 1
    records = _read_records(str(warcs[0]))
    assert records[0][0] == "http://a.com"
    assert records[0][1] == "200"
    assert records[0][2] == b"<html>hi</html>"


def test_create_warc_gz_404_and_500_records(tmp_path):
    """create_warc_gz writes dedicated error records for 404 and 500 response codes."""
    data = [
        {"url_origin": "http://a.com/404", "file": "missing", "timestamp": "20030409193011", "response": "404"},
        {"url_origin": "http://a.com/500", "file": "missing", "timestamp": "20030409193011", "response": "500"},
    ]
    out = tmp_path / "out"
    w.create_warc_gz(data, str(out), "site")
    statuses = sorted(s for _, s, _ in _read_records(str(next(out.glob("site-*.warc.gz")))))
    assert statuses == ["404", "500"]


def test_create_warc_gz_skips_missing_200_file(tmp_path):
    """create_warc_gz silently skips a 200 entry whose local file does not exist."""
    data = [
        {"url_origin": "http://a.com", "file": str(tmp_path / "nope.html"), "timestamp": "20030409193011", "response": "200"},
    ]
    out = tmp_path / "out"
    w.create_warc_gz(data, str(out), "site")
    # The WARC file is still created, but contains no records.
    assert _read_records(str(next(out.glob("site-*.warc.gz")))) == []


def test_create_warc_gz_infers_content_type_from_extension(tmp_path):
    """create_warc_gz sets Content-Type to image/png for .png files."""
    img = tmp_path / "pic.png"
    img.write_bytes(b"\x89PNG\r\n")
    data = [
        {"url_origin": "http://a.com/pic.png", "file": str(img), "timestamp": "20030409193011", "response": "200"},
    ]
    out = tmp_path / "out"
    w.create_warc_gz(data, str(out), "site")

    with open(next(out.glob("site-*.warc.gz")), "rb") as stream:
        record = next(ArchiveIterator(stream))
        assert record.http_headers.get_header("Content-Type") == "image/png"


def test_create_warc_gz_invalid_timestamp_still_writes_record(tmp_path):
    """create_warc_gz logs a bad timestamp but still writes the WARC record without a date."""
    src = tmp_path / "page.html"
    src.write_bytes(b"<html></html>")
    data = [
        {"url_origin": "http://a.com", "file": str(src), "timestamp": "not-a-date", "response": "200"},
    ]
    out = tmp_path / "out"
    w.create_warc_gz(data, str(out), "site")
    assert len(_read_records(str(next(out.glob("site-*.warc.gz"))))) == 1


def test_create_warc_gz_splits_on_size_threshold(tmp_path):
    """create_warc_gz starts a new part file once the size threshold is exceeded."""
    src = tmp_path / "page.html"
    src.write_bytes(b"x" * 200)
    # Three entries with a 100-byte threshold force a new file before each later entry.
    data = [
        {"url_origin": f"http://a.com/{i}", "file": str(src), "timestamp": "20030409193011", "response": "200"}
        for i in range(3)
    ]
    out = tmp_path / "out"
    w.create_warc_gz(data, str(out), "site", max_size_bytes=100)
    warcs = sorted(out.glob("site-*.warc.gz"))
    assert len(warcs) >= 2
    assert {os.path.basename(p) for p in warcs} >= {"site-0001.warc.gz", "site-0002.warc.gz"}


# --------------------------------------------------------------------------- #
# read_csv / process_csv_file
# --------------------------------------------------------------------------- #
def test_read_csv_returns_list_of_dicts(tmp_path):
    """read_csv parses a comma-delimited CSV into a list of row dicts with string values."""
    csv = tmp_path / "in.csv"
    csv.write_text("a,b\n1,2\n3,4\n")
    rows = w.read_csv(str(csv))
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


def test_process_csv_file_missing_file_is_noop(tmp_path):
    """process_csv_file logs a warning and returns without error when the CSV does not exist."""
    w.process_csv_file(str(tmp_path / "nope.csv"), str(tmp_path), "site")
    assert list(tmp_path.glob("*.warc.gz")) == []


def test_process_csv_file_empty_file_is_noop(tmp_path):
    """process_csv_file skips WARC creation when the CSV contains only a header row."""
    csv = tmp_path / "empty.csv"
    csv.write_text("url_origin,file,timestamp,response\n")  # header only
    w.process_csv_file(str(csv), str(tmp_path / "out"), "site")
    assert not (tmp_path / "out").exists() or list((tmp_path / "out").glob("*.warc.gz")) == []


def test_process_csv_file_end_to_end(tmp_path):
    """process_csv_file reads a CSV and produces a WARC with the correct payload."""
    page = tmp_path / "page.html"
    page.write_bytes(b"<html>ok</html>")
    csv = tmp_path / "in.csv"
    csv.write_text(
        "url_origin,file,timestamp,response\n"
        f"http://a.com,{page},20030409193011,200\n"
    )
    out = tmp_path / "out"
    w.process_csv_file(str(csv), str(out), "site")
    records = _read_records(str(next(out.glob("site-*.warc.gz"))))
    assert records[0][2] == b"<html>ok</html>"


# --------------------------------------------------------------------------- #
# combine_csv_files
# --------------------------------------------------------------------------- #
def test_combine_csv_files_concatenates(tmp_path):
    """combine_csv_files merges all CSVs in a directory into a single output file."""
    in_dir = tmp_path / "csvs"
    in_dir.mkdir()
    (in_dir / "a.csv").write_text("col\n1\n2\n")
    (in_dir / "b.csv").write_text("col\n3\n")
    out = tmp_path / "combined.csv"

    w.combine_csv_files(str(in_dir), str(out))

    combined = pd.read_csv(out)
    assert combined["col"].tolist() == [1, 2, 3]


def test_combine_csv_files_no_csvs_exits(tmp_path):
    """combine_csv_files calls sys.exit when no CSV files are found in the directory."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    with pytest.raises(SystemExit):
        w.combine_csv_files(str(empty_dir), str(tmp_path / "out.csv"))
