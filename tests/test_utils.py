"""Tests for utils.py."""

import pandas as pd
import pytest

import utils
from utils import (
    clean_urls,
    import_urls_from_csv,
    read_csv,
    remove_port_80,
)


def test_remove_port_80_strips_port():
    """Port :80 is removed from a URL that contains it."""
    assert remove_port_80("http://example.com:80/page") == "http://example.com/page"


def test_remove_port_80_leaves_other_urls_untouched():
    """URLs without :80 are returned unchanged."""
    assert remove_port_80("http://example.com/page") == "http://example.com/page"


def test_remove_port_80_naive_substring_match_mangles_other_ports():
    """Documents a known quirk: the check is a naive ':80' substring replace, so
    a ':8080' port has its leading ':80' stripped, leaving a mangled URL."""
    assert remove_port_80("http://example.com:8080/page") == "http://example.com80/page"


def test_read_csv_uses_semicolon_separator(tmp_path):
    """read_csv parses files with semicolons as the column delimiter."""
    csv = tmp_path / "data.csv"
    csv.write_text("a;b\n1;2\n")
    df = read_csv(str(csv))
    assert list(df.columns) == ["a", "b"]
    assert df.iloc[0]["a"] == 1


def test_clean_urls_strips_port_from_both_columns():
    """clean_urls removes :80 from both url_archive and url_origin columns."""
    df = pd.DataFrame(
        {
            "url_archive": ["http://a.com:80/x", "http://b.com/y"],
            "url_origin": ["http://c.com:80/z", "http://d.com/w"],
        }
    )
    out = clean_urls(df)
    assert out["url_archive"].tolist() == ["http://a.com/x", "http://b.com/y"]
    assert out["url_origin"].tolist() == ["http://c.com/z", "http://d.com/w"]


def test_import_urls_from_csv_returns_column_values(tmp_path):
    """import_urls_from_csv returns the values of the requested column as a list."""
    csv = tmp_path / "urls.csv"
    csv.write_text("Internet_Archive_URL;other\nhttp://a.com;1\nhttp://b.com;2\n")
    urls = import_urls_from_csv(str(csv), "Internet_Archive_URL")
    assert urls == ["http://a.com", "http://b.com"]


def test_import_urls_from_csv_missing_column_raises(tmp_path):
    """import_urls_from_csv raises KeyError when the column does not exist."""
    csv = tmp_path / "urls.csv"
    csv.write_text("a;b\n1;2\n")
    with pytest.raises(KeyError):
        import_urls_from_csv(str(csv), "does_not_exist")


def test_import_urls_from_csv_missing_file_raises():
    """import_urls_from_csv raises FileNotFoundError for a non-existent file."""
    with pytest.raises(FileNotFoundError):
        import_urls_from_csv("/no/such/file.csv", "Internet_Archive_URL")


@pytest.mark.xfail(
    reason="utils.create_warc_gz calls WARCWriter.write_webpage, which does not "
    "exist in warcio; the function is currently broken/dead code.",
    raises=AttributeError,
    strict=True,
)
def test_create_warc_gz_is_currently_broken(tmp_path):
    """create_warc_gz raises AttributeError because WARCWriter.write_webpage does not exist."""
    df = pd.DataFrame(
        {
            "url_origin": ["http://a.com"],
            "url_archive": ["http://web.archive.org/a"],
            "timestamp": ["20030409193011"],
        }
    )
    utils.create_warc_gz(str(tmp_path / "out.warc.gz"), df)
