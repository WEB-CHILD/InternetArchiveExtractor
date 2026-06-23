"""Tests for logging_config.py."""

import logging
import sys

import pytest

from logging_config import (
    LoggerWriter,
    get_logger,
    redirect_stdout_to_logger,
    setup_logging,
)


@pytest.fixture(autouse=True)
def _reset_root_logger():
    """Restore the root logger configuration after each test."""
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    try:
        yield
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)


def test_setup_logging_sets_level_and_console_handler():
    """setup_logging configures the root logger with the given level and one console handler."""
    logger = setup_logging(log_level=logging.DEBUG)
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_setup_logging_replaces_existing_handlers():
    """Calling setup_logging twice does not accumulate handlers; the second call replaces the first."""
    setup_logging(log_level=logging.INFO)
    setup_logging(log_level=logging.WARNING)
    assert len(logging.getLogger().handlers) == 1


def test_setup_logging_adds_file_handler_and_creates_parent(tmp_path):
    """setup_logging creates missing parent directories and adds a FileHandler when log_file is given."""
    log_file = tmp_path / "nested" / "app.log"
    logger = setup_logging(log_level=logging.INFO, log_file=str(log_file))
    assert log_file.parent.is_dir()
    assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)
    # Ensure the file handler is released so tmp_path cleanup works on all OSes.
    for h in logger.handlers:
        h.close()


def test_get_logger_returns_named_logger():
    """get_logger returns a logger whose name matches the argument."""
    assert get_logger("foo.bar").name == "foo.bar"


def test_logger_writer_logs_non_blank_messages():
    """LoggerWriter forwards non-blank messages to the logger and discards whitespace-only writes."""
    records = []
    logger = logging.getLogger("test.writer")
    logger.handlers = [_CapturingHandler(records)]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    writer = LoggerWriter(logger, logging.INFO)
    writer.write("hello\n")
    writer.write("   ")  # whitespace-only -> ignored
    writer.write("")  # empty -> ignored

    assert [r.getMessage() for r in records] == ["hello"]
    assert records[0].levelno == logging.INFO


def test_logger_writer_flush_is_noop():
    """LoggerWriter.flush() returns None without raising."""
    writer = LoggerWriter(logging.getLogger("x"), logging.INFO)
    assert writer.flush() is None


def test_redirect_stdout_to_logger_captures_and_restores():
    """redirect_stdout_to_logger routes stdout to INFO and stderr to ERROR, then restores both streams."""
    records = []
    logger = logging.getLogger("test.redirect")
    logger.handlers = [_CapturingHandler(records)]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    original_stdout, original_stderr = sys.stdout, sys.stderr
    with redirect_stdout_to_logger(logger):
        print("to stdout")
        print("to stderr", file=sys.stderr)

    # Streams restored afterwards.
    assert sys.stdout is original_stdout
    assert sys.stderr is original_stderr

    messages = {(r.getMessage(), r.levelno) for r in records}
    assert ("to stdout", logging.INFO) in messages
    assert ("to stderr", logging.ERROR) in messages


def test_redirect_stdout_to_logger_custom_levels():
    """redirect_stdout_to_logger respects a custom stderr_level argument."""
    records = []
    logger = logging.getLogger("test.redirect.levels")
    logger.handlers = [_CapturingHandler(records)]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    with redirect_stdout_to_logger(logger, stderr_level=logging.INFO):
        print("err as info", file=sys.stderr)

    assert records[0].levelno == logging.INFO


def test_redirect_restores_streams_on_exception():
    """redirect_stdout_to_logger restores sys.stdout even when the body raises an exception."""
    logger = logging.getLogger("test.redirect.exc")
    logger.handlers = [_CapturingHandler([])]
    logger.propagate = False

    original_stdout = sys.stdout
    with pytest.raises(RuntimeError):
        with redirect_stdout_to_logger(logger):
            raise RuntimeError("boom")
    assert sys.stdout is original_stdout


class _CapturingHandler(logging.Handler):
    def __init__(self, sink):
        super().__init__()
        self.sink = sink

    def emit(self, record):
        self.sink.append(record)
