# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Outgoing-link archiving (`warc_outlinks.py`): after each primary WARC is created, outgoing `<a>` hyperlinks and embedded resources (images, scripts, stylesheets, frames, media, objects) are extracted from every archived HTML page, downloaded from the Wayback Machine at the capture date of the referencing page, and packaged into a separate `<name>_outlinks-XXXX.warc.gz` file
- `--no-outlinks` flag to skip the outgoing-link archiving step
- Full unit test suite (`tests/`) covering all source modules: `constants`, `utils`, `wayback_date_object`, `logging_config`, `waybackup_to_warc`, `warc_outlinks`, and `internet_archive_downloader` (106 tests, no network access required)
- `pytest` added as an optional `test` dependency in `pyproject.toml`
- Test running instructions added to README

## [0.0.11] - 2026-03-10

### Added
- `--log-level` argument to control logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--log-file` argument to specify a log file for persistent logging
- Stdout/stderr redirection to logger for PyWayBackup output
- `LoggerWriter` class and `redirect_stdout_to_logger` context manager in logging_config module
- Support for Live URLs in download modes CUSTOM and FULL
- `--clean` flag to delete intermediate CSV, DB, and CDX files after processing
- `--workers` argument to set the number of parallel download threads (default: 5)
- `--warc-output` argument to specify the folder where WARC files are stored (default: `./output`)
- `--snapshot-folder` argument to specify where wayback snapshots are saved
- Warnings when WARC input data is empty or CSV file is missing, instead of silently creating an empty WARC

### Changed
- Enhanced CSV file combination with better error handling and debugging output during WARC creation
- Improved robustness when processing malformed CSV files
- Changed default download mode from DAY to FULL
- Improved filename sanitization using PyWayBackup's helper function
- Wildcard search is now enabled by default; explicit mode is only set when arguments are explicitly provided
- Updated pysqlite3 dependency to version 0.6.0
- Enhanced README with detailed usage instructions for download and convert modes

### Removed
- Unnecessary `temp_dir` from WARC creation process

## [0.0.10] - 2026-01-15

### Added
- Better log file handling with improved error tracking
- Small test file for development and testing
- Retry mechanism after dropping database index
- Individual domain download and cleanup functionality
- Support for finishing individual domains before downloading further
- PyPI logo and DOI badge to README

### Changed
- Updated package version metadata
- Enhanced documentation with clearer usage instructions
- Removed duplicate FULL mode from download options

### Fixed
- Handled index generation errors more gracefully
- Improved error handling during database operations

## [0.0.8] - 2025-12-19

### Added
- Initial public release on PyPI
- Zenodo metadata for academic citation
- Full domain download support (not just specific URLs)
- Customizable download periods (daily, weekly, monthly, yearly, full)
- Database reset functionality
- Support for 404 and 500 WARC entries
- Test corpus for validation
- Constants module for better code organization
- Multiple worker support for parallel downloads

### Changed
- Download interval changed to two weeks around archival date
- Improved filename sanitization
- Enhanced documentation and README
- Made project deployable to PyPI

### Fixed
- Input CSV handling as command-line argument
- Various code cleanup and refactoring