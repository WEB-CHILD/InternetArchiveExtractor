# InternetArchiveExtractor

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.17987609.svg)](https://doi.org/10.5281/zenodo.17987609)
[![PyPI version](https://img.shields.io/pypi/v/internet-archive-extractor.svg)](https://pypi.org/project/internet-archive-extractor/)

This repository extracts archived content from the Wayback Machine and converts collected metadata and downloaded snapshot files into compressed WARC files. The project supports two primary modes of operation: downloading snapshots from the Internet Archive and converting CSV metadata (produced by `pywaybackup`) into WARC-GZ archives.

## What this does (short)
- **Download mode**: Reads a CSV of Internet Archive (Wayback) URLs, and uses `pywaybackup` to download snapshots. For each URL processed, it automatically converts the downloaded snapshots to a WARC file and cleans up temporary files.
- **Convert mode**: Combines CSV files (from a directory) into a single CSV and then converts that CSV into a compressed WARC (`.warc.gz`) using `warcio`.

## Requirements
Install the Python dependencies from the repository `requirements.txt`:

```
pip install -r requirements.txt
```

Notable packages used:
- `pywaybackup` — downloads Wayback snapshots
- `pandas` — CSV handling and merging when combining multiple CSVs
- `warcio` — writing WARC records

See `requirements.txt` for the exact pinned versions used in this repository.

## Project layout (important files)
- `src/main.py` — command-line entry point that exposes `download` and `convert` modes.
- `src/internet_archive_downloader.py` — logic that reads an input CSV of Internet Archive URLs and runs `pywaybackup` to download snapshots. After each URL is downloaded, it automatically converts the CSV to WARC and cleans up temporary files.
- `src/waybackup_to_warc.py` — functions to combine CSV files, clean URLs (remove `:80`), and produce a `.warc.gz` from a CSV of records.
ng.

## How to run
Usage pattern for the main runner (`src/main.py`):

```bash
# Download mode
python src/main.py download <input> [--column_name COLUMN] [--period PERIOD] [--reset] [--start_time START] [--end_time END] [--snapshot-folder FOLDER] [--warc-output FOLDER] [--workers N] [--clean] [--no-outlinks] [--exclude-tld TLD ...] [--timeout N] [--max-retries N]

# Download mode, outgoing links from existing WARCS only (no <input> needed)
python src/main.py download --outlinks-only [--warc-output FOLDER] [--workers N] [--exclude-tld TLD ...] [--timeout N] [--max-retries N]

# Convert mode
python src/main.py convert <input> --output OUTPUT [--warc-output FOLDER]
```

### Modes and example usage:

#### Download mode — download snapshots listed in a CSV

**Description**: Reads a CSV containing full Wayback URLs such as `https://web.archive.org/web/20251002062751/https://example.com/page` and downloads snapshots for a specified period around the archived date. After downloading each URL, the tool automatically:
1. Converts the downloaded snapshots to a WARC file (saved in `output/` directory, or custom location via `--warc-output`)
2. Cleans up temporary files from `waybackup_snapshots/` directory (if `--clean` flag is used)

**Note**: In download mode, WARC filenames are automatically generated from the URL. The `--output` flag is not used in this mode.

**Required `input`**: Path to the CSV file to read (e.g. `resources/curated_urls.csv`). The default column name expected is `Internet_Archive_URL`.

**Example**:

```bash
python src/main.py download resources/curated_urls.csv --column_name Internet_Archive_URL --period DAY
```

**Flags**:
- `--column_name` — Name of the CSV column containing Wayback URLs (default: `Internet_Archive_URL`)
- `--period` — Download period options:
  - `DAY` (default) — Downloads snapshots ±1 day around the archived date
  - `WEEK` — Downloads snapshots ±1 week around the archived date
  - `FULL` — Downloads all snapshots from 1995-2005
  - `CUSTOM` — Downloads snapshots within a custom date range (requires `--start_time` and `--end_time`)
- `--start_time` — Start time for CUSTOM period in `YYYYMMDDHHMMSS` format
- `--end_time` — End time for CUSTOM period in `YYYYMMDDHHMMSS` format
- `--reset` — If present, forces re-download by passing `reset=True` to `pywaybackup`
- `--snapshot-folder` — Path to the folder where pywaybackup stores downloaded snapshots (default: `./waybackup_snapshots`)
- `--warc-output` — Path to the folder where WARC files will be saved (default: `./output`)
- `--workers` — Number of worker threads for parallel downloading (default: `5`)
- `--clean` — If present, deletes intermediate CSV, DB, and CDX files after processing
- `--no-outlinks` — If present, skips the step that downloads and archives the outgoing links found in the created WARC files
- `--outlinks-only` — If present, skips downloading and WARC packaging entirely and only archives the outgoing links of the WARC files already on disk (see below)
- `--scan-workers` — Number of processes used to scan WARC files for outgoing links (default: one per CPU core). Scanning is CPU-bound HTML parsing, so this is parallelised across processes; use `1` to scan in a single process
- `--exclude-tld` — Top-level domains whose outgoing links are skipped entirely, e.g. `--exclude-tld .dk .com` (see below)
- `--timeout` — Per-request timeout in seconds when fetching outgoing links (default: `20`)
- `--max-retries` — Extra attempts when the archive throttles or errors (default: `3`)

**Example with CUSTOM period**:

```bash
python src/main.py download resources/curated_urls.csv --period CUSTOM --start_time 20000101000000 --end_time 20001231235959
```

**Example with custom snapshot and WARC output folders**:

```bash
python src/main.py download resources/curated_urls.csv --snapshot-folder /data/snapshots --warc-output /data/warcs
```

#### Outlinks-only run — archive outgoing links from WARC files already on disk

**Description**: With `--outlinks-only`, nothing is downloaded from the Wayback Machine's CDX index and no source WARC files are created. The tool scans the `--warc-output` folder for the WARC files already there, and for each one downloads its outgoing links into a matching `<name>_outlinks-XXXX.warc.gz` file. Use it to (re-)run just the second step after a download that finished without its outlinks, or that was interrupted.

Redirects are followed and preserved: each redirect the archived site actually served is written into the WARC as its own 3xx record carrying its `Location` header, and the final body is stored under the URL it actually came from rather than the URL originally requested. Wayback's own replay-level redirects — finding the nearest snapshot, or canonicalising a URL such as dropping a default `:80` — are plumbing rather than historical content, and are not recorded; they are told apart by the same `Memento-Datetime` signal.

Only responses the Wayback Machine served from a **real capture** are archived, which it signals with a `Memento-Datetime` header. This is independent of status code: an archived 404 or 403 — one the site genuinely served at capture time — is a real capture and is written to the WARC. A link Wayback has simply never captured answers with a present-day web.archive.org error page (~4.8 KB of `<title>Wayback Machine</title>` HTML); those are skipped so they cannot masquerade as historical web content. Misses are not listed individually — they are only counted in the run summary. On real 1990s link sets the miss rate can exceed 70%, so this matters for both WARC size and archival accuracy.

A redirect whose `Location` header carries raw 8-bit bytes — routine in 1990s national-language URLs, where Danish `å` is the single byte `0xe5` — makes `requests` raise a `UnicodeDecodeError` from inside its own redirect handling. That is not a `RequestException`, so left uncaught it aborts an entire run over one link. The session falls back to reading such a `Location` as latin-1 and follows the redirect; if the archive only holds the target under a different escaping, it comes back as an ordinary miss rather than a crash. More generally, no single link can end a run: an unexpected error while handling one result is logged, counted as a failure, and the run continues.

### Retries and timeouts

A response the archive actually served is final and is never retried — including a 404 for a URL it has no capture of, which returns on the first attempt with no backoff. Only throttling (`429`), server errors (`5xx`) and transport failures are retried, with exponential backoff that honours a `Retry-After` header (capped at 60 s so one thread cannot stall).

The defaults are deliberately generous: `--timeout 20` and `--max-retries 3`. Archive.org's tail latency is long — a sampled set of captures had a median response of 2.5 s but a maximum of 25 s, and its captures routinely redirect 3–6 times with each hop paying the timeout separately. A short timeout does **not** skip misses faster, since misses answer immediately; it only discards resources that do exist. On one real run a 5 s timeout with a single retry threw away 175 links, of which a re-fetch found 11 out of 12 sampled to be genuine captures.

Any request that could not be completed is recorded in `<name>_outlinks_failed.txt`, one Wayback request URL per line (e.g. `https://web.archive.org/web/20000302202605id_/http://example.com/page`). Each line is directly re-fetchable and still carries both the original URL and its capture timestamp. The file is written next to the outlinks WARC, is only created when there is at least one failure, and is overwritten on a re-run.

WARC part files (`<name>-0001.warc.gz`, `<name>-0002.warc.gz`, …) are grouped back into a single source, and existing `<name>_outlinks-XXXX.warc.gz` files are skipped as sources so their links are not fetched again. Note that an existing outlinks WARC for a given name **is overwritten** by the new run.

**`input` is not required** in this mode, and `--outlinks-only` cannot be combined with `--no-outlinks`.

### Excluding top-level domains

`--exclude-tld` skips outgoing links whose host sits under one of the given top-level domains. It applies to both the full `download` run and `--outlinks-only`.

```bash
python src/main.py download --outlinks-only --exclude-tld .dk .com
```

The leading dot is optional (`.dk` and `dk` are the same) and matching is case-insensitive. Multi-label suffixes work as written: `.co.uk` excludes only that, while `.uk` excludes all of it. The flag can be repeated, and ports, userinfo and a trailing root dot do not defeat the match — `http://user@Example.DK.:80/x` is excluded by `.dk`. A suffix only matches on a label boundary, so `.dk` does not exclude `example.dk.com` or `notdk`.

Excluded links are dropped during the scan, so they are never downloaded, never counted, and never cross into the download step. The saving can be large: on three real WARC files holding 141,034 outgoing links, `--exclude-tld .dk .com` left 6,846 — a 95% reduction — while adding about 1% to scan time.


**Example**:

```bash
python src/main.py download --outlinks-only
```

**Example against a custom WARC folder with more threads**:

```bash
python src/main.py download --outlinks-only --warc-output /data/warcs --workers 10
```

**Relevant flags**:
- `--warc-output` — Folder that is scanned for existing WARC files (default: `./output`)
- `--workers` — Number of concurrent download threads used to fetch the outgoing links (default: `5`)
- `--scan-workers` — Number of processes used to scan the WARC files (default: one per CPU core)
- `--exclude-tld` — Top-level domains whose outgoing links are skipped entirely, e.g. `.dk .com`
- `--timeout` — Per-request timeout in seconds (default: `20`)
- `--max-retries` — Extra attempts when the archive throttles or errors (default: `3`)

**A note on performance**: the scan phase is CPU-bound HTML parsing, not I/O — on a 9.7 GB set of 19 WARC files it is over 99% HTML parsing and under 1% disk reads. It is therefore parallelised across processes (threads would be serialised by the GIL) and uses `selectolax` rather than Python's `html.parser`. Together these took that 9.7 GB scan from roughly 20 minutes to about 1 minute.

#### Convert mode — combine CSVs and produce a WARC

**Description**: Combine all `.csv` files from the specified directory into a single CSV (written to `combined_output.csv` by default) and convert that CSV to a WARC-GZ.

**Required `input`**: Path to a directory that contains CSV files to combine (e.g. `waybackup_snapshots/` or any folder with CSV exports).

**Required `--output`**: Base filename for the resulting WARC file (without extension). The tool will append `-0001.warc.gz`, `-0002.warc.gz`, etc.

**Example**:

```bash
python src/main.py convert waybackup_snapshots --output mysite_archive
```

**Optional flags**:
- `--warc-output` — Path to the folder where WARC files will be saved (default: `./output`)

**Example with custom WARC output folder**:

```bash
python src/main.py convert waybackup_snapshots --output mysite_archive --warc-output /data/warcs
```

**Notes**: 
- The script combines CSV files using `pandas.concat` and writes the combined CSV to `combined_output.csv`.
- The combined CSV is then read and converted into `<warc-output>/<output>.warc.gz`.
- The CSVs are expected to contain columns: `url_origin`, `url_archive`, `file`, `timestamp`, and `response`.


## Running the tests

The unit tests live in the `tests/` directory and are run with [pytest](https://docs.pytest.org/).

1. Install the test dependency (pytest is declared as an optional `test` dependency):

   ```bash
   pip install pytest
   # or, to install the project with its test extra:
   pip install -e ".[test]"
   ```

2. Run the full suite from the repository root:

   ```bash
   python -m pytest
   ```

Useful variations:

```bash
python -m pytest -v                                  # verbose, one line per test
python -m pytest tests/test_waybackup_to_warc.py     # a single test file
python -m pytest -k outlinks                          # only tests matching "outlinks"
```

The tests are configured in `pyproject.toml` (`[tool.pytest.ini_options]`), which
adds `src/` to the Python path automatically — so you don't need to install the
package to run them. The tests are self-contained and stub out all network and
`pywaybackup` calls, so no internet access is required.

## Important implementation notes
- **Automatic workflow in Download mode**: When downloading, each URL is processed individually:
  1. Downloads snapshots using `pywaybackup` to the snapshot folder (default: `waybackup_snapshots/`, configurable via `--snapshot-folder`)
  2. Generates a CSV file with snapshot metadata
  3. Automatically creates WARC file of downloaded data (saved to the WARC output folder, default: `output/`, configurable via `--warc-output`)
  4. Cleans up temporary files and subdirectories from the snapshot folder (if `--clean` flag is used)
- **Expected CSV columns**: The CSVs read by the converter must contain: `url_origin`, `url_archive`, `file`, `timestamp`, and `response`, which is created by the `pywaybackup`-package.
- **Missing files**: The converter will skip entries whose `file` path does not exist and prints a warning


## Example workflow

1. Create or obtain a CSV of Wayback URLs (column name `Internet_Archive_URL`), e.g. `resources/small_test.csv`.
2. Run download mode - this will automatically download, convert to WARC, and clean up for each URL:

   ```bash
   python src/main.py download resources/curated_urls.csv --column_name Internet_Archive_URL --period DAY
   ```

3. The resulting WARC files will be in the `output/` directory (or your custom `--warc-output` directory), named after each URL (e.g., `output/http_www_example_com_page.warc.gz`).

**Advanced workflow with custom directories**:

```bash
python src/main.py download resources/curated_urls.csv \
  --column_name Internet_Archive_URL \
  --period WEEK \
  --snapshot-folder /mnt/data/snapshots \
  --warc-output /mnt/data/archives \
  --workers 10 \
  --clean
```

This will:
- Download snapshots to `/mnt/data/snapshots/`
- Save WARC files to `/mnt/data/archives/`
- Use 10 parallel workers for faster downloads
- Clean up temporary files after each URL is processed


## Troubleshooting
- **Missing CSV columns**: If the script can't find expected CSV columns, inspect the CSV(s) created by `pywaybackup` and ensure the required column names (`file`, `timestamp`, `response`, `url_origin`, `url_archive`) are present.
- **Download failures**: If downloads fail, try rerunning with `--reset` to force re-downloads.
- **Custom period errors**: When using `--period CUSTOM`, both `--start_time` and `--end_time` must be provided in `YYYYMMDDHHMMSS` format.
- **Database index errors**: The tool handles SQLAlchemy `OperationalError` exceptions about existing database indexes gracefully - these are warnings, not fatal errors.


## Next steps / Improvements
- Add argument validation to require `--output` for `convert` mode

