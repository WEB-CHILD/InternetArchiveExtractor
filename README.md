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
python src/main.py download <input> [--column_name COLUMN] [--period PERIOD] [--reset] [--start_time START] [--end_time END] [--snapshot-folder FOLDER] [--warc-output FOLDER] [--workers N] [--clean]

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

**Example with CUSTOM period**:

```bash
python src/main.py download resources/curated_urls.csv --period CUSTOM --start_time 20000101000000 --end_time 20001231235959
```

**Example with custom snapshot and WARC output folders**:

```bash
python src/main.py download resources/curated_urls.csv --snapshot-folder /data/snapshots --warc-output /data/warcs
```

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

