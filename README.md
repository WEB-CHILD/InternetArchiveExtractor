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
python src/main.py <mode> <input> [--output OUTPUT] [--column_name COLUMN] [--period PERIOD] [--reset] [--start_time START] [--end_time END]
```

### Modes and example usage:

#### Download mode — download snapshots listed in a CSV

**Description**: Reads a CSV containing full Wayback URLs such as `https://web.archive.org/web/20251002062751/https://example.com/page` and downloads snapshots for a specified period around the archived date. After downloading each URL, the tool automatically:
1. Converts the downloaded snapshots to a WARC file (saved in `output/` directory)
2. Cleans up temporary files from `waybackup_snapshots/` directory

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

**Example with CUSTOM period**:

```bash
python src/main.py download resources/curated_urls.csv --period CUSTOM --start_time 20000101000000 --end_time 20001231235959
```

#### Convert mode — combine CSVs and produce a WARC

**Description**: Combine all `.csv` files from the specified directory into a single CSV (written to `combined_output.csv` by default) and convert that CSV to a WARC-GZ.

**Required `input`**: Path to a directory that contains CSV files to combine (e.g. `waybackup_snapshots/` or any folder with CSV exports).

**Required `--output`**: Name for the resulting WARC file (the code will append `.warc.gz`).

**Example**:

```bash
python src/main.py convert waybackup_snapshots --output mysite_archive
```

**Notes**: 
- The script combines CSV files using `pandas.concat` and writes the combined CSV to `combined_output.csv`.
- The combined CSV is then read and converted into `output/<output>.warc.gz`.
- The CSVs are expected to contain columns: `url_origin`, `url_archive`, `file`, `timestamp`, and `response`.


## Important implementation notes
- **Automatic workflow in Download mode**: When downloading, each URL is processed individually:
  1. Downloads snapshots using `pywaybackup` to `waybackup_snapshots/` directory
  2. Generates a CSV file with snapshot metadata
  3. Automatically create WARC file of downloaded data (saved to `output/` directory)
  4. Cleans up temporary files and subdirectories from `waybackup_snapshots/`
- **Expected CSV columns**: The CSVs read by the converter must contain: `url_origin`, `url_archive`, `file`, `timestamp`, and `response`, which is created by the `pywaybackup`-package.
- **Missing files**: The converter will skip entries whose `file` path does not exist and prints a warning


## Example workflow

1. Create or obtain a CSV of Wayback URLs (column name `Internet_Archive_URL`), e.g. `resources/small_test.csv`.
2. Run download mode - this will automatically download, convert to WARC, and clean up for each URL:

   ```bash
   python src/main.py download resources/curated_urls.csv --column_name Internet_Archive_URL --period DAY
   ```

3. The resulting WARC files will be in the `output/` directory, named after each URL (e.g., `output/http_www_example_com_page.warc.gz`).


## Troubleshooting
- **Missing CSV columns**: If the script can't find expected CSV columns, inspect the CSV(s) created by `pywaybackup` and ensure the required column names (`file`, `timestamp`, `response`, `url_origin`, `url_archive`) are present.
- **Download failures**: If downloads fail, try rerunning with `--reset` to force re-downloads.
- **Custom period errors**: When using `--period CUSTOM`, both `--start_time` and `--end_time` must be provided in `YYYYMMDDHHMMSS` format.
- **Database index errors**: The tool handles SQLAlchemy `OperationalError` exceptions about existing database indexes gracefully - these are warnings, not fatal errors.


## Next steps / Improvements
- Add argument validation to require `--output` for `convert` mode
- Add unit tests for CSV combining and WARC creation edge cases (missing files, bad timestamps)

