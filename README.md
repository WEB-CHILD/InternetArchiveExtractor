# InternetArchiveExtractor

This repository extracts archived content from the Wayback Machine and converts collected metadata and downloaded snapshot files into compressed WARC files. The project currently supports three primary modes of operation: downloading snapshots from the Internet Archive, combining/cleaning CSV metadata produced by Wayback backup tools, and converting that metadata + downloaded files into a WARC-GZ archive.

## What this does (short)
- Download mode: reads a CSV of Internet Archive (Wayback) URLs, determines snapshot ranges, and uses `pywaybackup` to download snapshots (the downloads are stored in the local `waybackup_snapshots/` folder by default).
- Convert mode: combines CSV files (from a directory) into a single CSV and then converts that CSV into a compressed WARC (`.warc.gz`) using `warcio`.
- Full mode: runs download then combine+convert to produce a WARC in one run.

## Requirements
Install the Python dependencies from the repository `requirements.txt`:

```
pip install -r requirements.txt
```

Notable packages used:
- pywaybackup — downloads Wayback snapshots
- pandas — CSV handling and merging when combining multiple CSVs
- warcio — writing WARC records

See `requirements.txt` for the exact pinned versions used in this repository.

## Project layout (important files)
- `src/main.py` — command-line entry point that exposes `download`, `convert`, and `full` modes.
- `src/internet_archive_downloader.py` — logic that reads an input CSV of Internet Archive URLs and runs `pywaybackup` to download snapshots.
- `src/waybackup_to_warc.py` — functions to combine CSV files, clean URLs (remove `:80`), and produce a `.warc.gz` from a CSV of records.
- `resources/` — example CSVs (e.g. `curated_urls.csv`) useful for quick testing.

## How to run
Usage pattern for the main runner (`src/main.py`):

```
python src/main.py <mode> <input> [--output OUTPUT] [--column_name COLUMN] [--period DAY|WEEK] [--reset]
```

Modes and example usage:

- Download mode — download snapshots listed in a CSV

	- Description: Reads a CSV containing full Wayback URLs such as `https://web.archive.org/web/20251002062751/https://example.com/page` and downloads snapshots for a small period around the archived date.
	- Required `input`: path to the CSV file to read (e.g. `resources/curated_urls.csv`). The default column name expected is `Internet_Archive_URL`.
	- Example:

		```
		python src/main.py download resources/curated_urls.csv --column_name Internet_Archive_URL --period DAY
		```

	- Flags:
		- `--period` — `DAY` (default) or `WEEK`. Controls whether the downloader fetches snapshots ±1 day or ±1 week around the archived date.
		- `--reset` — if present, passes `reset=True` to `pywaybackup` (useful to force re-download).

- Convert mode — combine CSVs and produce a WARC

	- Description: Combine all `.csv` files from the specified directory into a single CSV (written to `combined_output.csv` by default) and convert that CSV to a WARC-GZ.
	- Required `input`: path to a directory that contains CSV files to combine (e.g. `waybackup_snapshots/` or any folder with CSV exports).
	- `--output` should be provided to name the resulting WARC file (the code will append `.warc.gz`).
	- Example:

		```
		python src/main.py convert waybackup_snapshots --output mysite_archive
		```

	- Notes: The script combines CSV files using `pandas.concat` and writes the combined CSV to `combined_output.csv` (value of `COMBINED_CSV_PATH`). The combined CSV is then read and converted into `output/<output>.warc.gz`.

- Full mode — download then convert

	- Description: Downloads snapshots from the input CSV, then combines CSVs (from `waybackup_snapshots`) and converts them into a WARC.
	- Example:

		```
		python src/main.py full resources/curated_urls.csv --output combined_site_archive
		```


## Important implementation notes
- Default combined CSV file path: `combined_output.csv` (the module-level `COMBINED_CSV_PATH` in `src/waybackup_to_warc.py`).
- The CSVs read by the converter are expected to contain columns like `url_origin`, `url_archive`, `file`, `timestamp`, and `response` (see `src/waybackup_to_warc.py` for required field names used when creating WARC records).
- The converter will skip entries whose `file` path does not exist and prints a warning. It also emits simple 404/500 WARC entries when those response codes are encountered.

## Example quick workflow
1. Create or obtain a CSV of Wayback URLs (column name `Internet_Archive_URL`), e.g. `resources/curated_urls.csv`.
2. Download snapshots for those URLs:

	 ```
	 python src/main.py download resources/curated_urls.csv --column_name Internet_Archive_URL
	 ```

	 This writes per-site CSVs and downloaded files into `waybackup_snapshots/` (and related subfolders) using `pywaybackup`.

3. Combine CSVs and convert to WARC:

	 ```
	 python src/main.py convert waybackup_snapshots --output archived_site
	 ```

4. The resulting WARC will be written to `output/archived_site.warc.gz`.

## Troubleshooting
- If `--output` is not provided for `convert`/`full`, the conversion step may attempt to use a `None` filename. Always provide `--output` when converting.
- If the script can't find expected CSV columns, inspect the CSV(s) created by `pywaybackup` and ensure the required column names (`file`, `timestamp`, `response`, `url_origin`) are present.
- If downloads fail, try rerunning with `--reset` to force re-downloads.

## Next steps / Improvements
- Add argument validation to require `--output` for `convert` and `full` modes.
- Add unit tests for CSV combining and WARC creation edge cases (missing files, bad timestamps).

