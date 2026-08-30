import sys
import argparse
import logging
from enum import Enum
from waybackup_to_warc import combine_csv_files, process_csv_file, COMBINED_CSV_PATH
from internet_archive_downloader import download_urls_from_csv, fetch_outlinks_for_existing_warcs
from constants import Period
from logging_config import setup_logging, get_logger

# Setup logging

parser = argparse.ArgumentParser(description="Internet Archive Extractor")

parser.add_argument("mode", help="The mode to run the script in: 'download' or 'convert'")
parser.add_argument("input", nargs="?", help="The input file or directory path. Not required when --outlinks-only is set.")
parser.add_argument("--output", help="The output file name for the generated WARC file. Only applicable for modes: 'convert' or 'full'.")
parser.add_argument("--column_name", default="Internet_Archive_URL", help="The column name in the CSV file that contains the URLs for download. Default is 'Internet_Archive_URL'.")
parser.add_argument("--period", default="FULL", help="The period around the archived date to download. Options are: 'DAY', 'WEEK', 'FULL' and 'CUSTOM'. Default is 'DAY'.")
parser.add_argument("--reset", action="store_true", help="If set, resets the download process completely.")
parser.add_argument("--start_time", help="The start time for the CUSTOM period download in 'YYYYMMDDHHMMSS' format.")
parser.add_argument("--end_time", help="The end time for the CUSTOM period download in 'YYYYMMDDHHMMSS' format.")
parser.add_argument("--clean", action="store_true", help="If set, deletes the intermediate CSV, DB and CDX files after processing.")
parser.add_argument("--workers", type=int, default=5, help="Number of worker threads to use for downloading. Default is 5.")
parser.add_argument("--snapshot-folder", default="./waybackup_snapshots", help="Path to the snapshot folder where pywaybackup stores downloaded files. Default is './waybackup_snapshots'.")
parser.add_argument("--warc-output", default="./output", help="Path to the output folder where WARC files will be stored. Default is './output'.")
parser.add_argument("--no-outlinks", action="store_true", help="If set, skips the step that downloads and archives the outgoing links found in the created WARC files.")
parser.add_argument("--scan-workers", type=int, default=None, help="Number of processes used to scan WARC files for outgoing links. Scanning is CPU-bound, so this defaults to one per CPU core. Use 1 to scan in a single process.")
parser.add_argument("--outlinks-only", action="store_true", help="If set, skips downloading and WARC packaging entirely and only archives the outgoing links of the WARC files already present in the --warc-output folder. Only applicable for mode: 'download'.")
parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level. Default is INFO.")
parser.add_argument("--log-file", help="Path to a log file. If not specified, logs only to console.")

class Mode(Enum):
    """
    Enum for the different modes of operation. 
    """
    DOWNLOAD = 1
    CONVERT = 2

args = parser.parse_args()

# Convert log level string to logging constant
log_level = getattr(logging, args.log_level.upper())
setup_logging(log_level=log_level, log_file=args.log_file)
logger = get_logger("InternetArchiveExtractor")

try:
    Mode(args.mode.upper())  
except ValueError:
    try:
        Mode[args.mode.upper()]  
    except KeyError:
        logger.error(f"Invalid mode: {args.mode}. Choose from 'download' or 'convert'.")
        sys.exit(1)

try:
    Period(args.period.upper())  
except ValueError:
    try:
        Period[args.period.upper()]  
    except KeyError:
        logger.error(f"Invalid period: {args.period}. Choose from 'DAY', 'WEEK', 'FULL' or 'CUSTOM'.")
        sys.exit(1)

def choose_mode():
    download_period = Period(args.period.upper())
    download_reset = args.reset
    dir_cleanup = args.clean
    workers = args.workers
    snapshot_folder = args.snapshot_folder
    warc_output = args.warc_output
    fetch_outlinks = not args.no_outlinks
    scan_workers = args.scan_workers

    # Checking of arguments and exiting if any are incompatible.
    outlinks_only = args.outlinks_only

    if outlinks_only and args.mode.upper() != Mode.DOWNLOAD.name:
        logger.error("--outlinks-only is only applicable for mode: 'download'.")
        sys.exit(1)

    if not outlinks_only and not args.input:
        logger.error("An input file or directory path is required (omit it only with --outlinks-only).")
        sys.exit(1)

    if outlinks_only and args.no_outlinks:
        logger.error("--outlinks-only and --no-outlinks cannot be used together.")
        sys.exit(1)

    if download_period == Period.CUSTOM and not outlinks_only:
        logger.info("CUSTOM period selected.")
        if not args.start_time or not args.end_time:
            logger.error("For CUSTOM period, both --start_time and --end_time must be provided.")
            sys.exit(1)

    if args.mode.upper() == Mode.DOWNLOAD.name:
        if outlinks_only:
            logger.info("Outlinks-only mode selected: using the WARC files already on disk.")
            fetch_outlinks_for_existing_warcs(warc_output, workers, scan_workers)
            return
        logger.info("Download mode selected.")
        download_urls_from_csv(args.input, args.column_name, args.start_time, args.end_time, download_period, download_reset, dir_cleanup, workers, snapshot_folder, warc_output, fetch_outlinks, scan_workers)
    elif args.mode.upper() == Mode.CONVERT.name:
        logger.info("Convert mode selected.")
        combine_csv_files(args.input, COMBINED_CSV_PATH)
        process_csv_file(COMBINED_CSV_PATH, warc_output, args.output)
    else:
        logger.error(f"Invalid mode: {args.mode}. Choose from 'download' or 'convert'.")
        sys.exit(1)

def main():
    choose_mode()


if __name__ == "__main__":
    main()