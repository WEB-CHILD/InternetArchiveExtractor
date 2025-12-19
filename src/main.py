import sys
import argparse
from enum import Enum
from waybackup_to_warc import combine_csv_files, process_csv_file, COMBINED_CSV_PATH
from internet_archive_downloader import download_urls_from_csv
from constants import Period

parser = argparse.ArgumentParser(description="Internet Archive Extractor")

parser.add_argument("mode", help="The mode to run the script in: 'download', 'convert' or 'full'.")
parser.add_argument("input", help="The input file or directory path.")
parser.add_argument("--output", help="The output file name for the generated WARC file. Only applicable for modes: 'convert' or 'full'.")
parser.add_argument("--column_name", default="Internet_Archive_URL", help="The column name in the CSV file that contains the URLs for download. Default is 'Internet_Archive_URL'.")
parser.add_argument("--period", default="DAY", help="The period around the archived date to download. Options are: 'DAY', 'WEEK', 'FULL' and 'CUSTOM'. Default is 'DAY'.")
parser.add_argument("--reset", action="store_true", help="If set, resets the download process completely.")
parser.add_argument("--start_time", help="The start time for the CUSTOM period download in 'YYYYMMDDHHMMSS' format.")
parser.add_argument("--end_time", help="The end time for the CUSTOM period download in 'YYYYMMDDHHMMSS' format.")

class Mode(Enum):
    """
    Enum for the different modes of operation.
    """
    FULL = 1
    DOWNLOAD = 2
    CONVERT = 3

args = parser.parse_args()

try:
    Mode(args.mode.upper())  
except ValueError:
    try:
        Mode[args.mode.upper()]  
    except KeyError:
        print(f"Invalid mode: {args.mode}. Choose from 'download', 'convert' or 'full'.")
        sys.exit(1)

try:
    Period(args.period.upper())  
except ValueError:
    try:
        Period[args.period.upper()]  
    except KeyError:
        print(f"Invalid period: {args.period}. Choose from 'DAY', 'WEEK' or 'FULL'.")
        sys.exit(1)

def choose_mode():
    download_period = Period(args.period.upper())
    download_reset = args.reset

    if download_period == Period.CUSTOM:
        print("CUSTOM period selected.")
        if not args.start_time or not args.end_time:
            print("For CUSTOM period, both --start_time and --end_time must be provided.")
            sys.exit(1)
        



    if args.mode.upper() == Mode.DOWNLOAD.name:
        print("Download mode selected.")
        download_urls_from_csv(args.input, args.column_name, args.start_time, args.end_time, download_period, download_reset)
    elif args.mode.upper() == Mode.CONVERT.name:
        print("Convert mode selected.")
        combine_csv_files(args.input, COMBINED_CSV_PATH)
        process_csv_file(COMBINED_CSV_PATH, 'output', args.output)
    elif args.mode.upper() == Mode.FULL.name:
        print("Full mode selected.")

        download_urls_from_csv(args.input, args.column_name, args.start_time, args.end_time, download_period, download_reset)
        combine_csv_files("waybackup_snapshots", COMBINED_CSV_PATH)
        process_csv_file(COMBINED_CSV_PATH, 'output', args.output)
    else:
        print(f"Invalid mode: {args.mode}. Choose from 'download', 'convert' or 'full'.")
        sys.exit(1)

def main():
    choose_mode()


if __name__ == "__main__":
    main()