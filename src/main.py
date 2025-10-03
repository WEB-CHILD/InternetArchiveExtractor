import sys
import argparse
from enum import Enum
from waybackup_to_warc import combine_csv_files, process_csv_file, COMBINED_CSV_PATH
from internet_archive_downloader import download_urls_from_csv

parser = argparse.ArgumentParser(description="Internet Archive Extractor")

parser.add_argument("mode", help="The mode to run the script in: 'download', 'convert' or 'full'.")
parser.add_argument("input", help="The input file or directory path.")
parser.add_argument("--output", help="The output file name for the generated WARC file. Only applicable for modes: 'convert' or 'full'.")
parser.add_argument("--column_name", default="Internet_Archive_URL", help="The column name in the CSV file that contains the URLs for download. Default is 'Internet_Archive_URL'.")

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

def choose_mode():
    if args.mode.upper() == Mode.DOWNLOAD.name:
        print("Download mode selected.")
        download_urls_from_csv(args.input, args.column_name)
    elif args.mode.upper() == Mode.CONVERT.name:
        print("Convert mode selected.")
        combine_csv_files(args.input, COMBINED_CSV_PATH)
        process_csv_file(COMBINED_CSV_PATH, 'output', args.output)
    elif args.mode.upper() == Mode.FULL.name:
        print("Full mode selected.")

        download_urls_from_csv(args.input, args.column_name)
        combine_csv_files("waybackup_snapshots", COMBINED_CSV_PATH)
        process_csv_file(COMBINED_CSV_PATH, 'output', args.output)
    else:
        print(f"Invalid mode: {args.mode}. Choose from 'download', 'convert' or 'full'.")
        sys.exit(1)

# TODO: Bind both parts of the program together in this main class, which should support argparse for input and output files and making it possible to 
# run both parts of the program in one go or seperate as needed..
def main():
    choose_mode()


if __name__ == "__main__":
    main()