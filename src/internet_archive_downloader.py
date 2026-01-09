import os
import shutil
import re

from pywaybackup import PyWayBackup
from wayback_date_object import WaybackDateObject
from waybackup_to_warc import process_csv_file
from constants import Period
from sqlalchemy.exc import OperationalError

from utils import import_urls_from_csv


def get_wayback_date_and_archived_url(wayback_url: str):
    """
    Extracts the archive date and archived URL from a Wayback Machine URL.

    Args:
        wayback_url (str): The URL from the Wayback Machine in the format
            'https://web.archive.org/web/<timestamp>/<archived_url>'.

    Returns:
        tuple: A tuple containing:
            - date (WaybackDateObject): The extracted date as a WaybackDateObject.
            - archived_url (str): The original URL archived by the Wayback Machine.

    Raises:
        AttributeError: If the input URL does not match the expected Wayback Machine format.
    """
    match = re.match(r"https://web\.archive\.org/web/(\d+)/(.*)", wayback_url)
    if match:
        date = WaybackDateObject(match.group(1))
        archived_url = match.group(2)
        return date, archived_url

def download_urls_from_csv(csv_file_path: str, url_column_name: str, start_time: str = None, end_time: str = None, download_period: Period = None, download_reset: bool = False):
    """
    Reads a CSV file containing Internet Archive URLs (eg. https://web.archive.org/web/20251002062751/https://cas.au.dk/erc-webchild),
    retrieves their corresponding Wayback Machine archived URLs and dates, and downloads the archived content for each URL for a period of two weeks around the archived date.

    Args:
        csv_file_path (str): The file path to the CSV file containing the Internet Archive URLs.
        url_column_name (str): The name of the column in the CSV file that contains the URLs.
        start_time (str, optional): The start time for CUSTOM period.
        end_time (str, optional): The end time for CUSTOM period.
        download_period (Period, optional): The period to download. Defaults to Period.DAY.
        download_reset (bool, optional): Whether to reset downloads. Defaults to False.

    Returns:
        None

    Side Effects:
        - Downloads archived content for each URL from the Wayback Machine.
        - Handles and prints TypeError exceptions that may occur during download.
    """
    if download_period is None:
        download_period = Period.DAY
    
    internet_archive_urls = import_urls_from_csv(csv_file_path, url_column_name)

    for url in internet_archive_urls:
        wayback_date, archived_url = get_wayback_date_and_archived_url(url)


        match download_period:
            case Period.DAY:
                print("DAY period selected and applied to download.")
                start_date = WaybackDateObject(wayback_date.wayback_format())
                start_date.decrement_day()

                end_date = WaybackDateObject(wayback_date.wayback_format())
                end_date.increment_day()
            case Period.WEEK:
                print("WEEK period selected and applied to download.")
                start_date = WaybackDateObject(wayback_date.wayback_format())
                start_date.decrement_week()

                end_date = WaybackDateObject(wayback_date.wayback_format())
                end_date.increment_week()
            case Period.FULL:
                print("FULL period selected and applied to download.")
                start_date = WaybackDateObject("19950101000000")
                end_date = WaybackDateObject("20051231235959")
            case Period.CUSTOM:
                print("CUSTOM period selected and applied to download.")
                start_date = WaybackDateObject(start_time)
                end_date = WaybackDateObject(end_time)
            case _:
                raise ValueError(f"Unsupported download period: {download_period}")

        try:

            print(f"Calling download_single_url with URL: {archived_url}, start_date: {start_date.wayback_format()}, end_date: {end_date.wayback_format()}")
            # Download each URL
            download_single_url(archived_url, start_date.wayback_format(), end_date.wayback_format())
            print("Download completed, proceeding to WARC packaging.")
           
            # Package downloaded files into WARC
            print("Creating WARC file for URL:", archived_url)


            waybackup_filename = create_waybackup_filename(archived_url)

            warcfile_name = waybackup_filename.replace("waybackup_", "")
            warcfile_name = warcfile_name.replace(".", "_")

            process_csv_file("./waybackup_snapshots/" + waybackup_filename, 'output',  warcfile_name)

            cleanup_temporary_files()
           

        
        except OperationalError as e:
            if "index" in str(e) and "already exists" in str(e):
                print(f"Warning: Database index already exists, continuing... ({e})")
            else:
                raise
        except TypeError as e:
            print(f"TypeError occurred: {e}")

def create_waybackup_filename(archived_url):
    """
    Constructs a waybackup CSV filename from an archived URL.
    
    Converts URL format to PyWayBackup's filename convention by replacing 
    protocol separators and slashes with dots, removing duplicate punctuation.

    Conversion is as follows:
    - "http://" becomes "http."
    - "https://" becomes "https."
    - All "/" characters are replaced with "."
    - Duplicate punctuation characters are reduced to a single instance. E.g., ".." becomes "."
    
    Args:
        archived_url (str): The archived URL (e.g., "http://www.example.com/page")
    
    Returns:
        str: Formatted filename (e.g., "waybackup_http.www.example.com.page.csv")
    """
    waybackup_filename = archived_url.replace("http://","http.").replace("https://","https.") + ".csv"
    waybackup_filename = "waybackup_" + waybackup_filename
    waybackup_filename = re.sub(r'/', '.', waybackup_filename)
    waybackup_filename = re.sub(r'([^\w\s])\1+', r'\1', waybackup_filename)
    return waybackup_filename

def cleanup_temporary_files():
    """
    Cleans up temporary files and directories created during the download process.
    
    This function removes all content from the 'waybackup_snapshots' directory to free up disk space.
    """

    temp_dir = "./waybackup_snapshots"
    if os.path.exists(temp_dir):
        for item in os.listdir(temp_dir):
            item_path = os.path.join(temp_dir, item)
            if os.path.isfile(item_path): # delete individual files
                os.remove(item_path)
            elif os.path.isdir(item_path): # delete subdirectories
                shutil.rmtree(item_path)
        print(f"Temporary directory '{temp_dir}' has been cleaned.")
    else:
        print(f"No temporary directory '{temp_dir}' found to clean.")

    
def download_single_url(url: str, start_date: str, end_date: str, download_reset: bool = False):
    """
    Downloads all available snapshots of a given URL from the Internet Archive's Wayback Machine within a specified date range.

    Args:
        url (str): The URL to download snapshots for.
        start_date (str): The start date (inclusive) in 'YYYYMMDD' format.
        end_date (str): The end date (inclusive) in 'YYYYMMDD' format.
        download_reset (bool, optional): Whether to reset downloads. Defaults to False.

    Returns:
        None

    Side Effects:
        - Prints progress and debug information to the console.
        - Downloads and saves the snapshots to disk.
        - Prints the relative paths of the downloaded snapshots.
    """

    print(f"Downloading {url} from {start_date} to {end_date}")

    if download_reset:
        print("Download reset is enabled.")

    backup = PyWayBackup(
    url=url,
    all=True,
    start=start_date,
    end=end_date,
    silent=False,
    debug=True,
    log=True,
    keep=True,
    workers=5,
    reset=download_reset,
    explicit=False
    )

    backup.run()
    #backup_paths = backup.paths(rel=True)
    #print(backup_paths)


def main():
    # Currently only doesnt support other files than the one presented here. Just need convertng to useing arguments.
    # ONLY USED FOR TESTING PURPOSES
    download_urls_from_csv("./resources/small_test.csv", "Internet_Archive_URL")

if __name__ == "__main__":
    main()