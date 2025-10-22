from pywaybackup import PyWayBackup
from wayback_date_object import WaybackDateObject
from constants import Period, DOWNLOAD_PERIOD, DOWNLOAD_RESET
import re

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

def download_urls_from_csv(csv_file_path: str, url_column_name: str):
    """
    Reads a CSV file containing Internet Archive URLs (eg. https://web.archive.org/web/20251002062751/https://cas.au.dk/erc-webchild),
    retrieves their corresponding Wayback Machine archived URLs and dates, and downloads the archived content for each URL for a period of two weeks around the archived date.

    Args:
        csv_file_path (str): The file path to the CSV file containing the Internet Archive URLs.
        url_column_name (str): The name of the column in the CSV file that contains the URLs.

    Returns:
        None

    Side Effects:
        - Downloads archived content for each URL from the Wayback Machine.
        - Handles and prints TypeError exceptions that may occur during download.
    """
    internet_archive_urls = import_urls_from_csv(csv_file_path, url_column_name)

    for url in internet_archive_urls:
        wayback_date, archived_url = get_wayback_date_and_archived_url(url)

        if DOWNLOAD_PERIOD == Period.DAY:
            start_date = WaybackDateObject(wayback_date.wayback_format())
            start_date.decrement_day()

            end_date = WaybackDateObject(wayback_date.wayback_format())
            end_date.increment_day()

        elif DOWNLOAD_PERIOD == Period.WEEK:
            start_date = WaybackDateObject(wayback_date.wayback_format())
            start_date.decrement_week()

            end_date = WaybackDateObject(wayback_date.wayback_format())
            end_date.increment_week()

        try:
            download_single_url(archived_url, start_date.wayback_format(), end_date.wayback_format())
        except TypeError as e:
            print()
        
    
def download_single_url(url: str, start_date: str, end_date: str):
    """
    Downloads all available snapshots of a given URL from the Internet Archive's Wayback Machine within a specified date range.

    Args:
        url (str): The URL to download snapshots for.
        start_date (str): The start date (inclusive) in 'YYYYMMDD' format.
        end_date (str): The end date (inclusive) in 'YYYYMMDD' format.

    Returns:
        None

    Side Effects:
        - Prints progress and debug information to the console.
        - Downloads and saves the snapshots to disk.
        - Prints the relative paths of the downloaded snapshots.
    """

    print(f"Downloading {url} from {start_date} to {end_date}")
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
    reset=DOWNLOAD_RESET,
    explicit=False
    )

    backup.run()
    backup_paths = backup.paths(rel=True)
    print(backup_paths)


def main():
    # Currently only doesnt support other files than the one presented here. Just need convertng to useing arguments.
    download_urls_from_csv("./resources/curated_urls.csv", "Internet_Archive_URL")

if __name__ == "__main__":
    main()