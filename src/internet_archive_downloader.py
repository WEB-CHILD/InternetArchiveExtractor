from datetime import datetime
import glob
import os
import shutil
import re
import logging

from pywaybackup import PyWayBackup
from pywaybackup.helper import sanitize_filename
from wayback_date_object import WaybackDateObject
from waybackup_to_warc import process_csv_file
from warc_outlinks import fetch_and_archive_outlinks
from constants import Period
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, inspect, text
from logging_config import get_logger, redirect_stdout_to_logger

from utils import import_urls_from_csv

logger = get_logger(__name__)


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
    else:
        print("Assuming URL is a live URL without a timestamp, returning date as None.")
        return None, wayback_url


def download_urls_from_csv(csv_file_path: str, url_column_name: str, start_time: str = None, end_time: str = None, download_period: Period = None, download_reset: bool = False, dir_cleanup: bool = False, workers: int = None, snapshot_folder: str = "./waybackup_snapshots", warc_output: str = "./output", fetch_outlinks: bool = True):
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
        snapshot_folder (str, optional): Path to the snapshot folder. Defaults to "./waybackup_snapshots".
        warc_output (str, optional): Path to the WARC output folder. Defaults to "./output".
        fetch_outlinks (bool, optional): If True, after each WARC is created, download the
            outgoing links found in its archived pages and package them into a separate
            "<name>_outlinks" WARC. Defaults to True.

    Returns:
        None

    Side Effects:
        - Downloads archived content for each URL from the Wayback Machine.
        - Handles and prints TypeError exceptions that may occur during download.
    """
    if download_period is None:
        download_period = Period.FULL
    
    internet_archive_urls = import_urls_from_csv(csv_file_path, url_column_name)

    for url in internet_archive_urls:
        wayback_date, archived_url = get_wayback_date_and_archived_url(url)

        match download_period:
            case Period.DAY:
                logger.info("DAY period selected and applied to download.")
                start_date = WaybackDateObject(wayback_date.wayback_format())
                start_date.decrement_day()

                end_date = WaybackDateObject(wayback_date.wayback_format())
                end_date.increment_day()
            case Period.WEEK:
                logger.info("WEEK period selected and applied to download.")
                start_date = WaybackDateObject(wayback_date.wayback_format())
                start_date.decrement_week()

                end_date = WaybackDateObject(wayback_date.wayback_format())
                end_date.increment_week()
            case Period.FULL:
                logger.info("FULL period selected and applied to download.")
                start_date = WaybackDateObject("19950101000000")
                end_date = WaybackDateObject("20051231235959")
            case Period.CUSTOM:
                logger.info("CUSTOM period selected and applied to download.")
                start_date = WaybackDateObject(start_time)
                end_date = WaybackDateObject(end_time)
            case _:
                raise ValueError(f"Unsupported download period: {download_period}")

        try:
            logger.debug(f"Calling download_single_url with URL: {archived_url}, start_date: {start_date.wayback_format()}, end_date: {end_date.wayback_format()}")
            
            try:
                # Download each URL
                download_single_url(archived_url, start_date.wayback_format(), end_date.wayback_format(), download_reset, workers, snapshot_folder)
                logger.info("Download completed, proceeding to WARC packaging.")
            except OperationalError as e:
                if "index" in str(e) and "already exists" in str(e):
                    logger.warning(f"Index already exists, dropping waybackup indexes and retrying... ({e})")
                    drop_snapshot_indexes(snapshot_folder)

                    # Retry the download after dropping indexes
                    download_single_url(archived_url, start_date.wayback_format(), end_date.wayback_format(), download_reset, workers, snapshot_folder)
                else:
                    raise

            # Package downloaded files into WARC
            logger.info(f"Creating WARC file for URL: {archived_url}")


            waybackup_filename = create_waybackup_filename(archived_url)
            warcfile_name = waybackup_filename.replace("waybackup_", "")
            warcfile_name = warcfile_name.replace(".csv", "")
            warcfile_name = warcfile_name.replace(".", "_")

            process_csv_file(os.path.join(snapshot_folder, waybackup_filename), warc_output,  warcfile_name)

            # Fetch and archive outgoing links from the freshly created WARC files
            if fetch_outlinks:
                create_outlinks_warc(warc_output, warcfile_name, workers)

            drop_snapshot_indexes(snapshot_folder)
            copy_log_files(snapshot_folder)
            if dir_cleanup:
                cleanup_temporary_files(snapshot_folder)
           

        
        except OperationalError as e:
            if "index" in str(e) and "already exists" in str(e):
                logger.warning(f"Database index already exists, continuing... ({e})")
            else:
                raise
        except TypeError as e:
            logger.error(f"TypeError occurred: {e}")

def create_waybackup_filename(archived_url):
    """
    Constructs a waybackup CSV filename from an archived URL.
    
    Uses PyWayBackup's sanitization to ensure all special characters are safely
    converted to periods, matching the behavior of PyWayBackup's filename generation.
    Additionally removes duplicate punctuation characters.
    
    This handles special characters like: ?, =, #, !, ~, :, /, etc.

    Args:
        archived_url (str): The archived URL (e.g., "http://www.example.com/page")
    
    Returns:
        str: Formatted filename (e.g., "waybackup_http.www.example.com.page.csv")
    """
    sanitized = sanitize_filename(archived_url)
    sanitized = re.sub(r'([^\w\s])\1+', r'\1', sanitized)
    return f"waybackup_{sanitized}.csv"

def create_outlinks_warc(warc_output: str, warcfile_name: str, threads: int = None):
    """
    Finds the WARC files just created for a source URL and archives their outgoing links.

    Locates every "<warcfile_name>-XXXX.warc.gz" part file in the output folder, extracts
    the outgoing links from the archived HTML pages, downloads each linked resource from
    the Wayback Machine using the same capture date as the referencing page, and packages
    the results into a separate "<warcfile_name>_outlinks-XXXX.warc.gz" WARC file.

    Args:
        warc_output (str): Path to the WARC output folder.
        warcfile_name (str): Base name of the source WARC file(s) (without the "-XXXX" suffix).
        threads (int, optional): Number of concurrent download threads. Defaults to 5.

    Returns:
        None
    """
    pattern = os.path.join(warc_output, f"{warcfile_name}-*.warc.gz")
    source_warcs = sorted(glob.glob(pattern))

    if not source_warcs:
        logger.warning(f"No WARC files found for outgoing-link extraction: {pattern}")
        return

    logger.info(f"Archiving outgoing links from {len(source_warcs)} WARC file(s) for '{warcfile_name}'.")
    fetch_and_archive_outlinks(
        source_warcs, warc_output, warcfile_name,
        threads=(threads if threads is not None else 5),
    )

def cleanup_temporary_files(snapshot_folder: str = "./waybackup_snapshots"):
    """
    Cleans up temporary files and directories created during the download process.
    
    This function removes all content from the snapshot directory to free up disk space.
    
    Args:
        snapshot_folder (str, optional): Path to the snapshot folder. Defaults to "./waybackup_snapshots".
    """

    if os.path.exists(snapshot_folder):
        for item in os.listdir(snapshot_folder):
            item_path = os.path.join(snapshot_folder, item)
            if os.path.isfile(item_path): # delete individual files
                os.remove(item_path)
            elif os.path.isdir(item_path): # delete subdirectories
                shutil.rmtree(item_path)
        logger.debug(f"Temporary directory '{snapshot_folder}' has been cleaned.")
    else:
        logger.debug(f"No temporary directory '{snapshot_folder}' found to clean.")

    
def download_single_url(url: str, start_date: str, end_date: str, download_reset: bool = False, workers: int = None, snapshot_folder: str = "./waybackup_snapshots"):
    """
    Downloads all available snapshots of a given URL from the Internet Archive's Wayback Machine within a specified date range.

    Args:
        url (str): The URL to download snapshots for.
        start_date (str): The start date (inclusive) in 'YYYYMMDD' format.
        end_date (str): The end date (inclusive) in 'YYYYMMDD' format.
        download_reset (bool, optional): Whether to reset downloads. Defaults to False.
        workers (int, optional): Number of worker threads. Defaults to None.
        snapshot_folder (str, optional): Path to the snapshot folder. Defaults to "./waybackup_snapshots".

    Returns:
        None

    Side Effects:
        - Logs progress and debug information to the console.
        - Downloads and saves the snapshots to disk.
    """

    logger.info(f"Downloading {url} from {start_date} to {end_date}")

    if download_reset:
        logger.info("Download reset is enabled.")

    # PyWayBackup writes normal progress messages (e.g., "process cdx") to stderr,
    # so map stderr to INFO here to avoid false ERROR logs.
    with redirect_stdout_to_logger(logger, stderr_level=logging.INFO):
        backup = PyWayBackup(
            url=url,
            all=True,
            start=start_date,
            end=end_date,
            silent=False,
            debug=True,
            log=True,
            keep=True,
            workers=(workers if workers is not None else 5),
            reset=download_reset,
            explicit=('?' in url),
            output=snapshot_folder
        )
        
        backup.run()
        #backup_paths = backup.paths(rel=True)
        #print(backup_paths)


def drop_snapshot_indexes(snapshot_folder: str = "./waybackup_snapshots"):
    """
    Drops all indexes in all SQLite database files 
    found in the specified directory.
    
    Args:
        snapshot_folder (str): The directory path to search for SQLite database files.
                        Defaults to "./waybackup_snapshots".
    
    Returns:
        None
    
    Side Effects:
        - Connects to each .db file found in the directory
        - Drops all indexes in each database
        - Logs status messages for each operation
    """

    if not os.path.exists(snapshot_folder):
        logger.info(f"Directory '{snapshot_folder}' does not exist.")
        return
    
    db_files = [f for f in os.listdir(snapshot_folder) if f.endswith('.db')]
    
    if not db_files:
        logger.info(f"No database files found in '{snapshot_folder}'.")
        return
    
    for db_file in db_files:
        db_path = os.path.join(snapshot_folder, db_file)
        logger.info(f"Processing database: {db_file}")
        
        try:
            engine = create_engine(f"sqlite:///{db_path}")
            inspector = inspect(engine)
            
            with engine.connect() as conn:
                for table_name in inspector.get_table_names():
                    indexes = inspector.get_indexes(table_name)
                    for idx in indexes:
                        try:
                            conn.execute(text(f"DROP INDEX IF EXISTS {idx['name']}"))
                            logger.info(f"  Dropped index: {idx['name']} from table: {table_name}")
                        except Exception as idx_error:
                            logger.error(f"  Failed to drop index {idx['name']}: {idx_error}")
                conn.commit()
            
            engine.dispose()
            logger.info(f"Finished processing database: {db_file}")
        
        except Exception as e:
            logger.error(f"Error processing database {db_file}: {e}")

def copy_log_files(snapshot_folder: str = "./waybackup_snapshots", dest_dir: str = "./logs"):
    """
    Copies all .log files from the source directory to the destination directory.
    
    Args:
        snapshot_folder (str): The source directory to search for .log files.
                         Defaults to "./waybackup_snapshots".
        dest_dir (str): The destination directory to copy log files to.
                       Defaults to "./logs".
    
    Returns:
        None
    
    Side Effects:
        - Creates the destination directory if it doesn't exist
        - Copies all .log files from source to destination
        - Logs status messages for each operation
    """
    if not os.path.exists(snapshot_folder):
        logger.debug(f"Source directory '{snapshot_folder}' does not exist.")
        return
    
    # Create destination directory if it doesn't exist
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
        logger.debug(f"Created destination directory: {dest_dir}")
    
    log_files = [f for f in os.listdir(snapshot_folder) if f.endswith('.log')]
    
    if not log_files:
        logger.debug(f"No .log files found in '{snapshot_folder}'.")
        return
    
    for log_file in log_files:
        source_path = os.path.join(snapshot_folder, log_file)
        dest_path = os.path.join(dest_dir, log_file)

        # Check if file already exists and append timestamp if needed
        if os.path.exists(dest_path):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename, extension = os.path.splitext(log_file)
            new_filename = f"{filename}_{timestamp}.{extension}"
            dest_path = os.path.join(dest_dir, new_filename)
            logger.info(f"File already exists, renaming to: {new_filename}")
        
        try:
            shutil.copy2(source_path, dest_path)
            logger.debug(f"Copied: {log_file} -> {dest_dir}")
        except Exception as e:
            logger.error(f"Failed to copy {log_file}: {e}")
    
    logger.debug(f"Finished copying {len(log_files)} log file(s) to '{dest_dir}'.")

def main():
    # Currently only doesnt support other files than the one presented here. Just need convertng to useing arguments.
    # ONLY USED FOR TESTING PURPOSES
    download_urls_from_csv("./resources/small_test.csv", "Internet_Archive_URL")

if __name__ == "__main__":
    main()