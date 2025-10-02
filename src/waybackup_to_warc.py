import glob
import os
import sys
import pandas as pd
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter
from warcio.recordloader import ArcWarcRecord
from warcio.statusandheaders import StatusAndHeaders
import csv
from datetime import datetime

def remove_port_80(url):
    if ":80" in url:
        return url.replace(":80", "")
    return url

def process_csv(file_path):
    data = pd.read_csv(file_path)
    data['url_archive'] = data['url_archive'].apply(remove_port_80)
    data['url_origin'] = data['url_origin'].apply(remove_port_80)
    return data

def write_404_warc_entry(writer, url, warc_date):
    http_headers = StatusAndHeaders('404 Not Found', [('Content-Type', 'text/html')], protocol='HTTP/1.0')
    warc_type = 'response'
    record = writer.create_warc_record(
        url,
        warc_type,
        payload = None,
        http_headers = http_headers,
        warc_headers_dict={'WARC-Date': warc_date} if warc_date else None
    )
    print(f"Writing 404 record for URL: {url}")
    writer.write_record(record)

def write_500_warc_entry(writer, url, warc_date):
    http_headers = StatusAndHeaders('500 Internal Server Error', [('Content-Type', 'text/html')], protocol='HTTP/1.0')
    warc_type = 'response'
    record = writer.create_warc_record(
        url,
        warc_type,
        payload = None,
        http_headers = http_headers,
        warc_headers_dict={'WARC-Date': warc_date} if warc_date else None
    )
    print(f"Writing 500 record for URL: {url}")
    writer.write_record(record)

def create_warc_gz(data, output_dir, output_filename):
    """
    Creates a compressed WARC (Web ARChive) file (.warc.gz) from a list of data entries.
    Each entry in `data` should be a dictionary containing at least the following keys:
        - 'url_origin': The original URL of the resource.
        - 'file': The local file path to the resource content.
        - 'timestamp': The timestamp of the capture in 'YYYYMMDDHHMMSS' format.
        - 'response': The HTTP response code as a string or integer (e.g., '200', '404', '500').
    The function processes each entry and writes a corresponding WARC record:
        - For HTTP 404 and 500 responses, special WARC records are created using helper functions.
        - For successful responses (HTTP 200), the content is read from the specified file and written as a WARC response record.
        - The content type is inferred from the file extension.
        - If the file does not exist, the entry is skipped and a warning is printed.
    Parameters:
        data (list of dict): List of dictionaries containing resource metadata and file paths.
        output_dir (str): Directory where the output WARC file will be saved.
        output_filename (str): Base name for the output WARC file (without extension).
    Side Effects:
        - Creates the output directory if it does not exist.
        - Writes a compressed WARC file to disk.
        - Prints progress and summary information to stdout.
    Returns:
        None
    """

    total_counter = 0
    success_counter = 0
    internal_service_error_counter = 0
    not_found_counter = 0
    os.makedirs(output_dir, exist_ok=True)
    warc_path = os.path.join(output_dir, output_filename + '.warc.gz')
    with open(warc_path, 'wb') as stream:
        writer = WARCWriter(stream, gzip=True)
        for row in data:
            total_counter += 1
            response_code = str(row['response']).strip()
            url = row['url_origin']
            file_path = row['file']

            if total_counter % 5000 == 0:
                print(f"Processing entry number: {total_counter}:")
            # Convert timestamp to ISO 8601 format for WARC-Date
            try:
                dt = datetime.strptime(row['timestamp'].strip(), "%Y%m%d%H%M%S")
                warc_date = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception as e:
                print(f"Invalid timestamp {row['timestamp']}: {e}")
                warc_date = None

            # As 404 AND 500 

            if response_code == '404':
                write_404_warc_entry(writer, url, warc_date)
                not_found_counter += 1
                continue
            elif response_code == '500':
                write_500_warc_entry(writer, url, warc_date)
                internal_service_error_counter += 1
                continue

        
            if not os.path.isfile(file_path):
                print(f"File not found: {file_path}")
                print(f"Timestamp for URL is: {row['timestamp']}")
                print(f"Response code is: {row['response']}")
                # TODO: If response code is 404 a 404 record should be created
                # TODO: If response code is 500 a 500 record should be created etc. 
                continue

            # Set content type based on file extension (simple default)
            content_type = 'text/html'
            ext = os.path.splitext(file_path)[1].lower()
            if ext in ['.jpg', '.jpeg']:
                content_type = 'image/jpeg'
            elif ext == '.png':
                content_type = 'image/png'
            elif ext == '.gif':
                content_type = 'image/gif'
            elif ext == '.css':
                content_type = 'text/css'
            elif ext == '.js':
                content_type = 'application/javascript'
            elif ext == '.pdf':
                content_type = 'application/pdf'
            elif ext == '.txt':
                content_type = 'text/plain'

        
            http_headers = StatusAndHeaders('200 OK', [('Content-Type', content_type)], protocol='HTTP/1.0')
            warc_type = 'response'

            with open(file_path, 'rb') as payload:
                record = writer.create_warc_record(
                    url,
                    warc_type,
                    payload=payload,
                    http_headers=http_headers,
                    warc_headers_dict={'WARC-Date': warc_date} if warc_date else None
                )
                writer.write_record(record)
            success_counter += 1


        print(
            f"\nWARC creation summary:\n"
            f"  Successful records:     {success_counter}\n"
            f"  Not found (404):        {not_found_counter}\n"
            f"  Internal errors (500):  {internal_service_error_counter}\n"
            f"  Total processed:        {len(data)}"
        )

def read_csv(input_csv):
    with open(input_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def process_csv_file(csv_file_path, output_dir, output_filename):
    data = read_csv(csv_file_path)
    create_warc_gz(data, output_dir, output_filename)

def combine_csv_files(input_directory, output_file):
    # Find all CSV files in the directory
    csv_files = glob.glob(os.path.join(input_directory, "*.csv"))
    # Read and concatenate all CSV files
    df_list = [pd.read_csv(f) for f in csv_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    # Write the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"Combined {len(csv_files)} files into {output_file}")
          



def main():
    """
    Main entry point for the script. Expects two command-line arguments: the path to a directory with multiple CSV files and the desired filename for the output warc.gx file.
    - Combines CSV files from the provided path into a single file with the name 'combined_output.csv'.
    - Processes the combined CSV file and generates a warc.gz file using the specified output filename.
    Usage:
        python main.py <csv_file_path> <output_filename>
    """
    
    combined_csv_path = "combined_output.csv"
    if len(sys.argv) < 3:
        print("Usage: python main.py <csv_file_path> <output_filename>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    output_filename = sys.argv[2]
    
    combine_csv_files(csv_file_path, combined_csv_path)
    process_csv_file(combined_csv_path, 'output', output_filename)



if __name__ == "__main__":
    main()