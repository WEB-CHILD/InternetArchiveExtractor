import pandas as pd
import os
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter
from warcio.recordloader import ArcWarcRecord
from warcio.statusandheaders import StatusAndHeaders
import io
import sys
import gzip
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

def create_warc_gz(data, output_dir):
    count = 0
    os.makedirs(output_dir, exist_ok=True)
    warc_path = os.path.join(output_dir, 'output2.warc.gz')
    with open(warc_path, 'wb') as stream:
        writer = WARCWriter(stream, gzip=True)
        for row in data:
            url = row['url_origin']
            file_path = row['file']
            # Convert timestamp to ISO 8601 format for WARC-Date
            try:
                dt = datetime.strptime(row['timestamp'].strip(), "%Y%m%d%H%M%S")
                warc_date = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception as e:
                print(f"Invalid timestamp {row['timestamp']}: {e}")
                warc_date = None
            if not os.path.isfile(file_path):
                print(f"File not found: {file_path}")
                print(f"Timestamp for URL is: {row['timestamp']}")
                print(f"Response code is: {row['response']}")
                # TODO: If response code is 404 a 404 record should be created
                # TODO: If response code is 500 a 500 record should be created etc. 
                count += 1
                continue
            # TODO Set content type based on file extension
            http_headers = StatusAndHeaders('200 OK', [('Content-Type', 'text/html')], protocol='HTTP/1.0')
            with open(file_path, 'rb') as payload:
                record = writer.create_warc_record(
                    url,
                    'response',
                    payload=payload,
                    http_headers=http_headers,
                    warc_headers_dict={'WARC-Date': warc_date} if warc_date else None
                )
                writer.write_record(record)
        print(f"Processed {len(data) - count} records, {count} files not found.")

def read_csv(input_csv):
    with open(input_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <csv_file_path>")
        sys.exit(1)
    csv_file_path = sys.argv[1]
    output_dir = 'output'  # Update with your desired output directory
    data = read_csv(csv_file_path)
    create_warc_gz(data, output_dir)

if __name__ == "__main__":
    main()