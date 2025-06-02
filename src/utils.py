def read_csv(file_path):
    import pandas as pd
    return pd.read_csv(file_path)

def remove_port_80(url):
    if ':80' in url:
        return url.replace(':80', '')
    return url

def clean_urls(dataframe):
    dataframe['url_archive'] = dataframe['url_archive'].apply(lambda x: remove_port_80(x))
    dataframe['url_origin'] = dataframe['url_origin'].apply(lambda x: remove_port_80(x))
    return dataframe

def create_warc_gz(file_path, dataframe):
    from warcio.archiveiterator import ArchiveIterator
    from warcio.warcwriter import WARCWriter
    import gzip

    with gzip.open(file_path, 'wb') as stream:
        writer = WARCWriter(stream, gzip=True)
        for index, row in dataframe.iterrows():
            writer.write_webpage(row['url_origin'], row['timestamp'], content_type='text/html')
            writer.write_webpage(row['url_archive'], row['timestamp'], content_type='text/html')