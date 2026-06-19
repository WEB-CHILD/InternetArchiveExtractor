def read_csv(file_path):
    import pandas as pd
    return pd.read_csv(file_path, sep=";")

def remove_port_80(url):
    if ':80' in url:
        return url.replace(':80', '')
    return url

def clean_urls(dataframe):
    dataframe['url_archive'] = dataframe['url_archive'].apply(lambda x: remove_port_80(x))
    dataframe['url_origin'] = dataframe['url_origin'].apply(lambda x: remove_port_80(x))
    return dataframe

def import_urls_from_csv(file_path, column_name):
    """
    Imports URLs from a specified column in a CSV file.

    Args:
        file_path (str): The path to the CSV file.
        column_name (str): The name of the column containing URLs.

    Returns:
        list: A list of URLs extracted from the specified column.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        KeyError: If the specified column does not exist in the CSV file.
        pd.errors.EmptyDataError: If the CSV file is empty.
    """

    df = read_csv(file_path)
    url_list = df[column_name].tolist()
    return url_list