# Python WARC App

This project is designed to read a CSV file containing URLs, remove any instances of port 80 from those URLs, and generate WARC-GZ files based on the cleaned data.
The CSV file can be constructed by using the following tool: [Python Wayback Machine Downloader](https://github.com/bitdruid/python-wayback-machine-downloader)

## Project Structure

```
python-warc-app
├── src
│   ├── main.py        # Main script for processing CSV and generating WARC files
│   └── utils.py       # Utility functions for reading CSV and modifying URLs
├── requirements.txt    # List of dependencies for the project
└── README.md           # Documentation for the project
```

## Requirements

To run this project, you need to install the following dependencies:

- `warcio`: For creating WARC files.
- `pandas`: For handling CSV data.

You can install the required packages using pip:

```
pip install -r requirements.txt
```

## Usage

1. Place your CSV file in the appropriate directory.
2. Update the `src/main.py` file to specify the path to your CSV file.
3. Run the main script:

```
python src/main.py
```

This will process the CSV file, remove port 80 from the URLs, and generate the corresponding WARC-GZ files.

## Contributing

Feel free to submit issues or pull requests if you have suggestions or improvements for the project.