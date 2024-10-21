# S&P 500 Data Downloader

This Python script downloads stock data for specified tickers (like AAPL, MSFT, GOOGL, etc.) from Yahoo Finance using the `yfinance` library. The script also allows you to store the downloaded data either locally on your computer or upload it directly to Google Cloud Storage (GCS). Additionally, it checks if data already exists in the storage location (local or GCS) to prevent duplicate downloads.

## Features

- **Download stock data**: Fetch data for a given number of past days, excluding today.
- **Local or cloud storage**: Option to store the data locally as CSV files or upload it directly to Google Cloud Storage (GCS).
- **Market day validation**: Only downloads data for valid market days by using the NYSE calendar.
- **Error handling**: Logs warnings and errors when data is unavailable or upload/download fails.

## Requirements

To run the script, you need the following Python packages:

- `yfinance`
- `pandas`
- `argparse`
- `google-cloud-storage`
- `pandas_market_calendars`

You can install these packages by running:

```bash
pip install yfinance pandas google-cloud-storage pandas_market_calendars
```

# Google Cloud Credentials
Make sure you set your Google Cloud Application Credentials by setting the environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path_to_your_service_account_key.json"
```

## Usage
```bash
python3 main.py --days <NUMBER_OF_DAYS> --local --output_dir <LOCAL_DIRECTORY>
```

OR


```bash
python3 main.py --days <NUMBER_OF_DAYS> --bucket <GCS_BUCKET_NAME>
```
--days <NUMBER_OF_DAYS>: Number of past days (excluding today) for which to download stock data.
--local: If provided, saves the data locally. Requires --output_dir.
--output_dir <LOCAL_DIRECTORY>: The directory where data will be saved if the --local flag is used.
--bucket <GCS_BUCKET_NAME>: The name of your Google Cloud Storage bucket where the data will be uploaded (if not using local storage).

## Example Usage
# To download stock data for the past 3 days and store it locally:

```bash
python3 main.py --days 3 --local --output_dir /path/to/local/folder
```

# To download stock data for the past 5 days and upload it to GCS:
```bash
python3 main.py --days 5 --bucket my-gcs-bucket
```

# Future Improvements / TODOs
- 
File Integrity Check: If a file already exists, add logic to verify if the file is corrupted or incomplete, and re-download the data if necessary. This would prevent keeping bad or invalid files in storage.

- Split Code into Modules: Refactor the codebase to split functions into multiple files for better organization and code cleanliness. For example:

   - One file for utility functions (e.g., checking if the market is open, downloading data).
   - Another file for handling storage (local or GCS).
   - A main file to tie everything together.
   - Data Validation: Implement validation logic to compare the downloaded data against previously saved data to ensure accuracy and consistency.


- Configurable Logging: Allow the log level to be configurable via command-line arguments (e.g., --log-level DEBUG), to adjust verbosity as needed.

