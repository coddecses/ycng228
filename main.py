import yfinance as yf
import pandas as pd
import logging
import os
from google.cloud import storage
import argparse
import pandas_market_calendars as mcal
from io import BytesIO


# Set Google Application Credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/genevievenantel/Downloads/peppy-web-437616-r6-ec3f12e7c15e.json'

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Get the NYSE calendar
nyse = mcal.get_calendar('NYSE')

# Function to check if a date is a market day
def is_market_open(date):
    valid_days = nyse.valid_days(start_date=date, end_date=date)
    return not valid_days.empty

# Function to check if data for a given day already exists locally
def data_exists_locally(file_path):
    return os.path.exists(file_path)

# Function to check if data for a given day already exists in GCS
def data_exists_in_gcs(bucket_name, file_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Failed to check file existence in GCS: {e}")
        return False

# Function to download the data for a single day
def download_data(tickers, date):
    try:
        data = yf.download(tickers, start=date, end=date + pd.DateOffset(days=1))
        if data.empty:
            logging.warning(f"No data fetched for {tickers} on {date}.")
        return data
    except Exception as e:
        logging.error(f"Error downloading data for {tickers} on {date}: {e}")
        return pd.DataFrame()

# Function to save data locally
def save_data_locally(data, file_path):
    data.to_csv(file_path)
    logging.info(f"Data saved locally at {file_path}")


# Function to upload data directly to GCS from memory
def upload_to_gcs_from_memory(bucket_name, data, file_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Convert the DataFrame to CSV in memory
        csv_buffer = BytesIO()
        data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  # Move to the start of the file-like object
        
        # Upload from the in-memory buffer
        blob.upload_from_file(csv_buffer, content_type='text/csv')
        logging.info(f"Data uploaded to Google Cloud Storage as {file_name}")
    except Exception as e:
        logging.error(f"Failed to upload to GCS: {e}")

# Main logic to handle data download and storage
def download_and_store_data(tickers, days, local, bucket_name, output_dir):
    today = pd.Timestamp.today().normalize()
    start_date = (today - pd.DateOffset(days=days)).normalize()  # Start date excluding today

    logging.info(f"Downloading data from {start_date} to {today - pd.DateOffset(days=1)} for {tickers}")

    # Loop through each day (excluding today) and download data if it's a market day
    for single_date in pd.date_range(start_date, today - pd.DateOffset(days=1), freq='D'):
        logging.info(f"Checking market status for {single_date.date()}")

        # Check if the market is open on this day
        if is_market_open(single_date):
            logging.info(f"Market is open on {single_date.date()}. Processing data.")
            file_name = f"sp500_data_{single_date.strftime('%Y-%m-%d')}.csv"

            if local:
                # Ensure output directory is specified for local storage
                if not output_dir:
                    logging.error("Output directory must be specified when saving locally.")
                    return
                file_path = os.path.join(output_dir, file_name)
                
                # Check if data already exists locally
                if data_exists_locally(file_path):
                    logging.info(f"Data for {single_date.date()} already exists locally. Skipping.")
                    continue
            else:
                # Check if data already exists in GCS
                if data_exists_in_gcs(bucket_name, file_name):
                    logging.info(f"Data for {single_date.date()} already exists in GCS. Skipping.")
                    continue

            try:
                logging.info(f"Attempting to download data for tickers: {tickers} on date: {single_date}")
                
                # Download data for the current day
                data = download_data(tickers, single_date)

                if not data.empty:
                    logging.info(f"Successfully downloaded data for {tickers} on {single_date}")
                else:
                    logging.warning(f"No data available for {tickers} on {single_date}")
                    
            except Exception as e:
                logging.error(f"Failed to download data for {tickers} on {single_date} due to error: {e}")

            if not data.empty:
                if local:
                    # Save locally
                    save_data_locally(data, file_path)
                else:
                    # Upload directly to GCS from memory
                    upload_to_gcs_from_memory(bucket_name, data, file_name)
            else:
                logging.warning(f"No data available for {single_date.date()}")
        else:
            logging.info(f"Skipping {single_date.date()} - Market is closed.")


# Argument parser
def main():
    parser = argparse.ArgumentParser(description="Download S&P 500 data and store locally or in a cloud bucket.")
    
    # Argument for number of days
    parser.add_argument('--days', type=int, default=5, help='Number of past days to fetch data for.')
    
    # Flag to determine whether to store data locally
    parser.add_argument('--local', action='store_true', help='Store data locally instead of in the cloud.')
    
    # Argument for Google Cloud Storage bucket name
    parser.add_argument('--bucket', type=str, help='Google Cloud Storage bucket name.')
    
    # Only add output_dir argument if local storage is requested
    parser.add_argument('--output_dir', type=str, help='Directory to save the output files (required if --local is set).')

    args = parser.parse_args()

    # Validate the arguments
    if args.local and not args.output_dir:
        parser.error("--output_dir is required if --local is set")
    
    if not args.local and not args.bucket:
        parser.error("--bucket is required if uploading directly to Google Cloud Storage")
    
    # Tickers for S&P 500 companies (or your selection)
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

    logging.info(f"Downloading data for tickers: {tickers} for the last {args.days} days.")
    download_and_store_data(tickers, args.days, args.local, args.bucket, args.output_dir)

if __name__ == "__main__":
    main()
