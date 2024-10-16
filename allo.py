import os
import argparse
import yfinance as yf
import logging
from google.cloud import storage

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to download stock data
def download_sp500_data(tickers, days):
    logging.info(f"Downloading data for tickers: {tickers} for the last {days} days.")
    data = yf.download(tickers, period=f'{days}d')
    return data

# Function to save data locally
def save_data_local(data, local_path, filename):
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    file_path = os.path.join(local_path, filename)
    data.to_csv(file_path)
    logging.info(f"Data saved locally at {file_path}")
    return file_path

# Function to upload to Google Cloud Storage (GCS)
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_filename(source_file_name)
    logging.info(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")

# Main function to handle both local and cloud storage options
def main(tickers, days, local_path=None, bucket_name=None):
    # Download S&P 500 stock data
    data = download_sp500_data(tickers, days)

    # File to store the data
    filename = 'sp500_data.csv'

    if local_path:
        # Save data locally
        file_path = save_data_local(data, local_path, filename)
    elif bucket_name:
        # Save data to a temporary file and upload to Google Cloud Storage
        temp_local_path = '/tmp'
        file_path = save_data_local(data, temp_local_path, filename)
        destination_blob_name = f'sp500_data/{filename}'
        upload_to_gcs(bucket_name, file_path, destination_blob_name)
    else:
        logging.error("No storage option provided. Please provide either a local path or a Google Cloud Storage bucket name.")

if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description="Download S&P 500 data and store it locally or in Google Cloud Storage.")
    parser.add_argument('--days', required=True, type=int, help="Number of days of stock data to download")
    parser.add_argument('--local', required=False, help="Path to save the data locally")
    parser.add_argument('--bucket', required=False, help="Google Cloud Storage bucket name")
    
    args = parser.parse_args()

    # Subset of S&P 500 tickers (replace with the full list as needed)
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

    # Run the main function
    main(tickers, args.days, args.local, args.bucket)
