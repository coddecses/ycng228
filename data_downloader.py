import logging
from utils import is_market_day
import yfinance as yf
import os
import pandas as pd  # Add this import

logger = logging.getLogger()

def download_and_store_data(tickers, days, local, bucket, output_dir):
    """
    Download and store data for the given tickers for a specific number of past market days.
    Either store the data locally or in a Google Cloud Storage bucket.
    """
    today = pd.Timestamp.today().normalize()
    start_date = today - pd.DateOffset(days=days)
    
    logger.info(f"Starting download for tickers: {tickers} for the last {days} days.")
    logger.info(f"Checking market days from {start_date} to {today}.")
    
    # Loop through each date and check if it's a market day
    for single_date in pd.date_range(start=start_date, end=today):
        if is_market_day(single_date):
            logger.info(f"Processing data for: {single_date.date()}")
            try:
                # Fetch data for each ticker
                data = yf.download(tickers, start=single_date, end=single_date + pd.Timedelta(days=1), group_by="ticker")
                
                # Store the data
                for ticker in tickers:
                    file_name = f"{ticker}_{single_date.date()}.csv"
                    if local:
                        save_data_to_csv(data[ticker], output_dir, file_name)
                    else:
                        save_data_to_gcs(data[ticker], bucket, file_name)

            except Exception as e:
                logger.error(f"Error downloading data for {tickers} on {single_date.date()}: {e}")
