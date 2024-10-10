import yfinance as yf
import pandas as pd

# Define the tickers
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Download stock data for the past 30 days
data = yf.download(tickers, period='30d')

# Save the data to a CSV file
csv_file = 'stock_data.csv'
data.to_csv(csv_file)

print(f"Data saved to {csv_file}")
