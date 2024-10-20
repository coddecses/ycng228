import pandas_market_calendars as mcal
import logging
import pandas as pd

# Initialize logger
logger = logging.getLogger()

# Get the NYSE calendar
nyse = mcal.get_calendar('NYSE')

def is_market_day(date):
    """
    Check if a given date is a valid market day.
    :param date: The date to check
    :return: True if it's a valid trading day, False otherwise
    """
    # Get valid trading days (exclude weekends and holidays)
    valid_days = nyse.valid_days(start_date=date - pd.DateOffset(days=20), end_date=date)

    if pd.Timestamp(date) in valid_days:
        return True
    logger.info(f"Skipping - NYSE is not trading on {date.date()}")
    return False
