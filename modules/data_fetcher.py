import yfinance as yf
import streamlit as st
import pandas as pd

# The cache is placed here because fetching data is the I/O-bound, time-consuming step.
@st.cache_data(ttl="5m") # 5 min cache for adjusted data
def fetch_data(ticker_symbol):
    """
    Fetches historical stock data from Yahoo Finance for a given ticker.
    """
    try:
        # Download data for the last 2 years
        data = yf.download(ticker_symbol, period="2y", progress=False, auto_adjust=True)

        if data.empty:
            return None

        # Flatten MultiIndex columns (e.g., ('Close', '') -> 'Close')
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        # --- FIX: Remove duplicates just in case ---
        # This prevents the "Duplicate column names found" error that crashes Streamlit
        data = data.loc[:, ~data.columns.duplicated()]
        
        return data
    except Exception as e:
        # If any other error occurs, print it for debugging
        print(f"Error fetching data for {ticker_symbol}: {e}")
        return None


@st.cache_data(ttl="15m") # Cache unadjusted price data for 15 mins
def fetch_unadjusted_data(ticker_symbol, period="1y"):
    """
    Fetches UNADJUSTED historical data from Yahoo Finance, cached.
    """
    try:
        data = yf.download(
            ticker_symbol,
            period=period,
            progress=False,
            auto_adjust=False  # We want raw prices here
        )

        if data.empty:
            return None

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        # --- FIX: THIS PREVENTS THE CRASH ---
        # yfinance sometimes returns both 'Adj Close' and 'Close' which creates duplicates after flattening
        data = data.loc[:, ~data.columns.duplicated()]

        return data
    except Exception as e:
        print(f"Error fetching unadjusted data for {ticker_symbol}: {e}")
        return None