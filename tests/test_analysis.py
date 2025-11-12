import yfinance as yf
import pandas as pd

# --- Pick one ticker that shows "N/A" in your dashboard ---
ticker_to_test = "RELIANCE.NS" 
# (try others too: "^NSEI", "INFY.NS")

print(f"--- Testing Unadjusted Data Fetch for: {ticker_to_test} ---")

# --- This is the exact code block from your analysis.py ---
current_price_val = None
unadj_low_val = None

try:
    # Get 1 year of UNADJUSTED data
    unadj_data = yf.download(
        ticker_to_test, 
        period="1y", 
        auto_adjust=False, # <-- The key
        progress=False
    )
    
    if not unadj_data.empty:
        print(f"Success! Fetched DataFrame with {len(unadj_data)} rows.")
        
        # Get the unadjusted low from the last 1y
        unadj_low_val = unadj_data['Low'].min()

        # Get the LATEST unadjusted price from this data
        current_price_val = unadj_data['Close'].iloc[-1]
        
        print("\n--- Raw Data (Last 5 Days) ---")
        print(unadj_data.tail())
        
    else:
        print("FAIL: yf.download() returned an empty DataFrame.")

except Exception as e:
    print(f"An error occurred: {e}")

print("\n--- Results ---")
print(f"Current Unadjusted Price: {current_price_val}")
print(f"1-Year Unadjusted Low:    {unadj_low_val}")