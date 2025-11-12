# In modules/analysis.py

# In modules/analysis.py
# (Make sure your imports at the top include all of these)
from modules.data_fetcher import fetch_data, fetch_unadjusted_data
import pandas_ta as ta
import yfinance as yf
import pandas as pd

def get_technical_analysis(ticker_symbol, stock_name, swing_days, period_label):
    """
    Performs technical analysis on the fetched stock data.
    """
    # Step 1: Get the raw ADJUSTED data (unchanged)
    data = fetch_data(ticker_symbol)

    if data is None: return None
    if len(data) < 30:
        print(f"Skipping {ticker_symbol}: not enough data (got {len(data)} rows, need 30)")
        return None

    # Step 2: Perform all calculations on the raw (adjusted) data
    try:
        # --- Indicator Calculations ---
        data.ta.ichimoku(append=True)
        data.ta.rsi(append=True)
        latest = data.iloc[-1]
        prev = data.iloc[-2]
        prev_week = data.iloc[-6]
        weekly_change_pct = ((latest['Close'] - prev_week['Close']) / prev_week['Close']) * 100

        # --- Ichimoku Analysis (on adjusted data) ---
        if latest['ITS_9'] > latest['IKS_26']: tk_cross = "Bullish (TS above KS)"
        else: tk_cross = "Bearish (TS below KS)"
        
        if latest['Close'] > latest['ISA_9'] and latest['Close'] > latest['ISB_26']: cloud = "Bullish (Above Cloud)"
        elif latest['Close'] < latest['ISA_9'] and latest['Close'] < latest['ISB_26']: cloud = "Bearish (Below Cloud)"
        else: cloud = "Sideways (In Cloud)"
        
        if latest['Close'] > data['Close'].iloc[-27]: chikou = "Bullish"
        else: chikou = "Bearish"

        # --- RSI Analysis (on adjusted data) ---
        rsi_val = latest['RSI_14']
        if rsi_val > 70: rsi = f"{rsi_val:.2f} (Overbought)"
        elif rsi_val < 30: rsi = f"{rsi_val:.2f} (Oversold)"
        else: rsi = f"{rsi_val:.2f} (Neutral)"

        # --- Adjusted Swing Low Analysis ---
        # This correctly uses the dynamic 'swing_days'
        swing_data = data.tail(swing_days) 
        swing_low_price = swing_data['Low'].min()
        swing_low_date = swing_data['Low'].idxmin().strftime('%d-%b-%Y')
        change_from_low = ((latest['Close'] - swing_low_price) / swing_low_price) * 100
        
        # --- START: NEW DYNAMIC UNADJUSTED DATA FETCH ---
        current_price_val = None
        unadj_low_val = None
        
        # 1. Create a mapping dictionary
        period_map = {
            "1 Yr": "1y",
            "6 Months": "6mo",
            "3 Months": "3mo",
            "1 Month": "1mo"
        }
        # Get the yfinance period string (e.g., "3mo"), default to "1y"
        yf_period = period_map.get(period_label, "1y")

        # 2. Use the dynamic yf_period in the fetch function
        unadj_data = fetch_unadjusted_data(ticker_symbol, period=yf_period)
        
        if unadj_data is not None and not unadj_data.empty:
            try:
                # Get the unadjusted low from the *selected period*
                unadj_low_val = unadj_data['Low'].min()
                # Get the LATEST unadjusted price from this data
                current_price_val = unadj_data['Close'].iloc[-1]
            except Exception as e:
                print(f"Error processing unadjusted data for {ticker_symbol}: {e}")
        # --- END: NEW DYNAMIC UNADJUSTED DATA FETCH ---

        # --- Consolidate Results ---
        # The f-strings in the result dictionary are already dynamic
        # and will use the 'period_label' (e.g., "3 Months")
        result = {
            "Index": stock_name,
            "Adj Price": f"{latest['Close']:,.2f}",
            "Current Price": f"{current_price_val:,.2f}" if isinstance(current_price_val, (int, float)) else "N/A",
            "Change %": f"{(latest['Close'] / prev['Close'] - 1) * 100:.2f}%",
            "Weekly %": f"{weekly_change_pct:.2f}%",
            "Tenkan/Kijun": tk_cross,
            "Chikou Span": chikou,
            "Cloud": cloud,
            "RSI": rsi,
            f"{period_label} Low (Adj)": f"{swing_low_price:,.2f}",
            f"{period_label} Low (Un-adj)": f"{unadj_low_val:,.2f}" if isinstance(unadj_low_val, (int, float)) else "N/A",
            f"{period_label} Low Date": swing_low_date,
            f"% from {period_label} Low": f"{change_from_low:.2f}%"
        }
        return result
    except Exception as e:
        print(f"Error analyzing data for {ticker_symbol}: {e}")
        return None

# (Your get_generic_analysis function remains after this)

def get_generic_analysis(ticker_symbol, asset_name):
    """
    Performs a simple, generic analysis for non-NSE assets (global, crypto, etc).
    """
    # Step 1: Get the raw data using our data_fetcher
    data = fetch_data(ticker_symbol)

    if data is None: return None
    
    # ... (Tiered logic and SMA calculations are unchanged) ...
    data_length = len(data)
    if data_length < 200:
        print(f"Skipping {ticker_symbol}: not enough data (got {data_length} rows, need 200)")
        return None

    try:
        data.ta.rsi(length=14, append=True)
        data.ta.sma(length=20, append=True)
        data.ta.sma(length=50, append=True)
        data.ta.sma(length=200, append=True)
        
        latest = data.iloc[-1]
        prev = data.iloc[-2]
        prev_week = data.iloc[-6]
        daily_change_pct = ((latest['Close'] - prev['Close']) / prev['Close']) * 100
        weekly_change_pct = ((latest['Close'] - prev_week['Close']) / prev['Close']) * 100

        rsi_val = latest['RSI_14']
        if rsi_val > 70: rsi = f"{rsi_val:.2f} (Overbought)"
        elif rsi_val < 30: rsi = f"{rsi_val:.2f} (Oversold)"
        else: rsi = f"{rsi_val:.2f} (Neutral)"

        price = latest['Close']
        if price > latest['SMA_20']: trend_short = "Bullish (Above 20-SMA)"
        else: trend_short = "Bearish (Below 20-SMA)"
        if price > latest['SMA_50']: trend_medium = "Bullish (Above 50-SMA)"
        else: trend_medium = "Bearish (Below 50-SMA)"
        if price > latest['SMA_200']: trend_long = "Bullish (Above 200-SMA)"
        else: trend_long = "Bearish (Below 200-SMA)"
            
        # --- START: MODIFIED UNADJUSTED DATA FETCH ---
        current_price_val = None
        unadj_low_val = None
        unadj_low_date_str = "N/A" # <-- 1. INITIALIZE DATE STRING
        
        unadj_data = fetch_unadjusted_data(ticker_symbol, period="1y") 
        
        if unadj_data is not None and not unadj_data.empty:
            try:
                current_price_val = unadj_data['Close'].iloc[-1]
                unadj_low_val = unadj_data['Low'].min()
                # --- 2. GET THE DATE OF THE LOW ---
                unadj_low_date = unadj_data['Low'].idxmin()
                unadj_low_date_str = unadj_low_date.strftime('%d-%b-%Y')
            except Exception as e:
                print(f"Error processing unadjusted data for {ticker_symbol} (generic): {e}")
        
        current_price_str = f"{current_price_val:,.2f}" if isinstance(current_price_val, (int, float)) else "N/A"
        unadj_low_str = f"{unadj_low_val:,.2f}" if isinstance(unadj_low_val, (int, float)) else "N/A"
        # --- END: MODIFIED UNADJUSTED DATA FETCH ---

        # --- 3. ADD THE LOW DATE TO THE RESULT ---
        result = {
            "Asset": asset_name,
            "Adj Price": f"{latest['Close']:,.2f}",
            "Current Price": current_price_str,
            "1 Yr Low (Un-adj)": unadj_low_str,
            "1 Yr Low Date": unadj_low_date_str, # <-- NEW COLUMN
            "Change %": f"{daily_change_pct:.2f}%",
            "Weekly %": f"{weekly_change_pct:.2f}%",
            "RSI (14)": rsi,
            "Short Trend (20D)": trend_short,
            "Medium Trend (50D)": trend_medium,
            "Long Trend (200D)": trend_long
        }
        return result

    except Exception as e:
        print(f"Error analyzing data for {ticker_symbol}: {e}")
        return None