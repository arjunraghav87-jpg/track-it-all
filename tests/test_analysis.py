import pandas as pd
import yfinance as yf
import pandas_ta as ta
import time

# --- 1. WEEKLY RESAMPLER ---
def resample_to_weekly(daily_df):
    if daily_df is None or daily_df.empty: return None
    print(f"   [DEBUG] Resampling {len(daily_df)} daily rows...")
    weekly_df = daily_df.resample('W-FRI').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
    })
    return weekly_df

# --- 2. FETCH LOGIC ---
def test_fetch_master_data(tickers_list):
    if not tickers_list: return None
    tickers_list = list(set(tickers_list))
    print(f"\n🚀 STARTING FETCH for {len(tickers_list)} tickers...")
    print("   (Batch=10, Threads=False, Sleep=2.0s)\n")
    
    all_data_frames = []
    chunk_size = 10 
    
    try:
        for i in range(0, len(tickers_list), chunk_size):
            batch = tickers_list[i : i + chunk_size]
            print(f"   ⏳ Fetching batch {i//chunk_size + 1}: {batch}")
            try:
                # SAFE FETCH
                batch_data = yf.download(batch, period="2y", auto_adjust=True, progress=False, threads=False)
                if not batch_data.empty:
                    all_data_frames.append(batch_data)
                    print(f"      ✅ Success! Data shape: {batch_data.shape}")
            except Exception as e:
                print(f"      ❌ Error: {e}")
                continue
            time.sleep(2.0) 

        if not all_data_frames: return None
        final_df = pd.concat(all_data_frames, axis=1)
        
        # CLEANUP: Remove duplicate columns immediately
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]
        return final_df
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        return None

# --- 3. MAIN TEST ---
if __name__ == "__main__":
    # Test just 2 stocks
    test_tickers = ["RELIANCE.NS", "INFY.NS"]
    
    print("--- 🧪 STEP 1: Fetching Data ---")
    master_data = test_fetch_master_data(test_tickers)
    
    if master_data is not None and not master_data.empty:
        test_symbol = "RELIANCE.NS"
        print(f"\n--- 🧪 STEP 2: Testing {test_symbol} ---")
        
        if test_symbol in master_data['Close']:
            # 1. Get Daily
            daily_data = master_data.loc[:, (slice(None), test_symbol)]
            daily_data.columns = daily_data.columns.droplevel(1)
            
            # 2. Resample to Weekly
            weekly_data = resample_to_weekly(daily_data)
            
            # 3. RUN ICHIMOKU (Using append=True to match Dashboard)
            print("\n--- 🧪 STEP 3: Ichimoku Calculation Test ---")
            try:
                # Using append=True avoids manual concat errors
                weekly_data.ta.ichimoku(append=True)
                
                # Check columns (names might vary slightly by version)
                # Usually: ITS_9 (Tenkan), IKS_26 (Kijun), ISA_9 (Senkou A), ISB_26 (Senkou B)
                cols = weekly_data.columns.tolist()
                print(f"   Columns: {cols[-5:]}") # Show last 5 created columns
                
                # Get Latest Row
                latest = weekly_data.iloc[-1]
                
                # Helper to safely get float value
                def get_val(row, col_name):
                    val = row.get(col_name)
                    if isinstance(val, pd.Series): val = val.iloc[0] # Handle duplicates if any
                    return val

                # Extract values
                close = get_val(latest, 'Close')
                tenkan = get_val(latest, 'ITS_9')
                kijun = get_val(latest, 'IKS_26')
                senkou_a = get_val(latest, 'ISA_9')
                senkou_b = get_val(latest, 'ISB_26')
                chikou = get_val(latest, 'ICS_26')

                print(f"\n   📊 LATEST WEEKLY VALUES ({latest.name.date()}):")
                print(f"   ----------------------------------------")
                print(f"   Close Price:  {close:.2f}")
                print(f"   Tenkan (9):   {tenkan:.2f}" if tenkan else "   Tenkan: NaN")
                print(f"   Kijun (26):   {kijun:.2f}" if kijun else "   Kijun: NaN")
                print(f"   Senkou A:     {senkou_a:.2f}" if senkou_a else "   Senkou A: NaN")
                print(f"   Senkou B:     {senkou_b:.2f}" if senkou_b else "   Senkou B: NaN")
                print(f"   Chikou:       {chikou:.2f}" if chikou else "   Chikou: NaN")
                print(f"   ----------------------------------------")
                
                if pd.notna(senkou_b):
                    print("   ✅ Ichimoku Cloud is successfully generated!")
                else:
                    print("   ⚠️ Values are NaN (Normal if < 52 weeks of data). Logic is valid.")

            except Exception as e:
                print(f"   ❌ Ichimoku Calculation Failed: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"❌ {test_symbol} not found.")
    else:
        print("❌ Fetch failed.")