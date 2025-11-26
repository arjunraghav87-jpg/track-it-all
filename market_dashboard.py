import streamlit as st
import pandas as pd
import gspread
import yfinance as yf
import pandas_ta as ta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import time

# --- Imports: We no longer import from modules/analysis ---
from modules.data_fetcher import fetch_data, fetch_unadjusted_data

# --- GOOGLE SHEET FUNCTIONS (Unchanged) ---
@st.cache_data(ttl=600)
def load_stocks_from_google_sheet():
    try:
        gc = gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        sh = gc.open("MyStockList") 
        worksheet = sh.sheet1 
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        stock_dict = pd.Series(df.Ticker.values, index=df.Name).to_dict()
        return stock_dict
    except Exception as e:
        st.error(f"Error loading Heavyweights from Google Sheet: {e}")
        return {}

@st.cache_data(ttl=600)
def load_model_portfolio_from_sheet():
    try:
        gc = gspread.service_account_from_dict(st.secrets["gcp_service_account"])
        sh = gc.open("MyStockList") 
        worksheet = sh.worksheet("ModelPortfolio") 
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        stock_dict = pd.Series(df.Ticker.values, index=df.Name).to_dict()
        return stock_dict
    except Exception as e:
        st.error(f"Error loading Model Portfolio from Google Sheet: {e}")
        return {}


# --- NEW MASTER DATA FETCHER (Batched to prevent Timeouts) ---
@st.cache_data(ttl=3600) #cache for 1 hour
def fetch_master_data(tickers_list):
    """
    Downloads data in SMALLER, SERIAL batches to avoid Yahoo Finance Rate Limits.
    """
    if not tickers_list:
        return None
    
    # Deduplicate tickers
    tickers_list = list(set(tickers_list))
    st.info(f"Fetching master data for {len(tickers_list)} unique tickers...")
    
    all_data_frames = []
    
    # --- CHANGE 1: Smaller batches (10 instead of 20) ---
    chunk_size = 10 
    
    progress_bar = st.progress(0)
    
    try:
        for i in range(0, len(tickers_list), chunk_size):
            batch = tickers_list[i : i + chunk_size]
            
            # Update progress
            progress_bar.progress(min(i / len(tickers_list), 1.0))
            
            try:
                # --- CHANGE 2: threads=False (Crucial for Streamlit Cloud) ---
                # threads=True sends all requests instantly -> Instant Ban.
                # threads=False sends them one by one -> Safe.
                batch_data = yf.download(batch, period="2y", auto_adjust=True, progress=False, threads=False)
                
                if not batch_data.empty:
                    all_data_frames.append(batch_data)
                    
            except Exception as e:
                print(f"Error fetching batch {i}: {e}")
                # Don't stop, just skip this batch
                continue
                
            # --- CHANGE 3: Longer sleep (2 seconds) ---
            time.sleep(2.0) 

        progress_bar.empty()

        if not all_data_frames:
            return None

        # Combine all batches
        final_df = pd.concat(all_data_frames, axis=1)
        
        # Remove duplicate columns if any overlap occurred
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]
        
        return final_df

    except Exception as e:
        st.error(f"Error in master data fetch: {e}")
        return None

# --- NEW ANALYSIS FUNCTIONS (Replaces modules/analysis) ---

def run_technical_analysis(data, name, period_days, period_label, ticker_symbol):
    """
    Runs technical analysis on a *pre-fetched* ADJUSTED DataFrame.
    It now also fetches unadjusted data for correct pricing.
    """
    if data is None or data.empty:
        return None
    
    # --- FIX: Drop NaN rows from the master data slice (for % calc) ---
    data = data.dropna(subset=['Close'])

    # Now, check length *after* dropping NaNs
    if len(data) < 30:
        st.warning(f"Skipping analysis for {name}: Insufficient data (need ~30 days, got {len(data)})")
        return None
    
    data = data.copy() # Make a copy to avoid cache warnings
    data.ta.ichimoku(append=True)
    data.ta.rsi(length=14, append=True)
    data = data.rename(columns={
        'ITS_9': 'Tenkan', 'IKS_26': 'Kijun',
        'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'
    })

    try:
        analysis = {}
        latest = data.iloc[-1]
        prev = data.iloc[-2]
        
        # Basic Info from ADJUSTED data
        analysis['Index'] = name
        analysis['Adj Price'] = f"{latest['Close']:,.2f}" # This is the adjusted price
        analysis['Change %'] = f"{(latest['Close'] - prev['Close']) / prev['Close']:.2%}"

        if len(data) >= 6:
            week_ago = data.iloc[-6] # 5 trading days ago
            weekly_pct = (latest['Close'] - week_ago['Close']) / week_ago['Close']
            analysis['Weekly %'] = f"{weekly_pct:.2%}"
        else:
            analysis['Weekly %'] = "N/A"

        # --- FINAL FIX: This block is now fully corrected ---
        # 1. Cloud Logic
        if latest['Close'] > latest['SenkouA'] and latest['Close'] > latest['SenkouB']:
            analysis['Cloud'] = "Bullish (Above Cloud)"
        elif latest['Close'] < latest['SenkouA'] and latest['Close'] < latest['SenkouB']:
            analysis['Cloud'] = "Bearish (Below Cloud)"
        else:
            analysis['Cloud'] = "Neutral (In Cloud)"

        # 2. Tenkan/Kijun Logic (Handles 'nan' values)
        tk_cross = "nan" # Default to 'nan' string
        if pd.isna(latest['Tenkan']) or pd.isna(latest['Kijun']):
            tk_cross = "nan" 
        elif latest['Tenkan'] > latest['Kijun']:
            tk_cross = "Bullish (Tenkan > Kijun)"
        else:
            tk_cross = "Bearish (Tenkan < Kijun)"
        analysis['Tenkan/Kijun'] = tk_cross

        # 3. Chikou Span Logic (Compares correct prices)
        if latest['Close'] > data['Close'].iloc[-27]: 
            analysis['Chikou Span'] = "Bullish"
        else:
            analysis['Chikou Span'] = "Bearish"
        # --- END OF FINAL FIX ---

        # RSI Analysis (from adjusted data)
        rsi_val = latest['RSI_14']
        if rsi_val > 70:
            analysis['RSI'] = f"Overbought ({rsi_val:.1f})"
        elif rsi_val < 30:
            analysis['RSI'] = f"Oversold ({rsi_val:.1f})"
        else:
            analysis['RSI'] = f"Neutral ({rsi_val:.1f})"

        # --- Adjusted Swing Low (from adjusted data) ---
        
        # --- FIX: DEPRECATION WARNING ---
        end_date_adj = data.index.max()
        start_date_adj = end_date_adj - pd.Timedelta(days=period_days)
        period_data_adj = data.loc[(data.index >= start_date_adj) & (data.index <= end_date_adj)]
        # --- END FIX ---
        
        if period_data_adj.empty or period_data_adj['Low'].isnull().all():
            adj_low_price = float('nan')
        else:
            adj_low_price = period_data_adj['Low'].min()
        
        analysis[f"{period_label} Low (Adj)"] = f"{adj_low_price:,.2f}"


        # --- Fetch UNADJUSTED data for correct prices (Unchanged) ---
        current_price_val = None
        unadj_low_val = float('nan')
        unadj_low_date_str = "N/A"

        period_map = {
            "1 Yr": "1y",
            "6 Months": "6mo",
            "3 Months": "3mo",
            "1 Month": "1mo"
        }
        yf_period = period_map.get(period_label, "1y") 

        unadj_data = fetch_unadjusted_data(ticker_symbol, period=yf_period)
        
        if unadj_data is not None and not unadj_data.empty:
            try:
                unadj_low_val = unadj_data['Low'].min()
                current_price_val = unadj_data['Close'].iloc[-1]
                unadj_low_date = unadj_data['Low'].idxmin()
                if not pd.isna(unadj_low_date):
                    unadj_low_date_str = unadj_low_date.strftime('%Y-%m-%d')
            except Exception as e:
                print(f"Error processing unadjusted data for {ticker_symbol}: {e}")

        analysis['Current Price'] = f"{current_price_val:,.2f}" if isinstance(current_price_val, (int, float)) else "N/A"
        analysis[f"{period_label} Low (Un-adj)"] = f"{unadj_low_val:,.2f}"
        analysis[f"{period_label} Low Date"] = unadj_low_date_str
        
        if isinstance(current_price_val, (int, float)) and not pd.isna(unadj_low_val) and unadj_low_val != 0:
            from_low_pct = (current_price_val - unadj_low_val) / unadj_low_val
            analysis[f"% from {period_label} Low"] = f"{from_low_pct:.2%}"
        else:
            analysis[f"% from {period_label} Low"] = "N/A"

        return analysis
    except Exception as e:
        st.warning(f"Analysis failed for {name}: {e}")
        return None

def run_generic_analysis(data, name, ticker_symbol):
    """
    Runs generic analysis for non-Indian stocks.
    """
    if data is None or data.empty:
        return None
    
    # --- FIX: Drop NaN rows from the master data slice ---
    data = data.dropna(subset=['Close'])
    
    data = data.copy() # Make a copy to avoid cache warnings
    
    if len(data) < 30:
        st.warning(f"Skipping analysis for {name} (generic): Insufficient data (need ~30 days, got {len(data)})")
        return None
    
    data.ta.ichimoku(append=True)
    data.ta.rsi(length=14, append=True)
    data = data.rename(columns={
        'ITS_9': 'Tenkan', 'IKS_26': 'Kijun',
        'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'
    })
    
    try:
        analysis = {}
        latest = data.iloc[-1]
        prev = data.iloc[-2]
        week_ago = data.iloc[-6]

        # --- FIX: DEPRECATION WARNING ---
        end_date_adj_g = data.index.max()
        start_date_adj_g = end_date_adj_g - pd.Timedelta(weeks=52)
        year_ago_data = data.loc[(data.index >= start_date_adj_g) & (data.index <= end_date_adj_g)]
        # --- END FIX ---

        analysis['Asset'] = name
        analysis['Change %'] = f"{(latest['Close'] - prev['Close']) / prev['Close']:.2%}"
        analysis['Weekly %'] = f"{(latest['Close'] - week_ago['Close']) / week_ago['Close']:.2%}"

        # 52-Week (from adjusted data)
        high_52w_adj = year_ago_data['High'].max()
        low_52w_adj = year_ago_data['Low'].min()
        
        # Ichimoku
        if latest['Close'] > latest['SenkouA'] and latest['Close'] > latest['SenkouB']:
            analysis['Cloud'] = "Bullish (Above Cloud)"
        elif latest['Close'] < latest['SenkouA'] and latest['Close'] < latest['SenkouB']:
            analysis['Cloud'] = "Bearish (Below Cloud)"
        else:
            analysis['Cloud'] = "Neutral (In Cloud)"

        # RSI
        rsi_val = latest['RSI_14']
        if rsi_val > 70:
            analysis['RSI'] = f"Overbought ({rsi_val:.1f})"
        elif rsi_val < 30:
            analysis['RSI'] = f"Oversold ({rsi_val:.1f})"
        else:
            analysis['RSI'] = f"Neutral ({rsi_val:.1f})"

        # --- Fetch UNADJUSTED data for correct prices (Unchanged) ---
        current_price_val = None
        unadj_low_52w_val = None
        unadj_high_52w_val = None
        
        unadj_data = fetch_unadjusted_data(ticker_symbol, period="1y") 
        
        if unadj_data is not None and not unadj_data.empty:
            try:
                current_price_val = unadj_data['Close'].iloc[-1]
                unadj_low_52w_val = unadj_data['Low'].min()
                unadj_high_52w_val = unadj_data['High'].max()
            except Exception as e:
                print(f"Error processing unadjusted data for {ticker_symbol} (generic): {e}")

        analysis['Price'] = f"{current_price_val:,.2f}" if isinstance(current_price_val, (int, float)) else "N/A"
        analysis['52W High'] = f"{unadj_high_52w_val:,.2f}" if isinstance(unadj_high_52w_val, (int, float)) else "N/A"
        analysis['52W Low'] = f"{unadj_low_52w_val:,.2f}" if isinstance(unadj_low_52w_val, (int, float)) else "N/A"

        if isinstance(current_price_val, (int, float)):
            if not pd.isna(unadj_low_52w_val) and unadj_low_52w_val != 0:
                from_low_pct = (current_price_val - unadj_low_52w_val) / unadj_low_52w_val
                analysis['% from 52W Low'] = f"{from_low_pct:.2%}"
            else:
                analysis['% from 52W Low'] = "N/A"
                
            if not pd.isna(unadj_high_52w_val) and unadj_high_52w_val != 0:
                from_high_pct = (current_price_val - unadj_high_52w_val) / unadj_high_52w_val
                analysis['% from 52W High'] = f"{from_high_pct:.2%}"
            else:
                analysis['% from 52W High'] = "N/A"
        else:
            analysis['% from 52W Low'] = "N/A"
            analysis['% from 52W High'] = "N/A"
        
        return analysis
    except Exception as e:
        st.warning(f"Analysis failed for {name}: {e}")
        return None

# --- CHARTING FUNCTIONS (Unchanged) ---
@st.cache_data
def create_ichimoku_chart(data, symbol_name):
    """
    Creates an interactive Plotly chart from a *pre-fetched* DataFrame.
    """
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            row_heights=[0.7, 0.3],
                            vertical_spacing=0.05,
                            subplot_titles=(f"{symbol_name} - Ichimoku Cloud", "RSI (14)"))
    # Candlestick
    fig.add_trace(go.Candlestick(x=data.index,
                                open=data['Open'], high=data['High'],
                                low=data['Low'], close=data['Close'],
                                name="Price"), row=1, col=1)
    # Ichimoku Cloud
    fig.add_trace(
        go.Scatter(x=data.index, y=data['SenkouA'], line=dict(color='rgba(0, 255, 0, 0.5)', width=1), name='Senkou A'),
        row=1, col=1)
    fig.add_trace(
        go.Scatter(x=data.index, y=data['SenkouB'], line=dict(color='rgba(255, 0, 0, 0.5)', width=1), name='Senkou B',
                   fill='tonexty', fillcolor='rgba(100, 100, 100, 0.1)'), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['Tenkan'], line=dict(color='blue', width=1.5), name='Tenkan'), row=1,
                  col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['Kijun'], line=dict(color='red', width=1.5), name='Kijun'), row=1,
                  col=1)
    fig.add_trace(
        go.Scatter(x=data.index, y=data['Chikou'], line=dict(color='green', width=1.5, dash='dot'), name='Chikou'),
        row=1, col=1)
    # RSI
    fig.add_trace(go.Scatter(x=data.index, y=data['RSI_14'], line=dict(color='purple', width=1.5), name='RSI'), row=2,
                  col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="red", opacity=0.6, row=2, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", opacity=0.6, row=2, col=1)
    # Layout
    fig.update_layout(title=f"{symbol_name} Technical Analysis", xaxis_rangeslider_visible=False, height=600,
                      showlegend=True, legend_orientation="h", legend_x=0, legend_y=1.1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="RSI", row=2, col=1)
    return fig

# --- STYLING FUNCTION (Unchanged) ---
def style_generic_dataframe(df):
    def color_cells(val):
        color = '#010101'
        if 'Bullish' in str(val): color = 'limegreen'
        elif 'Bearish' in str(val): color = 'tomato'
        elif 'Overbought' in str(val): color = 'darkorange'
        elif 'Oversold' in str(val): color = 'dodgerblue'
        elif '%' in str(val):
            try:
                num_val = float(str(val).replace('%', ''))
                if num_val > 0: color = 'limegreen'
                elif num_val < 0: color = 'tomato'
            except: pass
        return f'color: {color}'
    columns_to_style = df.columns
    return df.style.map(color_cells, subset=columns_to_style)

# --- PAGE CONFIGURATION (Unchanged) ---
st.set_page_config(
    page_title="Global Market Technical Dashboard",
    page_icon="🌍",
    layout="wide"
)
st.title("🌍 Global Market Technical Dashboard")
st.markdown("""
<style>
[data-testid="stColumnHeader"] {
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR SETTINGS (Unchanged) ---
st.sidebar.header("Dashboard Settings")
swing_periods = {
    "1 Yr": 252,
    "6 Months": 126,
    "3 Months": 63,
    "1 Month": 21
}
selected_period_label = st.sidebar.selectbox(
    "Select Swing Low Period (for Indian Stocks):",
    options=list(swing_periods.keys())
)
selected_period_days = swing_periods[selected_period_label]

if st.sidebar.button("Refresh All Data"):
    st.cache_data.clear()
    st.rerun()

# --- 5. DEFINE ALL TICKER DICTIONARIES ---

# --- FINAL FIX: Using your old, working ETF tickers ---
TICKERS_INDIAN_INDICES = {
    "NIFTY 50": "^NSEI",
    "NIFTY NEXT 50": "^NSMIDCP",
    "NIFTY 500": "^CRSLDX",
    "NIFTY MIDCAP 150": "MID150BEES.NS",
    "NIFTY SMALLCAP 250": "HDFCSML250.NS",
    "NIFTY MICROCAP 250": "MICROS250.NS", 
    "INDIA VIX": "^INDIAVIX"
}
# --- FINAL FIX: Using your old, working tickers ---
TICKERS_SECTORAL = {
    "NIFTY AUTO": "^CNXAUTO",
    "NIFTY BANK": "^NSEBANK",
    "NIFTY FIN SERVICE": "NIFTY_FIN_SERVICE.NS",
    "NIFTY FMCG": "^CNXFMCG",
    "NIFTY IT": "^CNXIT",
    "NIFTY MEDIA": "^CNXMEDIA",
    "NIFTY METAL": "^CNXMETAL",
    "NIFTY PHARMA": "^CNXPHARMA",
    "NIFTY REALTY": "^CNXREALTY",
    "NIFTY HEALTHCARE": "^NIFTYHEALTHCARE", # This may still fail, yfinance has limited data
    "NIFTY PSU BANK": "^CNXPSUBANK",
    "NIFTY PVT BANK": "NIFTY_PVT_BANK.NS",
    "NIFTY CONSUMER": "^CNXCONSUM",
    "NIFTY OIL & GAS - ETF": "OILIETF.NS",
    "NIFTY CHEMICALS": "^NIFTYCHEM", # This may still fail, yfinance has limited data
    "NIFTY PSE": "^CNXPSE"
}
# --- END OF FINAL FIX ---

TICKERS_GLOBAL = {
    "S&P 500": "^GSPC",
    "NASDAQ": "^IXIC",
    "Germany (DAX)": "^GDAXI",
    "Japan (Nikkei)": "^N225",
    "Hong Kong (Hang Seng)": "^HSI",
    "China (Shanghai)": "000001.SS",
    "UK (FTSE 100)": "^FTSE"
}
TICKERS_COMMODITIES = {
    "Gold": "GC=F",
    "Silver": "SI=F",
    "Crude Oil (WTI)": "CL=F",
    "Copper": "HG=F",
    "Natural Gas": "NG=F"
}
TICKERS_CRYPTO = {
    "Bitcoin": "BTC-USD",
    "Ethereum": "ETH-USD",
    "Solana": "SOL-USD",
    "BNB": "BNB-USD",
    "XRP": "XRP-USD"
}

# --- LOAD DYNAMIC TICKERS (Unchanged) ---
TICKERS_HEAVYWEIGHTS = load_stocks_from_google_sheet()
TICKERS_MODEL_PORTFOLIO = load_model_portfolio_from_sheet() 

# --- 6. MASTER DATA FETCH LOGIC (Unchanged) ---
st.header("Master Data Control")
with st.expander("Show Master Data Log"):
    
    unique_tickers = set()
    unique_tickers.update(TICKERS_INDIAN_INDICES.values())
    unique_tickers.update(TICKERS_SECTORAL.values())
    unique_tickers.update(TICKERS_HEAVYWEIGHTS.values())
    unique_tickers.update(TICKERS_GLOBAL.values())
    unique_tickers.update(TICKERS_COMMODITIES.values())
    unique_tickers.update(TICKERS_CRYPTO.values())
    unique_tickers.update(TICKERS_MODEL_PORTFOLIO.values()) 
    
    master_ticker_list = list(unique_tickers)
    
    if master_ticker_list:
        master_data = fetch_master_data(master_ticker_list)
        if master_data is None:
            st.error("Master data fetch failed. App cannot proceed.")
            st.stop()
    else:
        master_data = None
        st.error("No tickers found. Check Google Sheet permissions and names.")
        st.stop()


# --- 7. HELPER FUNCTION (for main tabs) ---
# (Unchanged)
def get_display_cols(period_label):
    return [
        'Index', 'Current Price', 'Adj Price', 'Change %', 'Weekly %',
        'Tenkan/Kijun', 'Chikou Span', 'Cloud', 'RSI',
        f"{period_label} Low (Un-adj)",
        f"{period_label} Low (Adj)",
        f"{period_label} Low Date",
        f"% from {period_label} Low"
    ]

# (Unchanged)
def get_color(val):
    color = '#010101' 
    if 'Bullish' in str(val): color = 'limegreen'
    elif 'Bearish' in str(val): color = 'tomato'
    elif 'Sideways' in str(val) or 'Neutral' in str(val): color = 'darkorange'
    elif 'Overbought' in str(val): color = 'darkorange'
    elif 'Oversold' in str(val): color = 'dodgerblue'
    if '%' in str(val):
        try:
            num_val = float(str(val).replace('%', ''))
            if num_val > 0: color = 'limegreen'
            elif num_val < 0: color = 'tomato'
        except: pass
    return color


# --- 8. TABBED INTERFACE (Unchanged) ---
tab_names = [
    "🇮🇳 Indian Indices",
    "🏭 Sectoral Indices",
    "📈 Heavyweight Stocks",
    "📈 Model Portfolio", 
    "🌎 Global Indices",
    "🥇 Commodities",
    "₿ Crypto"
]
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(tab_names)


# --- REFACTORED TABS (1, 2, 3, 4) ---

def render_technical_tab(tab_name, ticker_dict, period_days, period_label, key_prefix):
    """
    Reusable function to render a technical analysis tab.
    """
    st.header(tab_name)
    analysis_results = []
    
    for name, ticker in ticker_dict.items():
        if ticker in master_data['Close']:
            ticker_data = master_data.loc[:, (slice(None), ticker)]
            ticker_data.columns = ticker_data.columns.droplevel(1)
            
            analysis = run_technical_analysis(ticker_data, name, period_days, period_label, ticker)
            if analysis:
                analysis['Ticker'] = ticker
                analysis_results.append(analysis)
        else:
            st.warning(f"Data for {name} ({ticker}) not in master data. Skipping.")

    if analysis_results:
        df = pd.DataFrame(analysis_results)
        
        display_cols = get_display_cols(period_label)
        display_cols = [col for col in display_cols if col in df.columns]

        # (Filter/Sort logic is unchanged)
        with st.expander("Show Dashboard Controls (Sort & Filter)", expanded=False):
            sort_col1, sort_col2 = st.columns(2)
            with sort_col1:
                sort_by = st.selectbox("Sort by column", options=display_cols, key=f"sort_by_{key_prefix}")
            with sort_col2:
                sort_order = st.radio("Order", options=["Ascending", "Descending"], horizontal=True,
                                        key=f"sort_order_{key_prefix}")

            ascending = (sort_order == "Ascending")
            if any(keyword in sort_by for keyword in ['Price', 'RSI', '%', 'Low']):
                sort_key_col = f"{sort_by}_numeric"
                if not df.empty:
                    if '%' in str(df[sort_by].iloc[0]):
                        df[sort_key_col] = pd.to_numeric(
                            df[sort_by].astype(str).str.replace('%', '').str.replace(',', '').str.replace('N/A', 'nan'), errors='coerce')
                    else:
                        df[sort_key_col] = pd.to_numeric(df[sort_by].astype(str).str.replace(',', '').str.replace('N/A', 'nan'), errors='coerce')
                    df = df.sort_values(by=sort_key_col, ascending=ascending, na_position='last').drop(
                        columns=[sort_key_col])
            else:
                df = df.sort_values(by=sort_by, ascending=ascending, na_position='last')
        
        st.divider()
        
        # (Display logic is unchanged)
        display_cols_with_chart = display_cols + ["View Chart"]
        cols = st.columns(len(display_cols_with_chart))
        for col, header in zip(cols, display_cols_with_chart):
            col.markdown(f"**{header}**")
        st.divider()

        if df.empty:
            st.warning("No data matches your filter criteria.")
        else:
            for index, row in df.iterrows():
                ticker_symbol = row['Ticker']
                index_name = row['Index']
                row_cols = st.columns(len(display_cols_with_chart))
                for i, col_name in enumerate(display_cols):
                    val = row[col_name]
                    color = get_color(val)
                    row_cols[i].markdown(f"<span style='color: {color};'>{val}</span>", unsafe_allow_html=True)

                button_col = row_cols[-1]
                if button_col.button("View", key=f"{ticker_symbol}_{key_prefix}"):
                    with st.spinner(f"Loading chart for {index_name}..."):
                        chart_data = master_data.loc[:, (slice(None), ticker_symbol)]
                        chart_data.columns = chart_data.columns.droplevel(1)

                        chart_data = chart_data.copy()
                        chart_data.ta.ichimoku(append=True)
                        chart_data.ta.rsi(length=14, append=True)
                        chart_data = chart_data.rename(columns={
                            'ITS_9': 'Tenkan', 'IKS_26': 'Kijun',
                            'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'
                        })
                        fig = create_ichimoku_chart(chart_data, index_name)
                        with st.expander(f"Interactive Chart: {index_name}", expanded=True):
                            # --- FIX: DEPRECATION WARNING ---
                            st.plotly_chart(fig, width='stretch')
                            # --- END FIX ---
                st.divider()

with tab1:
    render_technical_tab("Indian Market Indices", TICKERS_INDIAN_INDICES, selected_period_days, selected_period_label, "tab1")

with tab2:
    render_technical_tab("Indian Sectoral Indices", TICKERS_SECTORAL, selected_period_days, selected_period_label, "tab2")

with tab3:
    render_technical_tab("Indian Heavyweight Stocks", TICKERS_HEAVYWEIGHTS, selected_period_days, selected_period_label, "tab3")

with tab4: 
    render_technical_tab("Model Portfolio", TICKERS_MODEL_PORTFOLIO, selected_period_days, selected_period_label, "tab4")


# --- REFACTORED TABS (5, 6, 7) ---

def render_generic_tab(tab_name, ticker_dict):
    """
    Reusable function to render a generic analysis tab.
    """
    st.header(tab_name)
    analysis_results = []
    
    for name, ticker in ticker_dict.items():
        if ticker in master_data['Close']:
            ticker_data = master_data.loc[:, (slice(None), ticker)]
            ticker_data.columns = ticker_data.columns.droplevel(1)
            
            analysis = run_generic_analysis(ticker_data, name, ticker)
            if analysis:
                analysis_results.append(analysis)
        else:
            st.warning(f"Data for {name} ({ticker}) not in master data. Skipping.")

    if analysis_results:
        df = pd.DataFrame(analysis_results)
        df = df.set_index("Asset")
        # --- FIX: DEPRECATION WARNING ---
        st.dataframe(style_generic_dataframe(df), width='stretch')
        # --- END FIX ---

with tab5:
    render_generic_tab("Global Indices", TICKERS_GLOBAL)

with tab6:
    render_generic_tab("Commodities", TICKERS_COMMODITIES)

with tab7:
    render_generic_tab("Cryptocurrency", TICKERS_CRYPTO)


# --- INDIVIDUAL ANALYSIS TOOL (Unchanged) ---
st.divider()
st.header("🔍 Individual Analysis & Data Debugger")
st.info("Enter any valid Ticker Symbol. It will try to use pre-fetched master data first.")

user_symbol = st.text_input(
    "Enter a valid Ticker Symbol (e.g., 'RELIANCE.NS', 'INFY.NS', '^GSPC')",
    "INFY.NS"
)

if user_symbol:
    hist_data = None
    info = {}
    error = None

    if user_symbol in master_ticker_list:
        st.success("Ticker found in master data list. Using pre-fetched data.")
        hist_data_raw = master_data.loc[:, (slice(None), user_symbol)]
        hist_data = hist_data_raw.copy()
        hist_data.columns = hist_data.columns.droplevel(1)
        
        hist_data.ta.ichimoku(append=True)
        hist_data.ta.rsi(length=14, append=True)
        hist_data = hist_data.rename(columns={
            'ITS_9': 'Tenkan', 'IKS_26': 'Kijun',
            'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'
        })
        
    else:
        st.warning("Ticker not in master list. Fetching live data (this will be slower)...")
        hist_data = fetch_data(user_symbol) # Uses your module
        if hist_data is not None and not hist_data.empty:
            hist_data = hist_data.copy()
            hist_data.ta.ichimoku(append=True)
            hist_data.ta.rsi(length=14, append=True)
            hist_data = hist_data.rename(columns={
                'ITS_9': 'Tenkan', 'IKS_26': 'Kijun',
                'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'
            })
    
    if hist_data is None or hist_data.empty:
        error = f"No historical data found for {user_symbol}."
    else:
        # --- FIX: Drop NaNs from fallback data too ---
        hist_data = hist_data.dropna(subset=['Close'])
        if hist_data.empty:
            error = f"No valid data after cleaning for {user_symbol}."
        # --- END FIX ---
        
        try:
            info = yf.Ticker(user_symbol).info
            if not info: info = {}
        except Exception as e:
            st.warning(f"Could not fetch Ticker(.info) data: {e}. Metrics may be limited.")

    if error:
        st.error(error)
    elif not hist_data.empty:
        st.success(f"Displaying analysis for: **{info.get('longName', user_symbol)}**")
        
        col1, col2 = st.columns(2)
        with col1:
            latest = hist_data.iloc[-1]
            prev = hist_data.iloc[-2]
            change_adj = latest['Close'] - prev['Close']
            percent_change_adj = (change_adj / prev['Close']) * 100

            st.metric(
                label="Last Price (Adjusted EOD)",
                value=f"{latest['Close']:,.2f}",
                delta=f"{change_adj:,.2f} ({percent_change_adj:.2f}%)"
            )
            
            current_price_unadj = info.get('currentPrice')
            if isinstance(current_price_unadj, (int, float)):
                value_to_display = f"{current_price_unadj:,.2f}"
            else:
                value_to_display = "N/A"
            st.metric(label="Current Price (Unadjusted LTP)", value=value_to_display)

            # --- 52w H/L from ADJUSTED data ---
            
            # --- FIX: DEPRECATION WARNING ---
            end_date_adj_d = hist_data.index.max()
            start_date_adj_d = end_date_adj_d - pd.Timedelta(weeks=52)
            hist_1y_adj = hist_data.loc[(hist_data.index >= start_date_adj_d) & (hist_data.index <= end_date_adj_d)]
            # --- END FIX ---

            high_52w_adj = hist_1y_adj['High'].max()
            low_52w_adj = hist_1y_adj['Low'].min()
            st.metric(label="52-Week High (Adjusted)", value=f"{high_52w_adj:,.2f}")
            st.metric(label="52-Week Low (Adjusted)", value=f"{low_52w_adj:,.2f}")
            
            market_cap_raw = info.get('marketCap', 0)
            if market_cap_raw and market_cap_raw > 0:
                market_cap_cr = market_cap_raw / 1_00_00_000
                market_cap_str = f"₹{market_cap_cr:,.2f} Cr"
            else: market_cap_str = "N/A"
            st.metric(label="Market Cap (from .info)", value=market_cap_str)

        with col2:
            st.subheader("Technical Analysis (from Adjusted data)")
            latest_rsi = latest['RSI_14']
            st.metric(label="RSI (14-Day)", value=f"{latest_rsi:.2f}")
            if latest_rsi > 70: st.warning("Overbought (RSI > 70)")
            elif latest_rsi < 30: st.success("Oversold (RSI < 30)")
            else: st.info("Neutral (30 < RSI < 70)")
        
        with st.expander("View Full Chart (Adjusted Data)"):
            stock_fig = create_ichimoku_chart(hist_data, info.get('longName', user_symbol))
            # --- FIX: DEPRECATION WARNING ---
            st.plotly_chart(stock_fig, width='stretch')
            # --- END FIX ---

        with st.expander("View Raw Data (Adjusted - Used for Analysis)"):
            st.dataframe(hist_data)
        
        with st.expander("View Raw Data (Unadjusted - For Price Comparison)"):
            with st.spinner("Fetching unadjusted data..."):
                unadj_debug_data = fetch_unadjusted_data(user_symbol, period="2y")
                if unadj_debug_data is None or unadj_debug_data.empty:
                    st.warning("Could not fetch unadjusted data.")
                else:
                    st.dataframe(unadj_debug_data)