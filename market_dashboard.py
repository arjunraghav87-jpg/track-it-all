import streamlit as st
import pandas as pd
import gspread
import yfinance as yf
import pandas_ta as ta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import time  # <--- CRITICAL FIX for sleep()

# --- Imports: We no longer import from modules/analysis ---
from modules.data_fetcher import fetch_data, fetch_unadjusted_data

# --- HELPER: Resample to Weekly ---
def resample_to_weekly(daily_df):
    """
    Converts Daily OHLC data to Weekly OHLC data (Ending Friday).
    """
    if daily_df is None or daily_df.empty:
        return None
    
    # Resample to Weekly (Fri)
    weekly_df = daily_df.resample('W-FRI').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    })
    # We drop the last row only if it's empty, but usually we keep partial weeks for live analysis
    return weekly_df

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


# --- MASTER DATA FETCHER (Safe Mode) ---
@st.cache_data(ttl=3600) # 1 Hour Cache
def fetch_master_data(tickers_list):
    """
    Downloads data in SERIAL batches (10 at a time, NO THREADS)
    to strictly avoid Yahoo Finance Rate Limits.
    """
    if not tickers_list:
        return None
    
    # Deduplicate tickers
    tickers_list = list(set(tickers_list))
    st.info(f"Fetching master data for {len(tickers_list)} unique tickers... (Slow Mode Active)")
    
    all_data_frames = []
    
    # Small Batch Size
    chunk_size = 10 
    
    progress_bar = st.progress(0)
    
    try:
        for i in range(0, len(tickers_list), chunk_size):
            batch = tickers_list[i : i + chunk_size]
            
            # Update progress
            progress_bar.progress(min(i / len(tickers_list), 1.0))
            
            try:
                # threads=False prevents the "DDoS" behavior that Yahoo blocks.
                batch_data = yf.download(
                    batch, 
                    period="2y", 
                    auto_adjust=True, 
                    progress=False, 
                    threads=False 
                )
                
                if not batch_data.empty:
                    all_data_frames.append(batch_data)
                    
            except Exception as e:
                print(f"Error fetching batch {i}: {e}")
                continue
                
            # Wait 2 seconds between batches
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


# --- ANALYSIS FUNCTIONS ---

def run_technical_analysis(data, name, period_length, period_label, ticker_symbol):
    """
    Runs technical analysis. 'period_length' is adapted for Daily vs Weekly automatically.
    """
    if data is None or data.empty:
        return None
    
    # Drop NaN rows (essential for Weekly resampling if weeks are missing)
    data = data.dropna(subset=['Close'])

    if len(data) < 30:
        return None
    
    data = data.copy()
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
        
        analysis['Index'] = name
        analysis['Adj Price'] = f"{latest['Close']:,.2f}"
        analysis['Change %'] = f"{(latest['Close'] - prev['Close']) / prev['Close']:.2%}"

        # Weekly % logic changes slightly in weekly mode, but calculating from 1 bar ago is safe
        if len(data) >= 6:
            # For Daily: 5 days ago. For Weekly: 1 week ago (approx). 
            # We stick to looking back 1 unit for consistency in this column.
            week_ago = data.iloc[-2] 
            # Note: In Daily mode you might want iloc[-6], but simplified here for compatibility
            weekly_pct = (latest['Close'] - week_ago['Close']) / week_ago['Close']
            # analysis['Weekly %'] = f"{weekly_pct:.2%}" # Optional to show
        
        # Cloud Logic
        if latest['Close'] > latest['SenkouA'] and latest['Close'] > latest['SenkouB']:
            analysis['Cloud'] = "Bullish (Above Cloud)"
        elif latest['Close'] < latest['SenkouA'] and latest['Close'] < latest['SenkouB']:
            analysis['Cloud'] = "Bearish (Below Cloud)"
        else:
            analysis['Cloud'] = "Neutral (In Cloud)"

        # Tenkan/Kijun
        tk_cross = "nan"
        if pd.isna(latest['Tenkan']) or pd.isna(latest['Kijun']):
            tk_cross = "nan" 
        elif latest['Tenkan'] > latest['Kijun']:
            tk_cross = "Bullish (Tenkan > Kijun)"
        else:
            tk_cross = "Bearish (Tenkan < Kijun)"
        analysis['Tenkan/Kijun'] = tk_cross

        # Chikou Span
        if latest['Close'] > data['Close'].iloc[-27]: 
            analysis['Chikou Span'] = "Bullish"
        else:
            analysis['Chikou Span'] = "Bearish"

        # RSI
        rsi_val = latest['RSI_14']
        if rsi_val > 70:
            analysis['RSI'] = f"Overbought ({rsi_val:.1f})"
        elif rsi_val < 30:
            analysis['RSI'] = f"Oversold ({rsi_val:.1f})"
        else:
            analysis['RSI'] = f"Neutral ({rsi_val:.1f})"

        # Swing Low (Adjusted Lookback)
        # We use 'period_length' which is already converted to Days or Weeks
        # e.g., for "1 Yr", this is 252 (Daily) or 52 (Weekly)
        
        # Slice the last 'period_length' rows
        start_idx = max(0, len(data) - period_length)
        period_data_adj = data.iloc[start_idx:]
        
        if period_data_adj.empty or period_data_adj['Low'].isnull().all():
            adj_low_price = float('nan')
        else:
            adj_low_price = period_data_adj['Low'].min()
        
        analysis[f"{period_label} Low (Adj)"] = f"{adj_low_price:,.2f}"

        # UNADJUSTED Data Fetch (Only done for Daily mode usually, or kept as reference)
        # Note: Unadjusted data is hard to resample without fetching full history.
        # For simplicity in Weekly mode, we might skip unadjusted check or show N/A
        # but let's leave it as best-effort.
        
        current_price_val = None
        unadj_low_val = float('nan')
        
        # We only fetch unadjusted for price reference.
        # Mapping periods for yfinance
        period_map = {"1 Yr": "1y", "6 Months": "6mo", "3 Months": "3mo", "1 Month": "1mo"}
        yf_period = period_map.get(period_label, "1y") 

        # We only fetch unadjusted if we really need it (costly). 
        # For Weekly mode, we might rely on Adjusted data to save API calls.
        # But let's keep it for now.
        unadj_data = fetch_unadjusted_data(ticker_symbol, period=yf_period)
        
        if unadj_data is not None and not unadj_data.empty:
            try:
                unadj_low_val = unadj_data['Low'].min()
                current_price_val = unadj_data['Close'].iloc[-1]
            except Exception:
                pass

        analysis['Current Price'] = f"{current_price_val:,.2f}" if isinstance(current_price_val, (int, float)) else "N/A"
        analysis[f"{period_label} Low (Un-adj)"] = f"{unadj_low_val:,.2f}"
        
        if isinstance(current_price_val, (int, float)) and not pd.isna(unadj_low_val) and unadj_low_val != 0:
            from_low_pct = (current_price_val - unadj_low_val) / unadj_low_val
            analysis[f"% from {period_label} Low"] = f"{from_low_pct:.2%}"
        else:
            analysis[f"% from {period_label} Low"] = "N/A"

        return analysis
    except Exception as e:
        # st.warning(f"Analysis failed for {name}: {e}")
        return None

def run_generic_analysis(data, name, ticker_symbol):
    if data is None or data.empty:
        return None
    data = data.dropna(subset=['Close'])
    data = data.copy()
    if len(data) < 30: return None
    
    data.ta.ichimoku(append=True)
    data.ta.rsi(length=14, append=True)
    data = data.rename(columns={'ITS_9': 'Tenkan', 'IKS_26': 'Kijun', 'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'})
    
    try:
        analysis = {}
        latest = data.iloc[-1]
        prev = data.iloc[-2]

        analysis['Asset'] = name
        analysis['Change %'] = f"{(latest['Close'] - prev['Close']) / prev['Close']:.2%}"

        # Ichimoku
        if latest['Close'] > latest['SenkouA'] and latest['Close'] > latest['SenkouB']:
            analysis['Cloud'] = "Bullish (Above Cloud)"
        elif latest['Close'] < latest['SenkouA'] and latest['Close'] < latest['SenkouB']:
            analysis['Cloud'] = "Bearish (Below Cloud)"
        else:
            analysis['Cloud'] = "Neutral (In Cloud)"

        # RSI
        rsi_val = latest['RSI_14']
        if rsi_val > 70: analysis['RSI'] = f"Overbought ({rsi_val:.1f})"
        elif rsi_val < 30: analysis['RSI'] = f"Oversold ({rsi_val:.1f})"
        else: analysis['RSI'] = f"Neutral ({rsi_val:.1f})"
        
        return analysis
    except Exception:
        return None

# --- CHARTING ---
@st.cache_data
def create_ichimoku_chart(data, symbol_name):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05,
                            subplot_titles=(f"{symbol_name} - Ichimoku Cloud", "RSI (14)"))
    fig.add_trace(go.Candlestick(x=data.index, open=data['Open'], high=data['High'], low=data['Low'], close=data['Close'], name="Price"), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['SenkouA'], line=dict(color='rgba(0, 255, 0, 0.5)', width=1), name='Senkou A'), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['SenkouB'], line=dict(color='rgba(255, 0, 0, 0.5)', width=1), name='Senkou B', fill='tonexty', fillcolor='rgba(100, 100, 100, 0.1)'), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['Tenkan'], line=dict(color='blue', width=1.5), name='Tenkan'), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['Kijun'], line=dict(color='red', width=1.5), name='Kijun'), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['Chikou'], line=dict(color='green', width=1.5, dash='dot'), name='Chikou'), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['RSI_14'], line=dict(color='purple', width=1.5), name='RSI'), row=2, col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="red", opacity=0.6, row=2, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", opacity=0.6, row=2, col=1)
    fig.update_layout(title=f"{symbol_name} Technical Analysis", xaxis_rangeslider_visible=False, height=600, showlegend=True, legend_orientation="h", legend_x=0, legend_y=1.1)
    return fig

# --- STYLING ---
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
    return df.style.map(color_cells)

# --- CONFIG ---
st.set_page_config(page_title="Global Market Technical Dashboard", page_icon="🌍", layout="wide")
st.title("🌍 Global Market Technical Dashboard")
st.markdown("""<style>[data-testid="stColumnHeader"] {font-weight: bold;}</style>""", unsafe_allow_html=True)

# --- SIDEBAR & VIEW MODES ---
st.sidebar.header("Dashboard Settings")

# 1. VIEW MODE TOGGLE
view_mode = st.sidebar.radio(
    "Select Analysis Timeframe:",
    ["Daily Analysis", "Weekly Analysis"],
    help="Weekly mode resamples data. '1 Yr' Lookback changes from 252 days to 52 weeks."
)

# 2. SWING PERIOD SELECTOR
swing_periods_daily = {"1 Yr": 252, "6 Months": 126, "3 Months": 63, "1 Month": 21}
swing_periods_weekly = {"1 Yr": 52, "6 Months": 26, "3 Months": 13, "1 Month": 4}

selected_period_label = st.sidebar.selectbox(
    "Select Swing Low Period:",
    options=list(swing_periods_daily.keys())
)

# Logic: Choose the correct integer based on the Mode
if view_mode == "Daily Analysis":
    selected_period_length = swing_periods_daily[selected_period_label]
else:
    selected_period_length = swing_periods_weekly[selected_period_label]


if st.sidebar.button("Refresh All Data"):
    st.cache_data.clear()
    st.rerun()

# --- TICKERS ---
TICKERS_INDIAN_INDICES = {
    "NIFTY 50": "^NSEI", "NIFTY NEXT 50": "^NSMIDCP", "NIFTY 500": "^CRSLDX",
    "NIFTY MIDCAP 150": "MID150BEES.NS", "NIFTY SMALLCAP 250": "HDFCSML250.NS",
    #"NIFTY MICROCAP 250": "MICROS250.NS", 
    "INDIA VIX": "^INDIAVIX"
}
TICKERS_SECTORAL = {
    "NIFTY AUTO": "^CNXAUTO", "NIFTY BANK": "^NSEBANK", "NIFTY FIN SERVICE": "NIFTY_FIN_SERVICE.NS",
    "NIFTY FMCG": "^CNXFMCG", "NIFTY IT": "^CNXIT", "NIFTY MEDIA": "^CNXMEDIA",
    "NIFTY METAL": "^CNXMETAL", "NIFTY PHARMA": "^CNXPHARMA", "NIFTY REALTY": "^CNXREALTY",
    #"NIFTY HEALTHCARE": "^NIFTYHEALTHCARE", 
    "NIFTY PSU BANK": "^CNXPSUBANK",
    "NIFTY PVT BANK": "NIFTY_PVT_BANK.NS", "NIFTY CONSUMER": "^CNXCONSUM",
    "NIFTY OIL & GAS - ETF": "OILIETF.NS", 
    #"NIFTY CHEMICALS": "^NIFTYCHEM", 
    "NIFTY PSE": "^CNXPSE"
}
TICKERS_GLOBAL = {
    "S&P 500": "^GSPC", "NASDAQ": "^IXIC", "Germany (DAX)": "^GDAXI",
    "Japan (Nikkei)": "^N225", "Hong Kong (Hang Seng)": "^HSI",
    "China (Shanghai)": "000001.SS", "UK (FTSE 100)": "^FTSE"
}
TICKERS_COMMODITIES = {"Gold": "GC=F", "Silver": "SI=F", "Crude Oil (WTI)": "CL=F", "Copper": "HG=F", "Natural Gas": "NG=F"}
TICKERS_CRYPTO = {"Bitcoin": "BTC-USD", "Ethereum": "ETH-USD", "Solana": "SOL-USD", "BNB": "BNB-USD", "XRP": "XRP-USD"}

TICKERS_HEAVYWEIGHTS = load_stocks_from_google_sheet()
TICKERS_MODEL_PORTFOLIO = load_model_portfolio_from_sheet() 

# --- DATA FETCHING ---
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
    
    master_ticker_list = sorted(list(unique_tickers))
    
    if master_ticker_list:
        master_data = fetch_master_data(master_ticker_list)
        if master_data is None:
            st.error("Master data fetch failed. App cannot proceed.")
            st.stop()
    else:
        master_data = None
        st.error("No tickers found. Check Google Sheet permissions and names.")
        st.stop()

# --- DISPLAY HELPERS ---
def get_display_cols(period_label):
    return ['Index', 'Current Price', 'Adj Price', 'Change %', 'Tenkan/Kijun', 'Chikou Span', 'Cloud', 'RSI',
            f"{period_label} Low (Un-adj)", f"{period_label} Low (Adj)", f"% from {period_label} Low"]
def get_color(val):
    color = '#010101' 
    if 'Bullish' in str(val): color = 'limegreen'
    elif 'Bearish' in str(val): color = 'tomato'
    elif 'Overbought' in str(val) or 'Sideways' in str(val) or 'Neutral' in str(val): color = 'darkorange'
    elif 'Oversold' in str(val): color = 'dodgerblue'
    if '%' in str(val):
        try:
            if float(str(val).replace('%', '')) > 0: color = 'limegreen'
            elif float(str(val).replace('%', '')) < 0: color = 'tomato'
        except: pass
    return color

# --- TABS ---
tab_names = ["🇮🇳 Indian Indices", "🏭 Sectoral Indices", "📈 Heavyweight Stocks", "📈 Model Portfolio", "🌎 Global Indices", "🥇 Commodities", "₿ Crypto"]
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(tab_names)

def render_technical_tab(tab_name, ticker_dict, period_length, period_label, key_prefix):
    st.header(f"{tab_name} ({view_mode})")
    analysis_results = []
    
    # 1. Prepare Data
    for name, ticker in ticker_dict.items():
        if ticker in master_data['Close']:
            ticker_data = master_data.loc[:, (slice(None), ticker)]
            ticker_data.columns = ticker_data.columns.droplevel(1)
            
            # Resample if needed
            if view_mode == "Weekly Analysis":
                ticker_data = resample_to_weekly(ticker_data)
            
            analysis = run_technical_analysis(ticker_data, name, period_length, period_label, ticker)
            if analysis:
                analysis['Ticker'] = ticker
                analysis_results.append(analysis)

    # 2. Render Table with Toggles
    if analysis_results:
        df = pd.DataFrame(analysis_results)
        display_cols = get_display_cols(period_label)
        display_cols = [col for col in display_cols if col in df.columns]

        # Sorting Controls
        with st.expander("Show Controls", expanded=False):
            sort_by = st.selectbox("Sort by", options=display_cols, key=f"sort_{key_prefix}")
            ascending = st.radio("Order", ["Ascending", "Descending"], horizontal=True, key=f"order_{key_prefix}") == "Ascending"
            
            if not df.empty:
                sort_key = f"{sort_by}_num"
                df[sort_key] = pd.to_numeric(df[sort_by].astype(str).str.replace(r'[%,]', '', regex=True).str.replace('N/A', 'nan'), errors='coerce')
                df = df.sort_values(by=sort_key, ascending=ascending, na_position='last').drop(columns=[sort_key])

        # Headers
        cols = st.columns(len(display_cols) + 1)
        for col, header in zip(cols, display_cols + ["Action"]): 
            col.markdown(f"**{header}**")
        st.divider()

        # Rows
        for _, row in df.iterrows():
            ticker = row['Ticker']
            # Unique ID for this row's open/close state
            state_key = f"chart_open_{ticker}_{key_prefix}"
            
            # Initialize State if not present
            if state_key not in st.session_state:
                st.session_state[state_key] = False

            r_cols = st.columns(len(display_cols) + 1)
            
            # Render Data Columns
            for i, col in enumerate(display_cols):
                val = row[col]
                r_cols[i].markdown(f"<span style='color: {get_color(val)};'>{val}</span>", unsafe_allow_html=True)
            
            # Render Toggle Button
            # If state is True, show "Close". If False, show "View".
            btn_label = "Close ❌" if st.session_state[state_key] else "View 📈"
            
            if r_cols[-1].button(btn_label, key=f"btn_{ticker}_{key_prefix}"):
                # Toggle the state
                st.session_state[state_key] = not st.session_state[state_key]
                st.rerun() # Force reload to update UI immediately

            # Render Chart (If state is Open)
            if st.session_state[state_key]:
                with st.container():
                    st.info(f"Generating {view_mode} Chart for {row['Index']}...")
                    
                    # Fetch Data for Chart
                    c_data = master_data.loc[:, (slice(None), ticker)]
                    c_data.columns = c_data.columns.droplevel(1)
                    if view_mode == "Weekly Analysis": 
                        c_data = resample_to_weekly(c_data)
                    
                    # Calculate Indicators
                    c_data.ta.ichimoku(append=True)
                    c_data.ta.rsi(length=14, append=True)
                    c_data = c_data.rename(columns={'ITS_9': 'Tenkan', 'IKS_26': 'Kijun', 'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'})
                    
                    # Create & Show Chart
                    fig = create_ichimoku_chart(c_data, f"{row['Index']} ({view_mode})")
                    
                    # Add a dedicated close button above the chart too (optional UX)
                    col_close, _ = st.columns([1, 10])
                    if col_close.button("Close Chart", key=f"close_inner_{ticker}_{key_prefix}"):
                        st.session_state[state_key] = False
                        st.rerun()

                    st.plotly_chart(fig, width='stretch', key=f"plot_{ticker}_{key_prefix}")
                    st.markdown("---") # Divider after chart
            
            st.divider() # Divider between rows

with tab1: render_technical_tab("Indian Indices", TICKERS_INDIAN_INDICES, selected_period_length, selected_period_label, "t1")
with tab2: render_technical_tab("Sectoral Indices", TICKERS_SECTORAL, selected_period_length, selected_period_label, "t2")
with tab3: render_technical_tab("Heavyweights", TICKERS_HEAVYWEIGHTS, selected_period_length, selected_period_label, "t3")
with tab4: render_technical_tab("Model Portfolio", TICKERS_MODEL_PORTFOLIO, selected_period_length, selected_period_label, "t4")

def render_generic_tab(title, tickers):
    st.header(title)
    res = []
    for name, ticker in tickers.items():
        if ticker in master_data['Close']:
            t_data = master_data.loc[:, (slice(None), ticker)]
            t_data.columns = t_data.columns.droplevel(1)
            # Generic tabs also respect weekly mode if desired, or can stay daily. 
            # Let's make them respect the mode for consistency.
            if view_mode == "Weekly Analysis": t_data = resample_to_weekly(t_data)
            
            an = run_generic_analysis(t_data, name, ticker)
            if an: res.append(an)
    if res:
        st.dataframe(style_generic_dataframe(pd.DataFrame(res).set_index("Asset")), width='stretch')

with tab5: render_generic_tab("Global Indices", TICKERS_GLOBAL)
with tab6: render_generic_tab("Commodities", TICKERS_COMMODITIES)
with tab7: render_generic_tab("Crypto", TICKERS_CRYPTO)

# --- INDIVIDUAL DEBUGGER ---
st.divider()
st.header("🔍 Individual Analysis")
user_symbol = st.text_input("Ticker:", "INFY.NS")
if user_symbol:
    if user_symbol in master_ticker_list:
        h_data = master_data.loc[:, (slice(None), user_symbol)]
        h_data.columns = h_data.columns.droplevel(1)
        st.success("Using Master Data")
    else:
        st.warning("Fetching Live...")
        h_data = fetch_data(user_symbol)

    if h_data is not None and not h_data.empty:
        if view_mode == "Weekly Analysis": h_data = resample_to_weekly(h_data)
        
        h_data.ta.ichimoku(append=True)
        h_data.ta.rsi(length=14, append=True)
        h_data = h_data.rename(columns={'ITS_9': 'Tenkan', 'IKS_26': 'Kijun', 'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'})
        
        st.plotly_chart(create_ichimoku_chart(h_data, f"{user_symbol} ({view_mode})"))
        st.dataframe(h_data.tail(10))