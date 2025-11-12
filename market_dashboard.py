import streamlit as st
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
# --- Updated Imports ---
from modules.analysis import get_technical_analysis, get_generic_analysis
from modules.data_fetcher import fetch_data  # <-- 1. IMPORT THE CENTRAL FETCHER


# --- 1. HELPER FUNCTIONS (FOR CARTING & STOCK ANALYSIS) ---

@st.cache_data(ttl=900)  # Cache chart data for 15 mins
def get_chart_data(symbol):
    """
    Fetches detailed historical data for charting, using the
    centralized fetch_data module for consistency.
    """
    try:
        # 2. USE THE CENTRALIZED DATA FETCHER (2y, auto-adjusted)
        hist = fetch_data(symbol)
        if hist is None or hist.empty:
            return None, "No data found for symbol (from fetch_data)."

        # Create a copy to avoid changing the cached object from fetch_data
        hist = hist.copy()

        # Calculate indicators *for the chart*
        hist.ta.ichimoku(append=True)
        hist = hist.rename(columns={
            'ITS_9': 'Tenkan', 'IKS_26': 'Kijun',
            'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'
        })
        hist.ta.rsi(length=14, append=True)
        return hist, None
    except Exception as e:
        return None, f"Error fetching chart data: {e}"


def create_ichimoku_chart(data, symbol_name):
    """
    Creates an interactive Plotly chart with Candlestick, Ichimoku, and RSI.
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


# --- 2. HELPER FUNCTION FOR STYLING NEW TABLES ---
def style_generic_dataframe(df):
    """
    Applies color styling to the generic analysis dataframe.
    """

    def color_cells(val):
        # --- Use Streamlit's "magic" auto-inverting color ---
        # This renders as black in light mode and auto-inverts
        # to white in dark mode.
        color = '#010101'

        # --- Apply special override colors ---
        if 'Bullish' in str(val):
            color = 'limegreen'
        elif 'Bearish' in str(val):
            color = 'tomato'
        elif 'Overbought' in str(val):
            color = 'darkorange'
        elif 'Oversold' in str(val):
            color = 'dodgerblue'
        elif '%' in str(val):
            try:
                num_val = float(str(val).replace('%', ''))
                if num_val > 0:
                    color = 'limegreen'
                elif num_val < 0:
                    color = 'tomato'
            except:
                pass
        return f'color: {color}'

    # --- Apply styling to all columns except the first one ("Asset") ---
    columns_to_style = df.columns[1:]
    return df.style.map(color_cells, subset=columns_to_style)


# --- 3. PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Global Market Technical Dashboard",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 Global Market Technical Dashboard")

# --- ADD THIS CSS SNIPPET ---
# This forces the column headers in st.dataframe to be bold
st.markdown("""
<style>
[data-testid="stColumnHeader"] {
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)
# --- END OF SNIPPET ---

# --- 4. SIDEBAR SETTINGS ---
st.sidebar.header("Dashboard Settings")
swing_periods = {
    # --- MODIFIED: Changed key to '1 Yr' to match new column names ---
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
# (Tickers dictionaries are unchanged)
TICKERS_INDIAN_INDICES = {
    "NIFTY 50": "^NSEI",
    "NIFTY NEXT 50": "^NSMIDCP",
    "NIFTY 500": "^CRSLDX",
    "NIFTY MIDCAP 150": "MID150BEES.NS",
    "NIFTY SMALLCAP 250": "HDFCSML250.NS",
    ##"NIFTY MICROCAP 250": "MICROS250.NS",
    "INDIA VIX": "^INDIAVIX"
}
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
    "NIFTY HEALTHCARE": "^NIFTYHEALTHCARE",
    "NIFTY PSU BANK": "^CNXPSUBANK",
    "NIFTY PVT BANK": "NIFTY_PVT_BANK.NS",
    "NIFTY CONSUMER": "^CNXCONSUM",
    "NIFTY OIL & GAS": "OILIETF.NS",
    "NIFTY CHEMICALS": "^NIFTYCHEM",
    "NIFTY PSE": "^CNXPSE"
}
TICKERS_HEAVYWEIGHTS = {
    "Reliance": "RELIANCE.NS",
    "HDFC Bank": "HDFCBANK.NS",
    "TCS": "TCS.NS",
    "Infosys": "INFY.NS",
    "ICICI Bank": "ICICIBANK.NS",
    "L&T": "LT.NS",
    "HUL": "HINDUNILVR.NS",
    "Axis Bank": "AXISBANK.NS",
    "SBI": "SBIN.NS",
    "Bharti Airtel": "BHARTIARTL.NS"
}
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

# --- 6. TABBED INTERFACE ---
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🇮🇳 Indian Indices",
    "🏭 Sectoral Indices",
    "📈 Heavyweight Stocks",
    "🌎 Global Indices",
    "🥇 Commodities",
    "₿ Crypto"
])

# --- TAB 1: INDIAN INDICES ---
with tab1:
    st.header("Indian Market Indices")

    # This loop now calls the updated get_technical_analysis
    analysis_results = []
    with st.spinner(f'Analyzing Indian Indices (this may be slower)...'):
        for name, ticker in TICKERS_INDIAN_INDICES.items():
            analysis = get_technical_analysis(ticker, name, selected_period_days, selected_period_label)
            if analysis:
                analysis['Ticker'] = ticker
                analysis_results.append(analysis)
            else:
                st.warning(f"Could not retrieve or analyze data for {name} ({ticker}).")

    if analysis_results:
        df = pd.DataFrame(analysis_results)
        
        # --- (Old df.rename block was here, now correctly DELETED) ---

        # The display logic now automatically handles the new columns
        display_cols = [
            'Index', 'Adj Price', 'Current Price', 'Change %', 'Weekly %',
            'Tenkan/Kijun', 'Chikou Span', 'Cloud', 'RSI',
            # --- MODIFIED: Use f-strings to make labels dynamic ---
            f"{selected_period_label} Low (Adj)",
            f"{selected_period_label} Low (Un-adj)",
            f"{selected_period_label} Low Date",
            f"% from {selected_period_label} Low"
        ]
        # Filter out columns that might not exist if sidebar changes
        display_cols = [col for col in display_cols if col in df.columns]

        with st.expander("Show Dashboard Controls (Sort & Filter)", expanded=False):
            # (Filter/Sort logic is unchanged, will pick up new columns)
            signal_cols = [col for col in display_cols if 'Signal' in col]
            if signal_cols:
                filter_cols = st.columns(len(signal_cols))
                for i, col_name in enumerate(signal_cols):
                    with filter_cols[i]:
                        unique_signals = df[col_name].unique()
                        selected_signals = st.multiselect(f"Filter by {col_name}", options=unique_signals,
                                                          default=unique_signals, key=f"filter_{col_name}_tab1")
                        df = df[df[col_name].isin(selected_signals)]

            sort_col1, sort_col2 = st.columns(2)
            with sort_col1:
                sort_by = st.selectbox("Sort by column", options=display_cols, key="sort_by_tab1")
            with sort_col2:
                sort_order = st.radio("Order", options=["Ascending", "Descending"], horizontal=True,
                                      key="sort_order_tab1")

            ascending = (sort_order == "Ascending")
            if any(keyword in sort_by for keyword in ['Price', 'RSI', '%', 'Low']):
                sort_key_col = f"{sort_by}_numeric"
                if not df.empty:
                    if '%' in str(df[sort_by].iloc[0]):
                        df[sort_key_col] = pd.to_numeric(
                            df[sort_by].astype(str).str.replace('%', '').str.replace(',', ''), errors='coerce')
                    else:
                        df[sort_key_col] = pd.to_numeric(df[sort_by].astype(str).str.replace(',', ''), errors='coerce')
                    df = df.sort_values(by=sort_key_col, ascending=ascending, na_position='last').drop(
                        columns=[sort_key_col])
            else:
                df = df.sort_values(by=sort_by, ascending=ascending, na_position='last')

        st.divider()

        # --- (Display logic is unchanged, will auto-expand) ---
        
        # --- NEW, FIXED get_color function ---
        def get_color(val):
            color = '#010101' # Use the "magic" auto-inverting color

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
                if button_col.button("View", key=ticker_symbol):
                    with st.spinner(f"Loading chart for {index_name}..."):
                        chart_data, error = get_chart_data(ticker_symbol)
                        if error: st.error(error)
                        else:
                            fig = create_ichimoku_chart(chart_data, index_name)
                            with st.expander(f"Interactive Chart: {index_name}", expanded=True):
                                st.plotly_chart(fig, width='stretch')
                st.divider()

# --- TAB 2: SECTORAL INDICES ---
with tab2:
    st.header("Indian Sectoral Indices")
    # (This uses the SAME logic as Tab 1)
    analysis_results_s = []
    with st.spinner(f'Analyzing Sectoral Indices (this may be slower)...'):
        for name, ticker in TICKERS_SECTORAL.items():
            analysis = get_technical_analysis(ticker, name, selected_period_days, selected_period_label)
            if analysis:
                analysis['Ticker'] = ticker
                analysis_results_s.append(analysis)
            else:
                st.warning(f"Could not retrieve or analyze data for {name} ({ticker}).")

    if analysis_results_s:
        df_s = pd.DataFrame(analysis_results_s)
        
        # --- (Old df_s.rename block was here, now correctly DELETED) ---
        
        # --- NEW, FIXED display_cols_s list ---
        display_cols_s = [
            'Index', 'Adj Price', 'Current Price', 'Change %', 'Weekly %',
            'Tenkan/Kijun', 'Chikou Span', 'Cloud', 'RSI',
            f"{selected_period_label} Low (Adj)",
            f"{selected_period_label} Low (Un-adj)",
            f"{selected_period_label} Low Date",
            f"% from {selected_period_label} Low"
        ]
        display_cols_s = [col for col in display_cols_s if col in df_s.columns]

        cols_s = st.columns(len(display_cols_s) + 1)
        for col, header in zip(cols_s, display_cols_s + ["View Chart"]):
            col.markdown(f"**{header}**")
        st.divider()

        for index, row in df_s.iterrows():
            ticker_symbol_s = row['Ticker']
            index_name_s = row['Index']
            row_cols_s = st.columns(len(display_cols_s) + 1)
            for i, col_name in enumerate(display_cols_s):
                val = row[col_name]
                color = get_color(val) # Uses the function from Tab 1
                row_cols_s[i].markdown(f"<span style='color: {color};'>{val}</span>", unsafe_allow_html=True)

            button_col_s = row_cols_s[-1]
            if button_col_s.button("View", key=ticker_symbol_s):
                with st.spinner(f"Loading chart for {index_name_s}..."):
                    chart_data, error = get_chart_data(ticker_symbol_s)
                    if error: st.error(error)
                    else:
                        fig = create_ichimoku_chart(chart_data, index_name_s)
                        with st.expander(f"Interactive Chart: {index_name_s}", expanded=True):
                            st.plotly_chart(fig, width='stretch')
            st.divider()

# --- TAB 3: HEAVYWEIGHT STOCKS ---
with tab3:
    st.header("Indian Heavyweight Stocks")
    # (This also uses the SAME logic as Tab 1)
    analysis_results_h = []
    with st.spinner(f'Analyzing Heavyweights (this may be slower)...'):
        for name, ticker in TICKERS_HEAVYWEIGHTS.items():
            analysis = get_technical_analysis(ticker, name, selected_period_days, selected_period_label)
            if analysis:
                analysis['Ticker'] = ticker
                analysis_results_h.append(analysis)
            else:
                st.warning(f"Could not retrieve or analyze data for {name} ({ticker}).")

    if analysis_results_h:
        df_h = pd.DataFrame(analysis_results_h)

        # --- (Old df_h.rename block was here, now correctly DELETED) ---
        
        # --- NEW, FIXED display_cols_h list ---
        display_cols_h = [
            'Index', 'Adj Price', 'Current Price', 'Change %', 'Weekly %',
            'Tenkan/Kijun', 'Chikou Span', 'Cloud', 'RSI',
            f"{selected_period_label} Low (Adj)",
            f"{selected_period_label} Low (Un-adj)",
            f"{selected_period_label} Low Date",
            f"% from {selected_period_label} Low"
        ]
        display_cols_h = [col for col in display_cols_h if col in df_h.columns]
        
        cols_h = st.columns(len(display_cols_h) + 1)
        for col, header in zip(cols_h, display_cols_h + ["View Chart"]):
            col.markdown(f"**{header}**")
        st.divider()

        for index, row in df_h.iterrows():
            ticker_symbol_h = row['Ticker']
            index_name_h = row['Index']
            row_cols_h = st.columns(len(display_cols_h) + 1)
            for i, col_name in enumerate(display_cols_h):
                val = row[col_name]
                color = get_color(val) # Uses the function from Tab 1
                row_cols_h[i].markdown(f"<span style='color: {color};'>{val}</span>", unsafe_allow_html=True)

            button_col_h = row_cols_h[-1]
            if button_col_h.button("View", key=ticker_symbol_h):
                with st.spinner(f"Loading chart for {index_name_h}..."):
                    chart_data, error = get_chart_data(ticker_symbol_h)
                    if error: st.error(error)
                    else:
                        fig = create_ichimoku_chart(chart_data, index_name_h)
                        with st.expander(f"Interactive Chart: {index_name_h}", expanded=True):
                            st.plotly_chart(fig, width='stretch')
            st.divider()

    # --- 5. INDIVIDUAL ANALYSIS & DATA DEBUGGER ---
    st.divider()
    st.header("🔍 Individual Analysis & Data Debugger")
    st.info("This tool uses the *same* `fetch_data` function as the tables above, allowing you to debug the raw data.")

    user_symbol = st.text_input(
        "Enter a valid Ticker Symbol (e.g., 'RELIANCE.NS', 'INFY.NS', '^GSPC')",
        "INFY.NS"
    )

    if user_symbol:
        hist_data = fetch_data(user_symbol) # This is the ADJUSTED data
        info = {}
        error = None

        if hist_data is None:
            error = f"No historical data found for {user_symbol} using fetch_data."
        else:
            try:
                info = yf.Ticker(user_symbol).info
                if not info: info = {}
            except Exception as e:
                st.warning(f"Could not fetch Ticker(.info) data: {e}. Metrics may be limited.")

            hist_data = hist_data.copy()
            hist_data.ta.rsi(length=14, append=True)
            hist_data.ta.ichimoku(append=True)
            hist_data = hist_data.rename(columns={
                'ITS_9': 'Tenkan', 'IKS_26': 'Kijun',
                'ISA_9': 'SenkouA', 'ISB_26': 'SenkouB', 'ICS_26': 'Chikou'
            })

        if error:
            st.error(error)
        elif not hist_data.empty:
            st.success(f"Displaying analysis for: **{info.get('longName', user_symbol)}**")

            col1, col2 = st.columns(2)
            with col1:
                # --- Metrics from ADJUSTED data ---
                current_price_adj = hist_data['Close'].iloc[-1]
                prev_close_adj = hist_data['Close'].iloc[-2]
                change_adj = current_price_adj - prev_close_adj
                percent_change_adj = (change_adj / prev_close_adj) * 100

                st.metric(
                    label="Last Price (Adjusted EOD)",
                    value=f"{current_price_adj:,.2f}",
                    delta=f"{change_adj:,.2f} ({percent_change_adj:.2f}%)"
                )
                
                # --- Get UNADJUSTED Current Price from .info ---
                current_price_unadj = info.get('currentPrice', 'N/A')
                st.metric(label="Current Price (Unadjusted LTP)", value=f"{current_price_unadj:,.2f}")

                # --- 52w H/L from ADJUSTED data ---
                hist_1y_adj = hist_data.last('52w')
                high_52w_adj = hist_1y_adj['High'].max()
                low_52w_adj = hist_1y_adj['Low'].min()
                st.metric(label="52-Week High (Adjusted)", value=f"{high_52w_adj:,.2f}")
                st.metric(label="52-Week Low (Adjusted)", value=f"{low_52w_adj:,.2f}")
                
                # --- Market Cap (from .info) ---
                market_cap_raw = info.get('marketCap', 0)
                if market_cap_raw and market_cap_raw > 0:
                    market_cap_cr = market_cap_raw / 1_00_00_000
                    market_cap_str = f"₹{market_cap_cr:,.2f} Cr"
                else: market_cap_str = "N/A"
                st.metric(label="Market Cap (from .info)", value=market_cap_str)

            with col2:
                # --- Analysis (from ADJUSTED data) ---
                st.subheader("Technical Analysis (from Adjusted data)")
                latest_rsi = hist_data['RSI_14'].iloc[-1]
                st.metric(label="RSI (14-Day)", value=f"{latest_rsi:.2f}")
                if latest_rsi > 70: st.warning("Overbought (RSI > 70)")
                elif latest_rsi < 30: st.success("Oversold (RSI < 30)")
                else: st.info("Neutral (30 < RSI < 70)")
            
            # --- Chart (from ADJUSTED data) ---
            with st.expander("View Full Chart (Adjusted Data)"):
                stock_fig = create_ichimoku_chart(hist_data, info.get('longName', user_symbol))
                st.plotly_chart(stock_fig, width='stretch')

            # --- Data Debugger (ADJUSTED) ---
            with st.expander("View Raw Data (Adjusted - Used for Analysis)"):
                st.dataframe(hist_data)
                st.caption("This is the 2-year, auto-adjusted dataset from fetch_data().")
                
            # --- START: NEW DEBUGGER (UNADJUSTED) ---
            with st.expander("View Raw Data (Unadjusted - For Price Comparison)"):
                with st.spinner("Fetching unadjusted data..."):
                    unadj_debug_data = yf.download(user_symbol, period="2y", auto_adjust=False, progress=False)
                    if unadj_debug_data.empty:
                        st.warning("Could not fetch unadjusted data.")
                    else:
                        st.dataframe(unadj_debug_data)
                        st.caption("This is the 2-year, un-adjusted dataset.")
            # --- END: NEW DEBUGGER ---

# --- TABS 4, 5, 6: GENERIC ANALYSIS TABS ---
# (These tabs use the NEW generic function)
with tab4:
    st.header("Global Indices")
    analysis_results_g = []
    with st.spinner(f'Analyzing Global Indices...'):
        for name, ticker in TICKERS_GLOBAL.items():
            analysis = get_generic_analysis(ticker, name)
            if analysis:
                analysis_results_g.append(analysis)
            else:
                st.warning(f"Could not retrieve or analyze data for {name} ({ticker}).")

    if analysis_results_g:
        df_g = pd.DataFrame(analysis_results_g)
        # --- MODIFICATION: Set Index to Asset and display ---
        df_g = df_g.set_index("Asset")
        st.dataframe(style_generic_dataframe(df_g), width='stretch')

with tab5:
    st.header("Commodities")
    analysis_results_c = []
    with st.spinner(f'Analyzing Commodities...'):
        for name, ticker in TICKERS_COMMODITIES.items():
            analysis = get_generic_analysis(ticker, name)
            if analysis:
                analysis_results_c.append(analysis)
            else:
                st.warning(f"Could not retrieve or analyze data for {name} ({ticker}).")

    if analysis_results_c:
        df_c = pd.DataFrame(analysis_results_c)
        # --- MODIFICATION: Set Index to Asset and display ---
        df_c = df_c.set_index("Asset")
        st.dataframe(style_generic_dataframe(df_c), width='stretch')

with tab6:
    st.header("Cryptocurrency")
    analysis_results_cr = []
    with st.spinner(f'Analyzing Crypto...'):
        for name, ticker in TICKERS_CRYPTO.items():
            analysis = get_generic_analysis(ticker, name)
            if analysis:
                analysis_results_cr.append(analysis)
            else:
                st.warning(f"Could not retrieve or analyze data for {name} ({ticker}).")

    if analysis_results_cr:
        df_cr = pd.DataFrame(analysis_results_cr)
        # --- MODIFICATION: Set Index to Asset and display ---
        df_cr = df_cr.set_index("Asset")
        st.dataframe(style_generic_dataframe(df_cr), width='stretch')