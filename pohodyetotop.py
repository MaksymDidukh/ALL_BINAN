"""
BTC/USDT — FULL BACKTEST + LIVE DEMO (FUTURES USDT-M)
Variant: PRO ASCII CANDLE CHART (style 5) + PRE-EXIT + USER ENTRY
"""

import os, time, json, threading, requests, sqlite3, websocket
from datetime import datetime
import pandas as pd
import numpy as np

# =========================
# CONFIGURATION — VARIABLES
# =========================
MARKET_SYMBOL = "BTCUSDT"
HIST_INTERVAL = "15m"
HIST_START_YEAR = 2025
HIST_START_MONTH = 11
HIST_START_DAY = 1
HIST_END_YEAR = 2025
HIST_END_MONTH = 12
HIST_END_DAY = 1
HIST_LIMIT = 500

CSV_KLINES_FILE = f"{HIST_START_MONTH}_{HIST_START_YEAR}_{HIST_INTERVAL}_{HIST_END_YEAR}_{HIST_END_MONTH}_{MARKET_SYMBOL}_klines.csv"
SQLITE_DB_FILE = f"{HIST_START_MONTH}_{HIST_START_YEAR}_{HIST_INTERVAL}_{HIST_END_YEAR}_{HIST_END_MONTH}_{MARKET_SYMBOL}_klines.db"

INITIAL_CAPITAL = 1000.0
BASE_POSITION_USD = 10.0
DEFAULT_LEVERAGE = 100
FEE_RATE = 0.0004  # 0.04% fee

# PRE-EXIT & USER ENTRY
PRE_EXIT_PCT = 0      # relative position for PRE-EXIT calculation (not strict; used later) 0ю5
USER_DELAY_CANDLES = 0   # сколько свечей после PRE-EXIT до USER ENTRY

# draw settings
CHART_HEIGHT = 9   # rows for ascii candle chart (higher -> more vertical resolution)
CANDLE_WIDTH = 5   # characters per candle column

# =========================
# PATTERNS CONFIG
# =========================
PATTERN_CONFIG = {
    "Bullish Flag": {"tp":1.0,"sl":0.5,"scale":0.5,"candle_pct":0.5},
    "Bearish Flag": {"tp":1.0,"sl":0.5,"scale":0.5,"candle_pct":0.5},
    "Ascending Triangle": {"tp":0.5,"sl":0.25,"scale":0.7,"candle_pct":0.7},
    "Descending Triangle": {"tp":0.5,"sl":0.25,"scale":0.7,"candle_pct":0.7},
    "Symmetrical Triangle": {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6},
    "Rising Wedge": {"tp":0.25,"sl":0.5,"scale":0.4,"candle_pct":0.4},
    "Falling Wedge": {"tp":0.25,"sl":0.5,"scale":0.4,"candle_pct":0.4},
    "FVG Up": {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6},
    "FVG Down": {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6},
    "BOS Up": {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6},
    "BOS Down": {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6},
    "Head & Shoulders": {"tp":0.75,"sl":0.5,"scale":0.8,"candle_pct":0.8},
}

LIVE_DEMO_ENABLE = False
FUTURES_WS_ENDPOINT = "wss://fstream.binance.com/ws"
REQUEST_TIMEOUT = 15
SLEEP_BETWEEN_REQS = 0.25

# =========================
# TIME HELPERS
# =========================
def ts_ms_from_dt(y,m,d,h=0,minute=0,second=0):
    return int(datetime(y,m,d,h,minute,second).timestamp() * 1000)

# =========================
# HISTORICAL DATA LOADER
# =========================
def binance_klines_rest(symbol, interval, start_ts=None, end_ts=None, limit=1000):
    url = "https://fapi.binance.com/fapi/v1/klines"
    all_data = []
    while True:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_ts: params["startTime"] = start_ts
        if end_ts: params["endTime"] = end_ts
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            print("REST API error", resp.status_code, resp.text)
            time.sleep(5)
            continue
        batch = resp.json()
        if not batch: break
        all_data.extend(batch)
        last_ts = batch[-1][0]
        if len(batch) < limit: break
        start_ts = last_ts + 1
        time.sleep(SLEEP_BETWEEN_REQS)
    df = pd.DataFrame(all_data, columns=[
        "open_time","open","high","low","close","volume","close_time",
        "quote_vol","trades","tb_base","tb_quote","ignore"
    ])
    df = df[["open_time","open","high","low","close","volume"]].astype(float)
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)
    return df

def download_history_if_needed():
    if not os.path.exists(CSV_KLINES_FILE) or os.path.getsize(CSV_KLINES_FILE) < 1000:
        print("Downloading history from Binance Futures API...")
        start_ts = ts_ms_from_dt(HIST_START_YEAR,HIST_START_MONTH,HIST_START_DAY)
        end_ts = ts_ms_from_dt(HIST_END_YEAR,HIST_END_MONTH,HIST_END_DAY,23,59)
        df_all, total_candles = [], 0
        while start_ts < end_ts:
            batch = binance_klines_rest(MARKET_SYMBOL,HIST_INTERVAL,start_ts=start_ts,end_ts=end_ts,limit=HIST_LIMIT)
            if batch.empty: break
            df_all.append(batch)
            last_ts = batch.index[-1].value // 10**6
            start_ts = last_ts + 1
            total_candles += len(batch)
            progress = min((start_ts - ts_ms_from_dt(HIST_START_YEAR,HIST_START_MONTH,HIST_START_DAY)) /
                           (end_ts - ts_ms_from_dt(HIST_START_YEAR,HIST_START_MONTH,HIST_START_DAY)),1.0)
            bar_len = 30
            filled_len = int(bar_len * progress)
            bar = "█"*filled_len + "-"*(bar_len-filled_len)
            print(f"\rProgress: |{bar}| {progress*100:.2f}% ({total_candles} candles)", end="")
            time.sleep(SLEEP_BETWEEN_REQS)
        print("\nDownload complete.")
        df = pd.concat(df_all)
        df.to_csv(CSV_KLINES_FILE)
        conn = sqlite3.connect(SQLITE_DB_FILE)
        df.to_sql("klines", conn, if_exists="replace")
        conn.close()
        print("Saved CSV and SQLite.")
    else:
        df = pd.read_csv(CSV_KLINES_FILE, parse_dates=["timestamp"], index_col="timestamp")
        df.index = pd.to_datetime(df.index, utc=True)
        print("Loaded CSV local history.")
    return df

# =========================
# PATTERN DETECTION
# =========================
def local_extrema(df, window=3):
    high_max = df['high'].rolling(window, center=True).max()
    low_min = df['low'].rolling(window, center=True).min()
    highs = df[df['high']==high_max].index.tolist()
    lows = df[df['low']==low_min].index.tolist()
    return highs,lows

def detect_flags(df, lookback=15, threshold=0.005):
    patterns=[]
    for i in range(lookback,len(df)):
        seg=df['close'].iloc[i-lookback:i]
        diff=seg.max()-seg.min()
        if diff/seg.min()<threshold:
            slope=(seg.iloc[-1]-seg.iloc[0])/seg.iloc[0]
            if slope>0: patterns.append((seg.index[0],seg.index[-1],"Bullish Flag"))
            elif slope<0: patterns.append((seg.index[0],seg.index[-1],"Bearish Flag"))
    return patterns

def detect_triangles(df, lookback=20):
    patterns=[]
    for i in range(lookback,len(df)):
        seg=df.iloc[i-lookback:i]
        x=np.arange(len(seg))
        high_slope,_=np.polyfit(x,seg['high'].values,1)
        low_slope,_=np.polyfit(x,seg['low'].values,1)
        if abs(high_slope)<0.01 and low_slope>0: patterns.append((seg.index[0],seg.index[-1],"Ascending Triangle"))
        elif abs(low_slope)<0.01 and high_slope<0: patterns.append((seg.index[0],seg.index[-1],"Descending Triangle"))
        elif high_slope>0 and low_slope<0: patterns.append((seg.index[0],seg.index[-1],"Symmetrical Triangle"))
    return patterns

def detect_fvg(df,gap_perc=0.002):
    patterns=[]
    for i in range(2,len(df)):
        prev_high,prev_low=df['high'].iloc[i-2],df['low'].iloc[i-2]
        curr_open=df['open'].iloc[i]
        if curr_open>prev_high*(1+gap_perc): patterns.append((df.index[i-2],df.index[i],"FVG Up"))
        elif curr_open<prev_low*(1-gap_perc): patterns.append((df.index[i-2],df.index[i],"FVG Down"))
    return patterns

def detect_bos_choch(df, window=3):
    patterns=[]
    highs,lows=local_extrema(df,window)
    for i in range(1,len(highs)):
        if df.loc[highs[i],'high']>df.loc[highs[i-1],'high']: patterns.append((highs[i-1],highs[i],"BOS Up"))
    for i in range(1,len(lows)):
        if df.loc[lows[i],'low']<df.loc[lows[i-1],'low']: patterns.append((lows[i-1],lows[i],"BOS Down"))
    return patterns

def detect_head_shoulders(df,window=5,threshold=0.003):
    patterns=[]
    for i in range(window,len(df)-window):
        left=df['high'].iloc[i-window:i].max()
        head=df['high'].iloc[i]
        right=df['high'].iloc[i+1:i+1+window].max()
        if head>left*(1+threshold) and head>right*(1+threshold): patterns.append((df.index[i-window],df.index[i+window],"Head & Shoulders"))
    return patterns

def detect_all_patterns(df):
    all_p=[]
    all_p+=detect_flags(df)
    all_p+=detect_triangles(df)
    all_p+=detect_fvg(df)
    all_p+=detect_bos_choch(df)
    all_p+=detect_head_shoulders(df)
    patterns_df=pd.DataFrame(all_p,columns=["Start","End","Pattern"])
    patterns_df["Start"]=pd.to_datetime(patterns_df["Start"],utc=True)
    patterns_df["End"]=pd.to_datetime(patterns_df["End"],utc=True)
    return patterns_df

# =========================
# ASCII PRO-CANDLE RENDERER (variant 5)
# =========================
ANSI_RED = "\033[91m"
ANSI_GREEN = "\033[92m"
ANSI_YELLOW = "\033[93m"
ANSI_RESET = "\033[0m"

def render_pro_ascii_candles(price_path, price_index_list, pre_idx=None, user_idx=None):
    """
    price_path: list of dicts [{"open","high","low","close"},...]
    price_index_list: list of timestamps (same length)
    pre_idx, user_idx: indices to mark with 'P' and 'U'
    """
    n = len(price_path)
    H = CHART_HEIGHT
    W = CANDLE_WIDTH
    # collect prices
    highs = [p["high"] for p in price_path]
    lows = [p["low"] for p in price_path]
    max_p = max(highs)
    min_p = min(lows)
    span = max_p - min_p if max_p>min_p else 1.0

    # map price -> row (0..H-1) (0 top)
    def row_of(price):
        rel = (price - min_p) / span
        # invert: top row = H-1, bottom row = 0? We'll put top row 0 visually; so row = int((1-rel)*(H-1))
        return int((1 - rel) * (H - 1))

    # create empty canvas rows: H rows of strings, width = n * W
    canvas = [list(" " * (n * W)) for _ in range(H)]
    # optionally keep a marker row above for labels (S/P/U/E)
    label_row_above = [" " * W for _ in range(n)]

    for i, p in enumerate(price_path):
        col0 = i * W
        col_mid = col0 + W // 2
        o = p["open"]; c = p["close"]; h = p["high"]; l = p["low"]
        row_high = row_of(h)
        row_low = row_of(l)
        row_open = row_of(o)
        row_close = row_of(c)
        # wick vertical line from row_high..row_low at col_mid
        for r in range(min(row_high, row_low), max(row_high, row_low) + 1):
            canvas[r][col_mid] = "|"
        # body from min(open,close) to max(open,close), fill block (width W-2 centered)
        body_top = min(row_open, row_close)
        body_bottom = max(row_open, row_close)
        left = col0 + 1
        right = col0 + W - 2
        # corners for pro look
        if body_top == body_bottom:  # doji - small box
            canvas[body_top][col_mid] = "■"
        else:
            # draw top & bottom horizontal
            for cc in range(left, right+1):
                canvas[body_top][cc] = "─"
                canvas[body_bottom][cc] = "─"
            # fill middle
            for rr in range(body_top+1, body_bottom):
                for cc in range(left, right+1):
                    canvas[rr][cc] = "█"
            # left and right vertical edges
            canvas[body_top][left] = "┌"; canvas[body_top][right] = "┐"
            canvas[body_bottom][left] = "└"; canvas[body_bottom][right] = "┘"

        # color mapping stored separately: store whether candle is green or red
        is_green = c >= o
        # store marker chars for label row
        label = " "
        if i == 0:
            label = "S"
        if i == n-1:
            label = "E"
        if pre_idx is not None and i == pre_idx:
            label = "P"
        if user_idx is not None and i == user_idx:
            label = "U" if label == " " else label + "U"  # keep existing if S/E/P and U coincide
        label_row_above[i] = label.center(W)

        # embed color markers into canvas by annotating an overlay map
        # we'll print color during final rendering based on open/close sign
        # store a small tag per column
        # to simplify, create a small array marking column color
        # (we will color the body characters when printing later)
        # mark by placing a marker at col0 (unused char) with 'g' or 'r'
        canvas[0][col0] = "g" if is_green else "r"

    # build printable lines: we need to replace placeholder color markers and remove them
    out_lines = []
    for r in range(H):
        line_chars = canvas[r]
        # build string and later color segments that are body characters: '█' and corners and '─' etc.
        s = "".join(line_chars)
        out_lines.append(s)

    # Now combine label row above + canvas lines + timestamp line
    # prepare colored print: for each candle set color by checking marker canvas[0][col0]
    # create final rows where we color body/fill chars for each candle column
    final_rows = []
    for r in range(H):
        row_builder = []
        for i in range(n):
            col0 = i * W
            seg = out_lines[r][col0:col0+W]
            # determine candle color by marker at canvas[0][col0]
            marker = canvas[0][col0]
            if marker == "g":
                # color body glyphs (█,┌,┐,└,┘,─,■,|) green; wicks keep neutral
                seg = seg.replace("█", ANSI_GREEN + "█" + ANSI_RESET)
                seg = seg.replace("┌", ANSI_GREEN + "┌" + ANSI_RESET)
                seg = seg.replace("┐", ANSI_GREEN + "┐" + ANSI_RESET)
                seg = seg.replace("└", ANSI_GREEN + "└" + ANSI_RESET)
                seg = seg.replace("┘", ANSI_GREEN + "┘" + ANSI_RESET)
                seg = seg.replace("─", ANSI_GREEN + "─" + ANSI_RESET)
                seg = seg.replace("■", ANSI_GREEN + "■" + ANSI_RESET)
            elif marker == "r":
                seg = seg.replace("█", ANSI_RED + "█" + ANSI_RESET)
                seg = seg.replace("┌", ANSI_RED + "┌" + ANSI_RESET)
                seg = seg.replace("┐", ANSI_RED + "┐" + ANSI_RESET)
                seg = seg.replace("└", ANSI_RED + "└" + ANSI_RESET)
                seg = seg.replace("┘", ANSI_RED + "┘" + ANSI_RESET)
                seg = seg.replace("─", ANSI_RED + "─" + ANSI_RESET)
                seg = seg.replace("■", ANSI_RED + "■" + ANSI_RESET)
            # wicks '|' keep default color
            row_builder.append(seg)
        final_rows.append("".join(row_builder))

    # create label line above
    label_line = "".join(label_row_above)
    # create timestamp line (compact, show short times)
    time_line = []
    for ts in price_index_list:
        # format short: YYYY-MM-DD HH:MM
        time_line.append(str(ts.strftime("%m-%d %H:%M")).center(W))
    time_line_str = "".join(time_line)

    # print: label row, canvas rows, time line
    print("  " + label_line)
    for fr in final_rows:
        print("  " + fr)
    print("  " + time_line_str)
    # done

# =========================
# BACKTEST ENGINE WITH PRO CHART
# =========================
def simulate_trade_for_pattern(df,row,user_delay_candles=USER_DELAY_CANDLES):
    start_ts, end_ts, pattern_name = row["Start"], row["End"], row["Pattern"]
    cfg = PATTERN_CONFIG.get(pattern_name, {})
    tp_pct = cfg.get("tp",0.1)
    sl_pct = cfg.get("sl",0.25)
    scale = cfg.get("scale",0.5)
    candle_pct = cfg.get("candle_pct",0.5)

    # safe access
    try:
        entry_candle = df.loc[start_ts]
        exit_candle = df.loc[end_ts]
    except Exception:
        print(f"DATA ERROR | {start_ts} → {end_ts}")
        return None

    entry_price = float(entry_candle["close"])
    exit_price = float(exit_candle["close"])
    entry_date = entry_candle.name
    exit_date = exit_candle.name

    direction = "LONG"
    if "Bear" in pattern_name or "Descending" in pattern_name or "Down" in pattern_name:
        direction = "SHORT"

    candle_size = (float(entry_candle["high"]) - float(entry_candle["low"])) / float(entry_candle["low"])

    print(f"[READY] {pattern_name} | Direction={direction} | Entry={entry_price:.2f}")
    print(f"  Entry candle size = {candle_size*100:.2f}% | Required candle_pct = {candle_pct}%")
    print(f"  Pattern scale = {scale*100:.2f}% | TP = {tp_pct*100:.2f}% | SL = {sl_pct*100:.2f}%")
    print(f"  Price range: start={entry_price:.2f} → end={exit_price:.2f}")
    print(f"  Entry date: {entry_date} | Exit date: {exit_date}")

    if candle_size < candle_pct/100:
        print(f"  Skipped: candle size too small ({candle_size*100:.2f}% < {candle_pct}%)\n")
        return None

    # prepare price_path and indices
    slice_df = df.loc[start_ts:end_ts][["open","high","low","close"]]
    if slice_df.empty:
        return None
    price_path = slice_df.to_dict("records")
    price_index_list = slice_df.index.tolist()

    # choose pre_exit index (heuristic): near end according to PRE_EXIT_PCT of path length
    pre_exit_idx = max(0, int((len(price_path)-1) * PRE_EXIT_PCT))
    user_entry_idx = min(len(price_path)-1, pre_exit_idx + user_delay_candles)
    pre_exit_price = price_path[pre_exit_idx]["close"]
    user_entry_price = price_path[user_entry_idx]["close"]

    # compute pnl from user entry
    price_move = (exit_price - user_entry_price) / user_entry_price if direction == "LONG" else (user_entry_price - exit_price) / user_entry_price
    pnl = price_move * BASE_POSITION_USD * DEFAULT_LEVERAGE
    fee = BASE_POSITION_USD * DEFAULT_LEVERAGE * FEE_RATE * 2
    pnl_net = pnl - fee
    roi_trade = (pnl_net / BASE_POSITION_USD) * 100

    # render pro ascii candles with marks
    print("  Price path mini-chart (PRO ASCII CANDLES):")
    render_pro_ascii_candles(price_path, price_index_list, pre_idx=pre_exit_idx, user_idx=user_entry_idx)

    # print summary with dates
    print(f"  PRE-EXIT candle: {price_index_list[pre_exit_idx]} | PRE-EXIT price: {pre_exit_price:.2f}")
    print(f"  USER ENTRY (simulated): {price_index_list[user_entry_idx]} | ENTRY price: {user_entry_price:.2f}")
    print(f"  EXIT date: {exit_date} | EXIT price: {exit_price:.2f}")
    print(f"  TRADE | User Entry={user_entry_price:.2f} → Exit={exit_price:.2f} | PNL={pnl_net:+.2f} USD | ROI={roi_trade:+.2f}%\n")

    return {
        "pattern": pattern_name,
        "entry_price": float(user_entry_price),
        "exit_price": float(exit_price),
        "entry_date": price_index_list[user_entry_idx],
        "exit_date": exit_date,
        "pnl_net": float(pnl_net),
        "roi": float(roi_trade),
        "candles": len(price_path),
        "candle_size_pct": candle_size*100
    }

def backtest_patterns(df,patterns_df):
    patterns_df = patterns_df.sort_values("Start").reset_index(drop=True)
    stats = []
    trades_details = []
    for pattern_name, group in patterns_df.groupby("Pattern"):
        group = group.reset_index(drop=True)
        capital = INITIAL_CAPITAL; equity_curve = []; wins = 0; losses = 0
        for _, row in group.iterrows():
            trade_info = simulate_trade_for_pattern(df, row)
            if trade_info is None: continue
            capital += trade_info["pnl_net"]; equity_curve.append(capital)
            if trade_info["pnl_net"] > 0: wins += 1
            else: losses += 1
            trades_details.append(trade_info)
        total = wins + losses
        winrate = wins/total*100 if total>0 else 0.0
        roi = (capital - INITIAL_CAPITAL)/INITIAL_CAPITAL*100
        max_drawdown = 0.0
        if equity_curve:
            peak = equity_curve[0]; max_dd = 0.0
            for val in equity_curve:
                if val > peak: peak = val
                dd = peak - val
                if dd > max_dd: max_dd = dd
            max_drawdown = max_dd
        stats.append({
            "Pattern": pattern_name, "Signals": len(group), "Trades": total, "Wins": wins, "Losses": losses,
            "Winrate%": round(winrate,2), "ROI%": round(roi,3), "MaxDrawdown": round(max_drawdown,3), "CapitalEnd": round(capital,3)
        })
    stats_df = pd.DataFrame(stats).sort_values("ROI%", ascending=False)
    trades_df = pd.DataFrame(trades_details)
    return stats_df, trades_df

# =========================
# MAIN
# =========================
def main():
    df = download_history_if_needed()
    print(f"History timeframe: {df.index.min()} → {df.index.max()} | candles: {len(df)}")
    patterns_df = detect_all_patterns(df)
    print(f"Detected patterns: {len(patterns_df)}")
    stats_df, trades_df = backtest_patterns(df, patterns_df)
    print("\n=== SUMMARY STATISTICS ===")
    print(stats_df.to_string(index=False))
    print("\n=== TRADES DETAILS (first 50) ===")
    if not trades_df.empty:
        print(trades_df.head(50).to_string(index=False))
        trades_df.to_csv("trades_pro_chart.csv", index=False)
        stats_df.to_csv("stats_pro_chart.csv", index=False)
        print("\nSaved trades_pro_chart.csv and stats_pro_chart.csv")
    else:
        print("No trades executed.")

if __name__ == "__main__":
    main()

# =========================
# USAGE NOTES (quick)
# =========================
# - Настройки вверху: HIST_INTERVAL, HIST_START_*, PRE_EXIT_PCT, USER_DELAY_CANDLES.
# - PRE-EXIT отмечается буквой 'P', USER ENTRY — 'U'. 'S' — start, 'E' — end.
# - CHART_HEIGHT и CANDLE_WIDTH контролируют разрешение ascii-графика.
# - Запуск: python app.py — скрипт скачает свечи (если нет CSV) и выполнит backtest с печатью мини-графиков.
