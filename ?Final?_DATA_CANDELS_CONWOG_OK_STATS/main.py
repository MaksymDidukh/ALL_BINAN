"""
BTC/USDT — FULL BACKTEST + LIVE DEMO (FUTURES USDT-M)
Variant: C — Полная логика паттернов с масштабом и фильтрацией + даты свечей
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
DEFAULT_LEVERAGE = 50
FEE_RATE = 0.0004  # 0.04% fee

# =========================
# Паттерны и их конфигурации
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
# BACKTEST ENGINE WITH SCALE + DATES
# =========================

def simulate_trade_for_pattern(df,row):
    start_ts, end_ts, pattern_name = row["Start"], row["End"], row["Pattern"]
    cfg = PATTERN_CONFIG.get(pattern_name, {})
    tp_pct = cfg.get("tp",0.1)
    sl_pct = cfg.get("sl",0.25)
    scale = cfg.get("scale",0.5)
    candle_pct = cfg.get("candle_pct",0.5)

    try: 
        entry_candle = df.loc[start_ts]
        exit_candle = df.loc[end_ts]
        entry_price = entry_candle["close"]
        exit_price = exit_candle["close"]
        entry_date = entry_candle.name
        exit_date = exit_candle.name
    except: 
        print(f"DATA ERROR | {start_ts} → {end_ts}")
        return None

    direction = "LONG"
    if "Bear" in pattern_name or "Descending" in pattern_name or "Down" in pattern_name: direction="SHORT"

    candle_size = (entry_candle["high"] - entry_candle["low"]) / entry_candle["low"]

    print(f"[READY] {pattern_name} | Direction={direction}")
    print(f"  Entry candle size = {candle_size*100:.2f}% | Required candle_pct = {candle_pct}%")
    print(f"  Pattern scale = {scale*100:.2f}% | TP = {tp_pct*100:.2f}% | SL = {sl_pct*100:.2f}%")
    print(f"  Price range: start={entry_price:.2f} → end={exit_price:.2f}")
    print(f"  Entry date: {entry_date} | Exit date: {exit_date}")

    if candle_size < candle_pct/100:
        print(f"  Skipped: candle size too small ({candle_size*100:.2f}% < {candle_pct}%)\n")
        return None

    price_move = (exit_price-entry_price)/entry_price if direction=="LONG" else (entry_price-exit_price)/entry_price
    if abs(price_move) < scale/100:
        print(f"  Skipped: price move too small ({price_move*100:.2f}% < scale {scale}%)\n")
        return None

    pnl = price_move*BASE_POSITION_USD*DEFAULT_LEVERAGE
    fee = BASE_POSITION_USD*DEFAULT_LEVERAGE*FEE_RATE*2
    pnl_net = pnl - fee
    roi_trade = (pnl_net/BASE_POSITION_USD)*100

    price_path = df.loc[start_ts:end_ts][["open","high","low","close"]].to_dict("records")

    closes = [p["close"] for p in price_path]
    min_price = min(closes)
    max_price = max(closes)
    scale_len = 30

    def price_to_bar(val):
        if max_price == min_price: return scale_len // 2
        idx = int((val - min_price) / (max_price - min_price) * scale_len)
        return min(idx, scale_len-1)

    bars=[]
    for i, p in enumerate(price_path):
        idx = price_to_bar(p["close"])
        line = [" "]*scale_len
        if i==0: line[idx]="S"
        elif i==len(price_path)-1: line[idx]="E"
        else:
            prev_close = price_path[i-1]["close"]
            if p["close"]>prev_close: line[idx]="\033[92m█\033[0m"
            elif p["close"]<prev_close: line[idx]="\033[91m█\033[0m"
            else: line[idx]="\033[93m█\033[0m"
        bars.append("".join(line))

    print("  Price path mini-chart:")
    for b in bars: print("   "+b)

    print(f"  TRADE EXECUTED | entry={entry_price:.2f} ({entry_date}) → exit={exit_price:.2f} ({exit_date}) | "
          f"PNL={pnl_net:+.2f} USD | ROI={roi_trade:+.2f}% | "
          f"candles={len(price_path)} | candle_size={candle_size*100:.2f}%\n")
    
    return {
        "pattern": pattern_name,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "entry_date": entry_date,
        "exit_date": exit_date,
        "pnl_net": pnl_net,
        "roi": roi_trade,
        "candles": len(price_path),
        "candle_size_pct": candle_size*100
    }

def backtest_patterns(df,patterns_df):
    patterns_df=patterns_df.sort_values("Start").reset_index(drop=True)
    stats=[]
    trades_details=[]
    for pattern_name,group in patterns_df.groupby("Pattern"):
        group=group.reset_index(drop=True)
        capital=INITIAL_CAPITAL; equity_curve=[]; wins=0; losses=0
        for _,row in group.iterrows():
            trade_info=simulate_trade_for_pattern(df,row)
            if trade_info is None: continue
            capital+=trade_info["pnl_net"]; equity_curve.append(capital)
            if trade_info["pnl_net"]>0: wins+=1
            else: losses+=1
            trades_details.append(trade_info)
        total=wins+losses
        winrate=wins/total*100 if total>0 else 0.0
        roi=(capital-INITIAL_CAPITAL)/INITIAL_CAPITAL*100
        max_drawdown=0.0
        if equity_curve:
            peak=equity_curve[0]; max_dd=0.0
            for val in equity_curve:
                if val>peak: peak=val
                dd=peak-val
                if dd>max_dd: max_dd=dd
            max_drawdown=max_dd
        stats.append({
            "Pattern":pattern_name,"Signals":len(group),"Trades":total,"Wins":wins,"Losses":losses,
            "Winrate%":round(winrate,2),"ROI%":round(roi,3),"MaxDrawdown":round(max_drawdown,3),"CapitalEnd":round(capital,3)
        })
    stats_df=pd.DataFrame(stats).sort_values("ROI%",ascending=False)
    trades_df=pd.DataFrame(trades_details)
    return stats_df, trades_df

# =========================
# LIVE DEMO
# =========================

class LiveFuturesDemo:
    def __init__(self,symbol=MARKET_SYMBOL,leverage=DEFAULT_LEVERAGE,initial_balance=INITIAL_CAPITAL):
        self.symbol=symbol; self.leverage=leverage; self.balance=float(initial_balance)
        self.position=None; self.ws=None; self.running=False; self.lock=threading.Lock()
        self.demo_take_profit_usd=50.0; self.demo_stop_loss_usd=-25.0; self.min_notif_pnl=5.0

    def start(self):
        if not LIVE_DEMO_ENABLE: return
        self.running=True
        self.ws=websocket.WebSocketApp(FUTURES_WS_ENDPOINT,on_open=self._on_open,on_message=self._on_message,on_error=self._on_error,on_close=self._on_close)
        t=threading.Thread(target=self.ws.run_forever)
        t.daemon=True; t.start()

    def _on_open(self,ws): print("[LIVE DEMO] WS connected")
    def _on_error(self,ws,err): print("[LIVE DEMO] WS error:",err)
    def _on_close(self,ws): print("[LIVE DEMO] WS closed")
    def _on_message(self,ws,msg): pass

# =========================
# MAIN EXECUTION
# =========================

def main():
    df = download_history_if_needed()
    patterns_df = detect_all_patterns(df)
    print(f"Detected patterns: {len(patterns_df)}")
    stats_df, trades_df = backtest_patterns(df, patterns_df)
    print("\n=== SUMMARY STATISTICS ===")
    print(stats_df.to_string(index=False))
    print("\n=== TRADES DETAILS ===")
    print(trades_df.to_string(index=False))
    trades_df.to_csv("trades_details.csv", index=False)
    print("\nSaved detailed trades to 'trades_details.csv'")

if __name__=="__main__":
    main()
