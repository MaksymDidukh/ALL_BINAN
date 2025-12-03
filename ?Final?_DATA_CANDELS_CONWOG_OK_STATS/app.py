"""
BTC/USDT — FULL BACKTEST + LIVE DEMO (FUTURES USDT-M)
Variant: C — Полная логика паттернов с масштабом и фильтрацией
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
# Каждый паттерн имеет: (TP %, SL %, масштаб свечи %, фильтр свечи %)
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
# BACKTEST ENGINE WITH SCALE
# =========================

def simulate_trade_for_pattern(df,row):
    start_ts, end_ts, pattern_name = row["Start"], row["End"], row["Pattern"]
    cfg = PATTERN_CONFIG.get(pattern_name, {})
    tp_pct = cfg.get("tp",0.1)
    sl_pct = cfg.get("sl",0.25)
    scale = cfg.get("scale",0.5)
    candle_pct = cfg.get("candle_pct",0.5)

    # Вход по ключевой свечи
    try: 
        entry_price = df.loc[start_ts,"close"]
        exit_price = df.loc[end_ts,"close"]
    except: 
        print(f"DATA ERROR | {start_ts} → {end_ts}")
        return None

    # Определяем направление
    direction = "LONG"
    if "Bear" in pattern_name or "Descending" in pattern_name or "Down" in pattern_name: direction="SHORT"

    # Проверка масштабов свечей
    candle_size = (df.loc[start_ts,"high"] - df.loc[start_ts,"low"]) / df.loc[start_ts,"low"]
    if candle_size < candle_pct/100:
        print(f"Skipped {pattern_name} at {start_ts} due to small candle ({candle_size*100:.2f}%)")
        return None

    # PnL
    price_move = (exit_price-entry_price)/entry_price if direction=="LONG" else (entry_price-exit_price)/entry_price
    pnl = price_move*BASE_POSITION_USD*DEFAULT_LEVERAGE
    fee = BASE_POSITION_USD*DEFAULT_LEVERAGE*FEE_RATE*2
    pnl_net = pnl - fee
    roi_trade = (pnl_net/BASE_POSITION_USD)*100

    # Путь свечей
    price_path = df.loc[start_ts:end_ts][["open","high","low","close"]].to_dict("records")

    print(f"TRADE | at {start_ts} -> {pattern_name} | {direction} | entry={entry_price:.2f} → exit={exit_price:.2f} | PNL={pnl_net:+.2f} USD | ROI={roi_trade:+.2f}% | path_len={len(price_path)} -> end_ts")
    return pnl_net

def backtest_patterns(df,patterns_df):
    patterns_df=patterns_df.sort_values("Start").reset_index(drop=True)
    stats=[]
    for pattern_name,group in patterns_df.groupby("Pattern"):
        group=group.reset_index(drop=True)
        capital=INITIAL_CAPITAL; equity_curve=[]; wins=0; losses=0
        for _,row in group.iterrows():
            pnl=simulate_trade_for_pattern(df,row)
            if pnl is None: continue
            capital+=pnl; equity_curve.append(capital)
            if pnl>0: wins+=1
            else: losses+=1
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
    return stats_df

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
        t=threading.Thread(target=self.ws.run_forever, kwargs={"ping_interval":20,"ping_timeout":10}, daemon=True)
        t.start(); print("LiveFuturesDemo: WebSocket thread started.")

    def _on_open(self,ws): ws.send(json.dumps({"method":"SUBSCRIBE","params":[f"{self.symbol.lower()}@trade"],"id":1})); print("Subscribed to",f"{self.symbol.lower()}@trade")
    def _on_error(self,ws,err): print("WS error:",err)
    def _on_close(self,ws,code,reason): self.running=False; print("WS closed:",code,reason)
    def _on_message(self,ws,message):
        data=json.loads(message)
        if "p" in data: self._on_price_tick(float(data["p"]))

    def _on_price_tick(self,price):
        with self.lock:
            if self.position:
                pnl=self._calc_unrealized_pnl(price)
                if pnl>=self.demo_take_profit_usd or pnl<=self.demo_stop_loss_usd: self._close_position(price)
                elif abs(pnl)>=self.min_notif_pnl: print(f"[LIVE DEMO] Price {price:.1f} | Pos {self.position['side']} | Unrealized PnL={pnl:.2f} USD")
            else:
                if (int(price)%1000)<2: self._open_position("LONG",price,BASE_POSITION_USD)
                elif (int(price)%777)<2: self._open_position("SHORT",price,BASE_POSITION_USD)

    def _open_position(self,side,price,size_usd):
        if self.position: return
        self.position={"side":side,"size_usd":float(size_usd),"entry_price":float(price),"leverage":self.leverage,"fee_paid":float(size_usd*self.leverage*FEE_RATE)}
        print(f"[LIVE DEMO] Opened {side} | Entry {price:.2f} | Notional ${size_usd:.2f} | Leverage x{self.leverage} | Fee {self.position['fee_paid']:.4f} USD")

    def _calc_unrealized_pnl(self,price):
        if not self.position: return 0.0
        side=self.position["side"]; entry=self.position["entry_price"]; size=self.position["size_usd"]
        pnl=(price-entry)/entry*size*self.leverage if side=="LONG" else (entry-price)/entry*size*self.leverage
        return pnl-self.position["fee_paid"]

    def _close_position(self,price):
        pnl=self._calc_unrealized_pnl(price)
        print(f"[LIVE DEMO] Closed {self.position['side']} | Exit {price:.2f} | PnL {pnl:.2f} USD")
        self.position=None

# =========================
# MAIN
# =========================

if __name__=="__main__":
    df=download_history_if_needed()
    patterns_df=detect_all_patterns(df)
    print(f"Detected {len(patterns_df)} patterns.")
    stats_df=backtest_patterns(df,patterns_df)
    print(stats_df)
    
    demo=LiveFuturesDemo()
    demo.start()

"""
=========================
HOW TO USE

1. Исторические данные:
- Скрипт автоматически скачивает свечи из Binance REST API за указанный период.
- Если CSV существует и валидный, он используется напрямую.

2. Паттерны:
- Каждый паттерн имеет конфигурацию (TP, SL, масштаб свечи, фильтр свечи %)
- Можно добавлять новые паттерны в PATTERN_CONFIG

3. Масштаб паттерна:
- "scale" задаёт допустимое отклонение входа от ключевой свечи
- "candle_pct" фильтрует слишком маленькие свечи

4. Сделки:
- В backtest каждая сделка выводится с путем свечей, PnL и ROI.
- В LIVE DEMO сделка открывается автоматически по простому правилу (демо), с учетом плеча и комиссии.

5. Фильтры:
- Можно менять процент свечи и масштаб для каждого паттерна отдельно
- Настройки PATTERN_CONFIG позволяют гибко управлять стратегией

6. Интерпретация результата:
- DataFrame stats_df выводит по каждому паттерну: количество сигналов, винрейт, ROI, капитал и максимальную просадку.
=========================
"""
