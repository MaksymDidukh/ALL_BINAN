"""
BTC/USDT — FULL BACKTEST + LIVE DEMO (FUTURES USDT-M)
Автор: Богатый Илон Маск
"""

import os
import time
import json
import threading
import requests
import sqlite3
import websocket
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Optional

# =========================
# CONFIG
# =========================
MARKET_SYMBOL = "BTCUSDT"
HIST_INTERVAL = "5m"
HIST_START_YEAR = 2025
HIST_START_MONTH = 10
HIST_END_YEAR = 2025
HIST_END_MONTH = 11
HIST_LIMIT = 1000
CSV_KLINES_FILE = "binance_klines_5m_2024.csv"
SQLITE_DB_FILE = "binance_klines_5m_2024.db"
INITIAL_CAPITAL = 1000.0
BASE_POSITION_USD = 10.0
DEFAULT_LEVERAGE = 50
FEE_RATE = 0.0004
DEFAULT_TP_PCT = 0.5
DEFAULT_SL_PCT = 0.25
PATTERN_TP_SL = {
    "Bullish Flag": (1.0, 0.5),
    "Bearish Flag": (1.0, 0.5),
    "Ascending Triangle": (0.5, 0.25),
    "Descending Triangle": (0.5, 0.25),
    "Symmetrical Triangle": (0.5, 0.25),
    "Rising Wedge": (0.25,0.5),
    "Falling Wedge": (0.25,0.5),
    "FVG Up": (0.5,0.25),
    "FVG Down": (0.5,0.25),
    "BOS Up": (0.5,0.25),
    "BOS Down": (0.5,0.25),
    "Head & Shoulders": (0.75,0.5),
}
LIVE_DEMO_ENABLE = True
FUTURES_WS_ENDPOINT = "wss://fstream.binance.com/ws"
REQUEST_TIMEOUT = 15
SLEEP_BETWEEN_REQS = 0.25
POLL_EVERY_SECONDS = 60
TRADE_LOG_CSV = "trade_log.csv"
PATTERNS_CSV = "patterns_detected.csv"
BACKTEST_STATS_CSV = "backtest_stats_per_pattern.csv"

# =========================
# HELPERS
# =========================
def now_str(): return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
def ts_ms_from_dt(y,m,d,h=0,minute=0,second=0):
    return int(datetime(y,m,d,h,minute,second).timestamp()*1000)

# =========================
# HISTORICAL DATA
# =========================
def binance_klines_rest(symbol, interval, start_ts=None, end_ts=None, limit=1000):
    url = "https://fapi.binance.com/fapi/v1/klines"
    all_data = []
    while True:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_ts: params["startTime"]=start_ts
        if end_ts: params["endTime"]=end_ts
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            time.sleep(5); continue
        batch = resp.json()
        if not batch: break
        all_data.extend(batch)
        last_ts = batch[-1][0]
        if len(batch)<limit: break
        start_ts = last_ts+1
        time.sleep(SLEEP_BETWEEN_REQS)
    df=pd.DataFrame(all_data,columns=["open_time","open","high","low","close","volume","close_time","quote_vol","trades","tb_base","tb_quote","ignore"])
    df=df[["open_time","open","high","low","close","volume"]]
    df[["open","high","low","close","volume"]]=df[["open","high","low","close","volume"]].astype(float)
    df["timestamp"]=pd.to_datetime(df["open_time"],unit="ms",utc=True)
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)
    return df

def download_history_if_needed():
    if not os.path.exists(CSV_KLINES_FILE) or os.path.getsize(CSV_KLINES_FILE)<1000:
        start_ts = ts_ms_from_dt(HIST_START_YEAR,HIST_START_MONTH,1)
        end_ts = ts_ms_from_dt(HIST_END_YEAR,HIST_END_MONTH,28,23,59)
        df = binance_klines_rest(MARKET_SYMBOL,HIST_INTERVAL,start_ts,end_ts,HIST_LIMIT)
        df.to_csv(CSV_KLINES_FILE)
        conn=sqlite3.connect(SQLITE_DB_FILE)
        df.to_sql("klines",conn,if_exists="replace")
        conn.close()
    else:
        df=pd.read_csv(CSV_KLINES_FILE,parse_dates=["timestamp"],index_col="timestamp")
        df.index=pd.to_datetime(df.index,utc=True)
    return df

# =========================
# PATTERNS
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
        try:
            high_slope,_=np.polyfit(x,seg['high'].values,1)
            low_slope,_=np.polyfit(x,seg['low'].values,1)
        except: continue
        if abs(high_slope)<0.01 and low_slope>0: patterns.append((seg.index[0],seg.index[-1],"Ascending Triangle"))
        elif abs(low_slope)<0.01 and high_slope<0: patterns.append((seg.index[0],seg.index[-1],"Descending Triangle"))
        elif high_slope>0 and low_slope<0: patterns.append((seg.index[0],seg.index[-1],"Symmetrical Triangle"))
    return patterns

def detect_fvg(df, gap_perc=0.002):
    patterns=[]
    for i in range(2,len(df)):
        prev_high=df['high'].iloc[i-2]; prev_low=df['low'].iloc[i-2]; curr_open=df['open'].iloc[i]
        if curr_open>prev_high*(1+gap_perc): patterns.append((df.index[i-2],df.index[i],"FVG Up"))
        elif curr_open<prev_low*(1-gap_perc): patterns.append((df.index[i-2],df.index[i],"FVG Down"))
    return patterns

def detect_bos_choch(df, window=3):
    patterns=[]; highs,lows=local_extrema(df,window)
    for i in range(1,len(highs)):
        if df.loc[highs[i],'high']>df.loc[highs[i-1],'high']: patterns.append((highs[i-1],highs[i],"BOS Up"))
    for i in range(1,len(lows)):
        if df.loc[lows[i],'low']<df.loc[lows[i-1],'low']: patterns.append((lows[i-1],lows[i],"BOS Down"))
    return patterns

def detect_head_shoulders(df, window=5, threshold=0.003):
    patterns=[]
    for i in range(window,len(df)-window):
        left=df['high'].iloc[i-window:i].max(); head=df['high'].iloc[i]; right=df['high'].iloc[i+1:i+1+window].max()
        if head>left*(1+threshold) and head>right*(1+threshold): patterns.append((df.index[i-window],df.index[i+window],"Head & Shoulders"))
    return patterns

def detect_all_patterns(df):
    all_p=[]
    all_p+=detect_flags(df)
    all_p+=detect_triangles(df)
    all_p+=detect_fvg(df)
    all_p+=detect_bos_choch(df)
    all_p+=detect_head_shoulders(df)
    df_p=pd.DataFrame(all_p,columns=["Start","End","Pattern"])
    if df_p.empty: return df_p
    df_p["Start"]=pd.to_datetime(df_p["Start"],utc=True)
    df_p["End"]=pd.to_datetime(df_p["End"],utc=True)
    df_p.sort_values("Start",inplace=True)
    df_p.reset_index(drop=True,inplace=True)
    return df_p

# =========================
# LOG
# =========================
def log_open_trade(side,entry,tp,sl,pattern,timestamp):
    print(f"[OPEN] t={timestamp} | {pattern} | {side} | entry={entry:.2f} | tp={tp:.2f} | sl={sl:.2f}")

def log_close_trade(side,entry,exit,result,pattern,timestamp,pnl=None,balance=None):
    pnl_str=f" | pnl={pnl:.2f}" if pnl is not None else ""
    bal_str=f" | balance={balance:.2f}" if balance is not None else ""
    print(f"[CLOSE] t={timestamp} | {pattern} | {side} | entry={entry:.2f} | exit={exit:.2f} | result={result}{pnl_str}{bal_str}")

# =========================
# BACKTEST ENGINE
# =========================
def simulate_trade(df,start_ts,end_ts,direction_hint=None,tp_pct=None,sl_pct=None,position_usd=BASE_POSITION_USD,leverage=DEFAULT_LEVERAGE):
    try: entry_price=df.loc[end_ts,'close']
    except: return None
    if tp_pct is None: tp_pct=DEFAULT_TP_PCT
    if sl_pct is None: sl_pct=DEFAULT_SL_PCT
    direction='LONG' if direction_hint is None else direction_hint
    if direction=='LONG': tp_price=entry_price*(1+tp_pct); sl_price=entry_price*(1-sl_pct)
    else: tp_price=entry_price*(1-tp_pct); sl_price=entry_price*(1+sl_pct)
    idxs=df.index; pos=idxs.get_loc(end_ts); pnl=None; exit_price=None; exit_reason="end"
    for j in range(pos+1,len(df)):
        high=df['high'].iloc[j]; low=df['low'].iloc[j]
        if direction=='LONG':
            if high>=tp_price: exit_price,tp_price=p,tp_price; pnl=(exit_price-entry_price)/entry_price*position_usd*leverage; exit_reason="TP"; break
            if low<=sl_price: exit_price,sl_price=sl_price,sl_price; pnl=(exit_price-entry_price)/entry_price*position_usd*leverage; exit_reason="SL"; break
        else:
            if low<=tp_price: exit_price,tp_price=tp_price,tp_price; pnl=(entry_price-exit_price)/entry_price*position_usd*leverage; exit_reason="TP"; break
            if high>=sl_price: exit_price,sl_price=sl_price,sl_price; pnl=(entry_price-exit_price)/entry_price*position_usd*leverage; exit_reason="SL"; break
    if pnl is None: exit_price=df['close'].iloc[-1]; pnl=(exit_price-entry_price)/entry_price*position_usd*leverage if direction=='LONG' else (entry_price-exit_price)/entry_price*position_usd*leverage; exit_reason="END"
    fee=position_usd*leverage*FEE_RATE*2; pnl_net=pnl-fee
    return {"entry":entry_price,"exit":exit_price,"pnl":pnl_net,"gross_pnl":pnl,"fee":fee,"reason":exit_reason,"tp_price":tp_price,"sl_price":sl_price,"direction":direction}

def backtest_patterns(df,patterns_df):
    stats=[]
    for pattern_name,group in patterns_df.groupby("Pattern"):
        group=group.reset_index(drop=True)
        capital=INITIAL_CAPITAL; equity_curve=[]; wins=0; losses=0; trades=[]
        tp_pct,sl_pct=PATTERN_TP_SL.get(pattern_name,(DEFAULT_TP_PCT,DEFAULT_SL_PCT))
        direction_hint=None
        if "Bull" in pattern_name or "Ascending" in pattern_name or "Up" in pattern_name: direction_hint="LONG"
        elif "Bear" in pattern_name or "Descending" in pattern_name or "Down" in pattern_name: direction_hint="SHORT"
        for _,row in group.iterrows():
            res=simulate_trade(df,row["Start"],row["End"],direction_hint,tp_pct,sl_pct)
            if res is None: continue
            capital+=res["pnl"]; equity_curve.append(capital); trades.append(res["pnl"])
            if res["pnl"]>0: wins+=1
            else: losses+=1
        total=wins+losses; winrate=wins/total*100 if total>0 else 0.0; roi=(capital-INITIAL_CAPITAL)/INITIAL_CAPITAL*100
        max_drawdown=0.0
        if equity_curve:
            peak=equity_curve[0]; max_dd=0.0
            for val in equity_curve:
                if val>peak: peak=val
                dd=peak-val
                if dd>max_dd: max_dd=dd
            max_drawdown=max_dd
        stats.append({"Pattern":pattern_name,"Signals":len(group),"Trades":total,"Wins":wins,"Losses":losses,"Winrate%":round(winrate,2),"ROI%":round(roi,3),"MaxDrawdown":round(max_drawdown,3),"CapitalEnd":round(capital,3)})
    return pd.DataFrame(stats).sort_values("ROI%",ascending=False) if stats else pd.DataFrame()

# =========================
# LIVE DEMO
# =========================
class LiveFuturesDemo:
    def __init__(self,symbol=MARKET_SYMBOL,leverage=DEFAULT_LEVERAGE,initial_balance=INITIAL_CAPITAL):
        self.symbol=symbol; self.leverage=leverage; self.balance=float(initial_balance)
        self.position=None; self.ws=None; self.lock=threading.Lock(); self.min_notif_pnl=5.0
    def start_ws(self):
        if not LIVE_DEMO_ENABLE: return
        self.ws=websocket.WebSocketApp(FUTURES_WS_ENDPOINT,on_open=self._on_open,on_message=self._on_message,on_error=self._on_error,on_close=self._on_close)
        t=threading.Thread(target=self.ws.run_forever, kwargs={"ping_interval":20,"ping_timeout":10}, daemon=True)
        t.start()
    def _on_open(self,ws): ws.send(json.dumps({"method":"SUBSCRIBE","params":[f"{self.symbol.lower()}@trade"],"id":1}))
    def _on_error(self,ws,err): print(now_str(),"WS error",err)
    def _on_close(self,ws,code,reason): self.running=False
    def _on_message(self,ws,message):
        try: data=json.loads(message); price=float(data["p"]); self._on_price_tick(price)
        except: pass
    def _on_price_tick(self,price):
        with self.lock:
            if self.position:
                pnl=self._calc_unrealized_pnl(price)
                side=self.position["side"]
                if side=="LONG":
                    if price>=self.position["tp_price"]: self._close_position(price,"TP"); return
                    if price<=self.position["sl_price"]: self._close_position(price,"SL"); return
                else:
                    if price<=self.position["tp_price"]: self._close_position(price,"TP"); return
                    if price>=self.position["sl_price"]: self._close_position(price,"SL"); return
                if abs(pnl)>=self.min_notif_pnl: print(now_str(),f"[LIVE DEMO] Price {price} | Pos {self.position['side']} | Unrealized PnL={pnl:.2f} USD")
    def _open_position(self,side,price,size_usd,tp_price,sl_price,pattern_name):
        if self.position: return False
        self.position={"side":side,"size_usd":size_usd,"entry_price":price,"leverage":self.leverage,"fee_paid":size_usd*self.leverage*FEE_RATE,"tp_price":tp_price,"sl_price":sl_price,"pattern":pattern_name}
        log_open_trade(side,price,tp_price,sl_price,pattern_name,now_str())
        return True
    def _calc_unrealized_pnl(self,price):
        if not self.position: return 0.0
        side=self.position["side"]; entry=self.position["entry_price"]; size=self.position["size_usd"]; lev=self.position["leverage"]
        dp=(price-entry)/entry if side=="LONG" else (entry-price)/entry
        return dp*size*lev-self.position.get("fee_paid",0.0)
    def _close_position(self,price,reason="TP"):
        if not self.position: return
        pnl=self._calc_unrealized_pnl(price); exit_fee=self.position["size_usd"]*self.position["leverage"]*FEE_RATE; net_pnl=pnl-exit_fee
        self.balance+=net_pnl; entry=self.position["entry_price"]; side=self.position["side"]; pattern=self.position.get("pattern","unknown")
        log_close_trade(side,entry,price,reason,pattern,now_str(),pnl, self.balance)
        append_trade_to_csv({"timestamp":now_str(),"pattern":pattern,"side":side,"entry":entry,"exit":price,"pnl":net_pnl})
        self.position=None

def append_trade_to_csv(trade_dict):
    df=pd.DataFrame([trade_dict])
    if os.path.exists(TRADE_LOG_CSV): df.to_csv(TRADE_LOG_CSV,mode='a',header=False,index=False)
    else: df.to_csv(TRADE_LOG_CSV,index=False)

# =========================
# POLLER FOR NEW PATTERNS
# =========================
def poll_and_trade(live_demo,df):
    while True:
        patterns=detect_all_patterns(df)
        for _,row in patterns.iterrows():
            start,end,pattern=row["Start"],row["End"],row["Pattern"]
            price=df.loc[end,"close"]
            tp_pct,sl_pct=PATTERN_TP_SL.get(pattern,(DEFAULT_TP_PCT,DEFAULT_SL_PCT))
            side="LONG" if "Bull" in pattern or "Up" in pattern else "SHORT"
            tp_price=price*(1+tp_pct) if side=="LONG" else price*(1-tp_pct)
            sl_price=price*(1-sl_pct) if side=="LONG" else price*(1+sl_pct)
            live_demo._open_position(side,price,BASE_POSITION_USD,tp_price,sl_price,pattern)
        time.sleep(POLL_EVERY_SECONDS)

# =========================
# MAIN
# =========================
if __name__=="__main__":
    df_hist=download_history_if_needed()
    live_demo=LiveFuturesDemo()
    live_demo.start_ws()
    t_poll=threading.Thread(target=poll_and_trade,args=(live_demo,df_hist),daemon=True)
    t_poll.start()
    # BACKTEST
    patterns_df=detect_all_patterns(df_hist)
    stats_df=backtest_patterns(df_hist,patterns_df)
    stats_df.to_csv(BACKTEST_STATS_CSV,index=False)
    print(stats_df.head(20))
    while True:
        time.sleep(60)
