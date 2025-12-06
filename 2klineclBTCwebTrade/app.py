#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Live candles plotter + history saver for Binance.
Features:
- Download historical klines from Binance REST API
- Save to CSV and SQLite db
- Plot candlesticks using matplotlib
- Subscribe to Binance websocket kline stream and update live candles
Config at top.
"""

import time
import json
import threading
import sqlite3
from datetime import datetime
from collections import deque

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.animation import FuncAnimation
import websocket

# ---------------------------
# Configuration
# ---------------------------
MARKET_SYMBOL = "BTCUSDT"            # биржевой символ
TIMEFRAME = "5m"                    # свечи: '1m','3m','5m','15m','1h',...
REST_LIMIT = 1000                   # max 1000 for Binance REST klines endpoint
CSV_KLINES_FILE = MARKET_SYMBOL + "_binance_klines_5m_2024.csv"
SQLITE_DB_FILE = MARKET_SYMBOL + "_binance_klines_5m_2024.db"
WEBSOCKET_BASE = "wss://stream.binance.com:9443/ws"
KLINE_STREAM = f"{MARKET_SYMBOL.lower()}@kline_{TIMEFRAME}"
MAX_CANDLES_DISPLAY = 200           # сколько свечей отображать на графике
# ---------------------------


# ---------------------------
# Utilities: time and formatting
# ---------------------------
def ts_to_dt(ts_ms: int) -> datetime:
    return datetime.utcfromtimestamp(ts_ms / 1000.0)


def dt_to_mpl(dt: datetime):
    return mdates.date2num(dt)


# ---------------------------
# REST: fetch historical klines
# ---------------------------
def fetch_historical_klines(symbol: str, interval: str, limit: int = 1000, end_time: int = None):
    """
    Returns list of klines from Binance public API.
    Each kline is a list as returned by Binance:
    [open_time, open, high, low, close, volume, close_time, quote_asset_volume, num_trades, ...]
    """
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if end_time is not None:
        params["endTime"] = int(end_time)
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def klines_to_dataframe(klines):
    # Map fields to DataFrame
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"]
    df = pd.DataFrame(klines, columns=cols)
    # convert types
    df["open_time"] = pd.to_datetime(df["open_time"].astype(np.int64), unit='ms')
    df["close_time"] = pd.to_datetime(df["close_time"].astype(np.int64), unit='ms')
    for c in ["open", "high", "low", "close", "volume", "quote_asset_volume",
              "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"]:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df["num_trades"] = df["num_trades"].astype(int)
    df.rename(columns={"open_time": "date"}, inplace=True)
    # keep only relevant columns
    df = df[["date", "open", "high", "low", "close", "volume", "num_trades", "close_time"]]
    df.reset_index(drop=True, inplace=True)
    return df


# ---------------------------
# Persistence: CSV and SQLite
# ---------------------------
def save_to_csv(df: pd.DataFrame, filename: str):
    df.to_csv(filename, index=False)
    print(f"[INFO] Saved {len(df)} candles to CSV: {filename}")


def init_sqlite(dbfile: str):
    conn = sqlite3.connect(dbfile, check_same_thread=False)
    c = conn.cursor()
    # Create table if not exists
    c.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        pair TEXT,
        timeframe TEXT,
        date TEXT,
        ts INTEGER,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        num_trades INTEGER,
        close_time TEXT,
        PRIMARY KEY(pair, timeframe, ts)
    )
    """)
    conn.commit()
    return conn


def insert_candles_sqlite(conn: sqlite3.Connection, pair: str, timeframe: str, df: pd.DataFrame):
    c = conn.cursor()
    rows = []
    for _, r in df.iterrows():
        ts = int(r['date'].timestamp() * 1000)
        rows.append((pair, timeframe, r['date'].isoformat(), ts, float(r['open']), float(r['high']),
                     float(r['low']), float(r['close']), float(r['volume']), int(r['num_trades']), r['close_time'].isoformat()))
    c.executemany("""
    INSERT OR REPLACE INTO candles (pair, timeframe, date, ts, open, high, low, close, volume, num_trades, close_time)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    print(f"[INFO] Inserted/updated {len(rows)} candles into SQLite DB: {conn}")


# ---------------------------
# Candles manager (holds pandas DataFrame + thread-safety)
# ---------------------------
class CandlesManager:
    def __init__(self, maxlen=2000):
        self.lock = threading.Lock()
        self.maxlen = maxlen
        # We'll keep deque of dicts then convert to DataFrame for plotting
        self._deque = deque(maxlen=maxlen)

    def load_from_df(self, df: pd.DataFrame):
        with self.lock:
            self._deque.clear()
            for _, r in df.iterrows():
                self._deque.append({
                    "date": pd.Timestamp(r['date']),
                    "open": float(r['open']),
                    "high": float(r['high']),
                    "low": float(r['low']),
                    "close": float(r['close']),
                    "volume": float(r['volume'])
                })

    def append_or_update(self, kline_dict):
        """
        kline_dict must contain: date (pd.Timestamp), open, high, low, close, volume, is_final (bool)
        If is_final is False — update the last candle; if True — append new final candle.
        """
        with self.lock:
            if not self._deque:
                # empty: just add
                self._deque.append({
                    "date": kline_dict["date"],
                    "open": kline_dict["open"],
                    "high": kline_dict["high"],
                    "low": kline_dict["low"],
                    "close": kline_dict["close"],
                    "volume": kline_dict["volume"]
                })
                return

            last = self._deque[-1]
            if last["date"] == kline_dict["date"]:
                # update existing
                last.update({
                    "open": kline_dict["open"],
                    "high": kline_dict["high"],
                    "low": kline_dict["low"],
                    "close": kline_dict["close"],
                    "volume": kline_dict["volume"]
                })
            elif kline_dict.get("is_final", False):
                # append finalized new candle
                self._deque.append({
                    "date": kline_dict["date"],
                    "open": kline_dict["open"],
                    "high": kline_dict["high"],
                    "low": kline_dict["low"],
                    "close": kline_dict["close"],
                    "volume": kline_dict["volume"]
                })
            else:
                # partial candle with different timestamp: replace last if close_time moved (rare)
                self._deque.append({
                    "date": kline_dict["date"],
                    "open": kline_dict["open"],
                    "high": kline_dict["high"],
                    "low": kline_dict["low"],
                    "close": kline_dict["close"],
                    "volume": kline_dict["volume"]
                })

    def to_dataframe(self, last_n=None):
        with self.lock:
            arr = list(self._deque)
        df = pd.DataFrame(arr)
        if df.empty:
            return df
        df['date'] = pd.to_datetime(df['date'])
        if last_n:
            df = df.iloc[-last_n:]
        return df


# ---------------------------
# Plotting: candlesticks using matplotlib
# ---------------------------
class LiveCandlesPlot:
    def __init__(self, candles_mgr: CandlesManager, display_count=200):
        self.cm = candles_mgr
        self.display_count = display_count

        # matplotlib setup
        plt.style.use('seaborn-darkgrid')
        self.fig, self.ax = plt.subplots(figsize=(12, 6))
        self.ax.set_title(f"{MARKET_SYMBOL} {TIMEFRAME} live candles")
        self.ax.set_xlabel("Time (UTC)")
        self.ax.set_ylabel("Price")

        # We'll maintain artists for candles and volumes
        self.candle_lines = []  # to store artists if needed
        self.volume_ax = self.ax.twinx()
        self.volume_ax.set_ylabel("Volume")
        self.volume_ax.set_zorder(0)
        self.ax.set_zorder(1)

    def draw_candles(self, df: pd.DataFrame):
        self.ax.clear()
        self.volume_ax.clear()
        if df.empty:
            return

        # convert dates to matplotlib
        df = df.copy()
        df['mdates'] = df['date'].apply(dt_to_mpl)
        width = (df['mdates'].iloc[1] - df['mdates'].iloc[0]) * 0.6 if len(df) > 1 else 0.0008

        # plot wick lines and bodies
        for idx, row in df.iterrows():
            m = row['mdates']
            o, h, l, c = row['open'], row['high'], row['low'], row['close']
            color = 'g' if c >= o else 'r'
            # wick
            self.ax.plot([m, m], [l, h], color='k', linewidth=0.7, zorder=2)
            # body
            self.ax.add_patch(plt.Rectangle((m - width/2, min(o, c)),
                                            width, abs(c - o),
                                            color=color, alpha=0.8, zorder=3))
        # format x axis
        self.ax.xaxis_date()
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d\n%H:%M'))
        self.fig.autofmt_xdate()
        # set limits
        pad = (df['high'].max() - df['low'].min()) * 0.08
        self.ax.set_ylim(df['low'].min() - pad, df['high'].max() + pad)
        self.ax.set_xlim(df['mdates'].iloc[0] - width, df['mdates'].iloc[-1] + width)

        # volume bars
        vols = df['volume']
        v_mdates = df['mdates']
        for vm, vol, op, cl in zip(v_mdates, vols, df['open'], df['close']):
            color = 'g' if cl >= op else 'r'
            self.volume_ax.bar(vm, vol, width=width, alpha=0.2, color=color)

        # labels
        self.ax.set_title(f"{MARKET_SYMBOL} {TIMEFRAME} — last {len(df)} candles (UTC)")
        self.ax.grid(True)

    def start_animation(self, interval_ms=1000):
        def update(frame):
            df = self.cm.to_dataframe(last_n=self.display_count)
            if df.empty:
                return
            self.draw_candles(df)

        self.ani = FuncAnimation(self.fig, update, interval=interval_ms, blit=False)
        plt.show()


# ---------------------------
# WebSocket: live kline subscription
# ---------------------------
class BinanceWebsocketClient:
    def __init__(self, symbol_stream: str, candles_mgr: CandlesManager):
        self.ws_url = f"{WEBSOCKET_BASE}/{symbol_stream}"
        self.cm = candles_mgr
        self.ws = None
        self.keep_running = False
        self.thread = None

    def on_open(self, ws):
        print(f"[WS] Connected to {self.ws_url}")

    def on_message(self, ws, message):
        try:
            msg = json.loads(message)
            # kline events structure:
            # { "e":"kline", "E":123456789, "s":"BTCUSDT", "k": { kline data } }
            if 'k' in msg:
                k = msg['k']
                # k contains: t (start), T (close), s, i, f, L, o,h,l,c,v, n, x (isFinal), q, V, Q, B
                start_ts = int(k['t'])
                open_p = float(k['o'])
                high_p = float(k['h'])
                low_p = float(k['l'])
                close_p = float(k['c'])
                volume = float(k['v'])
                is_final = bool(k['x'])
                dt = pd.Timestamp(ts_to_dt(start_ts))

                self.cm.append_or_update({
                    "date": dt,
                    "open": open_p,
                    "high": high_p,
                    "low": low_p,
                    "close": close_p,
                    "volume": volume,
                    "is_final": is_final
                })
                # optionally print when finalized
                if is_final:
                    print(f"[WS] Finalized candle {dt} O{open_p} H{high_p} L{low_p} C{close_p} V{volume}")
        except Exception as e:
            print("[WS] Error processing message:", e)

    def on_error(self, ws, error):
        print("[WS] Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("[WS] Closed connection", close_status_code, close_msg)

    def start(self):
        self.keep_running = True
        def run():
            while self.keep_running:
                try:
                    self.ws = websocket.WebSocketApp(self.ws_url,
                                                     on_open=self.on_open,
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close)
                    self.ws.run_forever(ping_interval=20, ping_timeout=10)
                except Exception as e:
                    print("[WS] Exception run_forever:", e)
                # reconnect delay
                if self.keep_running:
                    print("[WS] Reconnecting in 5s...")
                    time.sleep(5)
        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def stop(self):
        self.keep_running = False
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.thread:
            self.thread.join(timeout=2)


# ---------------------------
# Main entry
# ---------------------------
def main():
    print(f"[START] {MARKET_SYMBOL} {TIMEFRAME} Live Candles Plotter")

    # 1. Fetch history
    print("[STEP] Fetching historical klines via REST...")
    try:
        raw_klines = fetch_historical_klines(MARKET_SYMBOL, TIMEFRAME, limit=REST_LIMIT)
    except Exception as e:
        print("[ERROR] Failed to fetch historical klines:", e)
        return

    df_hist = klines_to_dataframe(raw_klines)
    if df_hist.empty:
        print("[ERROR] No historical data received.")
        return

    # 2. Save CSV
    save_to_csv(df_hist, CSV_KLINES_FILE)

    # 3. Save to SQLite
    conn = init_sqlite(SQLITE_DB_FILE)
    insert_candles_sqlite(conn, MARKET_SYMBOL, TIMEFRAME, df_hist)

    # 4. Prepare candles manager
    cm = CandlesManager(maxlen=5000)
    cm.load_from_df(df_hist)

    # 5. Start websocket client to receive live klines
    ws_client = BinanceWebsocketClient(KLINE_STREAM, cm)
    ws_client.start()

    # 6. Start plotting (blocking; matplotlib.show() loop). It will update using FuncAnimation.
    plotter = LiveCandlesPlot(candles_mgr=cm, display_count=MAX_CANDLES_DISPLAY)
    try:
        plotter.start_animation(interval_ms=1000)
    except KeyboardInterrupt:
        print("[MAIN] Interrupted by user.")
    finally:
        print("[MAIN] Stopping websocket client...")
        ws_client.stop()
        conn.close()
        print("[MAIN] Exiting.")


if __name__ == "__main__":
    main()
"""
═══════════════════════════════════════════════════════════════════════════
    LIVE TRADING PATTERN ANALYZER - PROFESSIONAL EDITION
    Real-time Binance WebSocket + Pattern Detection + Auto Signals
    Full TradingView-style charts with TP/SL levels
═══════════════════════════════════════════════════════════════════════════
    
Requirements:
    pip install flask flask-socketio pandas numpy requests websocket-client plotly

Usage:
    python flask_app.py
    Open: http://localhost:5000

Features:
    ✓ Real-time Binance Futures WebSocket
    ✓ 12 Pattern Detection Algorithms
    ✓ Auto Trading Signals with TP/SL
    ✓ TradingView-style Charts
    ✓ Pattern Visualization on Chart
    ✓ Trade History & Statistics
    ✓ Full Configuration Panel
    ✓ Multi-timeframe Support
"""

from flask import Flask, render_template_string, jsonify, request
from flask_socketio import SocketIO, emit
import pandas as pd
import numpy as np
import requests
import websocket
import threading
import json
import time
import traceback
from datetime import datetime, timedelta
from collections import deque

# ═══════════════════════════════════════════════════════════════════════════
# FLASK APPLICATION SETUP
# ═══════════════════════════════════════════════════════════════════════════
app = Flask(__name__)
app.config['SECRET_KEY'] = 'trading_pro_2024_secret_key'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading', 
                    ping_timeout=60, ping_interval=25)

# ═══════════════════════════════════════════════════════════════════════════
# GLOBAL STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════
live_candles = deque(maxlen=1000)
live_patterns = deque(maxlen=500)
live_signals = deque(maxlen=500)
active_trades = {}  # trade_id -> trade_data
closed_trades = deque(maxlen=1000)
ws_thread = None
ws_app = None
ws_lock = threading.Lock()
is_running = False

# Trading Configuration
config = {
    'symbol': 'BTCUSDT',
    'interval': '15m',
    'leverage': 100,
    'position_size': 10.0,
    'fee_rate': 0.0004,
    'initial_capital': 1000.0,
    'current_capital': 1000.0,
    'patterns': {
        'Bullish Flag': {
            'tp': 1.0, 'sl': 0.5, 'scale': 0.5, 
            'candle_pct': 0.5, 'enabled': True, 'color': '#10b981'
        },
        'Bearish Flag': {
            'tp': 1.0, 'sl': 0.5, 'scale': 0.5, 
            'candle_pct': 0.5, 'enabled': True, 'color': '#ef4444'
        },
        'FVG Up': {
            'tp': 0.5, 'sl': 0.25, 'scale': 0.6, 
            'candle_pct': 0.6, 'enabled': True, 'color': '#22c55e'
        },
        'FVG Down': {
            'tp': 0.5, 'sl': 0.25, 'scale': 0.6, 
            'candle_pct': 0.6, 'enabled': True, 'color': '#f87171'
        },
        'BOS Up': {
            'tp': 0.5, 'sl': 0.25, 'scale': 0.6, 
            'candle_pct': 0.6, 'enabled': True, 'color': '#3b82f6'
        },
        'BOS Down': {
            'tp': 0.5, 'sl': 0.25, 'scale': 0.6, 
            'candle_pct': 0.6, 'enabled': True, 'color': '#dc2626'
        },
        'Head & Shoulders': {
            'tp': 0.75, 'sl': 0.5, 'scale': 0.8, 
            'candle_pct': 0.8, 'enabled': True, 'color': '#8b5cf6'
        },
        'Ascending Triangle': {
            'tp': 0.5, 'sl': 0.25, 'scale': 0.7, 
            'candle_pct': 0.7, 'enabled': True, 'color': '#06b6d4'
        },
        'Descending Triangle': {
            'tp': 0.5, 'sl': 0.25, 'scale': 0.7, 
            'candle_pct': 0.7, 'enabled': True, 'color': '#f59e0b'
        },
        'Rising Wedge': {
            'tp': 0.25, 'sl': 0.5, 'scale': 0.4, 
            'candle_pct': 0.4, 'enabled': True, 'color': '#ec4899'
        },
        'Falling Wedge': {
            'tp': 0.25, 'sl': 0.5, 'scale': 0.4, 
            'candle_pct': 0.4, 'enabled': True, 'color': '#14b8a6'
        },
        'Double Top': {
            'tp': 0.6, 'sl': 0.3, 'scale': 0.7, 
            'candle_pct': 0.6, 'enabled': True, 'color': '#f472b6'
        }
    }
}

# Statistics
stats = {
    'total_signals': 0,
    'total_trades': 0,
    'wins': 0,
    'losses': 0,
    'winrate': 0.0,
    'total_pnl': 0.0,
    'roi': 0.0,
    'largest_win': 0.0,
    'largest_loss': 0.0,
    'avg_win': 0.0,
    'avg_loss': 0.0,
    'profit_factor': 0.0
}

# ═══════════════════════════════════════════════════════════════════════════
# PATTERN DETECTION ALGORITHMS
# ═══════════════════════════════════════════════════════════════════════════

def detect_fvg(df, gap_perc=0.002):
    """Fair Value Gap Detection"""
    patterns = []
    for i in range(2, len(df)):
        prev_high = df.iloc[i-2]['high']
        prev_low = df.iloc[i-2]['low']
        curr_open = df.iloc[i]['open']
        
        if curr_open > prev_high * (1 + gap_perc):
            patterns.append({
                'type': 'FVG Up',
                'start': df.index[i-2].isoformat(),
                'end': df.index[i].isoformat(),
                'price': float(curr_open),
                'direction': 'LONG',
                'strength': ((curr_open - prev_high) / prev_high) * 100
            })
        elif curr_open < prev_low * (1 - gap_perc):
            patterns.append({
                'type': 'FVG Down',
                'start': df.index[i-2].isoformat(),
                'end': df.index[i].isoformat(),
                'price': float(curr_open),
                'direction': 'SHORT',
                'strength': ((prev_low - curr_open) / prev_low) * 100
            })
    return patterns

def detect_bos(df, window=5):
    """Break of Structure Detection"""
    patterns = []
    if len(df) < window + 1:
        return patterns
    
    for i in range(window, len(df)-1):
        window_high = df.iloc[i-window:i]['high'].max()
        window_low = df.iloc[i-window:i]['low'].min()
        current_high = df.iloc[i]['high']
        current_low = df.iloc[i]['low']
        
        if current_high > window_high * 1.001:
            patterns.append({
                'type': 'BOS Up',
                'start': df.index[i-window].isoformat(),
                'end': df.index[i].isoformat(),
                'price': float(current_high),
                'direction': 'LONG',
                'strength': ((current_high - window_high) / window_high) * 100
            })
        
        if current_low < window_low * 0.999:
            patterns.append({
                'type': 'BOS Down',
                'start': df.index[i-window].isoformat(),
                'end': df.index[i].isoformat(),
                'price': float(current_low),
                'direction': 'SHORT',
                'strength': ((window_low - current_low) / window_low) * 100
            })
    return patterns

def detect_flags(df, lookback=15, threshold=0.005):
    """Bullish/Bearish Flag Detection"""
    patterns = []
    if len(df) < lookback:
        return patterns
    
    for i in range(lookback, len(df)):
        seg = df.iloc[i-lookback:i]['close']
        diff = seg.max() - seg.min()
        
        if seg.min() > 0 and (diff / seg.min()) < threshold:
            slope = (seg.iloc[-1] - seg.iloc[0]) / seg.iloc[0]
            
            if slope > 0.0001:
                patterns.append({
                    'type': 'Bullish Flag',
                    'start': df.index[i-lookback].isoformat(),
                    'end': df.index[i-1].isoformat(),
                    'price': float(seg.iloc[-1]),
                    'direction': 'LONG',
                    'strength': abs(slope) * 100
                })
            elif slope < -0.0001:
                patterns.append({
                    'type': 'Bearish Flag',
                    'start': df.index[i-lookback].isoformat(),
                    'end': df.index[i-1].isoformat(),
                    'price': float(seg.iloc[-1]),
                    'direction': 'SHORT',
                    'strength': abs(slope) * 100
                })
    return patterns

def detect_triangles(df, lookback=20):
    """Triangle Pattern Detection"""
    patterns = []
    if len(df) < lookback:
        return patterns
    
    for i in range(lookback, len(df)):
        seg = df.iloc[i-lookback:i]
        x = np.arange(len(seg))
        
        try:
            high_slope, _ = np.polyfit(x, seg['high'].values, 1)
            low_slope, _ = np.polyfit(x, seg['low'].values, 1)
            
            # Ascending Triangle
            if abs(high_slope) < 0.01 and low_slope > 0.01:
                patterns.append({
                    'type': 'Ascending Triangle',
                    'start': df.index[i-lookback].isoformat(),
                    'end': df.index[i-1].isoformat(),
                    'price': float(seg['close'].iloc[-1]),
                    'direction': 'LONG',
                    'strength': abs(low_slope) * 1000
                })
            
            # Descending Triangle
            elif abs(low_slope) < 0.01 and high_slope < -0.01:
                patterns.append({
                    'type': 'Descending Triangle',
                    'start': df.index[i-lookback].isoformat(),
                    'end': df.index[i-1].isoformat(),
                    'price': float(seg['close'].iloc[-1]),
                    'direction': 'SHORT',
                    'strength': abs(high_slope) * 1000
                })
        except:
            continue
    
    return patterns

def detect_head_shoulders(df, window=7, threshold=0.003):
    """Head & Shoulders Pattern Detection"""
    patterns = []
    if len(df) < window * 3:
        return patterns
    
    for i in range(window, len(df) - window):
        left_shoulder = df.iloc[i-window:i]['high'].max()
        head = df.iloc[i]['high']
        right_shoulder = df.iloc[i+1:i+1+window]['high'].max()
        
        if (head > left_shoulder * (1 + threshold) and 
            head > right_shoulder * (1 + threshold)):
            patterns.append({
                'type': 'Head & Shoulders',
                'start': df.index[i-window].isoformat(),
                'end': df.index[min(i+window, len(df)-1)].isoformat(),
                'price': float(head),
                'direction': 'SHORT',
                'strength': ((head - max(left_shoulder, right_shoulder)) / head) * 100
            })
    
    return patterns

def detect_double_tops(df, window=10, threshold=0.002):
    """Double Top/Bottom Detection"""
    patterns = []
    if len(df) < window * 2:
        return patterns
    
    highs = []
    for i in range(window, len(df) - window):
        if df.iloc[i]['high'] == df.iloc[i-window:i+window]['high'].max():
            highs.append((i, df.iloc[i]['high']))
    
    for i in range(len(highs) - 1):
        idx1, price1 = highs[i]
        idx2, price2 = highs[i + 1]
        
        if abs(price1 - price2) / price1 < threshold:
            patterns.append({
                'type': 'Double Top',
                'start': df.index[idx1].isoformat(),
                'end': df.index[idx2].isoformat(),
                'price': float((price1 + price2) / 2),
                'direction': 'SHORT',
                'strength': 2.0
            })
    
    return patterns

def detect_wedges(df, lookback=25):
    """Rising/Falling Wedge Detection"""
    patterns = []
    if len(df) < lookback:
        return patterns
    
    for i in range(lookback, len(df)):
        seg = df.iloc[i-lookback:i]
        x = np.arange(len(seg))
        
        try:
            high_slope, _ = np.polyfit(x, seg['high'].values, 1)
            low_slope, _ = np.polyfit(x, seg['low'].values, 1)
            
            # Rising Wedge (bearish)
            if high_slope > 0.01 and low_slope > 0.01 and high_slope < low_slope * 1.5:
                patterns.append({
                    'type': 'Rising Wedge',
                    'start': df.index[i-lookback].isoformat(),
                    'end': df.index[i-1].isoformat(),
                    'price': float(seg['close'].iloc[-1]),
                    'direction': 'SHORT',
                    'strength': abs(high_slope - low_slope) * 1000
                })
            
            # Falling Wedge (bullish)
            elif high_slope < -0.01 and low_slope < -0.01 and low_slope > high_slope * 1.5:
                patterns.append({
                    'type': 'Falling Wedge',
                    'start': df.index[i-lookback].isoformat(),
                    'end': df.index[i-1].isoformat(),
                    'price': float(seg['close'].iloc[-1]),
                    'direction': 'LONG',
                    'strength': abs(high_slope - low_slope) * 1000
                })
        except:
            continue
    
    return patterns

def detect_all_patterns(df):
    """Master Pattern Detection Function"""
    if len(df) < 10:
        return []
    
    all_patterns = []
    
    try:
        all_patterns.extend(detect_fvg(df))
        all_patterns.extend(detect_bos(df))
        all_patterns.extend(detect_flags(df))
        all_patterns.extend(detect_triangles(df))
        all_patterns.extend(detect_head_shoulders(df))
        all_patterns.extend(detect_double_tops(df))
        all_patterns.extend(detect_wedges(df))
    except Exception as e:
        print(f"Pattern detection error: {e}")
        traceback.print_exc()
    
    # Remove duplicates and sort by strength
    unique_patterns = []
    seen = set()
    
    for p in sorted(all_patterns, key=lambda x: x.get('strength', 0), reverse=True):
        key = (p['type'], p['start'], p['end'])
        if key not in seen:
            seen.add(key)
            unique_patterns.append(p)
    
    return unique_patterns[:20]  # Top 20 patterns

# ═══════════════════════════════════════════════════════════════════════════
# SIGNAL GENERATION & TRADE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

def generate_signal(pattern, current_price):
    """Generate trading signal from pattern"""
    cfg = config['patterns'].get(pattern['type'], {})
    if not cfg.get('enabled', True):
        return None
    
    tp_pct = cfg.get('tp', 0.5)
    sl_pct = cfg.get('sl', 0.25)
    direction = pattern.get('direction', 'LONG')
    
    try:
        entry = float(current_price)
        leverage = config.get('leverage', 1)
        position_size = config.get('position_size', 10.0)
        
        if direction == 'LONG':
            tp = entry * (1 + tp_pct / 100)
            sl = entry * (1 - sl_pct / 100)
        else:
            tp = entry * (1 - tp_pct / 100)
            sl = entry * (1 + sl_pct / 100)
        
        signal = {
            'id': f"sig_{int(time.time() * 1000)}",
            'timestamp': datetime.utcnow().isoformat(),
            'pattern': pattern['type'],
            'direction': direction,
            'entry': round(entry, 2),
            'tp': round(tp, 2),
            'sl': round(sl, 2),
            'leverage': leverage,
            'position_size': position_size,
            'status': 'ACTIVE',
            'strength': pattern.get('strength', 0),
            'color': cfg.get('color', '#00d4ff'),
            'pattern_start': pattern['start'],
            'pattern_end': pattern['end']
        }
        
        stats['total_signals'] += 1
        return signal
        
    except Exception as e:
        print(f"Signal generation error: {e}")
        return None

def check_signal_hit(signal, current_price):
    """Check if TP or SL is hit"""
    if signal['status'] != 'ACTIVE':
        return None
    
    direction = signal['direction']
    entry = signal['entry']
    tp = signal['tp']
    sl = signal['sl']
    
    if direction == 'LONG':
        if current_price >= tp:
            return close_trade(signal, current_price, 'TP')
        elif current_price <= sl:
            return close_trade(signal, current_price, 'SL')
    else:  # SHORT
        if current_price <= tp:
            return close_trade(signal, current_price, 'TP')
        elif current_price >= sl:
            return close_trade(signal, current_price, 'SL')
    
    return None

def close_trade(signal, exit_price, reason):
    """Close trade and calculate PNL"""
    entry = signal['entry']
    direction = signal['direction']
    leverage = signal['leverage']
    position_size = signal['position_size']
    fee_rate = config['fee_rate']
    
    # Calculate PNL
    if direction == 'LONG':
        price_change = (exit_price - entry) / entry
    else:
        price_change = (entry - exit_price) / entry
    
    gross_pnl = price_change * position_size * leverage
    fees = position_size * leverage * fee_rate * 2  # Entry + Exit
    net_pnl = gross_pnl - fees
    roi = (net_pnl / position_size) * 100
    
    # Update capital
    config['current_capital'] += net_pnl
    
    # Update stats
    stats['total_trades'] += 1
    if net_pnl > 0:
        stats['wins'] += 1
        stats['largest_win'] = max(stats['largest_win'], net_pnl)
    else:
        stats['losses'] += 1
        stats['largest_loss'] = min(stats['largest_loss'], net_pnl)
    
    stats['total_pnl'] += net_pnl
    stats['winrate'] = (stats['wins'] / stats['total_trades'] * 100) if stats['total_trades'] > 0 else 0
    stats['roi'] = ((config['current_capital'] - config['initial_capital']) / 
                    config['initial_capital'] * 100)
    
    # Calculate averages
    if stats['wins'] > 0:
        winning_trades = [t['pnl'] for t in closed_trades if t['pnl'] > 0]
        stats['avg_win'] = sum(winning_trades) / len(winning_trades) if winning_trades else 0
    
    if stats['losses'] > 0:
        losing_trades = [abs(t['pnl']) for t in closed_trades if t['pnl'] < 0]
        stats['avg_loss'] = sum(losing_trades) / len(losing_trades) if losing_trades else 0
    
    # Profit factor
    if stats['avg_loss'] > 0:
        stats['profit_factor'] = stats['avg_win'] / stats['avg_loss']
    
    closed_trade = {
        **signal,
        'exit_price': round(exit_price, 2),
        'close_timestamp': datetime.utcnow().isoformat(),
        'reason': reason,
        'pnl': round(net_pnl, 2),
        'roi': round(roi, 2),
        'status': 'CLOSED'
    }
    
    closed_trades.appendleft(closed_trade)
    return closed_trade

# ═══════════════════════════════════════════════════════════════════════════
# BINANCE WEBSOCKET HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

def on_ws_message(ws, message):
    """WebSocket message handler"""
    global live_candles, live_patterns, live_signals
    
    try:
        data = json.loads(message)
        if 'k' not in data:
            return
        
        kline = data['k']
        candle = {
            'timestamp': datetime.fromtimestamp(kline['t'] / 1000),
            'open': float(kline['o']),
            'high': float(kline['h']),
            'low': float(kline['l']),
            'close': float(kline['c']),
            'volume': float(kline['v']),
            'is_closed': bool(kline['x'])
        }
        
        live_candles.append(candle)
        current_price = candle['close']
        
        # Check active signals
        for signal in list(live_signals):
            if signal['status'] == 'ACTIVE':
                closed = check_signal_hit(signal, current_price)
                if closed:
                    signal['status'] = 'CLOSED'
                    socketio.emit('trade_closed', closed)
        
        # Pattern detection on closed candles
        if candle['is_closed']:
            df = pd.DataFrame(list(live_candles))
            if not df.empty and len(df) >= 10:
                df.set_index('timestamp', inplace=True)
                patterns = detect_all_patterns(df)
                
                for pattern in patterns:
                    cfg = config['patterns'].get(pattern['type'], {})
                    if cfg.get('enabled', True):
                        # Check if pattern is new
                        pattern_key = (pattern['type'], pattern['start'], pattern['end'])
                        existing = any(
                            (p['type'], p['start'], p['end']) == pattern_key 
                            for p in live_patterns
                        )
                        
                        if not existing:
                            live_patterns.appendleft(pattern)
                            socketio.emit('new_pattern', pattern)
                            
                            # Generate signal
                            signal = generate_signal(pattern, current_price)
                            if signal:
                                live_signals.appendleft(signal)
                                socketio.emit('new_signal', signal)
        
        # Emit live update
        socketio.emit('live_update', {
            'candle': {
                'timestamp': candle['timestamp'].isoformat(),
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close'],
                'volume': candle['volume'],
                'is_closed': candle['is_closed']
            },
            'price': current_price,
            'patterns': len(live_patterns),
            'signals': len([s for s in live_signals if s['status'] == 'ACTIVE']),
            'capital': round(config['current_capital'], 2)
        })
        
    except Exception as e:
        print(f"WebSocket message error: {e}")
        traceback.print_exc()

def on_ws_error(ws, error):
    print(f"WebSocket error: {error}")

def on_ws_close(ws, close_status_code, close_msg):
    global is_running
    is_running = False
    print(f"WebSocket closed: {close_status_code} - {close_msg}")

def on_ws_open(ws):
    global is_running
    is_running = True
    print("✓ WebSocket connected to Binance")

def start_websocket():
    """Start WebSocket connection"""
    global ws_app
    
    with ws_lock:
        symbol = config['symbol'].lower()
        interval = config['interval']
        ws_url = f"wss://fstream.binance.com/ws/{symbol}@kline_{interval}"
        
        try:
            ws_app = websocket.WebSocketApp(
                ws_url,
                on_message=on_ws_message,
                on_error=on_ws_error,
                on_close=on_ws_close,
                on_open=on_ws_open
            )
            ws_app.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"WebSocket start error: {e}")
            traceback.print_exc()
        finally:
            is_running = False

def stop_websocket():
    """Stop WebSocket connection"""
    global ws_app
    try:
        with ws_lock:
            if ws_app:
                ws_app.close()
                ws_app = None
                print("✓ WebSocket stopped")
    except Exception as e:
        print(f"WebSocket stop error: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_historical_data(limit=500):
    """Load historical candles from Binance"""
    try:
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': config['symbol'],
            'interval': config['interval'],
            'limit': limit
        }
        
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        live_candles.clear()
        
        for kline in data:
            candle = {
                'timestamp': datetime.fromtimestamp(kline[0] / 1000),
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5]),
                'is_closed': True
            }
            live_candles.append(candle)
        
        print(f"✓ Loaded {len(live_candles)} historical candles")
        return True
        
    except Exception as e:
        print(f"✗ Error loading historical data: {e}")
        traceback.print_exc()
        return False

# ═══════════════════════════════════════════════════════════════════════════
# HTML TEMPLATE - PROFESSIONAL TRADING INTERFACE
# ═══════════════════════════════════════════════════════════════════════════

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LIVE Pattern Analyzer Pro</title>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        :root {
            --bg-primary: #0a0e27;
            --bg-secondary: #1a1a2e;
            --bg-card: rgba(26, 26, 46, 0.8);
            --border-color: rgba(0, 212, 255, 0.2);
            --text-primary: #ffffff;
            --text-secondary: #9ca3af;
            --accent-blue: #00d4ff;
            --accent-purple: #7b2ff7;
            --green: #10b981;
            --red: #ef4444;
            --yellow: #f59e0b;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
            background: linear-gradient(135deg, var(--bg-primary) 0%, #16213e 100%);
            color: var(--text-primary);
            overflow-x: hidden;
            min-height: 100vh;
        }
        
        .header {
            background: rgba(0, 0, 0, 0.4);
            backdrop-filter: blur(20px);
            border-bottom: 2px solid var(--border-color);
            padding: 15px 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            position: sticky;
            top: 0;
            z-index: 1000;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
        }
        
        .logo {
            font-size: 1.8em;
            font-weight: 900;
            background: linear-gradient(90deg, var(--accent-blue), var(--accent-purple));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: -1px;
        }
        
        .live-badge {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 8px 18px;
            background: rgba(16, 185, 129, 0.15);
            border: 2px solid var(--green);
            border-radius: 25px;
            font-weight: 600;
            animation: pulse-border 2s infinite;
        }
        
        @keyframes pulse-border {
            0%, 100% { border-color: var(--green); box-shadow: 0 0 10px rgba(16, 185, 129, 0.3); }
            50% { border-color: rgba(16, 185, 129, 0.5); box-shadow: 0 0 20px rgba(16, 185, 129, 0.5); }
        }
        
        .pulse-dot {
            width: 10px;
            height: 10px;
            background: var(--green);
            border-radius: 50%;
            animation: pulse-dot 1.5s infinite;
        }
        
        @keyframes pulse-dot {
            0%, 100% { transform: scale(1); opacity: 1; }
            50% { transform: scale(1.3); opacity: 0.7; }
        }
        
        .container {
            max-width: 1800px;
            margin: 0 auto;
            padding: 20px;
            display: grid;
            grid-template-columns: 1fr 380px;
            gap: 20px;
        }
        
        .main-panel {
            display: flex;
            flex-direction: column;
            gap: 20px;
        }
        
        .card {
            background: var(--bg-card);
            backdrop-filter: blur(10px);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 20px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            transition: all 0.3s;
        }
        
        .card:hover {
            border-color: var(--accent-blue);
            transform: translateY(-2px);
            box-shadow: 0 12px 40px rgba(0, 212, 255, 0.2);
        }
        
        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }
        
        .card-title {
            font-size: 1.3em;
            font-weight: 700;
            color: var(--accent-blue);
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }
        
        .stat-box {
            background: rgba(0, 0, 0, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 12px;
            padding: 15px;
            text-align: center;
            transition: all 0.3s;
        }
        
        .stat-box:hover {
            background: rgba(0, 0, 0, 0.5);
            border-color: var(--accent-blue);
            transform: scale(1.05);
        }
        
        .stat-label {
            font-size: 0.75em;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 8px;
        }
        
        .stat-value {
            font-size: 1.8em;
            font-weight: 800;
            color: var(--accent-blue);
        }
        
        .stat-value.green { color: var(--green); }
        .stat-value.red { color: var(--red); }
        .stat-value.yellow { color: var(--yellow); }
        
        .price-ticker {
            font-size: 3.5em;
            font-weight: 900;
            text-align: center;
            padding: 30px;
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(123, 47, 247, 0.1));
            border-radius: 12px;
            margin-bottom: 20px;
            animation: price-glow 3s infinite;
        }
        
        @keyframes price-glow {
            0%, 100% { box-shadow: 0 0 20px rgba(0, 212, 255, 0.3); }
            50% { box-shadow: 0 0 40px rgba(123, 47, 247, 0.5); }
        }
        
        .signal-item {
            background: rgba(0, 0, 0, 0.4);
            border-left: 4px solid var(--green);
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 12px;
            transition: all 0.3s;
            animation: slideIn 0.4s ease-out;
        }
        
        @keyframes slideIn {
            from { transform: translateX(-30px); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        
        .signal-item:hover {
            background: rgba(0, 0, 0, 0.6);
            transform: translateX(5px);
        }
        
        .signal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }
        
        .signal-pattern {
            font-weight: 700;
            font-size: 1.1em;
        }
        
        .signal-badge {
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: 700;
            text-transform: uppercase;
        }
        
        .badge-long {
            background: rgba(16, 185, 129, 0.2);
            color: var(--green);
            border: 1px solid var(--green);
        }
        
        .badge-short {
            background: rgba(239, 68, 68, 0.2);
            color: var(--red);
            border: 1px solid var(--red);
        }
        
        .signal-prices {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin: 10px 0;
        }
        
        .price-item {
            text-align: center;
            padding: 8px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 8px;
        }
        
        .price-label {
            font-size: 0.7em;
            color: var(--text-secondary);
            margin-bottom: 4px;
        }
        
        .price-value {
            font-size: 1.1em;
            font-weight: 700;
            font-family: 'Courier New', monospace;
        }
        
        .config-section {
            margin-bottom: 20px;
        }
        
        label {
            display: block;
            font-size: 0.85em;
            color: var(--text-secondary);
            margin-bottom: 6px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        input, select {
            width: 100%;
            padding: 12px;
            background: rgba(0, 0, 0, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.2);
            border-radius: 8px;
            color: var(--text-primary);
            font-size: 1em;
            transition: all 0.3s;
        }
        
        input:focus, select:focus {
            outline: none;
            border-color: var(--accent-blue);
            background: rgba(0, 0, 0, 0.5);
            box-shadow: 0 0 15px rgba(0, 212, 255, 0.3);
        }
        
        button {
            width: 100%;
            padding: 14px 24px;
            background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
            border: none;
            border-radius: 10px;
            color: white;
            font-weight: 700;
            font-size: 1em;
            cursor: pointer;
            transition: all 0.3s;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 25px rgba(0, 212, 255, 0.4);
        }
        
        button:active {
            transform: translateY(0);
        }
        
        .pattern-toggle {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px;
            margin: 6px 0;
            background: rgba(0, 0, 0, 0.3);
            border-radius: 8px;
            transition: all 0.3s;
        }
        
        .pattern-toggle:hover {
            background: rgba(0, 0, 0, 0.5);
        }
        
        .toggle-switch {
            position: relative;
            width: 56px;
            height: 28px;
            background: #555;
            border-radius: 28px;
            cursor: pointer;
            transition: 0.3s;
        }
        
        .toggle-switch.active {
            background: var(--green);
        }
        
        .toggle-slider {
            position: absolute;
            top: 3px;
            left: 3px;
            width: 22px;
            height: 22px;
            background: white;
            border-radius: 50%;
            transition: 0.3s;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3);
        }
        
        .toggle-switch.active .toggle-slider {
            left: 31px;
        }
        
        .trade-history {
            max-height: 400px;
            overflow-y: auto;
        }
        
        .trade-history::-webkit-scrollbar {
            width: 8px;
        }
        
        .trade-history::-webkit-scrollbar-track {
            background: rgba(0, 0, 0, 0.2);
            border-radius: 4px;
        }
        
        .trade-history::-webkit-scrollbar-thumb {
            background: var(--accent-blue);
            border-radius: 4px;
        }
        
        .trade-item {
            background: rgba(0, 0, 0, 0.3);
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 10px;
            border-left: 3px solid;
        }
        
        .trade-item.win {
            border-left-color: var(--green);
        }
        
        .trade-item.loss {
            border-left-color: var(--red);
        }
        
        @media (max-width: 1200px) {
            .container {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="logo">⚡ Pattern Analyzer Pro</div>
        <div class="live-badge">
            <div class="pulse-dot"></div>
            <span id="liveStatus">Connecting...</span>
        </div>
    </div>

    <div class="container">
        <div class="main-panel">
            <!-- Trading Chart -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">📈 Live Trading Chart</div>
                    <div id="chartStatus"></div>
                </div>
                <div id="tradingChart" style="height: 550px;"></div>
            </div>

            <!-- Statistics Grid -->
            <div class="stats-grid">
                <div class="stat-box">
                    <div class="stat-label">Total Trades</div>
                    <div class="stat-value" id="totalTrades">0</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Winrate</div>
                    <div class="stat-value green" id="winrate">0%</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Total PNL</div>
                    <div class="stat-value" id="totalPnl">$0</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">ROI</div>
                    <div class="stat-value" id="roi">0%</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Capital</div>
                    <div class="stat-value yellow" id="capital">$1000</div>
                </div>
                <div class="stat-box">
                    <div class="stat-label">Active Signals</div>
                    <div class="stat-value" id="activeSignals">0</div>
                </div>
            </div>

            <!-- Active Signals -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">🎯 Active Signals</div>
                </div>
                <div id="signalsContainer"></div>
            </div>

            <!-- Trade History -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">📊 Trade History</div>
                </div>
                <div class="trade-history" id="tradeHistory"></div>
            </div>
        </div>

        <!-- Right Sidebar -->
        <div class="side-panel">
            <!-- Price Ticker -->
            <div class="card">
                <div class="price-ticker" id="priceTicker">—</div>
                <div class="stats-grid">
                    <div class="stat-box">
                        <div class="stat-label">24h High</div>
                        <div class="stat-value green" id="high24h">—</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">24h Low</div>
                        <div class="stat-value red" id="low24h">—</div>
                    </div>
                </div>
            </div>

            <!-- Configuration -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">⚙️ Configuration</div>
                </div>
                <div class="config-section">
                    <label>Symbol</label>
                    <select id="symbol">
                        <option value="BTCUSDT">BTC/USDT</option>
                        <option value="ETHUSDT">ETH/USDT</option>
                        <option value="BNBUSDT">BNB/USDT</option>
                        <option value="SOLUSDT">SOL/USDT</option>
                    </select>
                </div>
                <div class="config-section">
                    <label>Timeframe</label>
                    <select id="interval">
                        <option value="1m">1 Minute</option>
                        <option value="5m">5 Minutes</option>
                        <option value="15m" selected>15 Minutes</option>
                        <option value="1h">1 Hour</option>
                        <option value="4h">4 Hours</option>
                    </select>
                </div>
                <div class="config-section">
                    <label>Leverage</label>
                    <input type="number" id="leverage" value="100" min="1" max="125">
                </div>
                <div class="config-section">
                    <label>Position Size ($)</label>
                    <input type="number" id="positionSize" value="10" min="1">
                </div>
                <button onclick="updateConfig()">💾 Save Config</button>
            </div>

            <!-- Pattern Toggles -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">🎯 Patterns</div>
                </div>
                <div id="patternsToggle"></div>
            </div>
        </div>
    </div>

    <script>
        const socket = io();
        let candleData = [];
        let activeSignals = [];
        let closedTrades = [];
        let lastPrice = 0;
        let stats = {};

        // ═══════════════════════════════════════════════════════════════
        // SOCKET HANDLERS
        // ═══════════════════════════════════════════════════════════════
        
        socket.on('connect', () => {
            document.getElementById('liveStatus').textContent = 'LIVE';
            loadConfig();
        });

        socket.on('disconnect', () => {
            document.getElementById('liveStatus').textContent = 'Disconnected';
        });

        socket.on('live_update', (data) => {
            updatePrice(data.price);
            updateCandles(data.candle);
            updateStats(data);
        });

        socket.on('new_signal', (signal) => {
            activeSignals.unshift(signal);
            renderSignals();
            updateChart();
        });

        socket.on('trade_closed', (trade) => {
            const index = activeSignals.findIndex(s => s.id === trade.id);
            if (index !== -1) activeSignals.splice(index, 1);
            closedTrades.unshift(trade);
            renderSignals();
            renderTradeHistory();
            updateChart();
        });

        socket.on('new_pattern', (pattern) => {
            console.log('New pattern detected:', pattern);
        });

        // ═══════════════════════════════════════════════════════════════
        // DATA MANAGEMENT
        // ═══════════════════════════════════════════════════════════════
        
        function updateCandles(candle) {
            const time = new Date(candle.timestamp);
            
            if (candleData.length > 0) {
                const lastCandle = candleData[candleData.length - 1];
                const lastTime = new Date(lastCandle.time);
                
                if (lastTime.getTime() === time.getTime()) {
                    candleData[candleData.length - 1] = {
                        time: time,
                        open: candle.open,
                        high: candle.high,
                        low: candle.low,
                        close: candle.close,
                        volume: candle.volume
                    };
                } else {
                    candleData.push({
                        time: time,
                        open: candle.open,
                        high: candle.high,
                        low: candle.low,
                        close: candle.close,
                        volume: candle.volume
                    });
                }
            } else {
                candleData.push({
                    time: time,
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                    volume: candle.volume
                });
            }
            
            if (candleData.length > 200) candleData.shift();
            updateChart();
        }

        function updatePrice(price) {
            const ticker = document.getElementById('priceTicker');
            ticker.textContent = `${price.toFixed(2)}`;
            
            if (price > lastPrice) {
                ticker.className = 'price-ticker';
                ticker.style.color = '#10b981';
            } else if (price < lastPrice) {
                ticker.className = 'price-ticker';
                ticker.style.color = '#ef4444';
            }
            
            lastPrice = price;
            
            // Update 24h high/low
            if (candleData.length > 0) {
                const highs = candleData.map(c => c.high);
                const lows = candleData.map(c => c.low);
                document.getElementById('high24h').textContent = `${Math.max(...highs).toFixed(2)}`;
                document.getElementById('low24h').textContent = `${Math.min(...lows).toFixed(2)}`;
            }
        }

        function updateStats(data) {
            document.getElementById('capital').textContent = `${data.capital}`;
            document.getElementById('activeSignals').textContent = data.signals;
            
            fetch('/api/stats')
                .then(r => r.json())
                .then(s => {
                    stats = s;
                    document.getElementById('totalTrades').textContent = s.total_trades;
                    document.getElementById('winrate').textContent = `${s.winrate.toFixed(1)}%`;
                    
                    const pnlEl = document.getElementById('totalPnl');
                    pnlEl.textContent = `${s.total_pnl.toFixed(2)}`;
                    pnlEl.className = 'stat-value ' + (s.total_pnl >= 0 ? 'green' : 'red');
                    
                    const roiEl = document.getElementById('roi');
                    roiEl.textContent = `${s.roi.toFixed(2)}%`;
                    roiEl.className = 'stat-value ' + (s.roi >= 0 ? 'green' : 'red');
                });
        }

        // ═══════════════════════════════════════════════════════════════
        // CHART RENDERING
        // ═══════════════════════════════════════════════════════════════
        
        function updateChart() {
            if (candleData.length === 0) return;
            
            // Candlestick trace
            const candleTrace = {
                x: candleData.map(c => c.time),
                open: candleData.map(c => c.open),
                high: candleData.map(c => c.high),
                low: candleData.map(c => c.low),
                close: candleData.map(c => c.close),
                type: 'candlestick',
                name: 'Price',
                increasing: {line: {color: '#10b981', width: 2}},
                decreasing: {line: {color: '#ef4444', width: 2}},
                whiskerwidth: 0.5
            };
            
            const traces = [candleTrace];
            const shapes = [];
            const annotations = [];
            
            // Add TP/SL lines for active signals
            activeSignals.forEach((signal, idx) => {
                const color = signal.direction === 'LONG' ? '#10b981' : '#ef4444';
                
                // Entry line
                shapes.push({
                    type: 'line',
                    x0: candleData[Math.max(0, candleData.length - 50)].time,
                    x1: candleData[candleData.length - 1].time,
                    y0: signal.entry,
                    y1: signal.entry,
                    line: {color: color, width: 2, dash: 'solid'}
                });
                
                // TP line
                shapes.push({
                    type: 'line',
                    x0: candleData[Math.max(0, candleData.length - 50)].time,
                    x1: candleData[candleData.length - 1].time,
                    y0: signal.tp,
                    y1: signal.tp,
                    line: {color: '#10b981', width: 2, dash: 'dash'}
                });
                
                // SL line
                shapes.push({
                    type: 'line',
                    x0: candleData[Math.max(0, candleData.length - 50)].time,
                    x1: candleData[candleData.length - 1].time,
                    y0: signal.sl,
                    y1: signal.sl,
                    line: {color: '#ef4444', width: 2, dash: 'dash'}
                });
                
                // Annotations
                annotations.push({
                    x: candleData[candleData.length - 1].time,
                    y: signal.entry,
                    text: `${signal.pattern} ${signal.direction}`,
                    showarrow: true,
                    arrowhead: 2,
                    arrowcolor: color,
                    ax: 40,
                    ay: -40 * (idx + 1),
                    bgcolor: 'rgba(0,0,0,0.7)',
                    bordercolor: color,
                    font: {color: '#fff', size: 10}
                });
            });
            
            const layout = {
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0.3)',
                font: {color: '#fff', family: 'Inter'},
                xaxis: {
                    gridcolor: 'rgba(255,255,255,0.1)',
                    showgrid: true,
                    rangeslider: {visible: false}
                },
                yaxis: {
                    gridcolor: 'rgba(255,255,255,0.1)',
                    showgrid: true,
                    side: 'right'
                },
                shapes: shapes,
                annotations: annotations,
                margin: {l: 10, r: 60, t: 10, b: 40},
                hovermode: 'x unified'
            };
            
            const config = {responsive: true, displayModeBar: false};
            Plotly.newPlot('tradingChart', traces, layout, config);
        }

        // ═══════════════════════════════════════════════════════════════
        // UI RENDERING
        // ═══════════════════════════════════════════════════════════════
        
        function renderSignals() {
            const container = document.getElementById('signalsContainer');
            
            if (activeSignals.length === 0) {
                container.innerHTML = '<div style="text-align:center;color:#888;padding:20px;">No active signals</div>';
                return;
            }
            
            container.innerHTML = activeSignals.map(s => `
                <div class="signal-item">
                    <div class="signal-header">
                        <span class="signal-pattern">${s.pattern}</span>
                        <span class="signal-badge badge-${s.direction.toLowerCase()}">${s.direction}</span>
                    </div>
                    <div class="signal-prices">
                        <div class="price-item">
                            <div class="price-label">Entry</div>
                            <div class="price-value">${s.entry}</div>
                        </div>
                        <div class="price-item">
                            <div class="price-label">TP</div>
                            <div class="price-value" style="color:#10b981">${s.tp}</div>
                        </div>
                        <div class="price-item">
                            <div class="price-label">SL</div>
                            <div class="price-value" style="color:#ef4444">${s.sl}</div>
                        </div>
                    </div>
                    <div style="margin-top:10px;font-size:0.85em;color:#888;">
                        ${new Date(s.timestamp).toLocaleString()} • x${s.leverage} • ${s.position_size}
                    </div>
                </div>
            `).join('');
        }

        function renderTradeHistory() {
            const container = document.getElementById('tradeHistory');
            
            if (closedTrades.length === 0) {
                container.innerHTML = '<div style="text-align:center;color:#888;padding:20px;">No closed trades</div>';
                return;
            }
            
            container.innerHTML = closedTrades.slice(0, 20).map(t => `
                <div class="trade-item ${t.pnl >= 0 ? 'win' : 'loss'}">
                    <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                        <strong>${t.pattern}</strong>
                        <span class="signal-badge badge-${t.direction.toLowerCase()}">${t.direction}</span>
                    </div>
                    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;font-size:0.9em;">
                        <div>Entry: ${t.entry}</div>
                        <div>Exit: ${t.exit_price}</div>
                        <div style="font-weight:700;color:${t.pnl >= 0 ? '#10b981' : '#ef4444'}">
                            ${t.pnl >= 0 ? '+' : ''}${t.pnl.toFixed(2)}
                        </div>
                    </div>
                    <div style="margin-top:6px;font-size:0.8em;color:#888;">
                        ${t.reason} • ROI: ${t.roi >= 0 ? '+' : ''}${t.roi.toFixed(2)}%
                    </div>
                </div>
            `).join('');
        }

        function renderPatternToggles(patterns) {
            const container = document.getElementById('patternsToggle');
            container.innerHTML = Object.entries(patterns).map(([name, cfg]) => `
                <div class="pattern-toggle">
                    <span style="font-weight:600;">${name}</span>
                    <div class="toggle-switch ${cfg.enabled ? 'active' : ''}" 
                         onclick="togglePattern('${name}')">
                        <div class="toggle-slider"></div>
                    </div>
                </div>
            `).join('');
        }

        // ═══════════════════════════════════════════════════════════════
        // CONFIG MANAGEMENT
        // ═══════════════════════════════════════════════════════════════
        
        function loadConfig() {
            fetch('/api/config')
                .then(r => r.json())
                .then(cfg => {
                    document.getElementById('symbol').value = cfg.symbol;
                    document.getElementById('interval').value = cfg.interval;
                    document.getElementById('leverage').value = cfg.leverage;
                    document.getElementById('positionSize').value = cfg.position_size;
                    renderPatternToggles(cfg.patterns);
                });
        }

        function updateConfig() {
            const data = {
                symbol: document.getElementById('symbol').value,
                interval: document.getElementById('interval').value,
                leverage: parseInt(document.getElementById('leverage').value),
                position_size: parseFloat(document.getElementById('positionSize').value)
            };
            
            fetch('/api/update_config', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data)
            })
            .then(r => r.json())
            .then(() => {
                alert('✅ Configuration updated! Restarting...');
                location.reload();
            });
        }

        function togglePattern(name) {
            fetch('/api/toggle_pattern', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({pattern: name})
            })
            .then(() => loadConfig());
        }

        // Initial load
        loadConfig();
    </script>
</body>
</html>
"""

# ═══════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/config')
def get_config():
    return jsonify(config)

@app.route('/api/stats')
def get_stats():
    return jsonify(stats)

@app.route('/api/update_config', methods=['POST'])
def update_config_route():
    global ws_thread
    data = request.json or {}
    
    if 'symbol' in data:
        config['symbol'] = str(data['symbol']).upper()
    if 'interval' in data:
        config['interval'] = str(data['interval'])
    if 'leverage' in data:
        config['leverage'] = max(1, min(125, int(data['leverage'])))
    if 'position_size' in data:
        config['position_size'] = max(1.0, float(data['position_size']))
    
    # Restart WebSocket
    try:
        stop_websocket()
        time.sleep(1)
        load_historical_data()
        start_ws_thread()
    except Exception as e:
        print(f"Config update error: {e}")
    
    return jsonify({'status': 'success', 'config': config})

@app.route('/api/toggle_pattern', methods=['POST'])
def toggle_pattern_route():
    data = request.json or {}
    pattern = data.get('pattern')
    
    if pattern in config['patterns']:
        config['patterns'][pattern]['enabled'] = not config['patterns'][pattern]['enabled']
    
    return jsonify({'status': 'success'})

@socketio.on('connect')
def handle_connect():
    print('✓ Client connected')
    emit('status', {'connected': True})

@socketio.on('disconnect')
def handle_disconnect():
    print('✓ Client disconnected')

# ═══════════════════════════════════════════════════════════════════════════
# THREAD MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

def start_ws_thread():
    global ws_thread
    if ws_thread and ws_thread.is_alive():
        return
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()
    print("✓ WebSocket thread started")

# ═══════════════════════════════════════════════════════════════════════════
# MAIN APPLICATION ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("\n" + "="*70)
    print("    LIVE PATTERN ANALYZER PRO - Starting...")
    print("="*70)
    
    print("\n📊 Loading historical data...")
    if load_historical_data():
        print("✓ Historical data loaded successfully")
    else:
        print("✗ Failed to load historical data")
    
    print("\n🌐 Starting WebSocket connection...")
    start_ws_thread()
    
    print("\n✅ Server ready!")
    print("🔗 Open browser: http://localhost:5000")
    print("💡 Press Ctrl+C to stop\n")
    print("="*70 + "\n")
    
    try:
        socketio.run(app, debug=False, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down...")
        stop_websocket()
        print("✓ Server stopped")
