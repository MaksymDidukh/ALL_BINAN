#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Полный торговый скрипт — BACKTEST + LIVE TRADING + WEB UI (Flask + AJAX)
Variant: PRO (patterns, scale, PRE-EXIT, USER ENTRY, ASCII chart)
Цель: единый файл для запуска локально. Конфигурация в начале файла.
Автор: сгенерировано по запросу пользователя.
Условия: Вставь свои BINANCE_API_KEY и BINANCE_API_SECRET в CONFIG.
"""

import os
import time
import json
import math
import hmac
import hashlib
import sqlite3
import threading
import requests
import traceback
import queue
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

# Внешние зависимости:
# pip install flask flask_cors websocket-client pandas numpy
import pandas as pd
import numpy as np
from flask import Flask, jsonify, request, render_template_string, send_file
from flask_cors import CORS
import websocket

# =========================
# CONFIGURATION — VARIABLES
# =========================

# MARKETS / DATA
MARKET_SYMBOL = "BTCUSDT"              # торгуемая пара
HIST_INTERVAL = "15m"                   # интервал кластеров для истории и лайва
HIST_START_YEAR = 2025
HIST_START_MONTH = 5
HIST_START_DAY = 1
HIST_END_YEAR = 2025
HIST_END_MONTH = 12
HIST_END_DAY = 8
HIST_LIMIT = 500                       # batch size при скачивании

CSV_KLINES_FILE = f"{MARKET_SYMBOL}_{HIST_INTERVAL}_{HIST_START_YEAR}{HIST_START_MONTH:02d}_{HIST_END_YEAR}{HIST_END_MONTH:02d}_klines.csv"
SQLITE_DB_FILE = f"{MARKET_SYMBOL}_{HIST_INTERVAL}_{HIST_START_YEAR}{HIST_START_MONTH:02d}_{HIST_END_YEAR}{HIST_END_MONTH:02d}_klines.db"

# CAPITAL / RISK
INITIAL_CAPITAL = 1000.0
BASE_POSITION_USD = 10.0
DEFAULT_LEVERAGE = 30
FEE_RATE = 0.0004  # 0.04% one side

# LIVE / DEMO switches
LIVE_DEMO_ENABLE = False
AUTO_TRADING_ENABLE = False     # Авто-исполнение ордеров через API (включай с осторожностью)
PAPER_TRADING = True            # Если True - симуляция ордеров, не реальный API
USE_MARKET_ORDERS = True

# BINANCE API (заполни, если хочешь автоторговлю)
BINANCE_API_KEY = "YOUR_BINANCE_API_KEY"
BINANCE_API_SECRET = "YOUR_BINANCE_API_SECRET"

# Endpoints
FUTURES_REST_BASE = "https://fapi.binance.com"
FUTURES_WS_BASE = "wss://fstream.binance.com/ws"

# PATTERNS CONFIG (scale и др.)
PATTERN_CONFIG = {
    "Bullish Flag":       {"tp":1.0,"sl":0.25,"scale":0.1,"candle_pct":0.3},
    "Bearish Flag":       {"tp":1.0,"sl":0.25,"scale":0.1,"candle_pct":0.3},
    "Ascending Triangle": {"tp":1.0,"sl":0.25,"scale":0.8,"candle_pct":0.3},
    "Descending Triangle":{"tp":1.0,"sl":0.25,"scale":0.8,"candle_pct":0.3},
    "Symmetrical Triangle":{"tp":0.5,"sl":0.25,"scale":0.8,"candle_pct":0.3},
    "Rising Wedge":       {"tp":0.25,"sl":0.5,"scale":0.4,"candle_pct":0.4},
    "Falling Wedge":      {"tp":0.25,"sl":0.5,"scale":0.4,"candle_pct":0.4},
    "FVG Up":             {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.7},
    "FVG Down":           {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.7},
    "BOS Up":             {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6},
    "BOS Down":           {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6},
    "Head & Shoulders":   {"tp":0.75,"sl":0.5,"scale":0.8,"candle_pct":0.8},
}

# PRE-EXIT & USER ENTRY
PRE_EXIT_PCT = 0.0
USER_DELAY_CANDLES = 0

# ASCII chart display
CHART_HEIGHT = 18
CANDLE_WIDTH = 9

# Logging
LOG_FILE = "trading_engine.log"

# Threading / Queues
LIVE_QUEUE = queue.Queue()
EVENT_QUEUE = queue.Queue()

# Timeouts
REQUEST_TIMEOUT = 15
SLEEP_BETWEEN_REQS = 0.25

# Flask UI
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5000

# =========================
# SIMPLE LOGGER
# =========================

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
    line = f"{ts} | {level:<5} | {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# =========================
# HELPERS: TIME & SIGNING
# =========================

def ts_ms_from_dt(y:int,m:int,d:int,h:int=0,minute:int=0,second:int=0) -> int:
    return int(datetime(y,m,d,h,minute,second,tzinfo=timezone.utc).timestamp() * 1000)

def utcnow_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def sign_payload(payload: Dict[str, Any], secret: str) -> str:
    qs = "&".join([f"{k}={payload[k]}" for k in sorted(payload.keys())])
    signature = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return signature

# =========================
# SQLITE / CSV STORAGE
# =========================

def ensure_sqlite_schema(db_file: str = SQLITE_DB_FILE) -> None:
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS klines (
      timestamp INTEGER PRIMARY KEY,
      tstamp TEXT,
      open REAL,
      high REAL,
      low REAL,
      close REAL,
      volume REAL,
      interval TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER,
      pattern TEXT,
      side TEXT,
      entry_price REAL,
      exit_price REAL,
      pnl REAL,
      size_usd REAL,
      notes TEXT
    )""")
    conn.commit()
    conn.close()

def save_df_to_sqlite(df: pd.DataFrame, db_file: str = SQLITE_DB_FILE) -> None:
    ensure_sqlite_schema(db_file)
    conn = sqlite3.connect(db_file)
    df_to_save = df.copy()
    # ensure timestamp index as integer
    df_to_save["timestamp"] = (df_to_save.index.view(np.int64) // 10**6).astype(int)
    df_to_save["tstamp"] = df_to_save.index.astype(str)
    df_to_save = df_to_save[["timestamp","tstamp","open","high","low","close","volume"]]
    df_to_save.to_sql("klines", conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()

# =========================
# BINANCE HISTORICAL DATA LOADER
# =========================

def binance_klines_rest(symbol: str, interval: str, start_ts: Optional[int] = None, end_ts: Optional[int] = None, limit: int = 1000) -> pd.DataFrame:
    url = FUTURES_REST_BASE + "/fapi/v1/klines"
    all_data = []
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ts: params["startTime"] = start_ts
    if end_ts: params["endTime"] = end_ts
    # iterative fetch: adjust startTime if needed
    while True:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            log(f"REST API error {resp.status_code} {resp.text}", "ERROR")
            time.sleep(2)
            continue
        batch = resp.json()
        if not batch:
            break
        all_data.extend(batch)
        if len(batch) < limit:
            break
        last_ts = batch[-1][0]
        params["startTime"] = last_ts + 1
        time.sleep(SLEEP_BETWEEN_REQS)
    if not all_data:
        return pd.DataFrame()
    df = pd.DataFrame(all_data, columns=[
        "open_time","open","high","low","close","volume","close_time",
        "quote_vol","trades","tb_base","tb_quote","ignore"
    ])
    # convert
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("open_time")
    df = df[["open","high","low","close","volume"]].astype(float)
    df.index.name = "timestamp"
    df.sort_index(inplace=True)
    return df

def download_history_if_needed() -> pd.DataFrame:
    # load CSV if exists and sizeable
    if os.path.exists(CSV_KLINES_FILE) and os.path.getsize(CSV_KLINES_FILE) > 100:
        try:
            df = pd.read_csv(CSV_KLINES_FILE, parse_dates=["timestamp"], index_col="timestamp")
            df.index = pd.to_datetime(df.index, utc=True)
            log(f"Loaded CSV local history {CSV_KLINES_FILE}")
            return df
        except Exception as e:
            log("Failed loading CSV, will re-download: " + str(e), "WARN")
    # download
    log("Downloading history from Binance Futures API...")
    start_ts = ts_ms_from_dt(HIST_START_YEAR,HIST_START_MONTH,HIST_START_DAY)
    end_ts = ts_ms_from_dt(HIST_END_YEAR,HIST_END_MONTH,HIST_END_DAY,23,59,59)
    df_parts = []
    cur_start = start_ts
    while cur_start < end_ts:
        batch = binance_klines_rest(MARKET_SYMBOL, HIST_INTERVAL, start_ts=cur_start, end_ts=end_ts, limit=HIST_LIMIT)
        if batch.empty:
            break
        df_parts.append(batch)
        last_ts = int(batch.index[-1].view(np.int64) // 10**6)
        cur_start = last_ts + 1
        # progress log
        log(f"Downloaded {len(batch)} candles, last ts {batch.index[-1]}")
    if not df_parts:
        log("No history downloaded.", "ERROR")
        return pd.DataFrame()
    df = pd.concat(df_parts)
    df = df[~df.index.duplicated(keep="first")]
    df.sort_index(inplace=True)
    df.to_csv(CSV_KLINES_FILE)
    save_df_to_sqlite(df)
    log(f"Saved CSV {CSV_KLINES_FILE} and SQLite {SQLITE_DB_FILE}")
    return df

# =========================
# PATTERN DETECTION MODULE
# =========================

def local_extrema(df: pd.DataFrame, window: int = 5):
    high_max = df['high'].rolling(window, center=True).max()
    low_min = df['low'].rolling(window, center=True).min()
    highs = df[df['high']==high_max].index.tolist()
    lows = df[df['low']==low_min].index.tolist()
    return highs,lows

def detect_flags(df: pd.DataFrame, lookback: int = 20, threshold: float = 0.005):
    patterns = []
    for i in range(lookback, len(df)):
        seg = df['close'].iloc[i-lookback:i]
        diff = seg.max() - seg.min()
        if seg.min() <= 0: continue
        if diff / seg.min() < threshold:
            slope = (seg.iloc[-1] - seg.iloc[0]) / seg.iloc[0]
            if slope > 0.0:
                patterns.append((seg.index[0], seg.index[-1], "Bullish Flag"))
            elif slope < 0.0:
                patterns.append((seg.index[0], seg.index[-1], "Bearish Flag"))
    return patterns

def detect_triangles(df: pd.DataFrame, lookback: int = 24):
    patterns = []
    for i in range(lookback, len(df)):
        seg = df.iloc[i-lookback:i]
        x = np.arange(len(seg))
        try:
            high_coef = np.polyfit(x, seg['high'].values, 1)[0]
            low_coef = np.polyfit(x, seg['low'].values, 1)[0]
        except Exception:
            continue
        # heuristics
        if abs(high_coef) < 0.001 and low_coef > 0.0:
            patterns.append((seg.index[0], seg.index[-1], "Ascending Triangle"))
        elif abs(low_coef) < 0.001 and high_coef < 0.0:
            patterns.append((seg.index[0], seg.index[-1], "Descending Triangle"))
        elif high_coef < 0 and low_coef > 0:
            patterns.append((seg.index[0], seg.index[-1], "Symmetrical Triangle"))
    return patterns

def detect_fvg(df: pd.DataFrame, gap_perc: float = 0.002):
    patterns = []
    for i in range(2, len(df)):
        prev_high, prev_low = df['high'].iloc[i-2], df['low'].iloc[i-2]
        curr_open = df['open'].iloc[i]
        if curr_open > prev_high * (1 + gap_perc):
            patterns.append((df.index[i-2], df.index[i], "FVG Up"))
        elif curr_open < prev_low * (1 - gap_perc):
            patterns.append((df.index[i-2], df.index[i], "FVG Down"))
    return patterns

def detect_bos_choch(df: pd.DataFrame, window: int = 5):
    patterns = []
    highs, lows = local_extrema(df, window)
    for i in range(1, len(highs)):
        try:
            if df.loc[highs[i], 'high'] > df.loc[highs[i-1], 'high']:
                patterns.append((highs[i-1], highs[i], "BOS Up"))
        except Exception:
            continue
    for i in range(1, len(lows)):
        try:
            if df.loc[lows[i], 'low'] < df.loc[lows[i-1], 'low']:
                patterns.append((lows[i-1], lows[i], "BOS Down"))
        except Exception:
            continue
    return patterns

def detect_head_shoulders(df: pd.DataFrame, window: int = 5, threshold: float = 0.01):
    patterns = []
    for i in range(window, len(df) - window):
        left_max = df['high'].iloc[i-window:i].max()
        head = df['high'].iloc[i]
        right_max = df['high'].iloc[i+1:i+1+window].max()
        if head > left_max * (1 + threshold) and head > right_max * (1 + threshold):
            patterns.append((df.index[i-window], df.index[i+window], "Head & Shoulders"))
    return patterns

def detect_all_patterns(df: pd.DataFrame) -> pd.DataFrame:
    all_p = []
    try:
        all_p += detect_flags(df)
        all_p += detect_triangles(df)
        all_p += detect_fvg(df)
        all_p += detect_bos_choch(df)
        all_p += detect_head_shoulders(df)
    except Exception as e:
        log("Pattern detection error: " + str(e), "ERROR")
    if not all_p:
        return pd.DataFrame(columns=["Start","End","Pattern"])
    patterns_df = pd.DataFrame(all_p, columns=["Start","End","Pattern"])
    patterns_df["Start"] = pd.to_datetime(patterns_df["Start"], utc=True)
    patterns_df["End"] = pd.to_datetime(patterns_df["End"], utc=True)
    patterns_df = patterns_df.sort_values("Start").reset_index(drop=True)
    return patterns_df

# =========================
# ASCII PRO-CANDLE RENDERER (variant compact)
# =========================

ANSI_RED = "\033[91m"
ANSI_GREEN = "\033[92m"
ANSI_RESET = "\033[0m"

def render_pro_ascii_candles(price_path: List[Dict[str, float]], price_index_list: List[pd.Timestamp], pre_idx: Optional[int] = None, user_idx: Optional[int] = None, height: int = CHART_HEIGHT, width: int = CANDLE_WIDTH) -> str:
    # returns string (not prints) for better control
    n = len(price_path)
    H = height
    W = width
    highs = [p["high"] for p in price_path]
    lows = [p["low"] for p in price_path]
    max_p = max(highs) if highs else 1.0
    min_p = min(lows) if lows else 0.0
    span = max_p - min_p if max_p > min_p else 1.0

    def row_of(price):
        rel = (price - min_p) / span
        return int((1 - rel) * (H - 1))

    canvas = [list(" " * (n * W)) for _ in range(H)]
    color_map = [" "] * n
    label_row = [" " * W for _ in range(n)]

    for i, p in enumerate(price_path):
        col0 = i * W
        col_mid = col0 + W // 2
        o = p["open"]; c = p["close"]; h = p["high"]; l = p["low"]
        row_high = row_of(h)
        row_low = row_of(l)
        row_open = row_of(o)
        row_close = row_of(c)
        # wick
        for r in range(min(row_high, row_low), max(row_high, row_low) + 1):
            try:
                canvas[r][col_mid] = "|"
            except Exception:
                pass
        # body
        body_top = min(row_open, row_close)
        body_bottom = max(row_open, row_close)
        left = col0 + 1
        right = col0 + W - 2
        if body_top == body_bottom:
            canvas[body_top][col_mid] = "■"
        else:
            for cc in range(left, right+1):
                canvas[body_top][cc] = "─"
                canvas[body_bottom][cc] = "─"
            for rr in range(body_top+1, body_bottom):
                for cc in range(left, right+1):
                    canvas[rr][cc] = "█"
            try:
                canvas[body_top][left] = "┌"; canvas[body_top][right] = "┐"
                canvas[body_bottom][left] = "└"; canvas[body_bottom][right] = "┘"
            except Exception:
                pass
        is_green = c >= o
        color_map[i] = "g" if is_green else "r"
        lbl = " "
        if i == 0: lbl = "S"
        if i == n-1: lbl = "E"
        if pre_idx is not None and i == pre_idx: lbl = "P"
        if user_idx is not None and i == user_idx:
            lbl = "U" if lbl == " " else lbl + "U"
        label_row[i] = lbl.center(W)

    # build final string
    out_lines = []
    # label row
    out_lines.append("  " + "".join(label_row))
    for r in range(H):
        row_builder = []
        for i in range(n):
            col0 = i * W
            seg = "".join(canvas[r][col0:col0+W])
            marker = color_map[i]
            if marker == "g":
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
            row_builder.append(seg)
        out_lines.append("  " + "".join(row_builder))
    # time row
    times = []
    for ts in price_index_list:
        try:
            times.append(str(ts.strftime("%m-%d %H:%M")).center(W))
        except Exception:
            times.append("".center(W))
    out_lines.append("  " + "".join(times))
    return "\n".join(out_lines)

# =========================
# TRADE SIMULATION / EXECUTOR
# =========================

class PaperBroker:
    def __init__(self):
        self.cash = INITIAL_CAPITAL
        self.positions = []  # list of dicts
        self.trade_log = []

    def open_position(self, side: str, entry_price: float, size_usd: float, leverage: int = DEFAULT_LEVERAGE, tp: Optional[float] = None, sl: Optional[float] = None, meta: str = ""):
        # size_usd - amount in USD allocated to position (margin)
        notional = size_usd * leverage
        qty = notional / entry_price  # approximate qty of asset
        pos = {
            "id": len(self.positions) + 1,
            "side": side,
            "entry_price": entry_price,
            "size_usd": size_usd,
            "leverage": leverage,
            "qty": qty,
            "tp": tp,
            "sl": sl,
            "open_ts": int(datetime.now(timezone.utc).timestamp() * 1000),
            "meta": meta
        }
        self.positions.append(pos)
        log(f"PAPER OPEN {side} entry {entry_price:.2f} size_usd {size_usd:.2f} lev {leverage}", "TRADE")
        return pos

    def close_position(self, pos_id: int, exit_price: float):
        pos = None
        for p in self.positions:
            if p["id"] == pos_id:
                pos = p
                break
        if not pos:
            return None
        # compute pnl
        if pos["side"] == "LONG":
            pnl = (exit_price - pos["entry_price"]) * pos["qty"]
        else:
            pnl = (pos["entry_price"] - exit_price) * pos["qty"]
        # fees
        fee = pos["size_usd"] * pos["leverage"] * FEE_RATE * 2
        pnl_net = pnl - fee
        self.cash += pnl_net
        self.positions.remove(pos)
        rec = {
            "id": pos["id"],
            "ts": int(datetime.now(timezone.utc).timestamp() * 1000),
            "pattern": pos.get("meta",""),
            "side": pos["side"],
            "entry_price": pos["entry_price"],
            "exit_price": exit_price,
            "pnl": pnl_net,
            "size_usd": pos["size_usd"],
            "notes": ""
        }
        self.trade_log.append(rec)
        log(f"PAPER CLOSE id {pos['id']} pnl_net {pnl_net:.2f}", "TRADE")
        # persist to sqlite
        try:
            conn = sqlite3.connect(SQLITE_DB_FILE)
            c = conn.cursor()
            c.execute("INSERT INTO trades (ts, pattern, side, entry_price, exit_price, pnl, size_usd, notes) VALUES (?,?,?,?,?,?,?,?)",
                      (rec["ts"], rec["pattern"], rec["side"], rec["entry_price"], rec["exit_price"], rec["pnl"], rec["size_usd"], rec["notes"]))
            conn.commit()
            conn.close()
        except Exception as e:
            log("Failed to persist trade: " + str(e), "ERROR")
        return rec

# Real executor (Binance futures REST signed)
class BinanceFuturesExecutor:
    def __init__(self, api_key: str, api_secret: str):
        self.key = api_key
        self.secret = api_secret
        self.base = FUTURES_REST_BASE
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.key})

    def _signed_request(self, method: str, path: str, payload: Dict[str, Any]):
        payload["timestamp"] = int(time.time() * 1000)
        qs = "&".join([f"{k}={payload[k]}" for k in sorted(payload.keys())])
        signature = hmac.new(self.secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        url = self.base + path + "?" + qs + "&signature=" + signature
        if method == "POST":
            r = self.session.post(url, timeout=REQUEST_TIMEOUT)
        elif method == "DELETE":
            r = self.session.delete(url, timeout=REQUEST_TIMEOUT)
        else:
            r = self.session.get(url, timeout=REQUEST_TIMEOUT)
        try:
            j = r.json()
        except Exception:
            log(f"Binance response not json: {r.text}", "ERROR")
            return {"error": r.text}
        if r.status_code >= 400:
            log(f"Binance API error {r.status_code} {j}", "ERROR")
        return j

    def place_order(self, symbol: str, side: str, order_type: str, quantity: float, price: Optional[float] = None, reduce_only: bool = False, time_in_force: str = "GTC"):
        path = "/fapi/v1/order"
        payload = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity,
            "reduceOnly": "true" if reduce_only else "false",
        }
        if price is not None:
            payload["price"] = price
            payload["timeInForce"] = time_in_force
        return self._signed_request("POST", path, payload)

    def change_margin_type(self, symbol: str, marginType: str = "ISOLATED"):
        path = "/fapi/v1/marginType"
        payload = {"symbol": symbol, "marginType": marginType}
        return self._signed_request("POST", path, payload)

    def set_leverage(self, symbol: str, leverage: int):
        path = "/fapi/v1/leverage"
        payload = {"symbol": symbol, "leverage": leverage}
        return self._signed_request("POST", path, payload)

# =========================
# BACKTEST ENGINE
# =========================

def simulate_trade_for_pattern(df: pd.DataFrame, row: pd.Series, broker: PaperBroker, user_delay_candles: int = USER_DELAY_CANDLES):
    start_ts, end_ts, pattern_name = row["Start"], row["End"], row["Pattern"]
    cfg = PATTERN_CONFIG.get(pattern_name, {})
    tp_pct = cfg.get("tp", 0.1)
    sl_pct = cfg.get("sl", 0.25)
    scale = cfg.get("scale", 0.5)
    candle_pct = cfg.get("candle_pct", 0.5)

    try:
        entry_candle = df.loc[start_ts]
        exit_candle = df.loc[end_ts]
    except Exception:
        log(f"DATA ERROR | {start_ts} → {end_ts}", "WARN")
        return None

    entry_price = float(entry_candle["close"])
    exit_price = float(exit_candle["close"])
    direction = "LONG"
    if "Bear" in pattern_name or "Descending" in pattern_name or "Down" in pattern_name:
        direction = "SHORT"

    candle_size = (float(entry_candle["high"]) - float(entry_candle["low"])) / max(float(entry_candle["low"]), 1e-9)
    if candle_size < candle_pct / 100.0:
        # too small
        return None

    slice_df = df.loc[start_ts:end_ts][["open","high","low","close"]]
    if slice_df.empty:
        return None
    price_path = slice_df.to_dict("records")
    price_index_list = slice_df.index.tolist()

    pre_exit_idx = max(0, int((len(price_path)-1) * PRE_EXIT_PCT))
    user_entry_idx = min(len(price_path)-1, pre_exit_idx + user_delay_candles)
    pre_exit_price = price_path[pre_exit_idx]["close"]
    user_entry_price = price_path[user_entry_idx]["close"]

    # compute pnl sim
    price_move = ((exit_price - user_entry_price) / user_entry_price) if direction == "LONG" else ((user_entry_price - exit_price) / user_entry_price)
    position_size_usd = BASE_POSITION_USD * (1.0 + scale)
    pnl = price_move * position_size_usd * DEFAULT_LEVERAGE
    fee = position_size_usd * DEFAULT_LEVERAGE * FEE_RATE * 2
    pnl_net = pnl - fee
    roi_trade = (pnl_net / position_size_usd) * 100 if position_size_usd != 0 else 0.0

    # create ascii chart string
    chart = render_pro_ascii_candles(price_path[-min(30,len(price_path)):], price_index_list[-min(30,len(price_index_list)):], pre_idx=pre_exit_idx, user_idx=user_entry_idx)

    # execute via broker (paper)
    pos = broker.open_position(direction, user_entry_price, position_size_usd, DEFAULT_LEVERAGE, tp=None, sl=None, meta=pattern_name)
    # simulate exit immediately at exit_price
    rec = broker.close_position(pos["id"], exit_price)

    result = {
        "pattern": pattern_name,
        "entry_price": float(user_entry_price),
        "exit_price": float(exit_price),
        "entry_date": price_index_list[user_entry_idx],
        "exit_date": exit_price,
        "pnl_net": float(rec["pnl"]),
        "roi": float(roi_trade),
        "candles": len(price_path),
        "candle_size_pct": candle_size * 100,
        "chart": chart
    }
    return result

def backtest_patterns(df: pd.DataFrame, patterns_df: pd.DataFrame):
    patterns_df = patterns_df.sort_values("Start").reset_index(drop=True)
    stats = []
    trades_details = []
    broker = PaperBroker()
    for pattern_name, group in patterns_df.groupby("Pattern"):
        group = group.reset_index(drop=True)
        capital = INITIAL_CAPITAL
        wins = 0; losses = 0
        for idx, row in group.iterrows():
            try:
                trade_info = simulate_trade_for_pattern(df, row, broker)
            except Exception as e:
                log("simulate_trade error: " + str(e), "ERROR")
                trade_info = None
            if trade_info is None:
                continue
            capital += trade_info["pnl_net"]
            if trade_info["pnl_net"] > 0:
                wins += 1
            else:
                losses += 1
            trades_details.append(trade_info)
        total = wins + losses
        winrate = wins / total * 100 if total > 0 else 0.0
        roi = (capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
        stats.append({
            "Pattern": pattern_name,
            "Signals": len(group),
            "Trades": total,
            "Wins": wins,
            "Losses": losses,
            "Winrate%": round(winrate,2),
            "ROI%": round(roi,3),
            "CapitalEnd": round(capital,3)
        })
    stats_df = pd.DataFrame(stats).sort_values("ROI%", ascending=False)
    trades_df = pd.DataFrame(trades_details)
    return stats_df, trades_df

# =========================
# LIVE STREAM HANDLER
# =========================

class LiveEngine:
    def __init__(self, symbol: str = MARKET_SYMBOL, interval: str = HIST_INTERVAL):
        self.symbol = symbol.lower()
        self.interval = interval
        self.ws_url = f"{FUTURES_WS_BASE}/{self.symbol}@kline_{self.interval}"
        self.ws = None
        self.live_df = pd.DataFrame(columns=["open","high","low","close","volume"])
        self._stop = threading.Event()
        self.broker = PaperBroker() if PAPER_TRADING else BinanceFuturesExecutor(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.lock = threading.Lock()

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if "k" not in data:
                return
            k = data["k"]
            is_closed = k.get("x", False)
            ts = pd.to_datetime(k["t"], unit="ms", utc=True)
            candle = {
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
            }
            with self.lock:
                self.live_df.loc[ts] = candle
                self.live_df.sort_index(inplace=True)
                # keep limited history
                if len(self.live_df) > 2000:
                    self.live_df = self.live_df.iloc[-2000:]
            # when candle closed -> check patterns on the recent window
            if is_closed:
                EVENT_QUEUE.put(("kline_closed", ts))
        except Exception as e:
            log("Live message process error: " + str(e), "ERROR")

    def _on_open(self, ws):
        log("Live WebSocket connected.", "INFO")

    def _on_close(self, ws, close_status_code, close_msg):
        log("Live WebSocket closed.", "WARN")

    def start(self):
        def run_ws():
            while not self._stop.is_set():
                try:
                    self.ws = websocket.WebSocketApp(self.ws_url,
                                                     on_message=self._on_message,
                                                     on_open=self._on_open,
                                                     on_close=self._on_close)
                    self.ws.run_forever(ping_interval=30, ping_timeout=10)
                except Exception as e:
                    log("WebSocket crashed: " + str(e), "ERROR")
                    time.sleep(2)
        t = threading.Thread(target=run_ws, daemon=True)
        t.start()
        # event loop thread
        t2 = threading.Thread(target=self._event_loop, daemon=True)
        t2.start()

    def stop(self):
        self._stop.set()
        try:
            if self.ws: self.ws.close()
        except Exception:
            pass

    def _event_loop(self):
        while not self._stop.is_set():
            try:
                evt = EVENT_QUEUE.get(timeout=1)
            except queue.Empty:
                continue
            if not evt:
                continue
            evt_type, payload = evt
            if evt_type == "kline_closed":
                with self.lock:
                    df = self.live_df.copy()
                if len(df) < 50:
                    continue
                pats = detect_all_patterns(df)
                if not pats.empty:
                    # choose latest pattern
                    last = pats.iloc[-1]
                    log(f"LIVE PATTERN DETECTED: {last['Pattern']} {last['Start']} → {last['End']}", "SIGNAL")
                    # put to live queue
                    LIVE_QUEUE.put((last, df))
                    # if auto trading enabled, execute
                    if AUTO_TRADING_ENABLE:
                        try:
                            self._execute_live_signal(last, df)
                        except Exception as e:
                            log("Auto execution error: " + str(e), "ERROR")

    def _execute_live_signal(self, row: pd.Series, df: pd.DataFrame):
        pattern_name = row["Pattern"]
        cfg = PATTERN_CONFIG.get(pattern_name, {})
        scale = cfg.get("scale", 0.5)
        size_usd = BASE_POSITION_USD * (1.0 + scale)
        direction = "LONG"
        if "Bear" in pattern_name or "Descending" in pattern_name or "Down" in pattern_name:
            direction = "SHORT"
        # entry: last closed candle close
        entry_price = float(df.iloc[-1]["close"])
        if PAPER_TRADING:
            pos = self.broker.open_position(direction, entry_price, size_usd, DEFAULT_LEVERAGE, meta=pattern_name)
            # for demo, set exit after N candles by scheduling a close
            # here we schedule close in background after 3 candles (approx)
            def closer(pid):
                time.sleep(int(self._interval_seconds() * 3))
                # take current price snapshot
                with self.lock:
                    cur_price = float(self.live_df.iloc[-1]["close"])
                self.broker.close_position(pid, cur_price)
            threading.Thread(target=closer, args=(pos["id"],), daemon=True).start()
        else:
            # real trading via REST
            executor = self.broker  # BinanceFuturesExecutor
            # set leverage
            try:
                executor.set_leverage(MARKET_SYMBOL, DEFAULT_LEVERAGE)
            except Exception as e:
                log("Set leverage failed: " + str(e), "ERROR")
            # compute quantity in base asset for market order
            notional = size_usd * DEFAULT_LEVERAGE
            qty = notional / entry_price
            qty = float(round(qty, 6))
            side = "BUY" if direction == "LONG" else "SELL"
            if USE_MARKET_ORDERS:
                # market order uses type MARKET, quantity required
                resp = executor.place_order(MARKET_SYMBOL, side, "MARKET", quantity=qty)
            else:
                resp = executor.place_order(MARKET_SYMBOL, side, "LIMIT", quantity=qty, price=entry_price, time_in_force="GTC")
            log(f"Real order response: {resp}", "TRADE")

    def _interval_seconds(self):
        # support few intervals
        if "m" in self.interval:
            return int(self.interval.replace("m","")) * 60
        if "h" in self.interval:
            return int(self.interval.replace("h","")) * 3600
        return 60

# =========================
# FLASK WEB UI (minimal, AJAX)
# =========================

app = Flask(__name__)
CORS(app)

# In-memory cache
CACHE = {
    "last_history_load": None,
    "history_len": 0,
    "patterns_count": 0,
}

@app.route("/")
def index():
    html = """
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>Trading Engine — Live & Backtest</title>
      <style>
        body { font-family: monospace; background:#0b0b0b; color:#ddd; padding:12px; }
        .box { background:#111; padding:8px; margin:8px 0; border-radius:6px; }
        button { padding:8px 12px; margin:4px; background:#222; color:#fff; border:1px solid #333; }
        pre { white-space:pre-wrap; word-wrap:break-word; }
      </style>
    </head>
    <body>
      <h2>Trading Engine — Live & Backtest</h2>
      <div class="box">
        <button onclick="ajax('/api/load_history')">Load History</button>
        <button onclick="ajax('/api/run_backtest')">Run Backtest</button>
        <button onclick="ajax('/api/start_live')">Start Live</button>
        <button onclick="ajax('/api/stop_live')">Stop Live</button>
        <button onclick="ajax('/api/stats')">Stats</button>
      </div>

      <div id="output" class="box"><pre>Ready.</pre></div>

      <script>
      function ajax(path) {
        document.getElementById('output').innerText = 'Processing...';
        fetch(path).then(r => r.json()).then(j => {
          document.getElementById('output').innerText = JSON.stringify(j, null, 2);
        }).catch(e => {
          document.getElementById('output').innerText = 'Error: ' + e;
        });
      }
      </script>
    </body>
    </html>
    """
    return render_template_string(html)

# API endpoints

HIST_DF = pd.DataFrame()

@app.route("/api/load_history")
def api_load_history():
    global HIST_DF
    try:
        HIST_DF = download_history_if_needed()
        CACHE["last_history_load"] = datetime.now(timezone.utc).isoformat()
        CACHE["history_len"] = len(HIST_DF)
        return jsonify({"status":"ok","text":"history_loaded","rows":len(HIST_DF)})
    except Exception as e:
        log("API load history error: " + str(e), "ERROR")
        return jsonify({"status":"error","error":str(e)}), 500

@app.route("/api/detect_patterns")
def api_detect_patterns():
    global HIST_DF
    if HIST_DF.empty:
        return jsonify({"status":"error","error":"no history loaded"}), 400
    pats = detect_all_patterns(HIST_DF)
    CACHE["patterns_count"] = len(pats)
    # convert timestamps to iso
    out = []
    for _, r in pats.iterrows():
        out.append({"start":str(r["Start"]), "end":str(r["End"]), "pattern":r["Pattern"]})
    return jsonify({"status":"ok","count":len(out),"patterns":out})

@app.route("/api/run_backtest")
def api_run_backtest():
    global HIST_DF
    if HIST_DF.empty:
        return jsonify({"status":"error","error":"no history loaded"}), 400
    pats = detect_all_patterns(HIST_DF)
    stats_df, trades_df = backtest_patterns(HIST_DF, pats)
    stats = stats_df.to_dict(orient="records")
    trades = trades_df.to_dict(orient="records")
    # save csvs
    stats_df.to_csv("stats_full.csv", index=False)
    trades_df.to_csv("trades_full.csv", index=False)
    return jsonify({"status":"ok","stats_count":len(stats),"trades_count":len(trades)})

live_engine_singleton: Optional[LiveEngine] = None
_live_lock = threading.Lock()

@app.route("/api/start_live")
def api_start_live():
    global live_engine_singleton
    with _live_lock:
        if live_engine_singleton is not None:
            return jsonify({"status":"ok","text":"live_already_running"})
        le = LiveEngine(MARKET_SYMBOL, HIST_INTERVAL)
        live_engine_singleton = le
        le.start()
        return jsonify({"status":"ok","text":"live_started"})

@app.route("/api/stop_live")
def api_stop_live():
    global live_engine_singleton
    with _live_lock:
        if live_engine_singleton is None:
            return jsonify({"status":"ok","text":"live_not_running"})
        try:
            live_engine_singleton.stop()
        except Exception as e:
            log("Stop live error: " + str(e), "ERROR")
        live_engine_singleton = None
        return jsonify({"status":"ok","text":"live_stopped"})

@app.route("/api/stats")
def api_stats():
    return jsonify(CACHE)

@app.route("/api/download/<path:filename>")
def api_download(filename):
    if os.path.exists(filename):
        return send_file(filename, as_attachment=True)
    return jsonify({"status":"error","error":"file_not_found"}), 404

# =========================
# STARTUP / CLI
# =========================

def main():
    log("Starting Trading Engine", "START")
    ensure_sqlite_schema()
    # optional auto load history
    try:
        global HIST_DF
        HIST_DF = download_history_if_needed()
        CACHE["last_history_load"] = datetime.now(timezone.utc).isoformat()
        CACHE["history_len"] = len(HIST_DF)
    except Exception as e:
        log("Initial history load failed: " + str(e), "WARN")

    # start flask in thread
    def run_flask():
        try:
            app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False, use_reloader=False)
        except Exception as e:
            log("Flask crashed: " + str(e), "ERROR")
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    # optionally start live engine automatically
    if LIVE_DEMO_ENABLE:
        with _live_lock:
            global live_engine_singleton
            if live_engine_singleton is None:
                live_engine_singleton = LiveEngine(MARKET_SYMBOL, HIST_INTERVAL)
                live_engine_singleton.start()

    # main loop: process LIVE_QUEUE items and print concise info
    try:
        while True:
            try:
                item = LIVE_QUEUE.get(timeout=1)
            except queue.Empty:
                time.sleep(0.1)
                continue
            if not item:
                continue
            last, df = item
            # quick summary
            pat = last["Pattern"]
            start = str(last["Start"])
            end = str(last["End"])
            log(f"QUEUE SIGNAL {pat} {start} → {end}", "SIGNAL")
    except KeyboardInterrupt:
        log("Shutdown requested by user.", "STOP")
        try:
            if live_engine_singleton:
                live_engine_singleton.stop()
        except Exception:
            pass
        log("Exited.", "STOP")

if __name__ == "__main__":
    main()
