# ============================================
# GLOBAL CONFIG — FULL PRO VERSION
# ============================================

import os

# ====== MARKET / SYMBOL ======
MARKET_SYMBOL = "BTCUSDT"
MARKET_SYMBOL_FUTURES = MARKET_SYMBOL.lower()
SUPPORTED_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]

# ====== HISTORICAL DATA RANGE ======
HIST_INTERVAL = "30m"
HIST_START_YEAR = 2025
HIST_START_MONTH = 1
HIST_START_DAY = 1
HIST_END_YEAR = 2025
HIST_END_MONTH = 12
HIST_END_DAY = 8
HIST_LIMIT = 500

# ====== DATA FILES ======
CSV_KLINES_FILE = f"{HIST_START_MONTH}_{HIST_START_YEAR}_{HIST_INTERVAL}_{HIST_END_YEAR}_{HIST_END_MONTH}_{MARKET_SYMBOL}_klines.csv"
SQLITE_DB_FILE = f"{HIST_START_MONTH}_{HIST_START_YEAR}_{HIST_INTERVAL}_{HIST_END_YEAR}_{HIST_END_MONTH}_{MARKET_SYMBOL}_klines.db"

# ====== ACCOUNT / BALANCE ======
INITIAL_CAPITAL = 1000.0
BASE_POSITION_USD = 10.0
DEFAULT_LEVERAGE = 75
MAX_LEVERAGE = 125
MIN_LEVERAGE = 1

# Trading Fees
FEE_RATE = 0.0004
MAKER_FEE_RATE = 0.0002
TAKER_FEE_RATE = 0.0004

# ====== RISK MANAGEMENT ======
RISK_MAX_DRAWDOWN = 0.35          # 35% max drawdown
RISK_PER_TRADE = 0.02             # 2% risk per trade
RISK_MAX_OPEN_TRADES = 3          # limit parallel positions
SLIPPAGE_PCT = 0.0005             # 0.05% slippage
LIQUIDATION_BUFFER = 0.015        # 1.5% before liquidation warning
AUTO_CLOSE_DD = True

# ====== SIGNAL ENGINE ======
PRE_EXIT_PCT = 0
USER_DELAY_CANDLES = 0
MIN_PATTERN_SCORE = 0.60
MIN_PATTERN_SCALE = 0.25
MIN_PATTERN_CANDLE_PCT = 0.25
ENABLE_MULTI_TIMEFRAME_CONFIRM = True
TIMEFRAME_CONFIRM_LIST = ["5m", "15m", "1h"]

# ====== CHART / RENDERING ======
CHART_HEIGHT = 20
CANDLE_WIDTH = 15
ASCII_BARS = True
ASCII_MARKERS = True

# ====== LIVE / DEMO MODES ======
LIVE_DEMO_ENABLE = False
LIVE_REAL_ENABLE = False
DEMO_BALANCE = 2000.0
DEMO_TRADE_DELAY_SEC = 0.5

# ====== BINANCE ======
FUTURES_WS_ENDPOINT = "wss://fstream.binance.com/ws"
REST_API_BASE = "https://fapi.binance.com"
REQUEST_TIMEOUT = 15
SLEEP_BETWEEN_REQS = 0.25
MAX_RETRY = 5
RETRY_DELAY = 2

# ====== TELEGRAM ======
TELEGRAM_ENABLED = True
TELEGRAM_BOT_TOKEN = "PASTE_YOUR_TOKEN"
TELEGRAM_CHAT_ID = "PASTE_CHAT_ID"
TELEGRAM_PARSE_MODE = "Markdown"

# ====== LOGGING ======
ENABLE_LOGGING = True
LOG_FILE = "trading_engine_log.txt"
MAX_LOG_SIZE_MB = 20
LOG_AUTO_ROTATE = True
LOG_LEVEL = "DEBUG"  # INFO / WARNING / ERROR / DEBUG

# ====== PATTERN WEIGHTS ======
PATTERN_WEIGHTS = {
    "trend_strength": 0.35,
    "volume_confirmation": 0.25,
    "volatility_regime": 0.15,
    "candle_shape": 0.15,
    "time_of_day": 0.10,
}

# ====== PATTERNS CONFIG (EXTENDED) ======
PATTERN_CONFIG = {
    "Bullish Flag":          {"tp":1.0,"sl":0.25,"scale":0.1,"candle_pct":0.3,"priority":2},
    "Bearish Flag":          {"tp":1.0,"sl":0.25,"scale":0.1,"candle_pct":0.3,"priority":2},

    "Ascending Triangle":    {"tp":1.0,"sl":0.25,"scale":0.8,"candle_pct":0.3,"priority":1},
    "Descending Triangle":   {"tp":1.0,"sl":0.25,"scale":0.8,"candle_pct":0.3,"priority":1},
    "Symmetrical Triangle":  {"tp":0.5,"sl":0.25,"scale":0.8,"candle_pct":0.3,"priority":3},

    "Rising Wedge":          {"tp":0.25,"sl":0.5,"scale":0.4,"candle_pct":0.4,"priority":3},
    "Falling Wedge":         {"tp":0.25,"sl":0.5,"scale":0.4,"candle_pct":0.4,"priority":3},

    "FVG Up":                {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.7,"priority":2},
    "FVG Down":              {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.7,"priority":2},

    "BOS Up":                {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6,"priority":1},
    "BOS Down":              {"tp":0.5,"sl":0.25,"scale":0.6,"candle_pct":0.6,"priority":1},

    "Head & Shoulders":      {"tp":0.75,"sl":0.5,"scale":0.8,"candle_pct":0.8,"priority":1},
    "Inverse H&S":           {"tp":0.75,"sl":0.5,"scale":0.8,"candle_pct":0.8,"priority":1},

    # Additional premium patterns
    "Double Top":            {"tp":0.6,"sl":0.3,"scale":0.5,"candle_pct":0.4,"priority":1},
    "Double Bottom":         {"tp":0.6,"sl":0.3,"scale":0.5,"candle_pct":0.4,"priority":1},
    "Triple Tap":            {"tp":0.8,"sl":0.4,"scale":0.6,"candle_pct":0.5,"priority":1},
    "Liquidity Grab":        {"tp":1.2,"sl":0.25,"scale":0.9,"candle_pct":0.6,"priority":0},
    "Order Block":           {"tp":1.0,"sl":0.3,"scale":0.7,"candle_pct":0.7,"priority":0},
    "Breaker Block":         {"tp":1.0,"sl":0.25,"scale":0.75,"candle_pct":0.6,"priority":0},
}

# ====== ADVANCED SETTINGS ======
ENABLE_VOLATILITY_FILTER = True
VOLATILITY_MIN = 0.20
VOLATILITY_MAX = 3.50

ENABLE_VOLUME_FILTER = True
VOLUME_MIN_MULTIPLIER = 1.25

ENABLE_TIME_FILTER = True
DISABLED_TIME_WINDOWS = [
    ("00:00", "01:00"),
    ("04:00", "05:00"),
]

# ====== BROADCAST MODE ======
BROADCAST_TG_SIGNALS = True
BROADCAST_TG_TRADES = True
BROADCAST_TG_ERRORS = True
BROADCAST_TG_SUMMARY = True

# ====== FAILSAFE ======
AUTO_RECONNECT_WS = True
SAFE_SHUTDOWN_ON_ERROR = True
PERSIST_STATE_FILE = "engine_state.json"
