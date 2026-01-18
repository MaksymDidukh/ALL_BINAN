import os
import sys
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta

# ================= CONFIG =================

START_DATE = "2025-01-01"
END_DATE   = "2025-12-31"

DAILY_BUY_USD = 10
TIMEFRAME = "1d"

BASE_URL = "https://data.binance.vision/data"
DATA_DIR = "./data"

SPOT_DIR = f"{DATA_DIR}/spot"
FUTURES_DIR = f"{DATA_DIR}/futures"

# =========================================


def ensure_dirs():
    os.makedirs(SPOT_DIR, exist_ok=True)
    os.makedirs(FUTURES_DIR, exist_ok=True)


def daterange(start, end):
    for n in range((end - start).days + 1):
        yield start + timedelta(n)


# ---------- PROGRESS BAR ----------

def progress_bar(current, total, prefix=""):
    bar_len = 30
    filled = int(bar_len * current / total)
    bar = "█" * filled + "░" * (bar_len - filled)
    percent = (current / total) * 100
    sys.stdout.write(
        f"\r{prefix} [{bar}] {percent:6.2f}% ({current}/{total})"
    )
    sys.stdout.flush()
    if current == total:
        print()

# ----------------------------------


def download_zip(url, path):
    r = requests.get(url, timeout=20)
    if r.status_code == 200:
        with open(path, "wb") as f:
            f.write(r.content)
        return True
    return False


def extract_zip(zip_path, out_dir):
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    os.remove(zip_path)


def load_day_kline(symbol, date, market):
    date_str = date.strftime("%Y-%m-%d")

    if market == "spot":
        url = f"{BASE_URL}/spot/daily/klines/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{date_str}.zip"
        out_dir = SPOT_DIR
    else:
        url = f"{BASE_URL}/futures/um/daily/klines/{symbol}/{TIMEFRAME}/{symbol}-{TIMEFRAME}-{date_str}.zip"
        out_dir = FUTURES_DIR

    zip_path = f"{out_dir}/{symbol}.zip"

    if not download_zip(url, zip_path):
        return None

    extract_zip(zip_path, out_dir)

    csv_path = f"{out_dir}/{symbol}-{TIMEFRAME}-{date_str}.csv"
    if not os.path.exists(csv_path):
        return None

    df = pd.read_csv(csv_path, header=None)
    os.remove(csv_path)

    df.columns = [
        "open_time","open","high","low","close","volume",
        "close_time","qav","trades","tb","tq","ignore"
    ]
    return df


def find_first_listing(symbol, market, start, end):
    for day in daterange(start, end):
        df = load_day_kline(symbol, day, market)
        if df is not None:
            return day, df
    return None, None


def find_max_after(symbol, market, start_day, end):
    max_price = 0
    day = start_day
    while day <= end:
        df = load_day_kline(symbol, day, market)
        if df is not None:
            max_price = max(max_price, df["high"].max())
        day += timedelta(days=1)
    return max_price


def main():
    print("=== BINANCE NEW LISTINGS DCA SIMULATION ===")
    ensure_dirs()

    start = datetime.fromisoformat(START_DATE)
    end   = datetime.fromisoformat(END_DATE)

    # Получаем список USDT-пар
    info = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=20).json()
    symbols = [s["symbol"] for s in info["symbols"] if s["quoteAsset"] == "USDT"]

    total_tasks = len(symbols) * 2
    done = 0

    total_spent = 0
    total_final = 0
    results = []

    for symbol in symbols:
        for market in ["spot", "futures"]:
            done += 1
            progress_bar(done, total_tasks, prefix=f"{symbol} {market}")

            first_day, first_df = find_first_listing(symbol, market, start, end)
            if first_df is None:
                continue

            entry = first_df.iloc[0]["open"]
            maxp = find_max_after(symbol, market, first_day, end)

            final = DAILY_BUY_USD * (maxp / entry) if maxp > entry else DAILY_BUY_USD

            total_spent += DAILY_BUY_USD
            total_final += final

            results.append({
                "symbol": symbol,
                "market": market,
                "listing_date": first_day.date(),
                "entry": round(entry, 6),
                "max": round(maxp, 6),
                "final_$": round(final, 2)
            })

    df = pd.DataFrame(results).sort_values("final_$", ascending=False)

    print("\n========== RESULT ==========")
    if not df.empty:
        print(df.head(20).to_string(index=False))
    print("----------------------------------")
    print(f"ПОТРАЧЕНО: {total_spent} USD")
    print(f"СТАЛО:    {round(total_final, 2)} USD")
    if total_spent > 0:
        print(f"ROI:      {(total_final / total_spent - 1) * 100:.2f} %")
    print(f"МОНЕТ:    {len(df)}")


if __name__ == "__main__":
    main()
