import requests
import json
import time
from datetime import datetime, timedelta
import os

DATA_FILE = 'futures_listings_v2.json'
BUY_AMOUNT_USDT = 100
WAIT_DAYS = 7

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_data(data):
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def get_current_futures_symbols():
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        symbols = {}
        for s in data.get('symbols', []):
            if (s.get('contractType') == 'PERPETUAL' and 
                s.get('quoteAsset') == 'USDT' and 
                s.get('status') == 'TRADING'):
                sym = s['symbol']
                onboard = s.get('onboardDate')  # миллисекунды
                symbols[sym] = onboard
        return symbols
    except Exception as e:
        print(f"❌ Ошибка получения фьючерсов: {e}")
        return {}

def get_spot_price(symbol):
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return float(response.json()['price'])
    except:
        return None

def main():
    data = load_data()
    print(f"🚀 Трекер v2 запущен — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Собираем только новые пары и ждём 7 дней перед симуляцией покупки $100...\n")

    while True:
        futures_dict = get_current_futures_symbols()
        current_symbols = set(futures_dict.keys())
        known = set(data.keys())

        # Новые пары
        new_ones = current_symbols - known
        for sym in sorted(new_ones):
            onboard_ms = futures_dict.get(sym)
            listing_time = onboard_ms / 1000 if onboard_ms else time.time()
            data[sym] = {
                "listing_time": listing_time,
                "bought": False,
                "buy_price": None,
                "current_price": None,
                "pnl_usdt": None
            }
            dt = datetime.fromtimestamp(listing_time)
            print(f"🆕 НОВАЯ ПАРА: {sym}  (дата обнаружения: {dt.strftime('%Y-%m-%d %H:%M')})")

        now = time.time()
        print("\n📊 Отчёт по парам, готовым к покупке (прошло >=7 дней):")
        bought_count = 0
        total_invested = 0
        total_pnl = 0

        for sym in list(data.keys()):
            if sym not in current_symbols:
                continue
            info = data[sym]
            days_passed = (now - info["listing_time"]) / 86400

            if not info["bought"] and days_passed >= WAIT_DAYS:
                spot_sym = sym.replace('USDT', '') + 'USDT'
                price = get_spot_price(spot_sym)
                if price:
                    info["bought"] = True
                    info["buy_price"] = round(price, 8)
                    print(f"✅ Прошло {days_passed:.1f} дней → Симулируем покупку ${BUY_AMOUNT_USDT} {spot_sym} по {price:.6f}")

            # Расчёт P&L
            if info.get("bought") and info.get("buy_price"):
                bought_count += 1
                spot_sym = sym.replace('USDT', '') + 'USDT'
                current_p = get_spot_price(spot_sym)
                if current_p:
                    info["current_price"] = round(current_p, 8)
                    qty = BUY_AMOUNT_USDT / info["buy_price"]
                    pnl = round(qty * (current_p - info["buy_price"]), 2)
                    info["pnl_usdt"] = pnl
                    total_pnl += pnl
                    total_invested += BUY_AMOUNT_USDT

        # Итоговый отчёт
        print("\n" + "="*90)
        print("📈 ИТОГОВЫЙ СИМУЛИРОВАННЫЙ ПОРТФЕЛЬ ($100 на каждую новую пару)")
        print("="*90)
        print(f"Куплено монет: {bought_count} шт. | Инвестировано: ${total_invested} | P&L: {total_pnl:+.2f} USDT")
        if bought_count > 0:
            print(f"Общая доходность: {total_pnl / total_invested * 100 if total_invested else 0:+.1f}%")
        print("-"*90)

        save_data(data)
        print(f"⏳ Следующая проверка через 60 минут... (сейчас {datetime.now().strftime('%H:%M')})\n")
        time.sleep(3600)

if __name__ == "__main__":
    main()
