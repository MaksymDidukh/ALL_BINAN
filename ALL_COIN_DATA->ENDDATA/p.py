import os
import sys
import requests
import zipfile
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

# ================= CONFIG =================

# РЕЖИМ РАБОТЫ: 'new_listings' или 'live'
MODE = "live"  # Измени на "new_listings" для анализа новых монет

# Для режима NEW LISTINGS
START_DATE = "2025-01-01"
END_DATE   = "2025-12-31"

# Для LIVE режима
BACKTEST_DAYS = 90  # Сколько дней назад анализировать
SYMBOL = "BTCUSDT"  # Можно изменить на любую пару

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
    bar_len = 40
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
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            with open(path, "wb") as f:
                f.write(r.content)
            return True
    except:
        pass
    return False


def extract_zip(zip_path, out_dir):
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    os.remove(zip_path)


def load_day_kline(symbol, date, market="spot"):
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
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    return df


def find_first_listing(symbol, market, start, end):
    for day in daterange(start, end):
        df = load_day_kline(symbol, day, market)
        if df is not None:
            return day, df
    return None, None


def find_max_and_stats(symbol, market, start_day, end):
    max_price = 0
    min_price = float('inf')
    prices = []
    volumes = []
    day = start_day
    
    while day <= end:
        df = load_day_kline(symbol, day, market)
        if df is not None:
            high = df["high"].max()
            low = df["low"].min()
            close = df["close"].iloc[-1]
            vol = df["volume"].sum()
            
            max_price = max(max_price, high)
            min_price = min(min_price, low)
            prices.append(close)
            volumes.append(vol)
        day += timedelta(days=1)
    
    return {
        'max': max_price,
        'min': min_price,
        'avg': np.mean(prices) if prices else 0,
        'volatility': np.std(prices) if len(prices) > 1 else 0,
        'total_volume': sum(volumes),
        'avg_volume': np.mean(volumes) if volumes else 0,
        'days_tracked': len(prices)
    }


def calculate_drawdown(entry, min_price):
    if entry > 0:
        return ((min_price - entry) / entry) * 100
    return 0


def print_header(mode):
    if mode == "live":
        title = "LIVE MODE - BACKTEST ANALYSIS"
    else:
        title = "NEW LISTINGS DCA STRATEGY"
    
    print(f"""
    ╔═══════════════════════════════════════════════════════════════════╗
    ║                                                                   ║
    ║        🚀 BINANCE {title:^30s} 🚀          ║
    ║                                                                   ║
    ║            Advanced Analytics & Performance Tracking             ║
    ║                                                                   ║
    ╚═══════════════════════════════════════════════════════════════════╝
    """)


def print_config(mode):
    print("📊 ПАРАМЕТРЫ СИМУЛЯЦИИ:")
    print("─" * 70)
    print(f"  🎯 Режим работы:          {mode.upper()}")
    
    if mode == "live":
        print(f"  📅 Период бектеста:       {BACKTEST_DAYS} дней")
        print(f"  🪙 Символ:                {SYMBOL}")
    else:
        print(f"  📅 Период анализа:        {START_DATE} → {END_DATE}")
        print(f"  🌐 Рынки:                 Spot + Futures")
    
    print(f"  💵 Инвестиция на монету:  ${DAILY_BUY_USD}")
    print(f"  📈 Таймфрейм:             {TIMEFRAME}")
    print("─" * 70)
    print()


def print_live_summary(symbol, data, total_invested, final_value):
    print("\n" + "="*70)
    print("📊 КРАТКАЯ СТАТИСТИКА LIVE РЕЖИМА")
    print("="*70)
    
    roi = ((final_value / total_invested) - 1) * 100 if total_invested > 0 else 0
    profit = final_value - total_invested
    
    entry_price = data['close'].iloc[0]
    current_price = data['close'].iloc[-1]
    max_price = data['high'].max()
    min_price = data['low'].min()
    
    price_change = ((current_price / entry_price) - 1) * 100
    max_gain = ((max_price / entry_price) - 1) * 100
    max_dd = ((min_price / entry_price) - 1) * 100
    
    volatility = data['close'].pct_change().std() * 100
    avg_volume = data['volume'].mean()
    
    print(f"\n💰 ФИНАНСОВЫЕ ПОКАЗАТЕЛИ:")
    print(f"  ├─ Инвестировано:              ${total_invested:,.2f}")
    print(f"  ├─ Текущая стоимость:          ${final_value:,.2f}")
    print(f"  ├─ Прибыль/Убыток:             ${profit:+,.2f}")
    print(f"  └─ ROI:                        {roi:+.2f}%")
    
    print(f"\n📈 ЦЕНОВАЯ ДИНАМИКА:")
    print(f"  ├─ Цена входа:                 ${entry_price:,.2f}")
    print(f"  ├─ Текущая цена:               ${current_price:,.2f}")
    print(f"  ├─ Максимум:                   ${max_price:,.2f} ({max_gain:+.2f}%)")
    print(f"  ├─ Минимум:                    ${min_price:,.2f} ({max_dd:+.2f}%)")
    print(f"  └─ Изменение цены:             {price_change:+.2f}%")
    
    print(f"\n📊 МЕТРИКИ РИСКА:")
    print(f"  ├─ Волатильность (дневная):    {volatility:.2f}%")
    print(f"  ├─ Максимальная просадка:      {max_dd:.2f}%")
    print(f"  └─ Средний объём:              {avg_volume:,.0f}")
    
    # Торговые дни
    positive_days = len(data[data['close'] > data['open']])
    negative_days = len(data[data['close'] <= data['open']])
    win_rate = (positive_days / len(data)) * 100 if len(data) > 0 else 0
    
    print(f"\n📅 ТОРГОВАЯ АКТИВНОСТЬ:")
    print(f"  ├─ Всего дней:                 {len(data)}")
    print(f"  ├─ Зелёных дней:               {positive_days} ({win_rate:.1f}%)")
    print(f"  └─ Красных дней:               {negative_days} ({100-win_rate:.1f}%)")
    
    # Оценка
    if roi > 50:
        emoji = "🚀🌙"
        comment = "ОГОНЬ! Отличный результат!"
    elif roi > 20:
        emoji = "✅📈"
        comment = "Хорошая прибыль!"
    elif roi > 0:
        emoji = "🤝"
        comment = "В плюсе, продолжаем!"
    else:
        emoji = "💎🙌"
        comment = "Diamond hands! Держимся!"
    
    print(f"\n  {emoji} {comment}")


def print_live_weekly_breakdown(data):
    print("\n" + "="*70)
    print("📅 ПОНЕДЕЛЬНАЯ СТАТИСТИКА")
    print("="*70)
    
    data['week'] = pd.to_datetime(data['open_time'], unit='ms').dt.to_period('W')
    weekly = data.groupby('week').agg({
        'open': 'first',
        'close': 'last',
        'high': 'max',
        'low': 'min',
        'volume': 'sum'
    })
    
    weekly['change_%'] = ((weekly['close'] / weekly['open']) - 1) * 100
    weekly['range_%'] = ((weekly['high'] - weekly['low']) / weekly['open']) * 100
    
    print("\n  Неделя     │  Открытие │  Закрытие │ Изменение │  Диапазон │   Объём")
    print("  " + "─"*70)
    
    for idx, row in weekly.iterrows():
        change_icon = "📈" if row['change_%'] > 0 else "📉"
        print(f"  {idx} │ ${row['open']:9,.2f} │ ${row['close']:9,.2f} │ {change_icon} {row['change_%']:+6.2f}% │   {row['range_%']:5.2f}% │ {row['volume']:10,.0f}")


def print_live_price_levels(data):
    print("\n" + "="*70)
    print("🎯 КЛЮЧЕВЫЕ ЦЕНОВЫЕ УРОВНИ")
    print("="*70)
    
    current = data['close'].iloc[-1]
    max_price = data['high'].max()
    min_price = data['low'].min()
    avg_price = data['close'].mean()
    
    # Уровни поддержки и сопротивления (упрощённо)
    support_1 = data['low'].nsmallest(10).mean()
    support_2 = data['low'].quantile(0.25)
    resistance_1 = data['high'].nlargest(10).mean()
    resistance_2 = data['high'].quantile(0.75)
    
    print(f"\n  💎 Текущая цена:          ${current:,.2f}")
    print(f"  📊 Средняя цена:          ${avg_price:,.2f} ({((current/avg_price-1)*100):+.2f}%)")
    print(f"\n  🔴 СОПРОТИВЛЕНИЯ:")
    print(f"     ├─ R2 (сильное):       ${resistance_1:,.2f} ({((resistance_1/current-1)*100):+.2f}%)")
    print(f"     └─ R1 (слабое):        ${resistance_2:,.2f} ({((resistance_2/current-1)*100):+.2f}%)")
    print(f"\n  🟢 ПОДДЕРЖКИ:")
    print(f"     ├─ S1 (слабая):        ${support_2:,.2f} ({((support_2/current-1)*100):+.2f}%)")
    print(f"     └─ S2 (сильная):       ${support_1:,.2f} ({((support_1/current-1)*100):+.2f}%)")
    
    # Процент до уровней
    to_resistance = ((resistance_1 / current) - 1) * 100
    to_support = ((support_1 / current) - 1) * 100
    
    print(f"\n  📏 Расстояние до сопротивления: {to_resistance:+.2f}%")
    print(f"  📏 Расстояние до поддержки:     {to_support:.2f}%")


def run_live_mode():
    print_header("live")
    print_config("live")
    
    print("🔄 Загрузка данных для live режима...")
    ensure_dirs()
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=BACKTEST_DAYS)
    
    all_data = []
    print(f"\n📥 Загрузка {BACKTEST_DAYS} дней данных для {SYMBOL}...\n")
    
    total_days = BACKTEST_DAYS
    loaded = 0
    
    for day in daterange(start_date, end_date):
        loaded += 1
        progress_bar(loaded, total_days, prefix=f"Загрузка данных")
        
        df = load_day_kline(SYMBOL, day, "spot")
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        print("\n❌ Не удалось загрузить данные. Проверьте символ и даты.")
        return
    
    data = pd.concat(all_data, ignore_index=True)
    print(f"\n✅ Загружено {len(data)} свечей")
    
    # DCA симуляция
    total_invested = len(data) * DAILY_BUY_USD
    entry_price = data['close'].iloc[0]
    current_price = data['close'].iloc[-1]
    
    total_coins = 0
    for _, row in data.iterrows():
        total_coins += DAILY_BUY_USD / row['close']
    
    final_value = total_coins * current_price
    
    print("\n" + "="*70)
    print("✅ АНАЛИЗ ЗАВЕРШЁН")
    print("="*70)
    
    print_live_summary(SYMBOL, data, total_invested, final_value)
    print_live_weekly_breakdown(data)
    print_live_price_levels(data)
    
    # Простая статистика движения
    print("\n" + "="*70)
    print("📊 ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА")
    print("="*70)
    
    daily_returns = data['close'].pct_change() * 100
    
    print(f"\n  📈 Лучший день:               {daily_returns.max():+.2f}%")
    print(f"  📉 Худший день:               {daily_returns.min():+.2f}%")
    print(f"  📊 Средний дневной рост:      {daily_returns.mean():+.2f}%")
    print(f"  🎲 Медиана дневного роста:    {daily_returns.median():+.2f}%")
    
    # Сохранение
    output_file = f"live_analysis_{SYMBOL}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    data.to_csv(output_file, index=False)
    print(f"\n💾 Данные сохранены в: {output_file}")
    
    print("\n" + "="*70)
    print("🎉 Спасибо за использование анализатора!")
    print("="*70 + "\n")


def print_market_stats(results_df):
    if results_df.empty:
        return
    
    print("\n" + "="*70)
    print("📈 СТАТИСТИКА ПО РЫНКАМ")
    print("="*70)
    
    for market in ['spot', 'futures']:
        market_df = results_df[results_df['market'] == market]
        if not market_df.empty:
            total_invested = len(market_df) * DAILY_BUY_USD
            total_value = market_df['final_$'].sum()
            roi = ((total_value / total_invested) - 1) * 100 if total_invested > 0 else 0
            
            winners = len(market_df[market_df['roi_%'] > 0])
            losers = len(market_df[market_df['roi_%'] <= 0])
            win_rate = (winners / len(market_df)) * 100 if len(market_df) > 0 else 0
            
            avg_roi = market_df['roi_%'].mean()
            max_gain = market_df['roi_%'].max()
            max_loss = market_df['roi_%'].min()
            
            print(f"\n🏪 {market.upper()} MARKET:")
            print(f"  ├─ Монет найдено:          {len(market_df)}")
            print(f"  ├─ Инвестировано:          ${total_invested:,.2f}")
            print(f"  ├─ Текущая стоимость:      ${total_value:,.2f}")
            print(f"  ├─ Общий ROI:              {roi:+.2f}%")
            print(f"  ├─ Средний ROI:            {avg_roi:+.2f}%")
            print(f"  ├─ Лучший рост:            {max_gain:+.2f}%")
            print(f"  ├─ Худший результат:       {max_loss:+.2f}%")
            print(f"  ├─ Прибыльных:             {winners} ({win_rate:.1f}%)")
            print(f"  └─ Убыточных:              {losers} ({100-win_rate:.1f}%)")


def print_performance_tiers(results_df):
    if results_df.empty:
        return
    
    print("\n" + "="*70)
    print("🏆 РАСПРЕДЕЛЕНИЕ ПО УРОВНЯМ ДОХОДНОСТИ")
    print("="*70)
    
    tiers = [
        ("🌙 MOONSHOT (>1000%)", 1000, float('inf')),
        ("🚀 ROCKET (500-1000%)", 500, 1000),
        ("💎 DIAMOND (200-500%)", 200, 500),
        ("⭐ STAR (100-200%)", 100, 200),
        ("✅ PROFIT (10-100%)", 10, 100),
        ("🤝 BREAKEVEN (-10 to +10%)", -10, 10),
        ("📉 LOSS (-50 to -10%)", -50, -10),
        ("💀 RIP (<-50%)", float('-inf'), -50)
    ]
    
    for label, min_roi, max_roi in tiers:
        count = len(results_df[(results_df['roi_%'] >= min_roi) & (results_df['roi_%'] < max_roi)])
        if count > 0:
            pct = (count / len(results_df)) * 100
            value = results_df[(results_df['roi_%'] >= min_roi) & (results_df['roi_%'] < max_roi)]['final_$'].sum()
            print(f"  {label:30s} │ {count:3d} монет ({pct:5.1f}%) │ ${value:,.2f}")


def print_top_performers(results_df, n=10):
    if results_df.empty:
        return
    
    print("\n" + "="*70)
    print(f"🌟 ТОП-{n} ЛУЧШИХ МОНЕТ")
    print("="*70)
    
    top = results_df.nlargest(n, 'roi_%')
    
    print("\n  #  │ Символ      │ Рынок   │ Дата листинга │ Вход $  │ Макс $  │   ROI   │ Стоимость")
    print("  " + "─"*95)
    
    for i, (_, row) in enumerate(top.iterrows(), 1):
        print(f"  {i:2d} │ {row['symbol']:11s} │ {row['market']:7s} │ {row['listing_date']} │ {row['entry']:7.4f} │ {row['max']:7.2f} │ {row['roi_%']:+7.1f}% │ ${row['final_$']:8.2f}")


def print_summary(results_df, total_spent, total_final):
    print("\n" + "="*70)
    print("💰 ИТОГОВЫЙ РЕЗУЛЬТАТ")
    print("="*70)
    
    total_roi = ((total_final / total_spent) - 1) * 100 if total_spent > 0 else 0
    profit = total_final - total_spent
    
    print(f"\n  💵 Всего инвестировано:     ${total_spent:,.2f}")
    print(f"  💎 Текущая стоимость:       ${total_final:,.2f}")
    print(f"  📈 Чистая прибыль/убыток:   ${profit:+,.2f}")
    print(f"  🎯 ROI:                     {total_roi:+.2f}%")
    print(f"  🪙 Всего монет:             {len(results_df)}")
    
    if total_roi > 200:
        emoji = "🚀🌙"
        comment = "ОГОНЬ! TO THE MOON! 🚀"
    elif total_roi > 100:
        emoji = "🎉💎"
        comment = "Отличный результат! Diamond hands!"
    elif total_roi > 50:
        emoji = "✅📈"
        comment = "Хорошая прибыль!"
    elif total_roi > 0:
        emoji = "🤝"
        comment = "В плюсе, но есть куда расти"
    else:
        emoji = "💀"
        comment = "Держись! Не продавай на дне!"
    
    print(f"\n  {emoji} {comment}")


def run_new_listings_mode():
    print_header("new_listings")
    print_config("new_listings")
    
    print("🔄 Подготовка директорий...")
    ensure_dirs()

    start = datetime.fromisoformat(START_DATE)
    end   = datetime.fromisoformat(END_DATE)

    print("🌐 Получение списка торговых пар...")
    info = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=20).json()
    symbols = [s["symbol"] for s in info["symbols"] if s["quoteAsset"] == "USDT"]
    
    print(f"✅ Найдено {len(symbols)} USDT пар")
    print(f"\n🚀 Начинаем анализ...\n")

    total_tasks = len(symbols) * 2
    done = 0

    total_spent = 0
    total_final = 0
    results = []

    for symbol in symbols:
        for market in ["spot", "futures"]:
            done += 1
            progress_bar(done, total_tasks, prefix=f"Анализ {symbol:12s} {market:7s}")

            first_day, first_df = find_first_listing(symbol, market, start, end)
            if first_df is None:
                continue

            entry = first_df.iloc[0]["open"]
            stats = find_max_and_stats(symbol, market, first_day, end)
            
            maxp = stats['max']
            minp = stats['min']

            final = DAILY_BUY_USD * (maxp / entry) if maxp > entry else DAILY_BUY_USD
            roi = ((maxp / entry) - 1) * 100 if entry > 0 else 0
            max_dd = calculate_drawdown(entry, minp)
            volatility_pct = (stats['volatility'] / entry * 100) if entry > 0 else 0

            total_spent += DAILY_BUY_USD
            total_final += final

            results.append({
                "symbol": symbol,
                "market": market,
                "listing_date": first_day.date(),
                "entry": round(entry, 6),
                "max": round(maxp, 6),
                "min": round(minp, 6),
                "final_$": round(final, 2),
                "roi_%": round(roi, 2),
                "max_drawdown_%": round(max_dd, 2),
                "volatility_%": round(volatility_pct, 2),
                "days_tracked": stats['days_tracked'],
                "avg_volume": round(stats['avg_volume'], 2)
            })

    df = pd.DataFrame(results).sort_values("roi_%", ascending=False)

    print("\n" + "="*70)
    print("✅ АНАЛИЗ ЗАВЕРШЁН")
    print("="*70)
    
    print_market_stats(df)
    print_performance_tiers(df)
    print_top_performers(df, 15)
    print_summary(df, total_spent, total_final)
    
    output_file = f"new_listings_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(output_file, index=False)
    print(f"\n💾 Результаты сохранены в: {output_file}")
    
    print("\n" + "="*70)
    print("🎉 Спасибо за использование анализатора!")
    print("="*70 + "\n")


def main():
    if MODE == "live":
        run_live_mode()
    elif MODE == "new_listings":
        run_new_listings_mode()
    else:
        print(f"❌ Неизвестный режим: {MODE}")
        print("Установите MODE = 'live' или MODE = 'new_listings'")


if __name__ == "__main__":
    main()
