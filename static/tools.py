"""
CryptoPulse — вспомогательные инструменты (консоль).
Не влияет на работу бота. Запускается отдельно.

Команды:
    python tools.py trend BTC          — тренд 1h/4h/1d по BTC
    python tools.py scan               — тренд по топ-20 монетам
    python tools.py patterns           — найденные паттерны по всем монетам (1h)
    python tools.py pattern BTC        — паттерны по BTC (подробно)
    python tools.py stats              — статистика по закрытым сделкам из bot_state.json
    python tools.py backtest BTC 30    — запустить backtest.py по одной монете за N дней
"""
import json
import math
import os
import sys
import time
from datetime import datetime

import requests

from patterns import detect_patterns, TALIB_AVAILABLE

BASE = "https://api.bybit.com"
BOT_STATE_FILE = os.getenv("BOT_STATE_FILE", "./data/bot_state.json")
HTF_EMA_PERIOD = 50
PATTERN_LOOKBACK = 3

TOP20 = ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX",
         "TON", "LINK", "DOT", "LTC", "TRX", "NEAR", "SUI", "MATIC",
         "ATOM", "APT", "ARB", "OP"]


def get_json(url, params=None):
    r = requests.get(url, params=params, timeout=12)
    r.raise_for_status()
    return r.json()


def fetch_klines(symbol, interval, limit=250):
    pair = symbol.upper() if symbol.upper().endswith("USDT") else symbol.upper() + "USDT"
    data = get_json(BASE + "/v5/market/kline",
                    {"category": "spot", "symbol": pair, "interval": interval, "limit": limit})
    if data.get("retCode") != 0:
        raise RuntimeError(data.get("retMsg"))
    raw = (data.get("result") or {}).get("list", [])
    out = []
    for item in reversed(raw):
        out.append({
            "ts": int(item[0]),
            "open": float(item[1]), "high": float(item[2]),
            "low": float(item[3]), "close": float(item[4]),
            "volume": float(item[5]),
        })
    return out


def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = (v - e) * k + e
    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        d = values[i] - values[i - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    ag, al = gains / period, losses / period
    if al == 0:
        return 100.0 if ag else 50.0
    return 100 - 100 / (1 + ag / al)


def trend_for(symbol, interval):
    try:
        candles = fetch_klines(symbol, interval, 250)
    except Exception as exc:
        return {"error": str(exc)}
    if len(candles) < 60:
        return {"error": "мало данных"}
    closes = [c["close"] for c in candles]
    e50 = ema(closes, 50)
    e200 = ema(closes, 200) if len(closes) >= 200 else None
    r = rsi(closes, 14)
    price = closes[-1]
    if e50 is None:
        return {"error": "мало данных для EMA50"}
    if price > e50 and (e200 is None or e50 > e200):
        direction = "up"
    elif price < e50 and (e200 is None or e50 < e200):
        direction = "down"
    else:
        direction = "flat"
    return {
        "price": price, "ema50": e50, "ema200": e200,
        "rsi": r, "direction": direction,
        "pct_from_ema50": (price / e50 - 1) * 100,
    }


def patterns_for(symbol):
    """Паттерны по 1h-свечам (последние 100)."""
    try:
        candles = fetch_klines(symbol, "60", 100)
    except Exception as exc:
        return {"error": str(exc)}
    if len(candles) < 10:
        return {"error": "мало данных"}
    res = detect_patterns(candles, lookback=PATTERN_LOOKBACK)
    return {
        "found": res["found"],
        "has_bullish": res["has_bullish"],
        "has_bearish": res["has_bearish"],
        "has_neutral": res["has_neutral"],
        "method": res["method"],
        "enabled": res["enabled"],
    }


def cmd_trend(symbol):
    symbol = symbol.upper()
    print(f"=== Тренд по {symbol} ===\n")
    for label, iv in [("1h", "60"), ("4h", "240"), ("1d", "D")]:
        t = trend_for(symbol, iv)
        if "error" in t:
            print(f"{label}: ошибка — {t['error']}")
            continue
        arrow = {"up": "↑", "down": "↓", "flat": "→"}[t["direction"]]
        rsi_s = f"  RSI {t['rsi']:.1f}" if t.get("rsi") else ""
        print(f"{label}: {arrow} {t['direction']:5s}  "
              f"цена ${t['price']:,.6f}  "
              f"EMA50 ${t['ema50']:,.6f} ({t['pct_from_ema50']:+.2f}%)"
              f"{rsi_s}")
    print()
    print("Если на 1h тренд ↓, бот с фильтром 1h НЕ откроет Long.")


def cmd_scan():
    print("=== Тренд 1h по топ-20 ===\n")
    up, down, flat, err = [], [], [], []
    for sym in TOP20:
        t = trend_for(sym, "60")
        if "error" in t:
            err.append(sym)
            continue
        d = t["direction"]
        line = f"{sym:6s} {d:5s}  цена ${t['price']:>14,.6f}  " \
               f"EMA50 ${t['ema50']:>14,.6f} ({t['pct_from_ema50']:+.2f}%)"
        if t.get("rsi"):
            line += f"  RSI {t['rsi']:.1f}"
        print(line)
        {"up": up, "down": down, "flat": flat}[d].append(sym)
        time.sleep(0.15)
    print()
    print(f"↑ up:   {', '.join(up) or '—'}")
    print(f"↓ down: {', '.join(down) or '—'}")
    print(f"→ flat: {', '.join(flat) or '—'}")
    if err:
        print(f"ошибки: {', '.join(err)}")


def cmd_patterns():
    """Паттерны по всем топ-20 монетам."""
    print(f"=== Паттерны 1h (TA-Lib: {'да' if TALIB_AVAILABLE else 'нет, fallback'}) ===\n")
    rows = []
    for sym in TOP20:
        p = patterns_for(sym)
        if "error" in p:
            continue
        if not p["found"]:
            continue
        bull = sum(1 for f in p["found"] if f["direction"] == "bullish")
        bear = sum(1 for f in p["found"] if f["direction"] == "bearish")
        neut = sum(1 for f in p["found"] if f["direction"] == "neutral")
        names = ", ".join(f["name"] for f in p["found"])
        rows.append((sym, bull, bear, neut, names))
        time.sleep(0.15)
    if not rows:
        print("Паттернов не найдено.")
        return
    rows.sort(key=lambda r: (-r[1], r[0]))
    print(f"{'Монета':8s} {'Bull':>4s} {'Bear':>4s} {'Neut':>4s}  Паттерны")
    print("-" * 90)
    for sym, b, be, n, names in rows:
        print(f"{sym:8s} {b:>4d} {be:>4d} {n:>4d}  {names}")


def cmd_pattern(symbol):
    """Паттерны по одной монете (подробно)."""
    symbol = symbol.upper()
    print(f"=== Паттерны {symbol} 1h (TA-Lib: {'да' if TALIB_AVAILABLE else 'нет, fallback'}) ===\n")
    p = patterns_for(symbol)
    if "error" in p:
        print(f"Ошибка: {p['error']}")
        return
    if not p["enabled"]:
        print("Недостаточно данных для анализа.")
        return
    print(f"Метод: {p['method']}")
    print(f"Бычьи: {'да' if p['has_bullish'] else 'нет'}")
    print(f"Медвежьи: {'да' if p['has_bearish'] else 'нет'}")
    print(f"Нейтральные: {'да' if p['has_neutral'] else 'нет'}")
    print()
    if not p["found"]:
        print("Паттернов не найдено.")
        return
    print("Найденные:")
    for f in p["found"]:
        arrow = {"bullish": "↑", "bearish": "↓", "neutral": "→"}[f["direction"]]
        print(f"  {arrow} [{f['direction']:8s}] {f['name']}  (свеча #{f['index']})")


def cmd_stats():
    if not os.path.exists(BOT_STATE_FILE):
        print(f"Файл {BOT_STATE_FILE} не найден.")
        return
    with open(BOT_STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)
    trades = state.get("closedTrades", [])
    positions = state.get("positions", [])
    balance = float(state.get("balance", 0))
    settings = state.get("settings", {})
    start = float(settings.get("startBalance", 1000))
    realized = float(state.get("realizedPnl", 0))

    print("=== Статистика CryptoPulse ===\n")
    print(f"Свободный баланс:     ${balance:,.2f}")
    print(f"Стартовый баланс:     ${start:,.2f}")
    print(f"Открытых позиций:     {len(positions)}")
    print(f"Закрытых сделок:      {len(trades)}")
    print(f"Реализованный P&L:    ${realized:+,.2f}")
    print(f"Отфильтровано 1h:     {state.get('filteredByHTF', 0)}")
    print(f"Отфильтровано паттерн: {state.get('filteredByPatterns', 0)}")
    print()

    def summarize(subset, label):
        if not subset:
            print(f"{label}: нет сделок")
            return
        wins = [t for t in subset if t["pnl"] > 0]
        losses = [t for t in subset if t["pnl"] < 0]
        sum_w = sum(t["pnl"] for t in wins)
        sum_l = abs(sum(t["pnl"] for t in losses))
        pf = (sum_w / sum_l) if sum_l > 0 else (float("inf") if sum_w > 0 else 0)
        wr = len(wins) / len(subset) * 100
        avg = sum(t["pnl"] for t in subset) / len(subset)
        print(f"{label} ({len(subset)}):")
        print(f"  winrate:     {wr:.1f}%")
        print(f"  profit factor: {'∞' if not math.isfinite(pf) else f'{pf:.2f}'}")
        print(f"  средний P&L: ${avg:+.2f}")
        print(f"  сумма:       ${sum(t['pnl'] for t in subset):+.2f}")

    if trades:
        summarize(trades[:20], "Последние 20")
        summarize(trades[:50], "Последние 50")
        summarize(trades, "Все")
        print()

    by_sym = {}
    for t in trades:
        s = t["symbol"]
        by_sym.setdefault(s, []).append(t["pnl"])
    if by_sym:
        print("=== По монетам (PnL) ===")
        rows = [(s, sum(p), len(p)) for s, p in by_sym.items()]
        rows.sort(key=lambda x: x[1], reverse=True)
        for s, pnl, n in rows:
            print(f"  {s:6s}  {pnl:+8.2f}$  ({n} сд.)")

    if positions:
        print("\n=== Открытые позиции ===")
        for p in positions:
            side = p.get("side", "?")
            print(f"  {p['symbol']:6s} {side:5s} вход ${p['entryPrice']:,.6f}  "
                  f"TP ${p['tp']:,.6f}  SL ${p['sl']:,.6f}  ${p['sizeUsd']:.0f}")


def cmd_backtest(symbol, days):
    import subprocess
    pair = symbol.upper() if symbol.upper().endswith("USDT") else symbol.upper() + "USDT"
    print(f"Запуск backtest.py --symbol {pair} --days {days}...")
    subprocess.run([sys.executable, "backtest.py", "--symbol", pair, "--days", str(days)])


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    cmd = sys.argv[1].lower()
    if cmd == "trend":
        if len(sys.argv) < 3:
            print("Укажи монету: python tools.py trend BTC")
            return
        cmd_trend(sys.argv[2])
    elif cmd == "scan":
        cmd_scan()
    elif cmd == "patterns":
        cmd_patterns()
    elif cmd == "pattern":
        if len(sys.argv) < 3:
            print("Укажи монету: python tools.py pattern BTC")
            return
        cmd_pattern(sys.argv[2])
    elif cmd == "stats":
        cmd_stats()
    elif cmd == "backtest":
        if len(sys.argv) < 3:
            print("Укажи монету: python tools.py backtest BTC 30")
            return
        days = int(sys.argv[3]) if len(sys.argv) > 3 else 30
        cmd_backtest(sys.argv[2], days)
    else:
        print(f"Неизвестная команда: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
