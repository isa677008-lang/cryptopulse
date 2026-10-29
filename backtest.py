"""
CryptoPulse — воспроизводимый бэктест.
Сравнивает 4 варианта:
  1) без фильтров
  2) только 1h-фильтр (цена > EMA50 на 1h)
  3) только паттерны (бычий паттерн на 1h)
  4) 1h + паттерны

Запуск:
    python backtest.py --symbol BTCUSDT --days 30
"""
import argparse
import json
import math
import time
from datetime import datetime, timezone

import requests

from patterns import detect_patterns

BASE = "https://api.bybit.com"
FEE_RATE = 0.001
SLIPPAGE = 0.0005

TP_PERCENT = 2.5
SL_PERCENT = 1.5
EMA_FAST = 9
EMA_SLOW = 21
RSI_PERIOD = 14
RSI_BUY_MIN = 45
RSI_BUY_MAX = 70
ADX_MIN = 20
VOLUME_FACTOR = 1.1
RISK_PER_TRADE_PCT = 1.0
MAX_POSITION_PCT = 20.0
START_BALANCE = 1000.0
HTF_EMA_PERIOD = 50
PATTERN_LOOKBACK = 3


def fetch_klines(symbol, interval, limit=1000, end_ms=None):
    params = {"category": "spot", "symbol": symbol, "interval": interval, "limit": limit}
    if end_ms:
        params["end"] = end_ms
    r = requests.get(BASE + "/v5/market/kline", params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
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


def load_series(symbol, interval, days):
    minutes = {"1": 1, "60": 60, "240": 240}.get(interval, 1)
    need = days * 24 * 60 // minutes + 100
    all_c = []
    end_ms = None
    while len(all_c) < need:
        chunk = fetch_klines(symbol, interval, 1000, end_ms)
        if not chunk:
            break
        all_c = chunk + all_c
        end_ms = chunk[0]["ts"] - 1
        time.sleep(0.25)
    return all_c[-need:]


def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    out = [None] * (period - 1) + [e]
    for v in values[period:]:
        e = (v - e) * k + e
        out.append(e)
    return out


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    out = [None] * period
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        d = values[i] - values[i - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    ag, al = gains / period, losses / period
    rs = (ag / al) if al > 0 else float("inf")
    out.append(100 - 100 / (1 + rs))
    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]
        g = d if d > 0 else 0.0
        l = -d if d < 0 else 0.0
        ag = (ag * (period - 1) + g) / period
        al = (al * (period - 1) + l) / period
        rs = (ag / al) if al > 0 else float("inf")
        out.append(100 - 100 / (1 + rs))
    return out


def adx_series(highs, lows, closes, period=14):
    n = len(closes)
    if n < period * 2:
        return [None] * n
    tr, pdm, mdm = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
        up = highs[i] - highs[i - 1]
        dn = lows[i - 1] - lows[i]
        pdm.append(up if up > dn and up > 0 else 0.0)
        mdm.append(dn if dn > up and dn > 0 else 0.0)

    def wilder(data, p):
        if len(data) < p:
            return []
        s = sum(data[:p])
        out = [s]
        for v in data[p:]:
            s = s - s / p + v
            out.append(s)
        return out

    tr_s = wilder(tr, period)
    pdm_s = wilder(pdm, period)
    mdm_s = wilder(mdm, period)
    dx = []
    for i in range(len(tr_s)):
        if tr_s[i] == 0:
            dx.append(0.0)
            continue
        pdi = 100 * pdm_s[i] / tr_s[i]
        mdi = 100 * mdm_s[i] / tr_s[i]
        s = pdi + mdi
        dx.append(100 * abs(pdi - mdi) / s if s > 0 else 0.0)
    if len(dx) < period:
        return [None] * n
    a = sum(dx[:period]) / period
    adx_vals = [a]
    for v in dx[period:]:
        a = (a * (period - 1) + v) / period
        adx_vals.append(a)
    pad = n - len(adx_vals)
    return [None] * pad + adx_vals


def htf_trend_series(closes_1m, htf_closes):
    n = len(closes_1m)
    htf_ts = [c["ts"] for c in htf_closes]
    htf_vals = ema([c["close"] for c in htf_closes], HTF_EMA_PERIOD) or []
    if not htf_vals:
        return ["flat"] * n
    out = []
    j = 0
    for i in range(n):
        cur_ts = closes_1m[i]["ts"]
        while j + 1 < len(htf_ts) and htf_ts[j + 1] <= cur_ts:
            j += 1
        e = htf_vals[j] if j < len(htf_vals) else None
        if e is None:
            out.append("flat")
        else:
            c = closes_1m[i]["close"]
            if c > e * 1.001:
                out.append("up")
            elif c < e * 0.999:
                out.append("down")
            else:
                out.append("flat")
    return out


def htf_pattern_series(candles_1m, htf_closes):
    """Для каждой 1m-свечи возвращает 'bullish'|'bearish'|'none' по последним закрытым 1h-свечам."""
    n = len(candles_1m)
    htf_ts = [c["ts"] for c in htf_closes]
    out = ["none"] * n
    if not htf_closes:
        return out

    # Предсчитываем паттерны для каждой 1h-свечи (на окне из последних N до неё)
    htf_patterns = []
    for i in range(len(htf_closes)):
        start = max(0, i - 5)
        window = htf_closes[start:i + 1]
        if len(window) < 5:
            htf_patterns.append({"has_bullish": False, "has_bearish": False})
            continue
        res = detect_patterns(window, lookback=1)  # только последняя свеча окна
        htf_patterns.append({
            "has_bullish": res["has_bullish"],
            "has_bearish": res["has_bearish"],
        })

    j = -1
    for i in range(n):
        cur_ts = candles_1m[i]["ts"]
        while j + 1 < len(htf_ts) and htf_ts[j + 1] <= cur_ts:
            j += 1
        # Берём последние PATTERN_LOOKBACK закрытых 1h-свечей
        bull = False
        bear = False
        for k in range(max(0, j - PATTERN_LOOKBACK + 1), j + 1):
            if k < len(htf_patterns):
                bull = bull or htf_patterns[k]["has_bullish"]
                bear = bear or htf_patterns[k]["has_bearish"]
        if bull:
            out[i] = "bullish"
        elif bear:
            out[i] = "bearish"
    return out


def run_backtest(candles_1m, htf_closes, use_htf: bool, use_patterns: bool, label=""):
    closes = [c["close"] for c in candles_1m]
    highs = [c["high"] for c in candles_1m]
    lows = [c["low"] for c in candles_1m]
    vols = [c["volume"] for c in candles_1m]

    ema_f = ema(closes, EMA_FAST)
    ema_s = ema(closes, EMA_SLOW)
    rsi_v = rsi(closes, RSI_PERIOD)
    adx_v = adx_series(highs, lows, closes, 14)
    htf_trend = htf_trend_series(candles_1m, htf_closes) if use_htf else ["up"] * len(candles_1m)
    htf_pat = htf_pattern_series(candles_1m, htf_closes) if use_patterns else ["bullish"] * len(candles_1m)

    if not (ema_f and ema_s and rsi_v and adx_v):
        return {"error": "not enough data", "label": label}

    balance = START_BALANCE
    trades = []
    position = None
    filtered = 0
    signals_total = 0

    for i in range(60, len(candles_1m) - 1):
        ef, es, rv, av = ema_f[i], ema_s[i], rsi_v[i], adx_v[i]
        if ef is None or es is None or rv is None or av is None:
            continue
        price = closes[i]
        avg_vol = sum(vols[i - 10:i - 1]) / 9 if i >= 10 else 0
        has_vol = vols[i - 1] > avg_vol * VOLUME_FACTOR if avg_vol > 0 else False

        buy = (price > ef > es and RSI_BUY_MIN <= rv < RSI_BUY_MAX
               and av >= ADX_MIN and has_vol)

        if position is None and buy:
            signals_total += 1
            if use_htf and htf_trend[i] != "up":
                filtered += 1
                continue
            if use_patterns and htf_pat[i] != "bullish":
                filtered += 1
                continue
            entry = candles_1m[i + 1]["open"] * (1 + SLIPPAGE)
            risk_amount = balance * (RISK_PER_TRADE_PCT / 100)
            size = risk_amount / (SL_PERCENT / 100)
            size = min(size, balance * (MAX_POSITION_PCT / 100))
            if size < 10:
                continue
            fee_in = size * FEE_RATE
            if balance < size + fee_in:
                continue
            balance -= size + fee_in
            position = {
                "entry": entry, "size": size, "fee_in": fee_in,
                "tp": entry * (1 + TP_PERCENT / 100),
                "sl": entry * (1 - SL_PERCENT / 100),
                "idx": i + 1, "qty": size / entry,
            }
            continue

        if position is not None:
            hi = highs[i + 1]
            lo = lows[i + 1]
            exit_price = None
            reason = None
            if hi >= position["tp"]:
                exit_price = position["tp"]
                reason = "TP"
            elif lo <= position["sl"]:
                exit_price = position["sl"]
                reason = "SL"
            elif (i + 1 - position["idx"]) >= 120:
                exit_price = closes[i + 1]
                reason = "Timeout"
            if exit_price is not None:
                exit_price *= (1 - SLIPPAGE)
                gross = (exit_price - position["entry"]) * position["qty"]
                fee_out = position["size"] * FEE_RATE
                net = gross - fee_out
                balance += position["size"] + net
                trades.append({"pnl": net, "reason": reason})
                position = None

    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] < 0]
    sum_w = sum(t["pnl"] for t in wins)
    sum_l = abs(sum(t["pnl"] for t in losses))
    pf = (sum_w / sum_l) if sum_l > 0 else (float("inf") if sum_w > 0 else 0)
    eq_curve = [START_BALANCE]
    eq = START_BALANCE
    for t in trades:
        eq += t["pnl"]
        eq_curve.append(eq)
    peak = -1e18
    dd = 0.0
    for v in eq_curve:
        peak = max(peak, v)
        dd = max(dd, peak - v)
    bh = (candles_1m[-1]["close"] / candles_1m[0]["close"] - 1) * 100

    return {
        "label": label,
        "trades": len(trades),
        "winrate": (len(wins) / len(trades) * 100) if trades else 0,
        "profitFactor": pf if math.isfinite(pf) else None,
        "return_pct": (balance / START_BALANCE - 1) * 100,
        "final_balance": balance,
        "max_drawdown_usd": dd,
        "avg_trade": (sum(t["pnl"] for t in trades) / len(trades)) if trades else 0,
        "buy_and_hold_pct": bh,
        "by_reason": {r: sum(1 for t in trades if t["reason"] == r) for r in ("TP", "SL", "Timeout")},
        "signals_total": signals_total,
        "filtered": filtered,
    }


def split_train_test(candles, ratio=0.7):
    cut = int(len(candles) * ratio)
    return candles[:cut], candles[cut:]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--out", default="backtest_result.json")
    args = ap.parse_args()

    symbol = args.symbol.upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"

    print(f"Загрузка 1m-свечей {symbol} за {args.days} дней...")
    c1m = load_series(symbol, "1", args.days)
    print(f"  1m: {len(c1m)} свечей")

    print(f"Загрузка 1h-свечей {symbol}...")
    c1h = load_series(symbol, "60", args.days + 5)
    print(f"  1h: {len(c1h)} свечей")

    if len(c1m) < 1000:
        print("Мало данных для бэктеста.")
        return

    train, test = split_train_test(c1m, 0.7)
    print(f"Train: {len(train)}, Test: {len(test)}")

    variants = [
        ("nofilter", False, False, "без фильтров"),
        ("htf_only", True, False, "1h"),
        ("pattern_only", False, True, "паттерны"),
        ("htf_pattern", True, True, "1h + паттерны"),
    ]

    results = {}
    for part_name, part in [("train", train), ("test", test)]:
        results[part_name] = {}
        print(f"\n=== {part_name.upper()} ===")
        for key, use_htf, use_pat, label in variants:
            r = run_backtest(part, c1h, use_htf, use_pat, label)
            results[part_name][key] = r
            if "error" in r:
                print(f"  {label}: ошибка — {r['error']}")
                continue
            pf_str = "∞" if r["profitFactor"] is None else f"{r['profitFactor']:.2f}"
            print(f"  {label:15s}: trades={r['trades']:3d}  "
                  f"winrate={r['winrate']:5.1f}%  "
                  f"PF={pf_str:>5}  "
                  f"return={r['return_pct']:+7.2f}%  "
                  f"DD=${r['max_drawdown_usd']:.2f}  "
                  f"filtered={r['filtered']}")

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol, "days": args.days,
        "params": {"tp": TP_PERCENT, "sl": SL_PERCENT, "fee": FEE_RATE,
                   "slippage": SLIPPAGE, "htf_ema": HTF_EMA_PERIOD,
                   "pattern_lookback": PATTERN_LOOKBACK},
        "results": results,
        "notes": [
            "Бэктест не учитывает funding (spot-only).",
            "Slippage=0.05%, fee=0.1% на вход и выход.",
            "Прошлые результаты не гарантируют будущих.",
            "Паттерны считаются на закрытых 1h-свечах.",
        ],
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\nСохранено в {args.out}")

    print("\n=== ИТОГ (TEST) ===")
    for key, use_htf, use_pat, label in variants:
        r = results["test"].get(key, {})
        if "error" in r:
            print(f"  {label:15s}: ошибка")
            continue
        pf_str = "∞" if r["profitFactor"] is None else f"{r['profitFactor']:.2f}"
        print(f"  {label:15s}: PF={pf_str:>5}  "
              f"return={r['return_pct']:+7.2f}%  "
              f"trades={r['trades']:3d}  "
              f"winrate={r['winrate']:5.1f}%  "
              f"filtered={r['filtered']}")
    print("\nЕсли все варианты PF < 1 — стратегия убыточна на этих данных,")
    print("и надо менять не TP/SL, а саму логику (таймфрейм, индикаторы).")


if __name__ == "__main__":
    main()
