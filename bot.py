import os, time, hmac, math, logging, threading, json, csv, io, signal, fcntl
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlencode
import requests

try:
    from groq import Groq
except ImportError:
    Groq = None

from flask import Flask, jsonify, send_from_directory, request, Response

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

from auth import require_admin
from accounting import (
    FEE_RATE, compute_equity, open_position_costs,
    close_position_costs, size_from_risk,
)
from patterns import detect_patterns, pattern_strength, TALIB_AVAILABLE

app = Flask(__name__, static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cryptopulse")

BASE = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "").rstrip("/")
WEBAPP_URL = os.getenv("TELEGRAM_WEBAPP_URL", PUBLIC_APP_URL + "/") if PUBLIC_APP_URL else ""
WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", PUBLIC_APP_URL + "/telegram-webhook") if PUBLIC_APP_URL else ""

TG_TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
TG_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
TG_OWNER_CHAT_ID = os.getenv("TELEGRAM_OWNER_CHAT_ID", "").strip()
TG_AUTO = os.getenv("TELEGRAM_AUTO_CONFIGURE", "false").lower() in ("1", "true", "yes", "on")
BG_WORKER = os.getenv("ENABLE_BACKGROUND_WORKER", "true").lower() in ("1", "true", "yes", "on")
ALLOW_SHORTS = os.getenv("ALLOW_SHORTS", "true").lower() in ("1", "true", "yes", "on")
ENABLE_AI = os.getenv("ENABLE_AI_COMMENT", "false").lower() in ("1", "true", "yes", "on")
STALE_PRICE_SECONDS = int(os.getenv("STALE_PRICE_SECONDS", "60"))

API_KEY = os.getenv("BYBIT_API_KEY", "").strip()
API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

TIMEOUT = 12
CACHE_SECONDS = 30
HISTORY_HOURS = 72
REFRESH_SECONDS = 60

BOT_STATE_FILE = os.getenv("BOT_STATE_FILE", "./data/bot_state.json")
BOT_TICK_SECONDS = 3
BOT_START_BALANCE = 1000.0
FUTURES_START_BALANCE = 200.0

TP_PERCENT = 2.5
SL_PERCENT = 1.5
FUTURES_LEVERAGE = 3.0
BE_TRIGGER_PERCENT = 0.8
TRAILING_TRIGGER_PERCENT = 1.5
TRAILING_DISTANCE_PERCENT = 0.6
RISK_PER_TRADE_PCT = 1.0
MAX_POSITION_PCT = 20.0
MAX_OPEN_POSITIONS = 20
MAX_TOTAL_EXPOSURE_PCT = 80.0

HTF_EMA_PERIOD = 50
HTF_INTERVAL_DEFAULT = "60"
HTF_TOLERANCE_PCT = 0.1
HTF_INTERVAL_MAP = {
    "1": "1m", "3": "3m", "5": "5m", "15": "15m", "30": "30m",
    "60": "1h", "120": "2h", "240": "4h", "360": "6h", "720": "12h",
    "D": "1d", "W": "1w"
}

PATTERN_LOOKBACK = 3

SIGNAL_UNIVERSE_SIZE = 100
SIGNAL_UNIVERSE_REFRESH_SECONDS = 180
PANIC_BTC_DROP_PCT = -3.0
EQUITY_PUSH_SECONDS = 15
DEFAULT_DAILY_MAX_TRADES = 15

DEFAULT_STRATEGY = {
    "emaFast": 9, "emaSlow": 21, "rsiPeriod": 14,
    "rsiBuyMin": 45, "rsiBuyMax": 70,
    "rsiSellMin": 30, "rsiSellMax": 55,
    "topCoins": ["ETH","SOL","BNB","XRP","DOGE","ADA","AVAX","TON","LINK","DOT","LTC","TRX","NEAR","SUI"],
}

session = requests.Session()
session.headers.update({"User-Agent": "CryptoPulse/5.0", "Accept": "application/json"})

lock = threading.RLock()
refresh_lock = threading.Lock()
db_lock = threading.Lock()
bot_state_lock = threading.RLock()
htf_lock = threading.Lock()
patterns_lock = threading.Lock()

cache = {"updated": 0, "prices": {}, "changes": {}, "turnover": {}, "source": "bybit",
         "error": None, "stale": True}
history = {}
usd_rub_cache = {"updated": 0, "rate": None}
bybit_status = {"available": False, "lastError": None, "lastCheck": None}
db_status = {"configured": bool(DATABASE_URL), "available": False, "lastError": None}
wallet_cache = {"updated": 0, "data": None}

htf_cache = {"updated": 0, "trends": {}, "current_interval": HTF_INTERVAL_DEFAULT}
patterns_cache = {"updated": 0, "by_symbol": {}}

bot_state = {
    "running": False, "paused": False, "emergency": False, "reason": "Не запущен",
    "mode": "spot",
    "settings": {
        "profitTarget": 20.0, "maxLoss": 50.0, "tradeSize": 25.0, "dailyMaxLoss": 30.0,
        "dailyMaxTrades": DEFAULT_DAILY_MAX_TRADES,
        "startBalance": BOT_START_BALANCE, "dailyStartBalance": BOT_START_BALANCE,
        "futuresStartBalance": FUTURES_START_BALANCE, "futuresBalance": FUTURES_START_BALANCE,
        "enableProfitTarget": True, "enableMaxLoss": True, "enableDailyMaxLoss": True,
        "fixedTradeSize": True, "enableDailyMaxTrades": True,
        "riskPerTradePct": RISK_PER_TRADE_PCT,
        "maxOpenPositions": MAX_OPEN_POSITIONS,
        "maxTotalExposurePct": MAX_TOTAL_EXPOSURE_PCT,
        "enableMaxPositions": True,
        "enableHTFFilter": True,
        "htfInterval": HTF_INTERVAL_DEFAULT,
        "enablePatterns": True,
    },
    "strategy": dict(DEFAULT_STRATEGY),
    "balance": BOT_START_BALANCE,
    "equityCurve": [], "positions": [], "closedTrades": [], "logs": [],
    "startedAt": None, "stoppedAt": None,
    "lastAction": None, "lastSignalScan": None, "scanResults": [],
    "scanStatus": "Ожидание запуска",
    "lastDate": datetime.now().strftime("%Y-%m-%d"),
    "dailyTradesReset": datetime.now().strftime("%Y-%m-%d"),
    "dailyTradesCount": 0,
    "cooldown": {},
    "realizedPnl": 0.0,
    "filteredByHTF": 0,
    "filteredByPatterns": 0,
}

strategy_history = {}
strategy_history_updated = 0.0
strategy_history_lock = threading.Lock()
SCAN_CANDLE_INTERVAL = "1"
SCAN_CANDLE_LIMIT = 200

history_loading_flag = threading.Event()

groq_client = None
if Groq and GROQ_API_KEY and ENABLE_AI:
    try:
        groq_client = Groq(api_key=GROQ_API_KEY)
    except Exception as e:
        log.error("Groq init failed: %s", e)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def get_json(url, params=None, headers=None):
    r = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _ensure_state_dir():
    d = os.path.dirname(os.path.abspath(BOT_STATE_FILE))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def bot_save():
    try:
        _ensure_state_dir()
        with bot_state_lock:
            data = {k: bot_state[k] for k in (
                "running", "paused", "emergency", "reason", "mode", "settings", "strategy",
                "balance", "positions", "closedTrades", "logs", "startedAt",
                "stoppedAt", "lastAction", "lastSignalScan", "scanResults", "lastDate",
                "dailyTradesReset", "dailyTradesCount", "cooldown", "realizedPnl",
                "filteredByHTF", "filteredByPatterns",
            )}
            data["equityCurve"] = bot_state["equityCurve"][-2000:]
            data["closedTrades"] = bot_state["closedTrades"][-1000:]
            data["logs"] = bot_state["logs"][-500:]
        tmp = BOT_STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, BOT_STATE_FILE)
    except Exception as exc:
        log.warning("save fail: %s", exc)

def bot_load():
    try:
        if not os.path.exists(BOT_STATE_FILE):
            return
        with open(BOT_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        with bot_state_lock:
            for k in data:
                if k in bot_state:
                    bot_state[k] = data[k]
            merged = dict(DEFAULT_STRATEGY)
            if bot_state.get("strategy"):
                merged.update(bot_state["strategy"])
            bot_state["strategy"] = merged
            for key in ("enableProfitTarget", "enableMaxLoss", "enableDailyMaxLoss",
                        "fixedTradeSize", "enableDailyMaxTrades", "enableMaxPositions",
                        "enableHTFFilter", "enablePatterns"):
                bot_state["settings"].setdefault(key, True)
            bot_state["settings"].setdefault("dailyMaxTrades", DEFAULT_DAILY_MAX_TRADES)
            bot_state["settings"].setdefault("riskPerTradePct", RISK_PER_TRADE_PCT)
            bot_state["settings"].setdefault("maxOpenPositions", MAX_OPEN_POSITIONS)
            bot_state["settings"].setdefault("maxTotalExposurePct", MAX_TOTAL_EXPOSURE_PCT)
            bot_state["settings"].setdefault("htfInterval", HTF_INTERVAL_DEFAULT)
            bot_state["settings"].setdefault("futuresBalance", FUTURES_START_BALANCE)
            bot_state["settings"].setdefault("futuresStartBalance", FUTURES_START_BALANCE)
            bot_state.setdefault("mode", "spot")
            if bot_state.get("running"):
                bot_state["running"] = False
                bot_state["reason"] = "Восстановлен после перезапуска — требуется ручной старт"
                bot_state["lastAction"] = bot_state["reason"]
    except Exception as exc:
        log.warning("load fail: %s", exc)

def bot_log(text, color="blue"):
    entry = {"time": datetime.now().strftime("%H:%M:%S"), "text": text,
             "color": color, "ts": time.time()}
    with bot_state_lock:
        bot_state["logs"].insert(0, entry)
        bot_state["logs"] = bot_state["logs"][:500]
    bot_save()

def fetch_prices():
    data = get_json(BASE + "/v5/market/tickers", {"category": "spot"})
    if data.get("retCode") != 0:
        raise RuntimeError(data.get("retMsg", "err"))
    prices, changes, turnover = {}, {}, {}
    for item in (data.get("result") or {}).get("list", []):
        pair = str(item.get("symbol", "")).upper()
        if not pair.endswith("USDT"):
            continue
        symbol = pair[:-4]
        try:
            value = float(item.get("lastPrice"))
        except Exception:
            continue
        if not math.isfinite(value) or value <= 0:
            continue
        prices[symbol] = value
        try:
            changes[symbol] = float(item.get("price24hPcnt")) * 100
        except Exception:
            changes[symbol] = None
        try:
            turnover[symbol] = float(item.get("turnover24h") or 0)
        except Exception:
            turnover[symbol] = 0
    if not prices:
        raise RuntimeError("no prices")
    bybit_status.update(available=True, lastError=None, lastCheck=now_iso())
    return prices, changes, turnover

def refresh(force=False):
    if cache["prices"] and time.time() - cache["updated"] < CACHE_SECONDS and not force:
        return dict(cache["prices"]), dict(cache["changes"]), cache["error"]
    with refresh_lock:
        if cache["prices"] and time.time() - cache["updated"] < CACHE_SECONDS and not force:
            return dict(cache["prices"]), dict(cache["changes"]), cache["error"]
        try:
            prices, changes, turnover = fetch_prices()
            remember(prices)
            cache.update(updated=time.time(), prices=prices, changes=changes,
                         turnover=turnover, error=None, stale=False)
            return dict(prices), dict(changes), None
        except Exception as exc:
            bybit_status.update(available=False, lastError=str(exc), lastCheck=now_iso())
            cache["error"] = str(exc)
            cache["stale"] = True
            if cache["prices"]:
                return dict(cache["prices"]), dict(cache["changes"]), str(exc)
            raise

def prices_age() -> float:
    return time.time() - cache["updated"] if cache["updated"] else float("inf")

def prices_fresh() -> bool:
    return (not cache["stale"]) and prices_age() < STALE_PRICE_SECONDS

def remember(prices, source="bybit"):
    timestamp = time.time()
    cutoff = timestamp - HISTORY_HOURS * 3600
    valid = {}
    with lock:
        for symbol, value in prices.items():
            try:
                value = float(value)
            except Exception:
                continue
            if not math.isfinite(value) or value <= 0:
                continue
            valid[symbol] = value
            pts = history.setdefault((source, symbol), [])
            pts.append({"timestamp": timestamp, "price": value})
            history[(source, symbol)] = [p for p in pts if p["timestamp"] >= cutoff]
    if not DATABASE_URL or not valid:
        return
    try:
        with db_lock:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """INSERT INTO price_history (source,symbol,price,observed_at)
                           VALUES (%s,%s,%s,to_timestamp(%s))
                           ON CONFLICT (source,symbol,observed_at) DO NOTHING""",
                        [(source, s, p, timestamp) for s, p in valid.items()],
                    )
                    cur.execute(
                        """DELETE FROM price_history
                           WHERE observed_at < NOW() - (%s * INTERVAL '1 hour')""",
                        (HISTORY_HOURS,),
                    )
                    conn.commit()
    except Exception as exc:
        db_status.update(available=False, lastError=str(exc))

def get_points(symbol):
    with lock:
        pts = list(history.get(("bybit", symbol), []))
    cutoff = time.time() - HISTORY_HOURS * 3600
    return [p for p in pts if p["timestamp"] >= cutoff]

def ema(values, period):
    values = [float(v) for v in values if v and float(v) > 0]
    if len(values) < period:
        return None
    result = sum(values[:period]) / period
    multiplier = 2 / (period + 1)
    for value in values[period:]:
        result = (value - result) * multiplier + result
    return result

def rsi(values, period=14):
    values = [float(v) for v in values if v and float(v) > 0]
    if len(values) < period + 1:
        return None
    changes = [values[i] - values[i - 1] for i in range(1, len(values))][-period:]
    gains = sum(max(v, 0) for v in changes) / period
    losses = sum(max(-v, 0) for v in changes) / period
    if losses == 0:
        return 100.0 if gains else 50.0
    return 100 - 100 / (1 + gains / losses)

def adx(highs, lows, closes, period=14):
    if len(closes) < period * 2:
        return None
    tr_list, plus_dm, minus_dm = [], [], []
    for i in range(1, len(closes)):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        tr_list.append(tr)
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)

    def smooth(data, p):
        if len(data) < p:
            return []
        res = [sum(data[:p])]
        for val in data[p:]:
            res.append(res[-1] - (res[-1] / p) + val)
        return res

    tr_smooth = smooth(tr_list, period)
    plus_smooth = smooth(plus_dm, period)
    minus_smooth = smooth(minus_dm, period)
    if not tr_smooth or not plus_smooth or not minus_smooth:
        return None
    dx_list = []
    for i in range(len(tr_smooth)):
        if tr_smooth[i] == 0:
            continue
        pdi = 100 * (plus_smooth[i] / tr_smooth[i])
        mdi = 100 * (minus_smooth[i] / tr_smooth[i])
        if (pdi + mdi) == 0:
            continue
        dx_list.append(100 * abs(pdi - mdi) / (pdi + mdi))
    if not dx_list:
        return None
    return sum(dx_list[-period:]) / period
def get_signal_universe(top_n=SIGNAL_UNIVERSE_SIZE):
    with lock:
        turnover = dict(cache.get("turnover", {}))
    anchors = list(DEFAULT_STRATEGY["topCoins"])
    result = []
    for s in anchors:
        if s not in result:
            result.append(s)
    if turnover:
        for sym, _ in sorted(turnover.items(), key=lambda x: x[1] or 0, reverse=True):
            if sym in result:
                continue
            result.append(sym)
            if len(result) >= top_n:
                break
    return result[:max(top_n, len(anchors))]

def fetch_klines(symbol, interval, limit=200):
    try:
        response = requests.get(
            BASE + "/v5/market/kline",
            params={"category": "spot", "symbol": symbol + "USDT",
                    "interval": interval, "limit": limit},
            headers={"User-Agent": "CryptoPulse/5.0", "Accept": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("retCode") != 0:
            return {"ok": False, "error": data.get("retMsg", "err"), "points": []}
        raw = (data.get("result") or {}).get("list", [])
        points = []
        for item in reversed(raw):
            try:
                points.append({
                    "timestamp": int(item[0]) / 1000,
                    "open": float(item[1]), "high": float(item[2]),
                    "low": float(item[3]), "price": float(item[4]),
                    "volume": float(item[5]),
                    "close_time": (int(item[0]) + 60_000) / 1000,
                })
            except (TypeError, ValueError, IndexError):
                continue
        return {"ok": True, "error": None, "points": points}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "points": []}

def refresh_htf_trends(symbols, interval="60", force=False):
    if not force and htf_cache["updated"] > 0 and htf_cache.get("current_interval") == interval and time.time() - htf_cache["updated"] < 300:
        return
    if htf_cache.get("current_interval") != interval:
        with htf_lock:
            htf_cache["trends"] = {}
            htf_cache["updated"] = 0
            htf_cache["current_interval"] = interval

    def get_one(sym):
        res = fetch_klines(sym, interval, 200)
        if not res["ok"] or len(res["points"]) < HTF_EMA_PERIOD + 5:
            return sym, None
        closes = [p["price"] for p in res["points"]]
        e50 = ema(closes, HTF_EMA_PERIOD)
        if e50 is None:
            return sym, None
        price = closes[-1]
        tol = HTF_TOLERANCE_PCT / 100
        if price > e50 * (1 + tol):
            direction = "up"
        elif price < e50 * (1 - tol):
            direction = "down"
        else:
            direction = "flat"
        return sym, {
            "direction": direction,
            "ema50": e50,
            "price": price,
            "pct_from_ema50": (price / e50 - 1) * 100,
            "ts": time.time(),
        }
    new_trends = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(get_one, s): s for s in symbols}
        for fut in as_completed(futures):
            try:
                sym, data = fut.result()
                if data:
                    new_trends[sym] = data
            except Exception:
                pass
    with htf_lock:
        htf_cache["trends"].update(new_trends)
        htf_cache["updated"] = time.time()
        htf_cache["current_interval"] = interval

def get_htf_direction(symbol):
    with htf_lock:
        entry = htf_cache["trends"].get(symbol)
    return entry["direction"] if entry else "unknown"

def refresh_patterns(symbols, force=False):
    if not force and time.time() - patterns_cache["updated"] < 300 and patterns_cache["by_symbol"]:
        return
    def get_one(sym):
        res = fetch_klines(sym, "60", 100)
        if not res["ok"] or len(res["points"]) < 10:
            return sym, None
        result = detect_patterns(res["points"], lookback=PATTERN_LOOKBACK)
        return sym, {
            "found": result["found"],
            "has_bullish": result["has_bullish"],
            "has_bearish": result["has_bearish"],
            "has_neutral": result["has_neutral"],
            "enabled": result["enabled"],
            "method": result["method"],
            "strength": pattern_strength(result),
            "ts": time.time(),
        }
    new_patterns = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(get_one, s): s for s in symbols}
        for fut in as_completed(futures):
            try:
                sym, data = fut.result()
                if data:
                    new_patterns[sym] = data
            except Exception:
                pass
    with patterns_lock:
        patterns_cache["by_symbol"].update(new_patterns)
        patterns_cache["updated"] = time.time()

def get_patterns_for(symbol):
    with patterns_lock:
        return patterns_cache["by_symbol"].get(symbol)

def refresh_strategy_history_background(symbols):
    global strategy_history_updated
    if history_loading_flag.is_set():
        return
    history_loading_flag.set()
    try:
        with bot_state_lock:
            bot_state["scanStatus"] = f"Загружаю 1m-свечи: 0/{len(symbols)}"
        completed = 0
        successful = 0
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                pool.submit(fetch_klines, symbol, SCAN_CANDLE_INTERVAL, SCAN_CANDLE_LIMIT): symbol
                for symbol in symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    res = future.result()
                    if res["ok"] and len(res["points"]) >= 50:
                        with strategy_history_lock:
                            strategy_history[symbol] = res["points"]
                        successful += 1
                except Exception:
                    pass
                completed += 1
                with bot_state_lock:
                    bot_state["scanStatus"] = f"Загружаю 1m-свечи: {completed}/{len(symbols)}"
        with strategy_history_lock:
            strategy_history_updated = time.time()
        with bot_state_lock:
            bot_state["scanStatus"] = f"Готово: {successful}/{len(symbols)}"
    except Exception as e:
        with bot_state_lock:
            bot_state["scanStatus"] = f"Ошибка: {e}"
    finally:
        history_loading_flag.clear()

def analyze(symbol, price, strategy=None):
    st = strategy or bot_state["strategy"]
    with strategy_history_lock:
        pts = list(strategy_history.get(symbol, []))
    if not pts or len(pts) < 50:
        return {"symbol": symbol, "price": price, "rsi14": None, "ema9": None,
                "ema21": None, "signal": "insufficient_history",
                "explanation": "Мало данных.", "sampleCount": len(pts) if pts else 0}

    now = time.time()
    closed = [p for p in pts if p.get("close_time", 0) < now]
    if len(closed) < 50:
        return {"symbol": symbol, "price": price, "rsi14": None, "ema9": None,
                "ema21": None, "signal": "stale",
                "explanation": "Нет закрытых свечей.", "sampleCount": len(closed)}

    values = [p["price"] for p in closed]
    highs = [p["high"] for p in closed]
    lows = [p["low"] for p in closed]
    volumes = [p.get("volume", 0) for p in closed]

    ema_fast = ema(values, int(st["emaFast"]))
    ema_slow = ema(values, int(st["emaSlow"]))
    rsi_value = rsi(values, int(st["rsiPeriod"]))
    adx_value = adx(highs, lows, values, 14)

    signal = "insufficient_history"
    explanation = "Мало данных."
    has_volume = False

    if adx_value is not None and adx_value < 20:
        return {"symbol": symbol, "price": price, "ema9": ema_fast, "ema21": ema_slow,
                "rsi14": rsi_value, "signal": "neutral",
                "explanation": f"Флэт (ADX {adx_value:.1f}).", "sampleCount": len(values)}

    if ema_fast is not None and ema_slow is not None and rsi_value is not None:
        if len(volumes) >= 10:
            avg_vol = sum(volumes[-10:-1]) / 9
            if volumes[-1] > avg_vol * 1.1:
                has_volume = True

        if price > ema_fast > ema_slow and st["rsiBuyMin"] <= rsi_value < st["rsiBuyMax"]:
            if has_volume:
                signal = "buy_candidate"
                # Исправлено: проверка на None
                adx_str = f"{adx_value:.1f}" if adx_value is not None else "—"
                explanation = f"Бычий сигнал (ADX {adx_str}) + Объем."
            else:
                explanation = "Мало объема."
        elif price < ema_fast < ema_slow and st["rsiSellMin"] < rsi_value <= st["rsiSellMax"]:
            if not ALLOW_SHORTS:
                signal = "neutral"
                explanation = "Шорт отключён (spot-only)."
            elif has_volume:
                signal = "sell_candidate"
                # Исправлено: проверка на None
                adx_str = f"{adx_value:.1f}" if adx_value is not None else "—"
                explanation = f"Медвежий сигнал (ADX {adx_str}) + Объем."
            else:
                explanation = "Мало объема."
        else:
            signal = "neutral"
            explanation = "Условия не совпали."

    return {"symbol": symbol, "price": price, "ema9": ema_fast, "ema21": ema_slow,
            "rsi14": rsi_value, "signal": signal, "explanation": explanation,
            "sampleCount": len(values)}

def usd_rub():
    if usd_rub_cache["rate"] and time.time() - usd_rub_cache["updated"] < 900:
        return usd_rub_cache["rate"]
    data = get_json("https://open.er-api.com/v6/latest/USD")
    rate = float(data["rates"]["RUB"])
    if not math.isfinite(rate) or rate <= 0:
        raise RuntimeError("bad rate")
    usd_rub_cache.update(updated=time.time(), rate=rate)
    return rate

def market_payload(force=False):
    prices, changes, warning = refresh(force)
    try:
        rate = usd_rub()
        rate_err = None
    except Exception as exc:
        rate = None
        rate_err = str(exc)
    coins = []
    for symbol, price in prices.items():
        pts = [p for p in get_points(symbol) if 2700 <= time.time() - p["timestamp"] <= 4500]
        change_1h = None
        if pts:
            ref = min(pts, key=lambda p: abs(time.time() - p["timestamp"] - 3600))["price"]
            change_1h = (price / ref - 1) * 100
        coins.append({
            "symbol": symbol, "name": symbol, "priceUsd": price,
            "priceRub": price * rate if rate else None,
            "change1h": change_1h, "change24h": changes.get(symbol),
            "source": "bybit",
        })
    coins.sort(key=lambda c: c["symbol"])
    return {
        "success": True, "updatedAt": now_iso(), "source": "bybit",
        "coins": coins, "usdRub": rate, "usdRubError": rate_err,
        "databaseAvailable": db_status["available"], "warning": warning,
        "stale": cache["stale"], "ageSeconds": prices_age(),
    }

def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("no db")
    if psycopg is None:
        raise RuntimeError("no psycopg")
    return psycopg.connect(DATABASE_URL, connect_timeout=8, row_factory=dict_row)

def init_db():
    if not DATABASE_URL:
        return
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS price_history (
                    id BIGSERIAL PRIMARY KEY, source TEXT NOT NULL, symbol TEXT NOT NULL,
                    price DOUBLE PRECISION NOT NULL CHECK(price > 0),
                    observed_at TIMESTAMPTZ NOT NULL, UNIQUE(source, symbol, observed_at))""")
                cur.execute("""CREATE INDEX IF NOT EXISTS idx_price_history_symbol_time
                    ON price_history(symbol, observed_at DESC)""")
                conn.commit()
        db_status.update(available=True, lastError=None)
    except Exception as exc:
        db_status.update(available=False, lastError=str(exc))

def signed_get(path, params):
    if not API_KEY or not API_SECRET:
        raise RuntimeError("no key")
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    query = urlencode(sorted((k, str(v)) for k, v in params.items()))
    signature = hmac.new(API_SECRET.encode(), (ts + API_KEY + recv_window + query).encode(), "sha256").hexdigest()
    headers = {"X-BAPI-API-KEY": API_KEY, "X-BAPI-TIMESTAMP": ts,
               "X-BAPI-RECV-WINDOW": recv_window, "X-BAPI-SIGN": signature,
               "X-BAPI-SIGN-TYPE": "2"}
    data = get_json(BASE + path, params=params, headers=headers)
    if data.get("retCode") != 0:
        raise RuntimeError(f"err {data.get('retCode')}: {data.get('retMsg')}")
    return data.get("result") or {}

def wallet_balance(force=False):
    if wallet_cache["data"] and time.time() - wallet_cache["updated"] < 60 and not force:
        return wallet_cache["data"]
    result = signed_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    assets = []
    total = 0.0
    for account in result.get("list", []):
        for coin in account.get("coin", []):
            try:
                balance = float(coin.get("walletBalance") or 0)
                usd_value = float(coin.get("usdValue") or 0)
            except Exception:
                continue
            if balance == 0 and usd_value == 0:
                continue
            assets.append({"coin": coin.get("coin", ""), "walletBalance": balance,
                           "usdValue": usd_value,
                           "availableToWithdraw": coin.get("availableToWithdraw")})
            total += usd_value
    assets.sort(key=lambda a: a["usdValue"], reverse=True)
    data = {"success": True, "available": True, "accountType": "UNIFIED",
            "totalUsd": total, "assets": assets, "updatedAt": now_iso()}
    wallet_cache.update(updated=time.time(), data=data)
    return data

def bot_open_position(symbol, price, side, reason=""):
    with bot_state_lock:
        mode = bot_state.get("mode", "spot")
        settings = bot_state["settings"]
        
        if mode == "futures":
            if side == "Short" and not ALLOW_SHORTS:
                bot_log(f"Отказ: шорт {symbol} запрещён.", "gold")
                return None
            balance_key = "futuresBalance"
        else:
            if side == "Short":
                bot_log(f"Отказ: шорт {symbol} запрещён (spot-only).", "gold")
                return None
            balance_key = "balance"

        if settings.get("enableMaxPositions", True):
            max_pos = int(settings.get("maxOpenPositions", MAX_OPEN_POSITIONS))
            if len(bot_state["positions"]) >= max_pos:
                return None
        
        total_exposure = sum(p["sizeUsd"] for p in bot_state["positions"] if p.get("mode") == mode)
        balance = float(settings[balance_key])
        
        if total_exposure >= balance * (settings.get("maxTotalExposurePct", MAX_TOTAL_EXPOSURE_PCT) / 100):
            return None
        for p in bot_state["positions"]:
            if p["symbol"] == symbol and p.get("mode") == mode:
                return None

        if settings.get("fixedTradeSize", True):
            size = float(settings["tradeSize"])
        else:
            size = size_from_risk(balance, settings.get("riskPerTradePct", RISK_PER_TRADE_PCT),
                                  SL_PERCENT, settings.get("maxPositionPct", MAX_POSITION_PCT))
        if size < 10:
            size = 10

        costs = open_position_costs(size)
        if balance < costs["cash_out"]:
            return None

        qty = size / price
        tp_pct = TP_PERCENT / 100
        sl_pct = SL_PERCENT / 100
        
        liq_price = None
        if side == "Long":
            tp = price * (1 + tp_pct)
            sl = price * (1 - sl_pct)
            if mode == "futures":
                liq_price = price * (1 - (1 / FUTURES_LEVERAGE) + 0.01)
        else:
            tp = price * (1 - tp_pct)
            sl = price * (1 + sl_pct)
            if mode == "futures":
                liq_price = price * (1 + (1 / FUTURES_LEVERAGE) - 0.01)

        position = {
            "id": int(time.time() * 1000) + len(bot_state["positions"]),
            "symbol": symbol, "side": side, "sizeUsd": size, "entryPrice": price,
            "qty": qty, "openedAt": now_iso(), "openedTs": time.time(),
            "tp": tp, "sl": sl, "reason": reason,
            "entryFee": costs["entry_fee"],
            "mode": mode,
            "liq_price": liq_price
        }
        
        bot_state["positions"].append(position)
        settings[balance_key] -= costs["cash_out"]
        bot_state["dailyTradesCount"] = bot_state.get("dailyTradesCount", 0) + 1
        bot_state["lastAction"] = f"Открыта {side} {symbol}/USDT @ ${price:,.4f} (${size:.0f}) [{mode.upper()}]"
    
    bot_log(f"Открыта {side} {symbol}/USDT. Вход ${price:,.4f}, TP ${tp:,.4f}, SL ${sl:,.4f} [{mode.upper()}]", "blue")
    push_equity_point()
    bot_save()
    return position

def bot_close_position(position, price, reason="manual"):
    with bot_state_lock:
        if position not in bot_state["positions"]:
            return None
        bot_state["positions"].remove(position)
        
        mode = position.get("mode", "spot")
        balance_key = "futuresBalance" if mode == "futures" else "balance"
        
        gross_pnl = (price - position["entryPrice"]) * position["qty"] if position["side"] == "Long" \
            else (position["entryPrice"] - price) * position["qty"]
            
        if mode == "futures":
            gross_pnl *= FUTURES_LEVERAGE
            
        costs = close_position_costs(position["sizeUsd"], gross_pnl)
        net_pnl = costs["net_pnl"]
        pnl_pct = (net_pnl / position["sizeUsd"]) * 100 if position["sizeUsd"] else 0
        
        bot_state["settings"][balance_key] += costs["cash_in"]
        bot_state["realizedPnl"] = bot_state.get("realizedPnl", 0.0) + net_pnl
        
        trade = {
            "symbol": position["symbol"], "side": position["side"],
            "sizeUsd": position["sizeUsd"], "entryPrice": position["entryPrice"],
            "exitPrice": price, "pnl": net_pnl, "pnlPct": pnl_pct,
            "entryFee": position.get("entryFee", 0.0),
            "exitFee": costs["exit_fee"],
            "reason": reason, "openedAt": position["openedAt"],
            "closedAt": now_iso(), "closedTs": time.time(),
            "mode": mode
        }
        bot_state["closedTrades"].insert(0, trade)
        bot_state["closedTrades"] = bot_state["closedTrades"][:1000]
        bot_state["lastAction"] = f"Закрыта {position['side']} {position['symbol']}/USDT: {net_pnl:+.2f}$ [{reason}] [{mode.upper()}]"
        if reason in ("SL", "Timeout", "Manual", "manual", "Breakeven", "Trailing", "TP", "LIQ"):
            bot_state["cooldown"][position["symbol"]] = time.time() + 300
            
    push_equity_point()
    bot_log(f"Закрыта {position['side']} {position['symbol']}/USDT @ ${price:,.4f}. P&L ${net_pnl:+.2f} ({pnl_pct:+.2f}%) [{reason}] [{mode.upper()}]",
            "green" if net_pnl >= 0 else "red")
    bot_save()
    return trade

def push_equity_point():
    with bot_state_lock:
        free = float(bot_state["balance"])
        futures_free = float(bot_state["settings"].get("futuresBalance", 0))
        positions = [dict(p) for p in bot_state["positions"]]
    try:
        prices, _, _ = refresh()
    except Exception:
        prices = {}
    eq_spot = compute_equity(free, [p for p in positions if p.get("mode") == "spot"], prices)
    eq_fut = compute_equity(futures_free, [p for p in positions if p.get("mode") == "futures"], prices)
    
    with bot_state_lock:
        total_eq = eq_spot["equity"] + eq_fut["equity"]
        bot_state["equityCurve"].append({"ts": time.time(), "balance": total_eq})
        bot_state["equityCurve"] = bot_state["equityCurve"][-2000:]

def bot_check_limits():
    with bot_state_lock:
        settings = bot_state["settings"]
        start = float(settings["startBalance"])
        free = float(bot_state["balance"])
        positions = [dict(p) for p in bot_state["positions"] if p.get("mode") == "spot"]
    try:
        prices, _, _ = refresh()
    except Exception:
        prices = {}
    eq = compute_equity(free, positions, prices)
    total_equity = eq["equity"]
    diff = total_equity - start
    today = datetime.now().strftime("%Y-%m-%d")
    with bot_state_lock:
        if bot_state["lastDate"] != today:
            bot_state["lastDate"] = today
            bot_state["settings"]["dailyStartBalance"] = total_equity
            bot_save()
        if settings.get("enableDailyMaxLoss", True):
            daily_diff = total_equity - float(settings.get("dailyStartBalance", total_equity))
            daily_max = float(settings.get("dailyMaxLoss", 30.0))
            if daily_diff <= -daily_max:
                return "daily_loss", f"Достигнут дневной лимит убытка (-${abs(daily_diff):.2f})"
        if settings.get("enableProfitTarget", True):
            if diff >= float(settings["profitTarget"]):
                return "profit", f"Достигнут лимит прибыли (+${diff:.2f})"
        if settings.get("enableMaxLoss", True):
            if diff <= -float(settings["maxLoss"]):
                return "loss", f"Достигнут лимит убытка (-${abs(diff):.2f})"
    return None, None

def bot_close_all(reason="manual"):
    try:
        prices, _, _ = refresh()
    except Exception:
        prices = {}
    with bot_state_lock:
        positions = list(bot_state["positions"])
    for position in positions:
        price = prices.get(position["symbol"])
        if price is None:
            bot_log(f"Не закрываю {position['symbol']}: нет цены.", "red")
            continue
        bot_close_position(position, price, reason)
def bot_tick():
    with bot_state_lock:
        if not bot_state["running"]:
            return
    if not prices_fresh():
        with bot_state_lock:
            bot_state["scanStatus"] = f"Цены устарели ({prices_age():.0f} с) — торговля приостановлена"
            bot_state["lastAction"] = "Ожидание свежих цен"
        return
    try:
        prices, _, _ = refresh(force=True)
    except Exception as exc:
        with bot_state_lock:
            bot_state["scanStatus"] = f"Ошибка цен: {exc}"
        bot_log(f"Цены: {exc}", "red")
        return
    today = datetime.now().strftime("%Y-%m-%d")
    with bot_state_lock:
        if bot_state.get("dailyTradesReset") != today:
            bot_state["dailyTradesReset"] = today
            bot_state["dailyTradesCount"] = 0
    btc_price = prices.get("BTC")
    if btc_price:
        pts = sorted(get_points("BTC"), key=lambda p: p["timestamp"])
        now = time.time()
        ref_candidates = [p for p in pts if 3500 <= now - p["timestamp"] <= 3700]
        if ref_candidates:
            ref = min(ref_candidates, key=lambda p: abs(now - p["timestamp"] - 3600))["price"]
            btc_1h = (btc_price / ref - 1) * 100
            if btc_1h <= PANIC_BTC_DROP_PCT:
                bot_log(f"PANIC: BTC {btc_1h:+.2f}% за час — закрываю всё", "red")
                bot_close_all("panic")
                with bot_state_lock:
                    bot_state["paused"] = True
                    bot_state["lastAction"] = f"Panic: BTC {btc_1h:.1f}%"
                    bot_state["scanStatus"] = "Panic — пауза"
                bot_save()
                return

    with bot_state_lock:
        positions = list(bot_state["positions"])
    for position in positions:
        price = prices.get(position["symbol"])
        if price is None:
            continue
        entry = position["entryPrice"]
        side = position["side"]
        mode = position.get("mode", "spot")
        
        if mode == "futures" and position.get("liq_price"):
            if (side == "Long" and price <= position["liq_price"]) or \
               (side == "Short" and price >= position["liq_price"]):
                bot_close_position(position, price, "LIQ")
                continue

        if side == "Long":
            profit_pct = (price - entry) / entry * 100
            if price >= position["tp"]:
                bot_close_position(position, price, "TP")
                continue
            if price <= position["sl"]:
                reason = "SL" if position["sl"] < entry else "Breakeven"
                bot_close_position(position, price, reason)
                continue
            if profit_pct >= BE_TRIGGER_PERCENT:
                if position["sl"] < entry:
                    position["sl"] = entry
                    bot_log(f"{position['symbol']}: Стоп в безубыток", "gold")
            if profit_pct >= TRAILING_TRIGGER_PERCENT:
                new_sl = price * (1 - TRAILING_DISTANCE_PERCENT / 100)
                if new_sl > position["sl"]:
                    position["sl"] = new_sl
        else:
            profit_pct = (entry - price) / entry * 100
            if price <= position["tp"]:
                bot_close_position(position, price, "TP")
                continue
            if price >= position["sl"]:
                reason = "SL" if position["sl"] > entry else "Breakeven"
                bot_close_position(position, price, reason)
                continue
            if profit_pct >= BE_TRIGGER_PERCENT:
                if position["sl"] > entry:
                    position["sl"] = entry
                    bot_log(f"{position['symbol']}: Стоп в безубыток", "gold")
            if profit_pct >= TRAILING_TRIGGER_PERCENT:
                new_sl = price * (1 + TRAILING_DISTANCE_PERCENT / 100)
                if new_sl < position["sl"]:
                    position["sl"] = new_sl

    kind, message = bot_check_limits()
    if kind:
        bot_log(message + ". Останавливаем.", "gold" if kind == "profit" else "red")
        bot_close_all("limits")
        with bot_state_lock:
            bot_state["running"] = False
            bot_state["reason"] = message
            bot_state["stoppedAt"] = now_iso()
            bot_state["lastAction"] = message
            bot_state["scanStatus"] = f"Стоп: {message}"
        bot_save()
        return

    with bot_state_lock:
        paused = bot_state.get("paused")
        last_eq_ts = bot_state["equityCurve"][-1]["ts"] if bot_state["equityCurve"] else 0
    if paused:
        if time.time() - last_eq_ts > EQUITY_PUSH_SECONDS:
            push_equity_point()
        return

    with bot_state_lock:
        daily_max = int(bot_state["settings"].get("dailyMaxTrades", DEFAULT_DAILY_MAX_TRADES))
        daily_count = int(bot_state.get("dailyTradesCount", 0))
        enable_max_trades = bot_state["settings"].get("enableDailyMaxTrades", True)
    if enable_max_trades and daily_count >= daily_max:
        bot_log(f"Дневной лимит сделок ({daily_count}/{daily_max})", "gold")
        with bot_state_lock:
            bot_state["lastAction"] = f"Лимит сделок: {daily_count}/{daily_max}"
        if time.time() - last_eq_ts > EQUITY_PUSH_SECONDS:
            push_equity_point()
        return
    if time.time() - last_eq_ts > EQUITY_PUSH_SECONDS:
        push_equity_point()

    st = bot_state["strategy"]
    top = st.get("topCoins", DEFAULT_STRATEGY["topCoins"])
    universe = get_signal_universe()
    scan_symbols = list(dict.fromkeys(top + universe))
    if not history_loading_flag.is_set() and time.time() - strategy_history_updated > SIGNAL_UNIVERSE_REFRESH_SECONDS:
        threading.Thread(target=refresh_strategy_history_background,
                         args=(scan_symbols,), daemon=True).start()

    with bot_state_lock:
        htf_enabled = bot_state["settings"].get("enableHTFFilter", True)
        htf_interval = bot_state["settings"].get("htfInterval", HTF_INTERVAL_DEFAULT)
        patterns_enabled = bot_state["settings"].get("enablePatterns", True)
        
    if htf_enabled:
        try:
            refresh_htf_trends(scan_symbols, interval=htf_interval)
        except Exception:
            log.exception("htf refresh failed")
    if patterns_enabled:
        try:
            refresh_patterns(scan_symbols)
        except Exception:
            log.exception("patterns refresh failed")

    scan = []
    for symbol in scan_symbols:
        price = prices.get(symbol)
        if price is None:
            continue
        result = analyze(symbol, price, st)
        scan.append({"symbol": symbol, "price": price, "rsi14": result["rsi14"],
                     "ema9": result["ema9"], "ema21": result["ema21"],
                     "signal": result["signal"], "explanation": result["explanation"],
                     "sampleCount": result["sampleCount"]})
    with bot_state_lock:
        bot_state["scanResults"] = scan
        bot_state["lastSignalScan"] = now_iso()
        signal_count = sum(1 for item in scan if item["signal"] in ("buy_candidate", "sell_candidate"))
        insufficient = sum(1 for item in scan if item["signal"] in ("insufficient_history", "stale"))
        bot_state["scanStatus"] = f"Проверено {len(scan)}/{len(scan_symbols)}; сигналов: {signal_count}"
        if insufficient:
            bot_state["scanStatus"] += f"; мало данных: {insufficient}"
    bot_save()

    with bot_state_lock:
        current = {p["symbol"] for p in bot_state["positions"]}
        mode = bot_state.get("mode", "spot")
    filtered_htf = 0
    filtered_patterns = 0
    for item in scan:
        if item["symbol"] in current:
            continue
        with bot_state_lock:
            cooldown_time = bot_state["cooldown"].get(item["symbol"], 0)
            if time.time() < cooldown_time:
                continue
        
        if item["signal"] == "buy_candidate":
            if htf_enabled:
                direction = get_htf_direction(item["symbol"])
                if direction != "up":
                    filtered_htf += 1
                    with bot_state_lock:
                        bot_state["filteredByHTF"] = bot_state.get("filteredByHTF", 0) + 1
                    continue
            if patterns_enabled:
                p = get_patterns_for(item["symbol"])
                if not p or not p.get("has_bullish"):
                    filtered_patterns += 1
                    with bot_state_lock:
                        bot_state["filteredByPatterns"] = bot_state.get("filteredByPatterns", 0) + 1
                    continue
            bot_open_position(item["symbol"], item["price"], "Long", "SmartScalp")
            
        elif item["signal"] == "sell_candidate" and ALLOW_SHORTS and mode == "futures":
            if htf_enabled:
                direction = get_htf_direction(item["symbol"])
                if direction != "down":
                    filtered_htf += 1
                    with bot_state_lock:
                        bot_state["filteredByHTF"] = bot_state.get("filteredByHTF", 0) + 1
                    continue
            if patterns_enabled:
                p = get_patterns_for(item["symbol"])
                if not p or not p.get("has_bearish"):
                    filtered_patterns += 1
                    with bot_state_lock:
                        bot_state["filteredByPatterns"] = bot_state.get("filteredByPatterns", 0) + 1
                    continue
            bot_open_position(item["symbol"], item["price"], "Short", "SmartScalp")

    with bot_state_lock:
        if filtered_htf or filtered_patterns:
            bot_state["lastAction"] = f"Отфильтровано: HTF {filtered_htf}, паттерны {filtered_patterns}. Позиций: {len(bot_state['positions'])}."
        elif not bot_state["positions"]:
            bot_state["lastAction"] = "Сканирует — сигналов нет."
        else:
            bot_state["lastAction"] = f"Держит {len(bot_state['positions'])} позиций. Ждёт TP/SL."

_worker_lock_fd = None

def acquire_worker_lock():
    global _worker_lock_fd
    if not BG_WORKER:
        return False
    lock_path = BOT_STATE_FILE + ".worker.lock"
    try:
        os.makedirs(os.path.dirname(os.path.abspath(lock_path)) or ".", exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _worker_lock_fd = fd
        return True
    except (OSError, BlockingIOError):
        log.warning("worker lock busy — background workers disabled in this process")
        return False

def bot_worker():
    while True:
        try:
            bot_tick()
        except Exception:
            log.exception("tick fail")
        time.sleep(BOT_TICK_SECONDS)

def universe_worker():
    time.sleep(10)
    while True:
        try:
            try:
                refresh(force=False)
            except Exception:
                pass
            if not history_loading_flag.is_set():
                refresh_strategy_history_background(get_signal_universe())
        except Exception:
            log.exception("universe fail")
        time.sleep(SIGNAL_UNIVERSE_REFRESH_SECONDS)

def worker():
    while True:
        try:
            refresh(force=True)
        except Exception:
            log.exception("bg fail")
        time.sleep(REFRESH_SECONDS)

def compute_stats():
    with bot_state_lock:
        trades = list(bot_state["closedTrades"])
        positions = list(bot_state["positions"])
        balance_free = float(bot_state["balance"])
        futures_free = float(bot_state["settings"].get("futuresBalance", 0))
        start = float(bot_state["settings"]["startBalance"])
        equity = list(bot_state["equityCurve"])
    try:
        prices, _, _ = refresh()
    except Exception:
        prices = {}
        
    eq_spot = compute_equity(balance_free, [p for p in positions if p.get("mode") == "spot"], prices)
    eq_fut = compute_equity(futures_free, [p for p in positions if p.get("mode") == "futures"], prices)
    
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] < 0]
    total = len(trades)
    winrate = len(wins) / total * 100 if total else 0
    sum_profit = sum(t["pnl"] for t in wins) if wins else 0
    sum_loss = abs(sum(t["pnl"] for t in losses)) if losses else 0
    profit_factor = (sum_profit / sum_loss) if sum_loss > 0 else (float("inf") if sum_profit > 0 else 0)
    avg_win = sum_profit / len(wins) if wins else 0
    avg_loss = -sum_loss / len(losses) if losses else 0
    best = max((t["pnl"] for t in trades), default=0)
    worst = min((t["pnl"] for t in trades), default=0)
    
    max_drawdown = 0.0
    peak = -1e18
    for point in equity:
        peak = max(peak, point["balance"])
        drawdown = peak - point["balance"]
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            
    by_symbol = {}
    for trade in trades:
        symbol = trade["symbol"]
        by_symbol.setdefault(symbol, {"symbol": symbol, "trades": 0, "pnl": 0.0, "wins": 0, "losses": 0})
        by_symbol[symbol]["trades"] += 1
        by_symbol[symbol]["pnl"] += trade["pnl"]
        if trade["pnl"] > 0:
            by_symbol[symbol]["wins"] += 1
        elif trade["pnl"] < 0:
            by_symbol[symbol]["losses"] += 1
            
    positions_live = []
    for p in positions:
        cur = prices.get(p["symbol"], p["entryPrice"])
        pnl = (cur - p["entryPrice"]) * p["qty"] if p["side"] == "Long" else (p["entryPrice"] - cur) * p["qty"]
        if p.get("mode") == "futures":
            pnl *= FUTURES_LEVERAGE
        positions_live.append({**p, "currentPrice": cur, "pnl": pnl,
                               "pnlPct": (pnl / p["sizeUsd"] * 100 if p["sizeUsd"] else 0)})
                               
    total_equity = eq_spot["equity"] + eq_fut["equity"]
    total_pnl = total_equity - start

    def recent_stats(n):
        subset = trades[:n]
        if not subset:
            return {"n": 0, "winrate": 0, "profitFactor": None, "avgPnl": 0, "sumPnl": 0}
        w = [t for t in subset if t["pnl"] > 0]
        l = [t for t in subset if t["pnl"] < 0]
        sw = sum(t["pnl"] for t in w)
        sl = abs(sum(t["pnl"] for t in l))
        pf = (sw / sl) if sl > 0 else (float("inf") if sw > 0 else 0)
        return {
            "n": len(subset),
            "winrate": len(w) / len(subset) * 100,
            "profitFactor": pf if math.isfinite(pf) else None,
            "avgPnl": sum(t["pnl"] for t in subset) / len(subset),
            "sumPnl": sum(t["pnl"] for t in subset),
        }

    return {
        "trades": total, "wins": len(wins), "losses": len(losses),
        "winrate": winrate, "sumProfit": sum_profit, "sumLoss": sum_loss,
        "profitFactor": profit_factor, "avgWin": avg_win, "avgLoss": avg_loss,
        "best": best, "worst": worst, "maxDrawdown": max_drawdown,
        "bySymbol": sorted(by_symbol.values(), key=lambda item: item["pnl"], reverse=True),
        "equity": total_equity, "freeCash": eq_spot["free_cash"] + eq_fut["free_cash"],
        "positionsValue": eq_spot["positions_value"] + eq_fut["positions_value"],
        "unrealizedPnl": eq_spot["unrealized_pnl"] + eq_fut["unrealized_pnl"],
        "startBalance": start, "totalPnl": total_pnl,
        "realizedPnl": bot_state.get("realizedPnl", 0.0),
        "openPositions": len(positions_live), "positionsLive": positions_live,
        "equityCurve": equity, "missingPrices": eq_spot["missing_prices"] + eq_fut["missing_prices"],
        "recent20": recent_stats(20),
        "recent50": recent_stats(50),
    }

@app.after_request
def headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    return response

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "CryptoPulse", "timestamp": now_iso()})

@app.route("/api/config")
def config():
    return jsonify({
        "success": True, "tradingMode": "simulation", "liveTradingEnabled": False,
        "allowShorts": ALLOW_SHORTS,
        "databaseConfigured": bool(DATABASE_URL),
        "databaseAvailable": db_status["available"],
        "stalePriceSeconds": STALE_PRICE_SECONDS,
        "adminApiEnabled": bool(os.getenv("BOT_ADMIN_SECRET", "").strip()),
        "htfFilter": {"enabled": True, "ema_period": HTF_EMA_PERIOD},
        "patterns": {"enabled": True, "talib": TALIB_AVAILABLE, "lookback": PATTERN_LOOKBACK},
    })

@app.route("/api/prices")
@app.route("/api/market-prices")
def prices_api():
    try:
        return jsonify(market_payload())
    except Exception:
        return jsonify({"success": False, "error": "Рынок недоступен", "coins": []}), 503

@app.route("/api/usd-rub")
def rub_api():
    try:
        return jsonify({"success": True, "rate": usd_rub(), "base": "USD", "currency": "RUB"})
    except Exception:
        return jsonify({"success": False, "error": "Курс недоступен"}), 503

@app.route("/api/account/balance")
@app.route("/api/bybit/balance")
def balance_api():
    try:
        return jsonify(wallet_balance())
    except Exception as exc:
        log.warning("bal: %s", exc)
        return jsonify({"success": False, "available": False, "error": "Баланс недоступен"}), 503

@app.route("/api/trading/signals")
def signals_api():
    try:
        payload = market_payload()
        st = bot_state["strategy"]
        universe = set(get_signal_universe())
        out = []
        for coin in payload["coins"]:
            if coin["symbol"] not in universe:
                continue
            item = analyze(coin["symbol"], coin["priceUsd"], st)
            item["change1h"] = coin["change1h"]
            item["change24h"] = coin["change24h"]
            out.append(item)
        return jsonify({"success": True, "signals": out})
    except Exception:
        return jsonify({"success": False, "signals": []}), 503

@app.route("/api/klines")
def klines_api():
    sym = (request.args.get("symbol") or "BTCUSDT").upper().strip()
    iv = (request.args.get("interval") or "1m").strip()
    bi = {"1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
          "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
          "1d": "D", "1w": "W"}.get(iv, iv)
    try:
        lim = max(1, min(int(request.args.get("limit", "200")), 1000))
    except Exception:
        lim = 200
    try:
        data = get_json(BASE + "/v5/market/kline",
                        {"category": "spot", "symbol": sym, "interval": bi, "limit": lim})
        if data.get("retCode") != 0:
            raise RuntimeError(data.get("retMsg"))
        raw = (data.get("result") or {}).get("list", [])
        candles = []
        for item in reversed(raw):
            try:
                candles.append({"time": int(item[0]), "open": float(item[1]),
                                "high": float(item[2]), "low": float(item[3]),
                                "close": float(item[4]),
                                "volume": float(item[5]) if len(item) > 5 else 0})
            except Exception:
                continue
        return jsonify({"success": True, "symbol": sym, "interval": iv, "candles": candles})
    except Exception as exc:
        return jsonify({"success": False, "error": "Свечи недоступны", "details": str(exc)}), 503

@app.route("/api/bot/state")
def bot_state_api():
    stats = compute_stats()
    with bot_state_lock:
        return jsonify({
            "success": True,
            "running": bot_state["running"], "paused": bot_state.get("paused", False),
            "emergency": bot_state["emergency"], "reason": bot_state["reason"],
            "mode": bot_state.get("mode", "spot"),
            "settings": bot_state["settings"], "strategy": bot_state["strategy"],
            "dailyTradesCount": bot_state.get("dailyTradesCount", 0),
            "balance": stats["equity"], "freeCash": stats["freeCash"],
            "positionsValue": stats["positionsValue"], "unrealizedPnl": stats["unrealizedPnl"],
            "realizedPnl": stats["realizedPnl"],
            "startBalance": stats["startBalance"], "totalPnl": stats["totalPnl"],
            "totalPnlPct": (stats["totalPnl"] / stats["startBalance"] * 100 if stats["startBalance"] else 0),
            "positions": stats["positionsLive"], "closedTrades": bot_state["closedTrades"][:100],
            "logs": bot_state["logs"][:100], "startedAt": bot_state["startedAt"],
            "stoppedAt": bot_state["stoppedAt"], "lastAction": bot_state["lastAction"],
            "lastSignalScan": bot_state["lastSignalScan"], "scanStatus": bot_state["scanStatus"],
            "scanResults": bot_state["scanResults"],
            "stale": cache["stale"], "ageSeconds": prices_age(),
            "bybitAvailable": bybit_status["available"],
            "bybitLastCheck": bybit_status["lastCheck"],
            "missingPrices": stats["missingPrices"],
            "filteredByHTF": bot_state.get("filteredByHTF", 0),
            "filteredByPatterns": bot_state.get("filteredByPatterns", 0),
            "stats": {
                "trades": stats["trades"], "wins": stats["wins"], "losses": stats["losses"],
                "winrate": stats["winrate"],
                "profitFactor": stats["profitFactor"] if math.isfinite(stats["profitFactor"]) else None,
                "avgWin": stats["avgWin"], "avgLoss": stats["avgLoss"],
                "best": stats["best"], "worst": stats["worst"],
                "maxDrawdown": stats["maxDrawdown"], "bySymbol": stats["bySymbol"],
                "equityCurve": stats["equityCurve"][-500:],
                "recent20": stats["recent20"],
                "recent50": stats["recent50"],
            },
        })

@app.route("/api/bot/mode", methods=["POST"])
@require_admin
def bot_mode_api():
    data = request.get_json(silent=True) or {}
    mode = data.get("mode", "spot")
    if mode not in ("spot", "futures"):
        return jsonify({"success": False, "error": "bad mode"}), 400
    with bot_state_lock:
        bot_state["mode"] = mode
    bot_log(f"Режим переключен на {mode.upper()}", "gold")
    bot_save()
    return jsonify({"success": True, "mode": mode})

@app.route("/api/bot/start", methods=["POST"])
@require_admin
def bot_start_api():
    if not prices_fresh():
        return jsonify({"success": False, "error": "Цены устарели — старт запрещён"}), 409
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        if "settings" in data and isinstance(data["settings"], dict):
            for key in ("profitTarget", "maxLoss", "tradeSize", "dailyMaxLoss",
                        "riskPerTradePct", "maxPositionPct"):
                if key in data["settings"]:
                    try:
                        bot_state["settings"][key] = float(data["settings"][key])
                    except Exception:
                        pass
            if "dailyMaxTrades" in data["settings"]:
                try:
                    bot_state["settings"]["dailyMaxTrades"] = max(1, int(data["settings"]["dailyMaxTrades"]))
                except Exception:
                    pass
            if "maxOpenPositions" in data["settings"]:
                try:
                    bot_state["settings"]["maxOpenPositions"] = max(1, int(data["settings"]["maxOpenPositions"]))
                except Exception:
                    pass
            if "maxTotalExposurePct" in data["settings"]:
                try:
                    bot_state["settings"]["maxTotalExposurePct"] = float(data["settings"]["maxTotalExposurePct"])
                except Exception:
                    pass
            if "htfInterval" in data["settings"]:
                val = str(data["settings"]["htfInterval"])
                if val in HTF_INTERVAL_MAP:
                    bot_state["settings"]["htfInterval"] = val
            for flag in ("enableProfitTarget", "enableMaxLoss", "enableDailyMaxLoss",
                         "fixedTradeSize", "enableDailyMaxTrades", "enableMaxPositions",
                         "enableHTFFilter", "enablePatterns"):
                if flag in data["settings"]:
                    bot_state["settings"][flag] = bool(data["settings"][flag])
        free = float(bot_state["balance"])
        pos_value = sum(p["sizeUsd"] for p in bot_state["positions"] if p.get("mode") == "spot")
        bot_state["settings"]["startBalance"] = free + pos_value
        bot_state["settings"]["dailyStartBalance"] = free + pos_value
        bot_state["running"] = True
        bot_state["paused"] = False
        bot_state["emergency"] = False
        bot_state["reason"] = "Бот запущен"
        bot_state["startedAt"] = now_iso()
        bot_state["stoppedAt"] = None
        bot_state["lastAction"] = "Запущен."
        bot_state["scanStatus"] = "Подготовка сканера…"
    push_equity_point()
    bot_log("Бот запущен.", "green")
    return jsonify({"success": True})

@app.route("/api/bot/stop", methods=["POST"])
@require_admin
def bot_stop_api():
    with bot_state_lock:
        bot_state["running"] = False
        bot_state["paused"] = False
        bot_state["reason"] = "Остановлен вручную"
        bot_state["stoppedAt"] = now_iso()
        bot_state["lastAction"] = "Остановлен вручную"
        bot_state["scanStatus"] = "Сканер остановлен"
    bot_log("Бот остановлен.", "red")
    return jsonify({"success": True})

@app.route("/api/bot/pause", methods=["POST"])
@require_admin
def bot_pause_api():
    data = request.get_json(silent=True) or {}
    paused = bool(data.get("paused", True))
    with bot_state_lock:
        bot_state["paused"] = paused
        bot_state["reason"] = "Пауза" if paused else "Работает"
        bot_state["lastAction"] = "Пауза — новые не открываю" if paused else "Возобновлён"
    bot_log("Пауза включена." if paused else "Пауза снята.", "gold")
    return jsonify({"success": True, "paused": paused})

@app.route("/api/bot/emergency", methods=["POST"])
@require_admin
def bot_emergency_api():
    bot_close_all("emergency")
    with bot_state_lock:
        bot_state["running"] = False
        bot_state["paused"] = False
        bot_state["emergency"] = True
        bot_state["reason"] = "Аварийная остановка"
        bot_state["stoppedAt"] = now_iso()
        bot_state["lastAction"] = "Аварийная остановка"
        bot_state["scanStatus"] = "Аварийная остановка"
    bot_log("АВАРИЙНАЯ ОСТАНОВКА.", "red")
    return jsonify({"success": True})

@app.route("/api/bot/close_position", methods=["POST"])
@require_admin
def bot_close_position_api():
    data = request.get_json(silent=True) or {}
    pos_id = data.get("id")
    with bot_state_lock:
        position = next((p for p in bot_state["positions"] if p["id"] == pos_id), None)
    if not position:
        return jsonify({"success": False, "error": "Позиция не найдена"}), 404
    try:
        prices, _, _ = refresh()
    except Exception:
        prices = {}
    price = prices.get(position["symbol"])
    if price is None:
        return jsonify({"success": False, "error": "Нет актуальной цены — закрытие отменено"}), 409
    bot_close_position(position, price, "Manual")
    return jsonify({"success": True})

@app.route("/api/bot/settings", methods=["POST"])
@require_admin
def bot_settings_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        for key in ("profitTarget", "maxLoss", "tradeSize", "dailyMaxLoss",
                    "riskPerTradePct", "maxPositionPct"):
            if key in data:
                try:
                    bot_state["settings"][key] = float(data[key])
                except Exception:
                    pass
        if "dailyMaxTrades" in data:
            try:
                bot_state["settings"]["dailyMaxTrades"] = max(1, int(data["dailyMaxTrades"]))
            except Exception:
                pass
        if "maxOpenPositions" in data:
            try:
                bot_state["settings"]["maxOpenPositions"] = max(1, int(data["maxOpenPositions"]))
            except Exception:
                pass
        if "maxTotalExposurePct" in data:
            try:
                bot_state["settings"]["maxTotalExposurePct"] = float(data["maxTotalExposurePct"])
            except Exception:
                pass
        if "htfInterval" in data:
            val = str(data["htfInterval"])
            if val in HTF_INTERVAL_MAP:
                bot_state["settings"]["htfInterval"] = val
        for flag in ("enableProfitTarget", "enableMaxLoss", "enableDailyMaxLoss",
                     "fixedTradeSize", "enableDailyMaxTrades", "enableMaxPositions",
                     "enableHTFFilter", "enablePatterns"):
            if flag in data:
                bot_state["settings"][flag] = bool(data[flag])
    bot_log("Настройки сохранены.", "blue")
    return jsonify({"success": True, "settings": bot_state["settings"]})

@app.route("/api/bot/strategy", methods=["POST"])
@require_admin
def bot_strategy_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        for key in ("emaFast", "emaSlow", "rsiPeriod", "rsiBuyMin", "rsiBuyMax",
                    "rsiSellMin", "rsiSellMax"):
            if key in data:
                try:
                    bot_state["strategy"][key] = int(data[key])
                except Exception:
                    pass
    bot_log("Стратегия обновлена.", "blue")
    return jsonify({"success": True, "strategy": bot_state["strategy"]})

@app.route("/api/bot/reset", methods=["POST"])
@require_admin
def bot_reset_api():
    with bot_state_lock:
        bot_state.update({
            "running": False, "paused": False, "emergency": False, "reason": "Сброшено",
            "balance": float(bot_state["settings"].get("startBalance", 1000.0)),
            "positions": [], "closedTrades": [], "logs": [], "equityCurve": [],
            "startedAt": None, "stoppedAt": None, "lastAction": "Сброшено",
            "scanResults": [], "scanStatus": "Сброшено", "dailyTradesCount": 0,
            "dailyTradesReset": datetime.now().strftime("%Y-%m-%d"),
            "cooldown": {}, "realizedPnl": 0.0, "filteredByHTF": 0, "filteredByPatterns": 0,
        })
        bot_state["settings"]["futuresBalance"] = float(bot_state["settings"].get("futuresStartBalance", FUTURES_START_BALANCE))
    bot_log("Бот сброшен.", "gold")
    return jsonify({"success": True})

@app.route("/api/bot/export.csv")
@require_admin
def bot_export_csv():
    with bot_state_lock:
        trades = list(bot_state["closedTrades"])
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["Time", "Mode", "Symbol",
                     "Side", "SizeUSD", "EntryPrice", "ExitPrice",
                     "PnL", "PnL%", "EntryFee", "ExitFee", "Reason"])
    for trade in trades:
        writer.writerow([
            trade.get("closedAt", ""), trade.get("mode", "spot"), trade.get("symbol", ""), trade.get("side", ""),
            f"{trade.get('sizeUsd', 0):.2f}", f"{trade.get('entryPrice', 0):.8f}",
            f"{trade.get('exitPrice', 0):.8f}", f"{trade.get('pnl', 0):.4f}",
            f"{trade.get('pnlPct', 0):.4f}", f"{trade.get('entryFee', 0):.6f}",
            f"{trade.get('exitFee', 0):.6f}", trade.get("reason", ""),
        ])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=cryptopulse_trades.csv"})

@app.route("/api/bot/diagnostics")
def bot_diagnostics_api():
    stats = compute_stats()
    symbols = list(htf_cache["trends"].keys()) or get_signal_universe()
    trends = []
    with htf_lock:
        for sym in symbols:
            entry = htf_cache["trends"].get(sym)
            if not entry:
                continue
            trends.append({
                "symbol": sym,
                "direction": entry["direction"],
                "price": entry["price"],
                "ema50": entry["ema50"],
                "pct_from_ema50": entry["pct_from_ema50"],
                "ageSeconds": time.time() - entry["ts"],
            })
    trends.sort(key=lambda x: x["symbol"])
    up = sum(1 for t in trends if t["direction"] == "up")
    down = sum(1 for t in trends if t["direction"] == "down")
    flat = sum(1 for t in trends if t["direction"] == "flat")
    with bot_state_lock:
        filtered_htf = bot_state.get("filteredByHTF", 0)
        filtered_patterns = bot_state.get("filteredByPatterns", 0)
        htf_enabled = bot_state["settings"].get("enableHTFFilter", True)
        htf_interval = bot_state["settings"].get("htfInterval", HTF_INTERVAL_DEFAULT)
        patterns_enabled = bot_state["settings"].get("enablePatterns", True)
        open_positions = len(bot_state["positions"])
    peak = -1e18
    current_dd = 0.0
    for p in stats["equityCurve"]:
        peak = max(peak, p["balance"])
        current_dd = max(current_dd, peak - p["balance"])
    return jsonify({
        "success": True,
        "htf": {
            "enabled": htf_enabled,
            "ema_period": HTF_EMA_PERIOD,
            "interval": htf_interval,
            "interval_label": HTF_INTERVAL_MAP.get(htf_interval, htf_interval),
            "trends": trends,
            "counts": {"up": up, "down": down, "flat": flat},
            "ageSeconds": time.time() - htf_cache["updated"] if htf_cache["updated"] else None,
        },
        "patterns": {
            "enabled": patterns_enabled,
            "talib": TALIB_AVAILABLE,
            "lookback": PATTERN_LOOKBACK,
            "ageSeconds": time.time() - patterns_cache["updated"] if patterns_cache["updated"] else None,
        },
        "filteredByHTF": filtered_htf,
        "filteredByPatterns": filtered_patterns,
        "openPositions": open_positions,
        "currentDrawdown": current_dd,
        "recent20": stats["recent20"],
        "recent50": stats["recent50"],
        "totalPnl": stats["totalPnl"],
        "totalPnlPct": (stats["totalPnl"] / stats["startBalance"] * 100) if stats["startBalance"] else 0,
    })

@app.route("/api/bot/patterns")
def bot_patterns_api():
    with patterns_lock:
        by_symbol = dict(patterns_cache["by_symbol"])
        updated = patterns_cache["updated"]
    out = []
    for sym, p in by_symbol.items():
        out.append({
            "symbol": sym,
            "found": p.get("found", []),
            "has_bullish": p.get("has_bullish", False),
            "has_bearish": p.get("has_bearish", False),
            "has_neutral": p.get("has_neutral", False),
            "method": p.get("method", "unknown"),
            "strength": p.get("strength", 0),
            "ageSeconds": time.time() - p["ts"] if p.get("ts") else None,
        })
    out.sort(key=lambda x: (-x["strength"], x["symbol"]))
    return jsonify({
        "success": True,
        "talib": TALIB_AVAILABLE,
        "lookback": PATTERN_LOOKBACK,
        "ageSeconds": time.time() - updated if updated else None,
        "patterns": out,
    })

def tg(method, payload):
    if not TG_TOKEN:
        raise RuntimeError("no token")
    r = session.post(f"https://api.telegram.org/bot{TG_TOKEN}/{method}", json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def tg_send(chat, text, markup=None):
    payload = {"chat_id": chat, "text": text}
    if markup:
        payload["reply_markup"] = markup
    return tg("sendMessage", payload)

def tg_notify(text):
    if not TG_TOKEN or not TG_OWNER_CHAT_ID:
        return
    try:
        tg_send(TG_OWNER_CHAT_ID, text)
    except Exception:
        pass

def keyboard():
    return {"inline_keyboard": [
        [{"text": "🌐 CryptoPulse", "web_app": {"url": WEBAPP_URL}},
         {"text": "📊 Цены", "callback_data": "prices"}],
        [{"text": "💱 USD/RUB", "callback_data": "usdrub"}],
    ]}

def telegram_prices():
    payload = market_payload()
    lines = ["CryptoPulse — Bybit Spot", ""]
    for coin in payload["coins"][:40]:
        change = "—" if coin["change24h"] is None else f'{coin["change24h"]:+.2f}%'
        lines.append(f'{coin["symbol"]}: ${coin["priceUsd"]:,.8f} | 24ч {change}')
    return "\n".join(lines)

@app.route("/telegram-webhook", methods=["POST"])
@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    if TG_SECRET:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not hmac.compare_digest(secret, TG_SECRET):
            return jsonify({"ok": False}), 401
    if not TG_TOKEN:
        return jsonify({"ok": False}), 503
    update = request.get_json(silent=True) or {}
    callback = update.get("callback_query")
    if callback:
        chat = ((callback.get("message") or {}).get("chat") or {}).get("id")
        try:
            tg("answerCallbackQuery", {"callback_query_id": callback.get("id")})
        except Exception:
            pass
        if chat:
            data = callback.get("data")
            try:
                if data == "prices":
                    tg_send(chat, telegram_prices(), keyboard())
                elif data == "usdrub":
                    rate = usd_rub()
                    tg_send(chat, f"💱 USD/RUB\n\n1 USD = ₽{rate:.2f}\n\nИсточник: open.er-api.com", keyboard())
            except Exception:
                tg_send(chat, "Недоступно.", keyboard())
        return jsonify({"ok": True})
    message = update.get("message") or {}
    chat = (message.get("chat") or {}).get("id")
    text = (message.get("text") or "").strip()
    if chat and text.startswith(("/start", "/help")):
        tg_send(chat, "CryptoPulse\nЦены, сигналы, курс USD/RUB.\nРеальная торговля отключена.", keyboard())
    elif chat and text.startswith("/prices"):
        try:
            tg_send(chat, telegram_prices(), keyboard())
        except Exception:
            tg_send(chat, "Недоступно.", keyboard())
    return jsonify({"ok": True})

def configure_telegram():
    if not TG_TOKEN:
        return
    try:
        tg("setChatMenuButton", {"menu_button": {"type": "web_app",
                                                 "text": "Открыть CryptoPulse",
                                                 "web_app": {"url": WEBAPP_URL}}})
        payload = {"url": WEBHOOK_URL}
        if TG_SECRET:
            payload["secret_token"] = TG_SECRET
        tg("setWebhook", payload)
    except Exception:
        log.exception("tg setup fail")

def _graceful_shutdown(signum, frame):
    log.info("shutdown signal %s", signum)
    try:
        with bot_state_lock:
            bot_state["stoppedAt"] = now_iso()
            bot_state["reason"] = "Остановлен сигналом ОС"
        bot_save()
    except Exception:
        pass

init_db()
bot_load()

if not bot_state["equityCurve"]:
    with bot_state_lock:
        free = float(bot_state["balance"])
        futures_free = float(bot_state["settings"].get("futuresBalance", 0))
        pos = sum(p["sizeUsd"] for p in bot_state["positions"])
        bot_state["equityCurve"].append({"ts": time.time(), "balance": free + futures_free + pos})

if TG_AUTO:
    configure_telegram()

if BG_WORKER and acquire_worker_lock():
    threading.Thread(target=worker, daemon=True).start()
    threading.Thread(target=bot_worker, daemon=True).start()
    threading.Thread(target=universe_worker, daemon=True).start()

signal.signal(signal.SIGTERM, _graceful_shutdown)
signal.signal(signal.SIGINT, _graceful_shutdown)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
