import os, time, hmac, math, logging, threading, json
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, send_from_directory, request

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

app = Flask(__name__, static_folder="static")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("cryptopulse")

BASE = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL", "https://cryptopulse-lpc9.onrender.com").rstrip("/")
WEBAPP_URL = os.getenv("TELEGRAM_WEBAPP_URL", PUBLIC_APP_URL + "/")
WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", PUBLIC_APP_URL + "/telegram-webhook")

TG_TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
TG_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
TG_AUTO = os.getenv("TELEGRAM_AUTO_CONFIGURE", "true").lower() in ("1", "true", "yes", "on")
BG_WORKER = os.getenv("ENABLE_BACKGROUND_WORKER", "true").lower() in ("1", "true", "yes", "on")

API_KEY = os.getenv("BYBIT_API_KEY", "").strip()
API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

TRADING_MODE = "simulation"
LIVE_TRADING_ENABLED = False

TIMEOUT = 12
CACHE_SECONDS = 30
HISTORY_HOURS = 72
REFRESH_SECONDS = 60

# --- СИМУЛЯЦИЯ БОТА ---
BOT_STATE_FILE = os.getenv("BOT_STATE_FILE", "bot_state.json")
BOT_TICK_SECONDS = 15          # как часто бот проверяет сигналы
BOT_TP_PERCENT = 2.0           # тейк-профит по каждой сделке, %
BOT_SL_PERCENT = 1.5           # стоп-лосс по каждой сделке, %
BOT_START_BALANCE = 100.0      # виртуальный стартовый депозит, $

session = requests.Session()
session.headers.update({"User-Agent": "CryptoPulse/2.0", "Accept": "application/json"})

lock = threading.RLock()
refresh_lock = threading.Lock()
db_lock = threading.Lock()

cache = {"updated": 0, "prices": {}, "changes": {}, "source": "bybit", "error": None}
history = {}
usd_rub_cache = {"updated": 0, "rate": None}

bybit_status = {"available": False, "lastError": None, "lastCheck": None}
db_status = {"configured": bool(DATABASE_URL), "available": False, "lastError": None}
wallet_cache = {"updated": 0, "data": None}

# --- Состояние бота (симуляция) ---
bot_state = {
    "running": False,
    "emergency": False,
    "reason": "Не запущен",
    "settings": {
        "profitTarget": 100.0,
        "maxLoss": 10.0,
        "tradeSize": 20.0,
        "startBalance": BOT_START_BALANCE
    },
    "balance": BOT_START_BALANCE,
    "positions": [],
    "closedTrades": [],
    "logs": [],
    "startedAt": None,
    "stoppedAt": None
}
bot_state_lock = threading.RLock()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_json(url, params=None, headers=None):
    response = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


# ============================================================
# СОХРАНЕНИЕ СОСТОЯНИЯ БОТА
# ============================================================

def bot_save():
    """Сохраняет состояние бота в JSON-файл."""
    try:
        with bot_state_lock:
            data = {
                "running": bot_state["running"],
                "emergency": bot_state["emergency"],
                "reason": bot_state["reason"],
                "settings": bot_state["settings"],
                "balance": bot_state["balance"],
                "positions": bot_state["positions"],
                "closedTrades": bot_state["closedTrades"][-500:],  # храним 500 последних
                "logs": bot_state["logs"][-300:],
                "startedAt": bot_state["startedAt"],
                "stoppedAt": bot_state["stoppedAt"]
            }
        with open(BOT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        log.warning("Bot state save failed: %s", exc)


def bot_load():
    """Загружает состояние бота из файла при старте."""
    global bot_state
    try:
        if not os.path.exists(BOT_STATE_FILE):
            return
        with open(BOT_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        with bot_state_lock:
            bot_state.update({
                "running": data.get("running", False),
                "emergency": data.get("emergency", False),
                "reason": data.get("reason", "Не запущен"),
                "settings": data.get("settings") or bot_state["settings"],
                "balance": data.get("balance", BOT_START_BALANCE),
                "positions": data.get("positions", []),
                "closedTrades": data.get("closedTrades", []),
                "logs": data.get("logs", []),
                "startedAt": data.get("startedAt"),
                "stoppedAt": data.get("stoppedAt")
            })
        log.info("Bot state loaded from %s", BOT_STATE_FILE)
    except Exception as exc:
        log.warning("Bot state load failed: %s", exc)


def bot_log(text, color="blue"):
    """Добавляет запись в журнал бота."""
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "text": text,
        "color": color,
        "ts": time.time()
    }
    with bot_state_lock:
        bot_state["logs"].insert(0, entry)
        bot_state["logs"] = bot_state["logs"][:300]
    bot_save()


# ============================================================
# БАЗА ДАННЫХ И ИСТОРИЯ ЦЕН
# ============================================================

def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not configured")
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    return psycopg.connect(DATABASE_URL, connect_timeout=8, row_factory=dict_row)


def init_db():
    if not DATABASE_URL:
        return
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS price_history (
                        id BIGSERIAL PRIMARY KEY,
                        source TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        price DOUBLE PRECISION NOT NULL CHECK(price > 0),
                        observed_at TIMESTAMPTZ NOT NULL,
                        UNIQUE(source, symbol, observed_at)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_price_history_symbol_time
                    ON price_history(symbol, observed_at DESC)
                """)
                cur.execute("""
                    SELECT source, symbol, price, EXTRACT(EPOCH FROM observed_at) AS ts
                    FROM price_history
                    WHERE observed_at >= NOW() - (%s * INTERVAL '1 hour')
                    ORDER BY observed_at
                """, (HISTORY_HOURS,))
                rows = cur.fetchall()
                conn.commit()
        with lock:
            for row in rows:
                key = (row["source"], row["symbol"])
                history.setdefault(key, []).append({
                    "timestamp": float(row["ts"]),
                    "price": float(row["price"])
                })
        db_status.update(available=True, lastError=None)
    except Exception as exc:
        db_status.update(available=False, lastError=str(exc))
        log.exception("DB init failed")


def remember(prices, source="bybit"):
    timestamp = time.time()
    cutoff = timestamp - HISTORY_HOURS * 3600
    valid = {}
    with lock:
        for symbol, value in prices.items():
            try:
                value = float(value)
            except (ValueError, TypeError):
                continue
            if not math.isfinite(value) or value <= 0:
                continue
            valid[symbol] = value
            points = history.setdefault((source, symbol), [])
            points.append({"timestamp": timestamp, "price": value})
            history[(source, symbol)] = [
                p for p in points if p["timestamp"] >= cutoff
            ]
    if not DATABASE_URL or not valid:
        return
    try:
        with db_lock:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO price_history (source, symbol, price, observed_at)
                        VALUES (%s, %s, %s, to_timestamp(%s))
                        ON CONFLICT (source, symbol, observed_at) DO NOTHING
                    """, [(source, s, p, timestamp) for s, p in valid.items()])
                    cur.execute("""
                        DELETE FROM price_history
                        WHERE observed_at < NOW() - (%s * INTERVAL '1 hour')
                    """, (HISTORY_HOURS,))
                    conn.commit()
        db_status.update(available=True, lastError=None)
    except Exception as exc:
        db_status.update(available=False, lastError=str(exc))
        log.warning("History save failed: %s", exc)


def get_points(symbol):
    with lock:
        points = list(history.get(("bybit", symbol), []))
    cutoff = time.time() - HISTORY_HOURS * 3600
    return [p for p in points if p["timestamp"] >= cutoff]


# ============================================================
# ИНДИКАТОРЫ
# ============================================================

def ema(values, period):
    values = [float(v) for v in values if v is not None and float(v) > 0]
    if len(values) < period:
        return None
    result = sum(values[:period]) / period
    multiplier = 2 / (period + 1)
    for value in values[period:]:
        result = (value - result) * multiplier + result
    return result


def rsi(values, period=14):
    values = [float(v) for v in values if v is not None and float(v) > 0]
    if len(values) < period + 1:
        return None
    changes = [values[i] - values[i-1] for i in range(1, len(values))][-period:]
    gain = sum(max(v, 0) for v in changes) / period
    loss = sum(max(-v, 0) for v in changes) / period
    if loss == 0:
        return 100.0 if gain else 50.0
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def analyze(symbol, price):
    points = sorted(get_points(symbol), key=lambda p: p["timestamp"])
    values = [p["price"] for p in points]
    if not values or values[-1] != price:
        values.append(price)
    ema9 = ema(values, 9)
    ema21 = ema(values, 21)
    rsi14 = rsi(values)
    signal = "insufficient_history"
    explanation = "Нужно больше исторических наблюдений."
    if len(values) >= 30 and ema9 is not None and ema21 is not None and rsi14 is not None:
        if price > ema9 > ema21 and 50 <= rsi14 < 68:
            signal = "buy_candidate"
            explanation = "Экспериментальный бычий сигнал EMA/RSI."
        elif price < ema9 < ema21 and 32 < rsi14 <= 50:
            signal = "sell_candidate"
            explanation = "Экспериментальный медвежий сигнал EMA/RSI."
        else:
            signal = "neutral"
            explanation = "Условия стратегии не совпали."
    return {
        "symbol": symbol, "price": price,
        "ema9": ema9, "ema21": ema21, "rsi14": rsi14,
        "sampleCount": len(values), "signal": signal,
        "explanation": explanation, "mode": "simulation"
    }


# ============================================================
# BYBIT
# ============================================================

def fetch_instruments():
    found = {}
    cursor = None
    for _ in range(20):
        params = {"category": "spot", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = get_json(BASE + "/v5/market/instruments-info", params)
        if data.get("retCode") != 0:
            raise RuntimeError(data.get("retMsg", "Bybit instruments error"))
        result = data.get("result") or {}
        for item in result.get("list", []):
            pair = str(item.get("symbol", "")).upper()
            if pair.endswith("USDT") and item.get("status") == "Trading":
                symbol = pair[:-4]
                if symbol and symbol not in {"USDC", "USDE", "FDUSD", "TUSD", "DAI", "USDP", "PYUSD"}:
                    found[symbol] = {"symbol": symbol, "name": symbol, "bybitSymbol": pair}
        cursor = result.get("nextPageCursor")
        if not cursor:
            break
    if not found:
        raise RuntimeError("Bybit returned no active spot USDT pairs")
    return found


def fetch_prices():
    data = get_json(BASE + "/v5/market/tickers", {"category": "spot"})
    if data.get("retCode") != 0:
        raise RuntimeError(data.get("retMsg", "Bybit ticker error"))
    prices = {}
    changes = {}
    for item in (data.get("result") or {}).get("list", []):
        pair = str(item.get("symbol", "")).upper()
        if not pair.endswith("USDT"):
            continue
        symbol = pair[:-4]
        try:
            value = float(item.get("lastPrice"))
        except (ValueError, TypeError):
            continue
        if not math.isfinite(value) or value <= 0:
            continue
        prices[symbol] = value
        try:
            changes[symbol] = float(item.get("price24hPcnt")) * 100
        except (ValueError, TypeError):
            changes[symbol] = None
    if not prices:
        raise RuntimeError("Bybit returned no usable spot prices")
    bybit_status.update(available=True, lastError=None, lastCheck=now_iso())
    return prices, changes


def refresh(force=False):
    if cache["prices"] and time.time() - cache["updated"] < CACHE_SECONDS and not force:
        return dict(cache["prices"]), dict(cache["changes"]), cache["error"]
    with refresh_lock:
        if cache["prices"] and time.time() - cache["updated"] < CACHE_SECONDS and not force:
            return dict(cache["prices"]), dict(cache["changes"]), cache["error"]
        try:
            prices, changes = fetch_prices()
            remember(prices)
            cache.update(updated=time.time(), prices=prices, changes=changes, source="bybit", error=None)
            return dict(prices), dict(changes), None
        except Exception as exc:
            bybit_status.update(available=False, lastError=str(exc), lastCheck=now_iso())
            cache["error"] = str(exc)
            if cache["prices"]:
                return dict(cache["prices"]), dict(cache["changes"]), str(exc)
            raise


# ============================================================
# USD/RUB
# ============================================================

def usd_rub():
    if usd_rub_cache["rate"] and time.time() - usd_rub_cache["updated"] < 900:
        return usd_rub_cache["rate"]
    data = get_json("https://open.er-api.com/v6/latest/USD")
    rate = float(data["rates"]["RUB"])
    if not math.isfinite(rate) or rate <= 0:
        raise RuntimeError("Invalid USD/RUB")
    usd_rub_cache.update(updated=time.time(), rate=rate)
    return rate


# ============================================================
# MARKET PAYLOAD
# ============================================================

def market_payload(force=False):
    prices, changes, warning = refresh(force)
    try:
        rate = usd_rub()
        rate_error = None
    except Exception as exc:
        rate = None
        rate_error = str(exc)
    coins = []
    for symbol, price in prices.items():
        points = [p for p in get_points(symbol) if 2700 <= time.time() - p["timestamp"] <= 4500]
        change1h = None
        if points:
            reference = min(points, key=lambda p: abs(time.time() - p["timestamp"] - 3600))["price"]
            change1h = (price / reference - 1) * 100
        coins.append({
            "symbol": symbol, "name": symbol, "priceUsd": price,
            "priceRub": price * rate if rate else None,
            "change1h": change1h,
            "change24h": changes.get(symbol),
            "source": "bybit"
        })
    coins.sort(key=lambda c: c["symbol"])
    return {
        "success": True, "updatedAt": now_iso(), "source": "bybit",
        "coins": coins, "usdRub": rate, "usdRubError": rate_error,
        "hourlyHistory": "postgresql-persistent" if DATABASE_URL else "in-memory-only",
        "databaseAvailable": db_status["available"],
        "warning": warning
    }


# ============================================================
# BYBIT ACCOUNT (read-only)
# ============================================================

def signed_get(path, params):
    if not API_KEY or not API_SECRET:
        raise RuntimeError("API key/secret not configured")
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    query = urlencode(sorted((k, str(v)) for k, v in params.items()))
    raw = timestamp + API_KEY + recv_window + query
    signature = hmac.new(API_SECRET.encode(), raw.encode(), "sha256").hexdigest()
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2"
    }
    data = get_json(BASE + path, params=params, headers=headers)
    if data.get("retCode") != 0:
        code = data.get("retCode")
        message = data.get("retMsg", "Bybit private API failed")
        raise RuntimeError(f"Bybit error {code}: {message}")
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
            except (TypeError, ValueError):
                continue
            if balance == 0 and usd_value == 0:
                continue
            assets.append({
                "coin": coin.get("coin", ""),
                "walletBalance": balance,
                "usdValue": usd_value,
                "availableToWithdraw": coin.get("availableToWithdraw")
            })
            total += usd_value
    assets.sort(key=lambda a: a["usdValue"], reverse=True)
    data = {
        "success": True, "available": True, "accountType": "UNIFIED",
        "totalUsd": total, "assets": assets, "updatedAt": now_iso()
    }
    wallet_cache.update(updated=time.time(), data=data)
    return data


# ============================================================
# ЛОГИКА СИМУЛЯЦИИ БОТА
# ============================================================

def bot_open_position(symbol, price, side):
    """Открывает виртуальную позицию."""
    with bot_state_lock:
        settings = bot_state["settings"]
        size_usd = float(settings["tradeSize"])
        if bot_state["balance"] < size_usd:
            bot_log(f"Недостаточно средств для открытия {side} {symbol}", "red")
            return None
        # не открывать вторую позицию по тому же символу
        for p in bot_state["positions"]:
            if p["symbol"] == symbol:
                return None
        qty = size_usd / price
        pos = {
            "id": int(time.time() * 1000),
            "symbol": symbol,
            "side": side,  # "Long" / "Short"
            "sizeUsd": size_usd,
            "entryPrice": price,
            "qty": qty,
            "openedAt": now_iso(),
            "tp": price * (1 + BOT_TP_PERCENT/100) if side == "Long" else price * (1 - BOT_TP_PERCENT/100),
            "sl": price * (1 - BOT_SL_PERCENT/100) if side == "Long" else price * (1 + BOT_SL_PERCENT/100)
        }
        bot_state["positions"].append(pos)
    bot_log(f"Открыта позиция {side} {symbol}/USDT. Размер: ${size_usd:.0f}", "blue")
    bot_save()
    return pos


def bot_close_position(pos, price, reason="manual"):
    """Закрывает виртуальную позицию, считает P&L."""
    with bot_state_lock:
        if pos not in bot_state["positions"]:
            return None
        bot_state["positions"].remove(pos)
        if pos["side"] == "Long":
            pnl = (price - pos["entryPrice"]) * pos["qty"]
        else:
            pnl = (pos["entryPrice"] - price) * pos["qty"]
        pnl_pct = (pnl / pos["sizeUsd"]) * 100 if pos["sizeUsd"] else 0
        bot_state["balance"] += pnl
        trade = {
            "symbol": pos["symbol"],
            "side": pos["side"],
            "sizeUsd": pos["sizeUsd"],
            "entryPrice": pos["entryPrice"],
            "exitPrice": price,
            "pnl": pnl,
            "pnlPct": pnl_pct,
            "reason": reason,
            "openedAt": pos["openedAt"],
            "closedAt": now_iso()
        }
        bot_state["closedTrades"].insert(0, trade)
        bot_state["closedTrades"] = bot_state["closedTrades"][:500]
    color = "green" if pnl >= 0 else "red"
    bot_log(f"Закрыта позиция {pos['side']} {pos['symbol']}/USDT. P&L: {pnl:+.2f}$ ({pnl_pct:+.2f}%) [{reason}]", color)
    bot_save()
    return trade


def bot_check_limits():
    """Проверяет общий TP/SL по виртуальному балансу."""
    with bot_state_lock:
        settings = bot_state["settings"]
        start = float(settings["startBalance"])
        balance = float(bot_state["balance"])
        profit_target = float(settings["profitTarget"])
        max_loss = float(settings["maxLoss"])
        diff = balance - start
        if diff >= profit_target:
            return ("profit", f"Достигнут лимит прибыли (+${diff:.2f})")
        if diff <= -max_loss:
            return ("loss", f"Достигнут лимит убытка (-${abs(diff):.2f})")
    return (None, None)


def bot_close_all(reason="manual"):
    """Закрывает все открытые позиции по текущим ценам."""
    try:
        prices, _, _ = refresh()
    except Exception:
        prices = {}
    with bot_state_lock:
        positions = list(bot_state["positions"])
    for pos in positions:
        price = prices.get(pos["symbol"], pos["entryPrice"])
        bot_close_position(pos, price, reason)


def bot_tick():
    """Один цикл работы бота."""
    with bot_state_lock:
        if not bot_state["running"]:
            return
    try:
        prices, _, _ = refresh(force=True)
    except Exception as exc:
        bot_log(f"Ошибка получения цен: {exc}", "red")
        return

    # 1) проверяем TP/SL по открытым позициям
    with bot_state_lock:
        positions = list(bot_state["positions"])
    for pos in positions:
        price = prices.get(pos["symbol"])
        if price is None:
            continue
        if pos["side"] == "Long":
            if price >= pos["tp"]:
                bot_close_position(pos, price, "TP")
                continue
            if price <= pos["sl"]:
                bot_close_position(pos, price, "SL")
                continue
        else:
            if price <= pos["tp"]:
                bot_close_position(pos, price, "TP")
                continue
            if price >= pos["sl"]:
                bot_close_position(pos, price, "SL")
                continue

    # 2) проверяем общие лимиты
    kind, msg = bot_check_limits()
    if kind:
        bot_log(msg + ". Останавливаем торговлю.", "gold" if kind == "profit" else "red")
        bot_close_all("limits")
        with bot_state_lock:
            bot_state["running"] = False
            bot_state["reason"] = msg
            bot_state["stoppedAt"] = now_iso()
        bot_save()
        return

    # 3) ищем новые сигналы (не более 3 открытых позиций одновременно)
    with bot_state_lock:
        if len(bot_state["positions"]) >= 3:
            return
    max_new = 3 - len(bot_state["positions"])
    opened = 0
    for symbol, price in prices.items():
        if opened >= max_new:
            break
        # торгуем только топовые монеты
        if symbol not in {"BTC","ETH","SOL","XRP","ADA","DOGE","BNB","TON","AVAX","LINK"}:
            continue
        a = analyze(symbol, price)
        if a["signal"] == "buy_candidate":
            p = bot_open_position(symbol, price, "Long")
            if p: opened += 1
        elif a["signal"] == "sell_candidate":
            p = bot_open_position(symbol, price, "Short")
            if p: opened += 1


def bot_worker():
    """Фоновый воркер, тикает каждые BOT_TICK_SECONDS."""
    while True:
        try:
            bot_tick()
        except Exception:
            log.exception("Bot tick failed")
        time.sleep(BOT_TICK_SECONDS)


# ============================================================
# SECURITY HEADERS
# ============================================================

@app.after_request
def headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    return response


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/health")
def health():
    return jsonify({
        "status": "ok", "service": "CryptoPulse", "timestamp": now_iso(),
        "databaseConfigured": bool(DATABASE_URL),
        "databaseAvailable": db_status["available"]
    })


@app.route("/api/config")
def config():
    return jsonify({
        "success": True, "tradingMode": "simulation",
        "liveTradingEnabled": False,
        "telegramConfigured": bool(TG_TOKEN),
        "databaseConfigured": bool(DATABASE_URL),
        "databaseAvailable": db_status["available"],
        "bybitApiConfigured": bool(API_KEY and API_SECRET),
        "coins": []
    })


@app.route("/api/prices")
@app.route("/api/market-prices")
def prices_api():
    try:
        return jsonify(market_payload())
    except Exception:
        log.exception("Market endpoint failed")
        return jsonify({"success": False, "error": "Рыночные данные временно недоступны", "coins": []}), 503


@app.route("/api/bybit-status")
def bybit_api_status():
    return jsonify({"success": True, **bybit_status})


@app.route("/api/usd-rub")
def rub_api():
    try:
        return jsonify({"success": True, "rate": usd_rub(), "base": "USD", "currency": "RUB"})
    except Exception:
        return jsonify({"success": False, "error": "Курс временно недоступен"}), 503


@app.route("/api/account/balance")
@app.route("/api/bybit/balance")
def balance_api():
    try:
        return jsonify(wallet_balance())
    except Exception as exc:
        log.warning("Balance unavailable: %s", exc)
        return jsonify({
            "success": False, "available": False,
            "error": "Баланс временно недоступен. Подробности доступны в логах сервера."
        }), 503


@app.route("/api/trading/status")
def trading_status():
    return jsonify({
        "success": True, "mode": "simulation",
        "liveTradingEnabled": False,
        "message": "Реальная торговля отключена. Работает виртуальная симуляция."
    })


@app.route("/api/trading/signals")
def signals_api():
    try:
        payload = market_payload()
        output = []
        for coin in payload["coins"]:
            item = analyze(coin["symbol"], coin["priceUsd"])
            item["change1h"] = coin["change1h"]
            item["change24h"] = coin["change24h"]
            output.append(item)
        return jsonify({
            "success": True, "mode": "simulation",
            "liveTradingEnabled": False,
            "strategy": "EMA9/EMA21 + RSI14 (experimental)",
            "signals": output
        })
    except Exception:
        log.exception("Signals failed")
        return jsonify({"success": False, "error": "Сигналы временно недоступны", "signals": []}), 503


@app.route("/api/klines")
def klines_api():
    symbol = (request.args.get("symbol") or "BTCUSDT").upper().strip()
    interval = (request.args.get("interval") or "5m").strip()
    limit = request.args.get("limit", "200")
    allowed = {"1","3","5","15","30","60","120","240","360","720","D","W","M",
               "1m","3m","5m","15m","30m","1h","2h","4h","6h","12h","1d","1w"}
    if interval not in allowed:
        return jsonify({"success": False, "error": "Недопустимый interval"}), 400
    bybit_interval = {"1m":"1","3m":"3","5m":"5","15m":"15","30m":"30",
                      "1h":"60","2h":"120","4h":"240","6h":"360","12h":"720",
                      "1d":"D","1w":"W"}.get(interval, interval)
    try:
        limit = max(1, min(int(limit), 1000))
    except ValueError:
        limit = 200
    try:
        data = get_json(BASE + "/v5/market/kline", {
            "category": "spot", "symbol": symbol,
            "interval": bybit_interval, "limit": limit
        })
        if data.get("retCode") != 0:
            raise RuntimeError(data.get("retMsg", "Bybit kline error"))
        raw = (data.get("result") or {}).get("list", [])
        candles = []
        for item in reversed(raw):
            try:
                candles.append({
                    "time": int(item[0]),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]) if len(item) > 5 else 0
                })
            except (ValueError, IndexError):
                continue
        return jsonify({"success": True, "symbol": symbol, "interval": interval, "candles": candles})
    except Exception as exc:
        log.exception("Klines failed")
        return jsonify({"success": False, "error": "Свечи временно недоступны", "details": str(exc)}), 503


# ============================================================
# BOT API (симуляция)
# ============================================================

@app.route("/api/bot/state")
def bot_state_api():
    with bot_state_lock:
        # считаем live P&L для позиций
        try:
            prices, _, _ = refresh()
        except Exception:
            prices = {}
        positions = []
        for p in bot_state["positions"]:
            current = prices.get(p["symbol"], p["entryPrice"])
            if p["side"] == "Long":
                pnl = (current - p["entryPrice"]) * p["qty"]
            else:
                pnl = (p["entryPrice"] - current) * p["qty"]
            positions.append({
                **p,
                "currentPrice": current,
                "pnl": pnl,
                "pnlPct": (pnl / p["sizeUsd"]) * 100 if p["sizeUsd"] else 0
            })
        # общий P&L
        total_pnl = sum(p["pnl"] for p in positions)
        settings = bot_state["settings"]
        start = float(settings["startBalance"])
        balance = bot_state["balance"] + total_pnl
        return jsonify({
            "success": True,
            "running": bot_state["running"],
            "emergency": bot_state["emergency"],
            "reason": bot_state["reason"],
            "settings": bot_state["settings"],
            "balance": balance,
            "startBalance": start,
            "totalPnl": total_pnl,
            "totalPnlPct": (total_pnl / start) * 100 if start else 0,
            "positions": positions,
            "closedTrades": bot_state["closedTrades"][:100],
            "logs": bot_state["logs"][:100],
            "startedAt": bot_state["startedAt"],
            "stoppedAt": bot_state["stoppedAt"]
        })


@app.route("/api/bot/start", methods=["POST"])
def bot_start_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        # обновляем настройки из запроса
        if "settings" in data and isinstance(data["settings"], dict):
            s = data["settings"]
            for key in ("profitTarget", "maxLoss", "tradeSize"):
                if key in s:
                    try:
                        bot_state["settings"][key] = float(s[key])
                    except (ValueError, TypeError):
                        pass
        bot_state["running"] = True
        bot_state["emergency"] = False
        bot_state["reason"] = "Бот запущен (симуляция)"
        bot_state["startedAt"] = now_iso()
        bot_state["stoppedAt"] = None
    bot_log("Бот запущен. Режим: Симуляция", "green")
    return jsonify({"success": True})


@app.route("/api/bot/stop", methods=["POST"])
def bot_stop_api():
    with bot_state_lock:
        bot_state["running"] = False
        bot_state["reason"] = "Остановлен вручную"
        bot_state["stoppedAt"] = now_iso()
    bot_log("Бот остановлен вручную", "red")
    return jsonify({"success": True})


@app.route("/api/bot/emergency", methods=["POST"])
def bot_emergency_api():
    bot_close_all("emergency")
    with bot_state_lock:
        bot_state["running"] = False
        bot_state["emergency"] = True
        bot_state["reason"] = "Аварийная остановка"
        bot_state["stoppedAt"] = now_iso()
    bot_log("АВАРИЙНАЯ ОСТАНОВКА. Все позиции закрыты.", "red")
    return jsonify({"success": True})


@app.route("/api/bot/settings", methods=["POST"])
def bot_settings_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        for key in ("profitTarget", "maxLoss", "tradeSize"):
            if key in data:
                try:
                    bot_state["settings"][key] = float(data[key])
                except (ValueError, TypeError):
                    pass
    bot_log("Настройки сохранены", "blue")
    return jsonify({"success": True, "settings": bot_state["settings"]})


@app.route("/api/bot/reset", methods=["POST"])
def bot_reset_api():
    """Сброс бота (для тестов)."""
    with bot_state_lock:
        bot_state["running"] = False
        bot_state["emergency"] = False
        bot_state["reason"] = "Сброшено"
        bot_state["balance"] = float(bot_state["settings"].get("startBalance", BOT_START_BALANCE))
        bot_state["positions"] = []
        bot_state["closedTrades"] = []
        bot_state["logs"] = []
        bot_state["startedAt"] = None
        bot_state["stoppedAt"] = None
    bot_log("Бот сброшен. Баланс восстановлен.", "gold")
    return jsonify({"success": True})


# ============================================================
# TELEGRAM WEBHOOK
# ============================================================

def tg(method, payload):
    if not TG_TOKEN:
        raise RuntimeError("Telegram token missing")
    response = session.post(f"https://api.telegram.org/bot{TG_TOKEN}/{method}", json=payload, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def tg_send(chat, text, markup=None):
    payload = {"chat_id": chat, "text": text}
    if markup:
        payload["reply_markup"] = markup
    return tg("sendMessage", payload)


def keyboard():
    return {"inline_keyboard": [[
        {"text": "🌐 CryptoPulse", "web_app": {"url": WEBAPP_URL}},
        {"text": "📊 Цены", "callback_data": "prices"}
    ]]}


def telegram_prices():
    payload = market_payload()
    lines = ["CryptoPulse — Bybit Spot USDT", ""]
    for coin in payload["coins"][:40]:
        change = "—" if coin["change24h"] is None else f'{coin["change24h"]:+.2f}%'
        lines.append(f'{coin["symbol"]}: ${coin["priceUsd"]:,.8f} | 24ч {change}')
    lines.append("\nРеальная торговля отключена.")
    return "\n".join(lines)


@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    if TG_SECRET:
        supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not hmac.compare_digest(supplied, TG_SECRET):
            return jsonify({"ok": False}), 401
    if not TG_TOKEN:
        return jsonify({"ok": False, "error": "Telegram token missing"}), 503
    update = request.get_json(silent=True) or {}
    callback = update.get("callback_query")
    if callback:
        message = callback.get("message") or {}
        chat = (message.get("chat") or {}).get("id")
        try:
            tg("answerCallbackQuery", {"callback_query_id": callback.get("id")})
        except Exception:
            pass
        if chat:
            try:
                if callback.get("data") == "prices":
                    tg_send(chat, telegram_prices(), keyboard())
            except Exception:
                tg_send(chat, "Не удалось получить данные, попробуй позже.", keyboard())
        return jsonify({"ok": True})
    message = update.get("message") or {}
    chat = (message.get("chat") or {}).get("id")
    text = (message.get("text") or "").strip()
    if chat and text.startswith(("/start", "/help")):
        tg_send(chat, "CryptoPulse\nЦены Bybit Spot USDT, сигналы и калькулятор.\nРеальная торговля отключена.", keyboard())
    elif chat and text.startswith("/prices"):
        try:
            tg_send(chat, telegram_prices(), keyboard())
        except Exception:
            tg_send(chat, "Цены временно недоступны.", keyboard())
    elif chat and text.startswith("/status"):
        tg_send(chat, "CryptoPulse работает. Режим simulation. Реальные ордера отключены.", keyboard())
    return jsonify({"ok": True})


def configure_telegram():
    if not TG_TOKEN:
        return
    try:
        tg("setChatMenuButton", {
            "menu_button": {"type": "web_app", "text": "Открыть CryptoPulse", "web_app": {"url": WEBAPP_URL}}
        })
        payload = {"url": WEBHOOK_URL}
        if TG_SECRET:
            payload["secret_token"] = TG_SECRET
        tg("setWebhook", payload)
    except Exception:
        log.exception("Telegram setup failed")


# ============================================================
# ФОНОВЫЙ ВОРКЕР (обновление цен)
# ============================================================

def worker():
    while True:
        try:
            refresh(force=True)
        except Exception:
            log.exception("Background refresh failed")
        time.sleep(REFRESH_SECONDS)


# ============================================================
# STARTUP
# ============================================================

init_db()
bot_load()

if TG_AUTO:
    configure_telegram()

if BG_WORKER:
    threading.Thread(target=worker, name="market-history-worker", daemon=True).start()

# Воркер симуляции бота
threading.Thread(target=bot_worker, name="bot-simulator", daemon=True).start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
