import os, time, hmac, math, logging, threading
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
PUBLIC_APP_URL = os.getenv(
    "PUBLIC_APP_URL",
    "https://cryptopulse-lpc9.onrender.com"
).rstrip("/")
WEBAPP_URL = os.getenv("TELEGRAM_WEBAPP_URL", PUBLIC_APP_URL + "/")
WEBHOOK_URL = os.getenv(
    "TELEGRAM_WEBHOOK_URL",
    PUBLIC_APP_URL + "/telegram-webhook"
)

TG_TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or ""
).strip()

TG_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
TG_AUTO = os.getenv("TELEGRAM_AUTO_CONFIGURE", "true").lower() in (
    "1", "true", "yes", "on"
)
BG_WORKER = os.getenv("ENABLE_BACKGROUND_WORKER", "true").lower() in (
    "1", "true", "yes", "on"
)

API_KEY = os.getenv("BYBIT_API_KEY", "").strip()
API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgres://", "postgresql://", 1
    )

TRADING_MODE = "simulation"
LIVE_TRADING_ENABLED = False

TIMEOUT = 12
CACHE_SECONDS = 30
HISTORY_HOURS = 72
REFRESH_SECONDS = 60

session = requests.Session()
session.headers.update({
    "User-Agent": "CryptoPulse/2.0",
    "Accept": "application/json"
})

lock = threading.RLock()
refresh_lock = threading.Lock()
db_lock = threading.Lock()

cache = {
    "updated": 0,
    "prices": {},
    "changes": {},
    "source": "bybit",
    "error": None
}

history = {}
usd_rub_cache = {"updated": 0, "rate": None}

bybit_status = {
    "available": False,
    "lastError": None,
    "lastCheck": None
}

db_status = {
    "configured": bool(DATABASE_URL),
    "available": False,
    "lastError": None
}

wallet_cache = {"updated": 0, "data": None}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_json(url, params=None, headers=None):
    response = session.get(
        url,
        params=params,
        headers=headers,
        timeout=TIMEOUT
    )
    response.raise_for_status()
    return response.json()


# ============================================================
# DATABASE AND PRICE HISTORY
# ============================================================

def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not configured")

    if psycopg is None:
        raise RuntimeError("psycopg not installed")

    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=8,
        row_factory=dict_row
    )


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
                    CREATE INDEX IF NOT EXISTS
                    idx_price_history_symbol_time
                    ON price_history(symbol, observed_at DESC)
                """)

                cur.execute("""
                    SELECT source, symbol, price,
                           EXTRACT(EPOCH FROM observed_at) AS ts
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
        db_status.update(
            available=False,
            lastError=str(exc)
        )
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

            points.append({
                "timestamp": timestamp,
                "price": value
            })

            history[(source, symbol)] = [
                point for point in points
                if point["timestamp"] >= cutoff
            ]

    if not DATABASE_URL or not valid:
        return

    try:
        with db_lock:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO price_history
                            (source, symbol, price, observed_at)
                        VALUES
                            (%s, %s, %s, to_timestamp(%s))
                        ON CONFLICT
                            (source, symbol, observed_at)
                        DO NOTHING
                    """, [
                        (source, symbol, price, timestamp)
                        for symbol, price in valid.items()
                    ])

                    cur.execute("""
                        DELETE FROM price_history
                        WHERE observed_at <
                            NOW() - (%s * INTERVAL '1 hour')
                    """, (HISTORY_HOURS,))

                    conn.commit()

        db_status.update(available=True, lastError=None)

    except Exception as exc:
        db_status.update(
            available=False,
            lastError=str(exc)
        )
        log.warning("History save failed: %s", exc)


def get_points(symbol):
    with lock:
        points = list(history.get(("bybit", symbol), []))

    cutoff = time.time() - HISTORY_HOURS * 3600

    return [
        point for point in points
        if point["timestamp"] >= cutoff
    ]


# ============================================================
# INDICATORS AND SIMULATION SIGNALS
# ============================================================

def ema(values, period):
    values = [
        float(value)
        for value in values
        if value is not None and float(value) > 0
    ]

    if len(values) < period:
        return None

    result = sum(values[:period]) / period
    multiplier = 2 / (period + 1)

    for value in values[period:]:
        result = (value - result) * multiplier + result

    return result


def rsi(values, period=14):
    values = [
        float(value)
        for value in values
        if value is not None and float(value) > 0
    ]

    if len(values) < period + 1:
        return None

    changes = [
        values[index] - values[index - 1]
        for index in range(1, len(values))
    ][-period:]

    gain = sum(max(value, 0) for value in changes) / period
    loss = sum(max(-value, 0) for value in changes) / period

    if loss == 0:
        return 100.0 if gain else 50.0

    relative_strength = gain / loss
    return 100 - (100 / (1 + relative_strength))


def analyze(symbol, price):
    points = sorted(
        get_points(symbol),
        key=lambda point: point["timestamp"]
    )

    values = [point["price"] for point in points]

    if not values or values[-1] != price:
        values.append(price)

    ema9 = ema(values, 9)
    ema21 = ema(values, 21)
    rsi14 = rsi(values)

    signal = "insufficient_history"
    explanation = "Нужно больше исторических наблюдений."

    if (
        len(values) >= 30
        and ema9 is not None
        and ema21 is not None
        and rsi14 is not None
    ):
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
        "symbol": symbol,
        "price": price,
        "ema9": ema9,
        "ema21": ema21,
        "rsi14": rsi14,
        "sampleCount": len(values),
        "signal": signal,
        "explanation": explanation,
        "mode": "simulation"
    }


# ============================================================
# BYBIT PUBLIC MARKET DATA
# ============================================================

def fetch_instruments():
    found = {}
    cursor = None

    for _ in range(20):
        params = {
            "category": "spot",
            "limit": 1000
        }

        if cursor:
            params["cursor"] = cursor

        data = get_json(
            BASE + "/v5/market/instruments-info",
            params
        )

        if data.get("retCode") != 0:
            raise RuntimeError(
                data.get("retMsg", "Bybit instruments error")
            )

        result = data.get("result") or {}

        for item in result.get("list", []):
            pair = str(item.get("symbol", "")).upper()

            if (
                pair.endswith("USDT")
                and item.get("status") == "Trading"
            ):
                symbol = pair[:-4]

                if symbol and symbol not in {
                    "USDC", "USDE", "FDUSD", "TUSD",
                    "DAI", "USDP", "PYUSD"
                }:
                    found[symbol] = {
                        "symbol": symbol,
                        "name": symbol,
                        "bybitSymbol": pair
                    }

        cursor = result.get("nextPageCursor")

        if not cursor:
            break

    if not found:
        raise RuntimeError(
            "Bybit returned no active spot USDT pairs"
        )

    return found


def fetch_prices():
    data = get_json(
        BASE + "/v5/market/tickers",
        {"category": "spot"}
    )

    if data.get("retCode") != 0:
        raise RuntimeError(
            data.get("retMsg", "Bybit ticker error")
        )

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

    bybit_status.update(
        available=True,
        lastError=None,
        lastCheck=now_iso()
    )

    return prices, changes


def refresh(force=False):
    if (
        cache["prices"]
        and time.time() - cache["updated"] < CACHE_SECONDS
        and not force
    ):
        return (
            dict(cache["prices"]),
            dict(cache["changes"]),
            cache["error"]
        )

    with refresh_lock:
        if (
            cache["prices"]
            and time.time() - cache["updated"] < CACHE_SECONDS
            and not force
        ):
            return (
                dict(cache["prices"]),
                dict(cache["changes"]),
                cache["error"]
            )

        try:
            prices, changes = fetch_prices()
            remember(prices)

            cache.update(
                updated=time.time(),
                prices=prices,
                changes=changes,
                source="bybit",
                error=None
            )

            return dict(prices), dict(changes), None

        except Exception as exc:
            bybit_status.update(
                available=False,
                lastError=str(exc),
                lastCheck=now_iso()
            )

            cache["error"] = str(exc)

            if cache["prices"]:
                return (
                    dict(cache["prices"]),
                    dict(cache["changes"]),
                    str(exc)
                )

            raise


# ============================================================
# USD/RUB
# ============================================================

def usd_rub():
    if (
        usd_rub_cache["rate"]
        and time.time() - usd_rub_cache["updated"] < 900
    ):
        return usd_rub_cache["rate"]

    data = get_json("https://open.er-api.com/v6/latest/USD")
    rate = float(data["rates"]["RUB"])

    if not math.isfinite(rate) or rate <= 0:
        raise RuntimeError("Invalid USD/RUB")

    usd_rub_cache.update(
        updated=time.time(),
        rate=rate
    )

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
        points = [
            point for point in get_points(symbol)
            if 2700 <= time.time() - point["timestamp"] <= 4500
        ]

        change1h = None

        if points:
            reference = min(
                points,
                key=lambda point: abs(
                    time.time() - point["timestamp"] - 3600
                )
            )["price"]

            change1h = (price / reference - 1) * 100

        coins.append({
            "symbol": symbol,
            "name": symbol,
            "priceUsd": price,
            "priceRub": price * rate if rate else None,
            "change1h": change1h,
            "change24h": changes.get(symbol),
            "source": "bybit"
        })

    coins.sort(key=lambda coin: coin["symbol"])

    return {
        "success": True,
        "updatedAt": now_iso(),
        "source": "bybit",
        "coins": coins,
        "usdRub": rate,
        "usdRubError": rate_error,
        "hourlyHistory": (
            "postgresql-persistent"
            if DATABASE_URL else "in-memory-only"
        ),
        "databaseAvailable": db_status["available"],
        "warning": warning
    }


# ============================================================
# READ-ONLY BYBIT ACCOUNT BALANCE
# ============================================================

def signed_get(path, params):
    if not API_KEY or not API_SECRET:
        raise RuntimeError("API key/secret not configured")

    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"

    query = urlencode(
        sorted((key, str(value)) for key, value in params.items())
    )

    raw = timestamp + API_KEY + recv_window + query

    signature = hmac.new(
        API_SECRET.encode(),
        raw.encode(),
        "sha256"
    ).hexdigest()

    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2"
    }

    data = get_json(
        BASE + path,
        params=params,
        headers=headers
    )

    if data.get("retCode") != 0:
        code = data.get("retCode")
        message = data.get(
            "retMsg",
            "Bybit private API failed"
        )

        log.warning(
            "Bybit private API error: retCode=%s, retMsg=%s",
            code,
            message
        )

        raise RuntimeError(
            f"Bybit error {code}: {message}"
        )

    return data.get("result") or {}


def wallet_balance(force=False):
    if (
        wallet_cache["data"]
        and time.time() - wallet_cache["updated"] < 60
        and not force
    ):
        return wallet_cache["data"]

    result = signed_get(
        "/v5/account/wallet-balance",
        {"accountType": "UNIFIED"}
    )

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
                "availableToWithdraw": coin.get(
                    "availableToWithdraw"
                )
            })

            total += usd_value

    assets.sort(
        key=lambda asset: asset["usdValue"],
        reverse=True
    )

    data = {
        "success": True,
        "available": True,
        "accountType": "UNIFIED",
        "totalUsd": total,
        "assets": assets,
        "updatedAt": now_iso()
    }

    wallet_cache.update(
        updated=time.time(),
        data=data
    )

    return data


# ============================================================
# SECURITY HEADERS
# ============================================================

@app.after_request
def headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault(
        "Referrer-Policy",
        "strict-origin-when-cross-origin"
    )
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=()"
    )
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
        "status": "ok",
        "service": "CryptoPulse",
        "timestamp": now_iso(),
        "databaseConfigured": bool(DATABASE_URL),
        "databaseAvailable": db_status["available"]
    })


@app.route("/api/config")
def config():
    return jsonify({
        "success": True,
        "tradingMode": "simulation",
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
        return jsonify({
            "success": False,
            "error": "Рыночные данные временно недоступны",
            "coins": []
        }), 503


@app.route("/api/bybit-status")
def bybit_api_status():
    return jsonify({
        "success": True,
        **bybit_status
    })


@app.route("/api/usd-rub")
def rub_api():
    try:
        return jsonify({
            "success": True,
            "rate": usd_rub(),
            "base": "USD",
            "currency": "RUB"
        })
    except Exception:
        return jsonify({
            "success": False,
            "error": "Курс временно недоступен"
        }), 503


@app.route("/api/account/balance")
@app.route("/api/bybit/balance")
def balance_api():
    try:
        return jsonify(wallet_balance())

    except Exception as exc:
        log.warning("Balance unavailable: %s", exc)

        return jsonify({
            "success": False,
            "available": False,
            "error": (
                "Баланс временно недоступен. "
                "Подробности доступны в логах сервера."
            )
        }), 503


@app.route("/api/trading/status")
def trading_status():
    return jsonify({
        "success": True,
        "mode": "simulation",
        "liveTradingEnabled": False,
        "message": "Реальная торговля отключена."
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
            "success": True,
            "mode": "simulation",
            "liveTradingEnabled": False,
            "strategy": "EMA9/EMA21 + RSI14 (experimental)",
            "signals": output
        })

    except Exception:
        log.exception("Signals failed")
        return jsonify({
            "success": False,
            "error": "Сигналы временно недоступны",
            "signals": []
        }), 503


# ============================================================
# TELEGRAM WEBHOOK
# ============================================================

def tg(method, payload):
    if not TG_TOKEN:
        raise RuntimeError("Telegram token missing")

    response = session.post(
        f"https://api.telegram.org/bot{TG_TOKEN}/{method}",
        json=payload,
        timeout=TIMEOUT
    )

    response.raise_for_status()
    return response.json()


def tg_send(chat, text, markup=None):
    payload = {
        "chat_id": chat,
        "text": text
    }

    if markup:
        payload["reply_markup"] = markup

    return tg("sendMessage", payload)


def keyboard():
    return {
        "inline_keyboard": [[
            {
                "text": "🌐 CryptoPulse",
                "web_app": {"url": WEBAPP_URL}
            },
            {
                "text": "📊 Цены",
                "callback_data": "prices"
            }
        ]]
    }


def telegram_prices():
    payload = market_payload()
    lines = [
        "CryptoPulse — Bybit Spot USDT",
        ""
    ]

    for coin in payload["coins"][:40]:
        change = (
            "—"
            if coin["change24h"] is None
            else f'{coin["change24h"]:+.2f}%'
        )

        lines.append(
            f'{coin["symbol"]}: '
            f'${coin["priceUsd"]:,.8f} | 24ч {change}'
        )

    lines.append("\nРеальная торговля отключена.")
    return "\n".join(lines)


@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    if TG_SECRET:
        supplied = request.headers.get(
            "X-Telegram-Bot-Api-Secret-Token", ""
        )

        if not hmac.compare_digest(supplied, TG_SECRET):
            return jsonify({"ok": False}), 401

    if not TG_TOKEN:
        return jsonify({
            "ok": False,
            "error": "Telegram token missing"
        }), 503

    update = request.get_json(silent=True) or {}
    callback = update.get("callback_query")

    if callback:
        message = callback.get("message") or {}
        chat = (message.get("chat") or {}).get("id")

        try:
            tg("answerCallbackQuery", {
                "callback_query_id": callback.get("id")
            })
        except Exception:
            pass

        if chat:
            try:
                if callback.get("data") == "prices":
                    tg_send(chat, telegram_prices(), keyboard())
            except Exception:
                tg_send(
                    chat,
                    "Не удалось получить данные, попробуй позже.",
                    keyboard()
                )

        return jsonify({"ok": True})

    message = update.get("message") or {}
    chat = (message.get("chat") or {}).get("id")
    text = (message.get("text") or "").strip()

    if chat and text.startswith(("/start", "/help")):
        tg_send(
            chat,
            "CryptoPulse\n"
            "Цены Bybit Spot USDT, сигналы и калькулятор.\n"
            "Реальная торговля отключена.",
            keyboard()
        )

    elif chat and text.startswith("/prices"):
        try:
            tg_send(chat, telegram_prices(), keyboard())
        except Exception:
            tg_send(chat, "Цены временно недоступны.", keyboard())

    elif chat and text.startswith("/status"):
        tg_send(
            chat,
            "CryptoPulse работает. Режим simulation. "
            "Реальные ордера отключены.",
            keyboard()
        )

    return jsonify({"ok": True})


def configure_telegram():
    if not TG_TOKEN:
        return

    try:
        tg("setChatMenuButton", {
            "menu_button": {
                "type": "web_app",
                "text": "Открыть CryptoPulse",
                "web_app": {"url": WEBAPP_URL}
            }
        })

        payload = {"url": WEBHOOK_URL}

        if TG_SECRET:
            payload["secret_token"] = TG_SECRET

        tg("setWebhook", payload)

    except Exception:
        log.exception("Telegram setup failed")


# ============================================================
# BACKGROUND PRICE HISTORY WORKER
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

if TG_AUTO:
    configure_telegram()

if BG_WORKER:
    threading.Thread(
        target=worker,
        name="market-history-worker",
        daemon=True
    ).start()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000"))
    )
