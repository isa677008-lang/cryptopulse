import os
import time
import html
import logging
import threading
import re
import math
import hmac
from datetime import datetime, timezone

import requests

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # PostgreSQL is optional for local/demo operation.
    psycopg = None
    dict_row = None

from flask import Flask, jsonify, send_from_directory, request


# ============================================================
# CONFIGURATION
# ============================================================

app = Flask(__name__, static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cryptopulse")

TELEGRAM_TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or ""
).strip()

PUBLIC_APP_URL = os.getenv(
    "PUBLIC_APP_URL",
    "https://cryptopulse-lpc9.onrender.com",
).strip().rstrip("/")

TELEGRAM_WEBAPP_URL = (
    os.getenv("TELEGRAM_WEBAPP_URL") or f"{PUBLIC_APP_URL}/"
).strip()

TELEGRAM_WEBHOOK_URL = (
    os.getenv("TELEGRAM_WEBHOOK_URL")
    or f"{PUBLIC_APP_URL}/telegram-webhook"
).strip()

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

TELEGRAM_AUTO_CONFIGURE = (
    os.getenv("TELEGRAM_AUTO_CONFIGURE", "true").lower()
    in ("1", "true", "yes", "on")
)

ENABLE_BACKGROUND_WORKER = (
    os.getenv("ENABLE_BACKGROUND_WORKER", "true").lower()
    in ("1", "true", "yes", "on")
)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgres://", "postgresql://", 1
    )

TRADING_MODE = "simulation"
LIVE_TRADING_ENABLED = False

REQUEST_TIMEOUT = 12
PRICE_CACHE_SECONDS = 30
HISTORY_RETENTION_HOURS = 72
HISTORY_REFRESH_SECONDS = 60

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
BYBIT_BASE_URL = "https://api.bybit.com"

COINS = {
    "BTC": {"name": "Bitcoin", "coingecko_id": "bitcoin", "bybit_symbol": "BTCUSDT"},
    "ETH": {"name": "Ethereum", "coingecko_id": "ethereum", "bybit_symbol": "ETHUSDT"},
    "BNB": {"name": "BNB", "coingecko_id": "binancecoin", "bybit_symbol": "BNBUSDT"},
    "XRP": {"name": "XRP", "coingecko_id": "ripple", "bybit_symbol": "XRPUSDT"},
    "SOL": {"name": "Solana", "coingecko_id": "solana", "bybit_symbol": "SOLUSDT"},
    "DOGE": {"name": "Dogecoin", "coingecko_id": "dogecoin", "bybit_symbol": "DOGEUSDT"},
    "ADA": {"name": "Cardano", "coingecko_id": "cardano", "bybit_symbol": "ADAUSDT"},
    "TRX": {"name": "TRON", "coingecko_id": "tron", "bybit_symbol": "TRXUSDT"},
    "AVAX": {"name": "Avalanche", "coingecko_id": "avalanche-2", "bybit_symbol": "AVAXUSDT"},
    "LINK": {"name": "Chainlink", "coingecko_id": "chainlink", "bybit_symbol": "LINKUSDT"},
    "DOT": {"name": "Polkadot", "coingecko_id": "polkadot", "bybit_symbol": "DOTUSDT"},
    "LTC": {"name": "Litecoin", "coingecko_id": "litecoin", "bybit_symbol": "LTCUSDT"},
    "BCH": {"name": "Bitcoin Cash", "coingecko_id": "bitcoin-cash", "bybit_symbol": "BCHUSDT"},
    "SHIB": {"name": "Shiba Inu", "coingecko_id": "shiba-inu", "bybit_symbol": "SHIBUSDT"},
    "TON": {"name": "Toncoin", "coingecko_id": "the-open-network", "bybit_symbol": "TONUSDT"},
    "USDT": {"name": "Tether", "coingecko_id": "tether", "bybit_symbol": None},
}


# ============================================================
# SHARED STATE
# ============================================================

state_lock = threading.RLock()
database_lock = threading.RLock()
refresh_lock = threading.Lock()

price_cache = {
    "updated_at": 0.0,
    "prices": {},
    "changes24h": {},
    "source": None,
    "error": None,
}

price_history = {}

usd_rub_cache = {"updated_at": 0.0, "rate": None}

bybit_status = {
    "available": False,
    "last_error": None,
    "last_check": None,
}

database_status = {
    "configured": bool(DATABASE_URL),
    "available": False,
    "last_error": None,
}

telegram_configured = bool(TELEGRAM_TOKEN)

# Chat ID -> bot message ID used by the calculator.
# The calculator edits its own prompt instead of sending a new result message.
calculator_waiting = {}
calculator_lock = threading.RLock()


# ============================================================
# HTTP HELPERS
# ============================================================

def make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "CryptoPulse/1.1",
        "Accept": "application/json",
    })
    return session


http = make_session()


def get_json(url, params=None, headers=None):
    response = http.get(
        url,
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


# ============================================================
# DATABASE AND PRICE HISTORY
# ============================================================

def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; install requirements.txt")

    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=8,
        row_factory=dict_row,
    )


def init_database():
    if not DATABASE_URL:
        database_status["available"] = False
        logger.warning("DATABASE_URL missing; history is RAM-only")
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS price_history (
                        id BIGSERIAL PRIMARY KEY,
                        source TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        price DOUBLE PRECISION NOT NULL CHECK (price > 0),
                        observed_at TIMESTAMPTZ NOT NULL,
                        UNIQUE (source, symbol, observed_at)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_price_history_symbol_time
                    ON price_history (symbol, observed_at DESC)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_price_history_source_symbol_time
                    ON price_history (source, symbol, observed_at DESC)
                """)
                conn.commit()

        database_status["available"] = True
        database_status["last_error"] = None
        load_database_history()
        logger.info("PostgreSQL initialized")

    except Exception as exc:
        database_status["available"] = False
        database_status["last_error"] = str(exc)
        logger.exception("Database initialization failed")
def load_database_history():
    if not DATABASE_URL:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT source, symbol, price,
                           EXTRACT(EPOCH FROM observed_at) AS timestamp
                    FROM price_history
                    WHERE observed_at >= NOW() - (%s * INTERVAL '1 hour')
                    ORDER BY observed_at ASC
                """, (HISTORY_RETENTION_HOURS,))
                rows = cur.fetchall()

        with state_lock:
            for row in rows:
                key = (str(row["source"]), str(row["symbol"]))
                price_history.setdefault(key, []).append({
                    "timestamp": float(row["timestamp"]),
                    "price": float(row["price"]),
                })

            for key, points in list(price_history.items()):
                unique = {p["timestamp"]: p for p in points}
                price_history[key] = sorted(
                    unique.values(), key=lambda p: p["timestamp"]
                )

        database_status["available"] = True
        database_status["last_error"] = None
        logger.info("Loaded %s historical observations", len(rows))

    except Exception as exc:
        database_status["available"] = False
        database_status["last_error"] = str(exc)
        logger.exception("Loading database history failed")


def save_history_to_database(source, prices, observed_at):
    if not DATABASE_URL or not prices:
        return

    records = []
    for symbol, price in prices.items():
        try:
            price = float(price)
        except (TypeError, ValueError):
            continue

        if math.isfinite(price) and price > 0:
            records.append((source, symbol, price, observed_at))

    if not records:
        return

    try:
        with database_lock:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO price_history (source, symbol, price, observed_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (source, symbol, observed_at) DO NOTHING
                    """, records)

                    cur.execute("""
                        DELETE FROM price_history
                        WHERE observed_at < NOW() - (%s * INTERVAL '1 hour')
                    """, (HISTORY_RETENTION_HOURS,))
                    conn.commit()

        database_status["available"] = True
        database_status["last_error"] = None

    except Exception as exc:
        database_status["available"] = False
        database_status["last_error"] = str(exc)
        logger.exception("Saving price history failed")


def remember_prices(source, prices, observed_at=None):
    if observed_at is None:
        observed_at = datetime.now(timezone.utc)

    timestamp = observed_at.timestamp()
    cutoff = timestamp - HISTORY_RETENTION_HOURS * 3600

    with state_lock:
        for symbol, price in prices.items():
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue

            if not math.isfinite(price) or price <= 0:
                continue

            key = (source, symbol)
            points = price_history.setdefault(key, [])

            if points and points[-1]["timestamp"] == timestamp:
                points[-1]["price"] = price
            else:
                points.append({"timestamp": timestamp, "price": price})

            price_history[key] = [
                point for point in points if point["timestamp"] >= cutoff
            ]

    save_history_to_database(source, prices, observed_at)


def get_history_points(source, symbol):
    key = (source, symbol)

    with state_lock:
        points = list(price_history.get(key, []))

    cutoff = time.time() - HISTORY_RETENTION_HOURS * 3600
    points = [p for p in points if p["timestamp"] >= cutoff]

    if not DATABASE_URL:
        return points

    now = time.time()
    has_hourly = any(
        45 * 60 <= now - p["timestamp"] <= 75 * 60 for p in points
    )
    has_daily = any(
        22 * 3600 <= now - p["timestamp"] <= 26 * 3600 for p in points
    )

    if has_hourly and has_daily:
        return points

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT price, EXTRACT(EPOCH FROM observed_at) AS timestamp
                    FROM price_history
                    WHERE source = %s AND symbol = %s
                      AND observed_at >= NOW() - INTERVAL '72 hours'
                    ORDER BY observed_at ASC
                """, (source, symbol))
                rows = cur.fetchall()

        merged = {p["timestamp"]: p for p in points}
        for row in rows:
            p = {
                "timestamp": float(row["timestamp"]),
                "price": float(row["price"]),
            }
            merged[p["timestamp"]] = p

        return sorted(merged.values(), key=lambda p: p["timestamp"])

    except Exception as exc:
        logger.warning("Historical lookup failed for %s: %s", symbol, exc)
        return points


def calculate_hourly_change(source, symbol, current_price):
    if current_price is None or current_price <= 0:
        return None

    now = time.time()
    candidates = [
        p for p in get_history_points(source, symbol)
        if 45 * 60 <= now - p["timestamp"] <= 75 * 60 and p["price"] > 0
    ]

    if not candidates:
        return None

    reference = min(
        candidates,
        key=lambda p: abs((now - p["timestamp"]) - 3600),
    )

    return (current_price / reference["price"] - 1) * 100


def calculate_change_24h(provider_change, source, symbol, current_price):
    if provider_change is not None:
        return provider_change

    if current_price is None or current_price <= 0:
        return None

    now = time.time()
    candidates = [
        p for p in get_history_points(source, symbol)
        if 22 * 3600 <= now - p["timestamp"] <= 26 * 3600 and p["price"] > 0
    ]

    if not candidates:
        return None

    reference = min(
        candidates,
        key=lambda p: abs((now - p["timestamp"]) - 86400),
    )

    return (current_price / reference["price"] - 1) * 100


def calculate_ema(values, period):
    """Calculate an EMA from chronological price observations."""
    values = [
        float(value)
        for value in values
        if value is not None and float(value) > 0
    ]
    if len(values) < period:
        return None

    seed = sum(values[:period]) / period
    multiplier = 2 / (period + 1)
    ema = seed
    for value in values[period:]:
        ema = (value - ema) * multiplier + ema
    return ema


def calculate_rsi(values, period=14):
    """Calculate RSI using average gains/losses over the latest observations."""
    values = [float(value) for value in values if value is not None and float(value) > 0]
    if len(values) < period + 1:
        return None

    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    recent = changes[-period:]
    gains = [change if change > 0 else 0.0 for change in recent]
    losses = [-change if change < 0 else 0.0 for change in recent]
    average_gain = sum(gains) / period
    average_loss = sum(losses) / period

    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    relative_strength = average_gain / average_loss
    return 100 - (100 / (1 + relative_strength))


def analyze_coin(source, symbol, current_price):
    """
    Build a cautious indicator-based candidate signal.
    EMA/RSI periods are observations, not guaranteed clock-time candles.
    """
    points = get_history_points(source, symbol)
    points = sorted(
        (point for point in points if point.get("price", 0) > 0),
        key=lambda point: point["timestamp"],
    )

    prices = [float(point["price"]) for point in points]
    if current_price is not None and current_price > 0:
        if not prices or abs(prices[-1] - current_price) > 1e-12:
            prices.append(float(current_price))

    sample_count = len(prices)
    ema_fast = calculate_ema(prices, 9)
    ema_slow = calculate_ema(prices, 21)
    rsi = calculate_rsi(prices, 14)

    signal = "insufficient_history"
    explanation = "Нужно больше исторических наблюдений."
    if sample_count >= 30 and ema_fast is not None and ema_slow is not None and rsi is not None:
        if current_price > ema_fast and ema_fast > ema_slow and 50 <= rsi < 68:
            signal = "buy_candidate"
            explanation = "Цена выше EMA 9, EMA 9 выше EMA 21, RSI подтверждает умеренный импульс."
        elif current_price < ema_fast and ema_fast < ema_slow and 32 < rsi <= 50:
            signal = "sell_candidate"
            explanation = "Цена ниже EMA 9, EMA 9 ниже EMA 21, RSI подтверждает отрицательный импульс."
        else:
            signal = "neutral"
            explanation = "Условия стратегии не дают согласованного сигнала."

    return {
        "symbol": symbol,
        "signal": signal,
        "price": float(current_price) if current_price is not None else None,
        "ema9": ema_fast,
        "ema21": ema_slow,
        "rsi14": rsi,
        "sampleCount": sample_count,
        "explanation": explanation,
        "mode": "simulation",
    }


# ============================================================
# COINGECKO AND BYBIT
# ============================================================

def fetch_coingecko_prices():
    ids = ",".join(c["coingecko_id"] for c in COINS.values())
    headers = {}

    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    data = get_json(
        f"{COINGECKO_BASE_URL}/simple/price",
        params={
            "ids": ids,
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        },
        headers=headers or None,
    )

    prices, changes = {}, {}

    for symbol, coin in COINS.items():
        item = data.get(coin["coingecko_id"], {})
        value = item.get("usd")

        if value is None:
            continue

        value = float(value)
        if not math.isfinite(value) or value <= 0:
            continue

        prices[symbol] = value
        change = item.get("usd_24h_change")
        changes[symbol] = float(change) if change is not None else None

    if not prices:
        raise RuntimeError("CoinGecko returned no usable prices")

    return prices, changes


def fetch_bybit_prices():
    prices = {}

    for symbol, coin in COINS.items():
        bybit_symbol = coin.get("bybit_symbol")
        if not bybit_symbol:
            continue

        try:
            data = get_json(
                f"{BYBIT_BASE_URL}/v5/market/tickers",
                params={"category": "spot", "symbol": bybit_symbol},
            )

            if data.get("retCode") != 0:
                continue

            items = data.get("result", {}).get("list", [])
            if items:
                value = items[0].get("lastPrice")
                if value is not None:
                    parsed = float(value)
                    if math.isfinite(parsed) and parsed > 0:
                        prices[symbol] = parsed

        except Exception as exc:
            logger.warning("Bybit request failed for %s: %s", symbol, exc)

    bybit_status["available"] = bool(prices)
    bybit_status["last_check"] = datetime.now(timezone.utc).isoformat()
    bybit_status["last_error"] = None if prices else "No usable prices"

    if not prices:
        raise RuntimeError("Bybit returned no usable prices")

    return prices


def refresh_prices(force=False):
    now = time.time()

    with state_lock:
        if (
            price_cache["prices"]
            and now - price_cache["updated_at"] < PRICE_CACHE_SECONDS
            and not force
        ):
            return {
                "prices": dict(price_cache["prices"]),
                "changes24h": dict(price_cache["changes24h"]),
                "source": price_cache["source"],
                "error": price_cache["error"],
            }

    with refresh_lock:
        now = time.time()

        with state_lock:
            if (
                price_cache["prices"]
                and now - price_cache["updated_at"] < PRICE_CACHE_SECONDS
                and not force
            ):
                return {
                    "prices": dict(price_cache["prices"]),
                    "changes24h": dict(price_cache["changes24h"]),
                    "source": price_cache["source"],
                    "error": price_cache["error"],
                }
                    "changes24h": dict(price_cache["changes24h"]),
                    "source": price_cache["source"],
                    "error": price_cache["error"],
                }

        errors = []

        try:
            prices, changes = fetch_coingecko_prices()
            source = "coingecko"
        except Exception as exc:
            errors.append(f"CoinGecko: {exc}")
            logger.warning("CoinGecko failed: %s", exc)

            try:
                prices = fetch_bybit_prices()
                changes = {}
                source = "bybit"
            except Exception as bybit_exc:
                errors.append(f"Bybit: {bybit_exc}")
                logger.warning("Bybit failed: %s", bybit_exc)

                with state_lock:
                    cached = dict(price_cache["prices"])
                    cached_changes = dict(price_cache["changes24h"])
                    cached_source = price_cache["source"]
                    price_cache["error"] = "; ".join(errors)

                if cached:
                    return {
                        "prices": cached,
                        "changes24h": cached_changes,
                        "source": cached_source,
                        "error": "; ".join(errors),
                    }
                raise RuntimeError("; ".join(errors))

        remember_prices(source, prices, datetime.now(timezone.utc))

        with state_lock:
            price_cache.update({
                "updated_at": time.time(),
                "prices": dict(prices),
                "changes24h": dict(changes),
                "source": source,
                "error": None,
            })

        return {
            "prices": dict(prices),
            "changes24h": dict(changes),
            "source": source,
            "error": None,
        }


# ============================================================
# USD/RUB AND MARKET PAYLOAD
# ============================================================

def fetch_usd_rub():
    now = time.time()

    with state_lock:
        cached_rate = usd_rub_cache["rate"]
        updated_at = usd_rub_cache["updated_at"]

    if cached_rate is not None and now - updated_at < 900:
        return cached_rate

    last_error = None

    try:
        data = get_json("https://open.er-api.com/v6/latest/USD")
        rate = data.get("rates", {}).get("RUB")

        if rate is not None and math.isfinite(float(rate)) and float(rate) > 0:
            rate = float(rate)
            with state_lock:
                usd_rub_cache.update({
                    "rate": rate,
                    "updated_at": time.time(),
                })
            return rate

    except Exception as exc:
        last_error = str(exc)
        logger.warning("USD/RUB request failed: %s", exc)

    if cached_rate is not None:
        return cached_rate

    raise RuntimeError(f"Could not obtain USD/RUB rate: {last_error}")


def build_market_payload(force=False):
    result = refresh_prices(force=force)
    prices = result["prices"]
    source = result.get("source") or "unknown"

    try:
        usd_rub = fetch_usd_rub()
        usd_rub_error = None
    except Exception as exc:
        usd_rub = None
        usd_rub_error = str(exc)
        logger.warning("USD/RUB unavailable: %s", exc)

    coins = []
    changes_24h = result.get("changes24h", {})

    for symbol, info in COINS.items():
        price_usd = prices.get(symbol)
        if price_usd is None:
            continue

        price_usd = float(price_usd)
        price_rub = price_usd * usd_rub if usd_rub is not None else None

        change_1h = calculate_hourly_change(source, symbol, price_usd)
        change_24h = calculate_change_24h(
            changes_24h.get(symbol), source, symbol, price_usd
        )

        coins.append({
            "symbol": symbol,
            "name": info["name"],
            "priceUsd": price_usd,
            "priceRub": price_rub,
            "change1h": change_1h,
            "change24h": change_24h,
            "source": source,
        })

    return {
        "success": True,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "coins": coins,
        "usdRub": usd_rub,
        "usdRubError": usd_rub_error,
        "hourlyHistory": "postgresql-persistent" if DATABASE_URL else "in-memory-only",
        "databaseAvailable": database_status["available"],
        "warning": result.get("error"),
    }


# ============================================================
# BASIC SECURITY HEADERS
# ============================================================

@app.after_request
def add_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    return response


@app.errorhandler(413)
def request_too_large(_error):
    return jsonify({"success": False, "error": "Request body too large"}), 413


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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "databaseConfigured": bool(DATABASE_URL),
        "databaseAvailable": bool(database_status["available"]),
    })


@app.route("/api/config")
def api_config():
    return jsonify({
        "success": True,
        "tradingMode": TRADING_MODE,
        "liveTradingEnabled": LIVE_TRADING_ENABLED,
        "telegramConfigured": telegram_configured,
        "telegramWebAppUrl": TELEGRAM_WEBAPP_URL,
        "databaseConfigured": bool(DATABASE_URL),
        "databaseAvailable": database_status["available"],
        "hourlyHistory": "postgresql-persistent" if DATABASE_URL else "in-memory-only",
        "coins": [
            {"symbol": symbol, "name": coin["name"]}
            for symbol, coin in COINS.items()
        ],
    })


@app.route("/api/prices")
def api_prices():
    try:
        return jsonify(build_market_payload())
    except Exception as exc:
        logger.exception("Price API failed")
        return jsonify({"success": False, "error": "Market data temporarily unavailable", "coins": []}), 503


@app.route("/api/market-prices")
def api_market_prices():
    return api_prices()


@app.route("/api/bybit-status")
def api_bybit_status():
    return jsonify({
        "success": True,
        "available": bybit_status["available"],
        "lastError": bybit_status["last_error"],
        "lastCheck": bybit_status["last_check"],
    })


@app.route("/api/usd-rub")
def api_usd_rub():
    try:
        return jsonify({
            "success": True,
            "rate": fetch_usd_rub(),
            "currency": "RUB",
            "base": "USD",
        })
    except Exception as exc:
        return jsonify({"success": False, "error": "Exchange rate temporarily unavailable"}), 503


@app.route("/api/trading/status")
def trading_status():
    return jsonify({
        "success": True,
        "mode": TRADING_MODE,
        "liveTradingEnabled": False,
        "message": "Simulation only. Real trading is disabled.",
    })


@app.route("/api/trading/signals")
def trading_signals():
    try:
        payload = build_market_payload()
        signals = []
        source = payload.get("source") or "unknown"

        for coin in payload["coins"]:
            analysis = analyze_coin(source, coin["symbol"], coin["priceUsd"])
            analysis["change1h"] = coin.get("change1h")
            analysis["change24h"] = coin.get("change24h")
            signals.append(analysis)

        return jsonify({
            "success": True,
            "mode": "simulation",
            "liveTradingEnabled": False,
            "strategy": "EMA9/EMA21 + RSI14 (experimental; observation-based)",
            "signals": signals,
        })

    except Exception as exc:
        logger.exception("Signal API failed")
        return jsonify({"success": False, "error": "Signals temporarily unavailable", "signals": []}), 503


# ============================================================
# TELEGRAM HELPERS AND KEYBOARD
# ============================================================

def telegram_api(method, payload):
    if not TELEGRAM_TOKEN:
        return {"ok": False, "description": "Telegram token is not configured"}

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    response = http.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def telegram_send_message(chat_id, text, reply_markup=None, parse_mode=None):
    payload = {"chat_id": chat_id, "text": text}

    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        result = telegram_api("sendMessage", payload)
        if not result.get("ok"):
            logger.error("Telegram sendMessage failed: %s", result.get("description"))
            return None

        return result.get("result", {}).get("message_id")

    except Exception:
        logger.exception("Could not send Telegram message")
        return None


def telegram_edit_message(
    chat_id,
    message_id,
    text,
    reply_markup=None,
    parse_mode=None,
):
    """Edit the existing bot message instead of sending another one."""
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }

    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        result = telegram_api("editMessageText", payload)

        if result.get("ok"):
            return True

        description = str(result.get("description", ""))

        # Telegram returns this when the requested content is already displayed.
        if "message is not modified" in description.lower():
            return True

        logger.warning("Telegram editMessageText failed: %s", description)
        return False

    except Exception:
        logger.exception("Could not edit Telegram message")
        return False


def telegram_delete_message(chat_id, message_id):
    """Try to remove the user's calculator input to keep the chat tidy."""
    if not message_id:
        return

    try:
        result = telegram_api(
            "deleteMessage",
            {
                "chat_id": chat_id,
                "message_id": message_id,
            },
        )

        if not result.get("ok"):
            logger.debug(
                "Telegram deleteMessage failed: %s",
                result.get("description"),
            )

    except Exception:
        # Deleting the user's message is optional; the bot should keep working.
        logger.debug("Could not delete Telegram message", exc_info=True)


def telegram_answer_callback(callback_id, text=None):
    try:
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        telegram_api("answerCallbackQuery", payload)
    except Exception:
        logger.exception("Could not answer Telegram callback")


def main_keyboard():
    return {
        "inline_keyboard": [
            [
                {
                    "text": "🌐 CryptoPulse",
                    "web_app": {"url": TELEGRAM_WEBAPP_URL},
                },
                {
                    "text": "📊 Цены в таблице",
                    "callback_data": "show_prices",
                },
            ],
            [
                {
                    "text": "🧮 Калькулятор",
                    "callback_data": "calculator",
                },
                {
                    "text": "💵 USDT / RUB",
                    "callback_data": "usdt_rub",
                },
            ],
        ]
    }


def edit_or_send_home(chat_id, message_id, greeting=True):
    if greeting:
        message = (
            "<b>CryptoPulse</b>\n\n"
            "Цены криптовалют, калькулятор и курс USDT/RUB.\n"
            "Реальная торговля отключена."
        )
    else:
        message = "Выбери действие в меню CryptoPulse."

    if message_id and telegram_edit_message(
        chat_id,
        message_id,
        message,
        reply_markup=main_keyboard(),
        parse_mode="HTML",
    ):
        return

    telegram_send_message(
        chat_id,
        message,
        reply_markup=main_keyboard(),
        parse_mode="HTML",
    )


def send_home(chat_id, greeting=True):
    # /start creates the initial menu message.
    if greeting:
        message = (
            "<b>CryptoPulse</b>\n\n"
            "Цены криптовалют, калькулятор и курс USDT/RUB.\n"
            "Реальная торговля отключена."
        )
    else:
        message = "Выбери действие в меню CryptoPulse."

    telegram_send_message(
        chat_id,
        message,
        reply_markup=main_keyboard(),
        parse_mode="HTML",
    )


# ============================================================
# TELEGRAM PRICE TABLE
# ============================================================

def format_price_usd(price):
    if price is None:
        return "—"

    price = float(price)

    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:,.4f}"
    if price >= 0.01:
        return f"{price:.5f}"

    return f"{price:.8f}"


def format_change(value):
    if value is None:
        return "—"

    value = float(value)

    if abs(value) < 0.005:
        value = 0.0

    return f"{value:+.2f}%"


def build_telegram_price_table(payload):
    rows = [
        f"{'Монета':<6} {'USD':>12} {'1ч':>8} {'24ч':>8}",
        "-" * 37,
    ]

    for coin in payload["coins"]:
        symbol = coin["symbol"][:6]
        price = format_price_usd(coin["priceUsd"])
        change_1h = format_change(coin["change1h"])
        change_24h = format_change(coin["change24h"])

        rows.append(
            f"{symbol:<6} {price:>12} {change_1h:>8} {change_24h:>8}"
        )

    return "\n".join(rows)


def render_prices_message():
    payload = build_market_payload()
    table = build_telegram_price_table(payload)

    source = html.escape(str(payload.get("source") or "unknown"))
    updated = datetime.now(timezone.utc).strftime("%H:%M UTC")

    return (
        "<b>📊 CryptoPulse — цены</b>\n"
        f"<pre>{html.escape(table)}</pre>\n"
        f"Источник: {source}\n"
        f"Обновлено: {updated}\n"
        "«—» означает, что данных для изменения пока недостаточно."
    )


def show_prices(chat_id, message_id):
    try:
        message = render_prices_message()

        if not telegram_edit_message(
            chat_id,
            message_id,
            message,
            reply_markup=main_keyboard(),
            parse_mode="HTML",
        ):
            telegram_send_message(
                chat_id,
                message,
                reply_markup=main_keyboard(),
                parse_mode="HTML",
            )

    except Exception:
        logger.exception("Telegram price table failed")

        error_message = "Не удалось получить цены. Попробуй ещё раз позже."

        if not telegram_edit_message(
            chat_id,
            message_id,
            error_message,
            reply_markup=main_keyboard(),
        ):
            telegram_send_message(
                chat_id,
                error_message,
                reply_markup=main_keyboard(),
            )


# ============================================================
# CALCULATOR
# ============================================================

def parse_calculator_input(text):
    """
    Supported examples:
      BTC 0.025
      0.025 BTC
      ETH 2
      100 USDT
      5000 RUB
    """
    cleaned = text.strip().upper().replace(",", ".")
    cleaned = re.sub(r"\s+", " ", cleaned)

    patterns = [
        r"^([A-Z]{2,10})\s+([0-9]+(?:\.[0-9]+)?)$",
        r"^([0-9]+(?:\.[0-9]+)?)\s+([A-Z]{2,10})$",
    ]

    for index, pattern in enumerate(patterns):
        match = re.match(pattern, cleaned)
        if not match:
            continue

        if index == 0:
            symbol, amount = match.group(1), float(match.group(2))
        else:
            amount, symbol = float(match.group(1)), match.group(2)

        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("Количество должно быть конечным числом больше нуля.")

        return symbol, amount

    raise ValueError(
        "Не удалось распознать запрос. Пример: BTC 0.025, ETH 2, 100 USDT или 5000 RUB."
    )


def calculate_value(text):
    symbol, amount = parse_calculator_input(text)

    if symbol == "RUB":
        rate = fetch_usd_rub()
        return (
            f"🧮 <b>Калькулятор</b>\n"
            f"{amount:,.2f} RUB ≈ ${amount / rate:,.2f}\n\n"
            "Это ориентировочный расчёт по справочному курсу USD/RUB."
        )

    if symbol in ("USD", "USDT"):
        rate = fetch_usd_rub()
        return (
            f"🧮 <b>Калькулятор</b>\n"
            f"{amount:,.4f} {symbol} ≈ {amount * rate:,.2f} RUB\n\n"
            "USDT здесь считается примерно равным 1 USD."
        )

    if symbol not in COINS or symbol == "USDT":
        raise ValueError("Эта монета не поддерживается калькулятором.")

    payload = build_market_payload()
    coin = next(
        (item for item in payload["coins"] if item["symbol"] == symbol),
        None,
    )

    if coin is None:
        raise RuntimeError(f"Текущая цена {symbol} недоступна.")

    total_usd = amount * coin["priceUsd"]
    rate = payload.get("usdRub")
    total_rub = total_usd * rate if rate is not None else None

    result = (
        f"🧮 <b>Калькулятор CryptoPulse</b>\n"
        f"{amount:g} {symbol}\n"
        f"Цена: ${format_price_usd(coin['priceUsd'])}\n"
        f"Стоимость: ${total_usd:,.2f}"
    )

    if total_rub is not None:
        result += f"\nВ рублях: {total_rub:,.2f} RUB"

    result += "\n\nРасчёт справочный. Торговые операции не выполняются."
    return result


def calculator_prompt():
    return (
        "🧮 <b>Калькулятор</b>\n\n"
        "Отправь сумму и тикер, например:\n"
        "<code>BTC 0.025</code>\n"
        "<code>ETH 2</code>\n"
        "<code>100 USDT</code>\n"
        "<code>5000 RUB</code>\n\n"
        "Для выхода напиши /cancel."
    )
def show_calculator(chat_id, message_id):
    with calculator_lock:
        calculator_waiting[chat_id] = message_id

    if not telegram_edit_message(
        chat_id,
        message_id,
        calculator_prompt(),
        reply_markup=main_keyboard(),
        parse_mode="HTML",
    ):
        with calculator_lock:
            calculator_waiting.pop(chat_id, None)

        telegram_send_message(
            chat_id,
            calculator_prompt(),
            reply_markup=main_keyboard(),
            parse_mode="HTML",
        )


# ============================================================
# USDT / RUB
# ============================================================

def render_usdt_rub_message():
    payload = build_market_payload()
    rate = payload.get("usdRub")

    if rate is None:
        rate = fetch_usd_rub()

    usdt = next(
        (coin for coin in payload["coins"] if coin["symbol"] == "USDT"),
        None,
    )

    usdt_usd = usdt["priceUsd"] if usdt else 1.0
    usdt_rub = usdt_usd * rate

    return (
        "<b>💵 USDT / RUB</b>\n\n"
        f"1 USDT ≈ {usdt_rub:,.2f} RUB\n"
        f"Справочный USD/RUB: {rate:,.4f}\n\n"
        "Это ориентировочная оценка, не цена покупки или продажи "
        "на конкретной бирже."
    )


def show_usdt_rub(chat_id, message_id):
    try:
        message = render_usdt_rub_message()

        if not telegram_edit_message(
            chat_id,
            message_id,
            message,
            reply_markup=main_keyboard(),
            parse_mode="HTML",
        ):
            telegram_send_message(
                chat_id,
                message,
                reply_markup=main_keyboard(),
                parse_mode="HTML",
            )

    except Exception:
        logger.exception("USDT/RUB request failed")

        error_message = (
            "Не удалось получить курс USDT/RUB. Попробуй позже."
        )

        if not telegram_edit_message(
            chat_id,
            message_id,
            error_message,
            reply_markup=main_keyboard(),
        ):
            telegram_send_message(
                chat_id,
                error_message,
                reply_markup=main_keyboard(),
            )


# ============================================================
# TELEGRAM WEBHOOK CONFIGURATION
# ============================================================

def configure_telegram():
    if not TELEGRAM_TOKEN:
        logger.warning("Telegram token missing; Telegram setup skipped")
        return

    if not TELEGRAM_WEBAPP_URL.startswith("https://"):
        logger.error("Telegram Mini App URL must use HTTPS")
        return

    try:
        result = telegram_api(
            "setChatMenuButton",
            {
                "menu_button": {
                    "type": "web_app",
                    "text": "Открыть CryptoPulse",
                    "web_app": {"url": TELEGRAM_WEBAPP_URL},
                }
            },
        )
        if result.get("ok"):
            logger.info("Telegram Mini App menu button configured")
        else:
            logger.error("Menu button configuration failed: %s", result)
    except Exception:
        logger.exception("Could not configure Telegram menu button")

    if not TELEGRAM_WEBHOOK_URL.startswith("https://"):
        logger.error("Telegram webhook URL must use HTTPS")
        return

    try:
        webhook_payload = {"url": TELEGRAM_WEBHOOK_URL}
        if TELEGRAM_WEBHOOK_SECRET:
            webhook_payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET
        result = telegram_api("setWebhook", webhook_payload)
        if result.get("ok"):
            logger.info("Telegram webhook configured")
        else:
            logger.error("Webhook configuration failed: %s", result)
    except Exception:
        logger.exception("Could not configure Telegram webhook")


# ============================================================
# TELEGRAM CALLBACKS AND MESSAGES
# ============================================================

@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    # Telegram supports a per-webhook secret header. Configure it in production.
    if TELEGRAM_WEBHOOK_SECRET:
        supplied_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not hmac.compare_digest(supplied_secret, TELEGRAM_WEBHOOK_SECRET):
            return jsonify({"ok": False, "error": "Unauthorized"}), 401

    if not TELEGRAM_TOKEN:
        return jsonify({
            "ok": False,
            "error": "Telegram token is not configured",
        }), 503

    update = request.get_json(silent=True) or {}
    if not isinstance(update, dict):
        return jsonify({"ok": False, "error": "Invalid update payload"}), 400

    # Handle inline keyboard button presses by editing the existing message.
    callback = update.get("callback_query")
    if callback:
        callback_id = callback.get("id")
        data = callback.get("data", "")
        message = callback.get("message") or {}

        chat_id = (message.get("chat") or {}).get("id")
        message_id = message.get("message_id")

        telegram_answer_callback(callback_id)

        if not chat_id or not message_id:
            return jsonify({"ok": True})

        # Any non-calculator screen cancels a pending calculator prompt.
        if data != "calculator":
            with calculator_lock:
                calculator_waiting.pop(chat_id, None)

        if data == "show_prices":
            show_prices(chat_id, message_id)

        elif data == "calculator":
            show_calculator(chat_id, message_id)

        elif data == "usdt_rub":
            show_usdt_rub(chat_id, message_id)

        return jsonify({"ok": True})

    message = (
        update.get("message")
        or update.get("edited_message")
        or {}
    )

    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return jsonify({"ok": True})

    command = text.split()[0].split("@")[0].lower()

    if command in ("/start", "/help"):
        with calculator_lock:
            calculator_waiting.pop(chat_id, None)

        send_home(chat_id)
        return jsonify({"ok": True})

    if command == "/cancel":
        with calculator_lock:
            prompt_message_id = calculator_waiting.pop(chat_id, None)

        telegram_delete_message(chat_id, message_id)
        if prompt_message_id:
            edit_or_send_home(
                chat_id,
                prompt_message_id,
                greeting=False,
            )
        else:
            send_home(chat_id, greeting=False)

        return jsonify({"ok": True})

    if command == "/status":
        reply = (
            "<b>CryptoPulse работает</b>\n"
            f"База данных: {'доступна' if database_status['available'] else 'не подтверждена'}\n"
            f"Режим: {TRADING_MODE}\n"
            "Реальные ордера: отключены"
        )

        telegram_send_message(
            chat_id,
            reply,
            reply_markup=main_keyboard(),
            parse_mode="HTML",
        )
        return jsonify({"ok": True})

    if command == "/prices":
        with calculator_lock:
            calculator_waiting.pop(chat_id, None)

        # This explicit command still works independently of the menu.
        try:
            table_message = render_prices_message()
            telegram_send_message(
                chat_id,
                table_message,
                reply_markup=main_keyboard(),
                parse_mode="HTML",
            )
        except Exception:
            logger.exception("Telegram price command failed")
            telegram_send_message(
                chat_id,
                "Не удалось получить цены. Попробуй ещё раз позже.",
                reply_markup=main_keyboard(),
            )

        return jsonify({"ok": True})

    with calculator_lock:
        prompt_message_id = calculator_waiting.get(chat_id)

    if prompt_message_id:
        try:
            result = calculate_value(text)

            with calculator_lock:
                calculator_waiting.pop(chat_id, None)

            # Remove the user's input when Telegram allows it.
            telegram_delete_message(chat_id, message_id)

            # Replace the calculator prompt with the result.
            if not telegram_edit_message(
                chat_id,
                prompt_message_id,
                result,
                reply_markup=main_keyboard(),
                parse_mode="HTML",
            ):
                telegram_send_message(
                    chat_id,
                    result,
                    reply_markup=main_keyboard(),
                    parse_mode="HTML",
                )

        except ValueError as exc:
            telegram_delete_message(chat_id, message_id)

            # Keep the calculator open if the input was invalid.
            error_message = (
                f"⚠️ {html.escape(str(exc))}\n\n"
                "Попробуй ещё раз, например: <code>BTC 0.025</code>."
            )

            telegram_edit_message(
                chat_id,
                prompt_message_id,
                error_message,
                reply_markup=main_keyboard(),
                parse_mode="HTML",
            )

        except Exception:
            logger.exception("Calculator failed")

            with calculator_lock:
                calculator_waiting.pop(chat_id, None)

            telegram_delete_message(chat_id, message_id)

            error_message = (
                "Не удалось выполнить расчёт. Попробуй ещё раз позже."
            )

            if not telegram_edit_message(
                chat_id,
                prompt_message_id,
                error_message,
                reply_markup=main_keyboard(),
            ):
                telegram_send_message(
                    chat_id,
                    error_message,
                    reply_markup=main_keyboard(),
                )

        return jsonify({"ok": True})

    # Avoid sending a new bot message for arbitrary text.
    # Keep the current menu message as the main interaction point.
    return jsonify({"ok": True})


# ============================================================
# BACKGROUND PRICE HISTORY WORKER
# ============================================================

def price_history_worker():
    logger.info("Price history worker started")

    while True:
        try:
            result = refresh_prices(force=True)
            logger.info(
                "Background price refresh completed; source=%s",
                result.get("source"),
            )
        except Exception:
            logger.exception("Background price refresh failed")

        time.sleep(HISTORY_REFRESH_SECONDS)


def start_price_history_worker():
    worker = threading.Thread(
        target=price_history_worker,
        name="price-history-worker",
        daemon=True,
    )
    worker.start()


# ============================================================
# STARTUP
# ============================================================

init_database()
if TELEGRAM_AUTO_CONFIGURE:
    configure_telegram()
else:
    logger.info("Telegram auto-configuration disabled by TELEGRAM_AUTO_CONFIGURE")

if ENABLE_BACKGROUND_WORKER:
    start_price_history_worker()
else:
    logger.info("Background price-history worker disabled by ENABLE_BACKGROUND_WORKER")


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
