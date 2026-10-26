import os
import time
import html
import logging
import threading
import re
import json
import hmac
import hashlib
from urllib.parse import urlencode
from datetime import datetime, timezone

import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask, jsonify, send_from_directory, request


# ============================================================
# CONFIGURATION
# ============================================================

app = Flask(__name__, static_folder="static")

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
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgres://", "postgresql://", 1
    )

TRADING_MODE = "simulation"
LIVE_TRADING_ENABLED = False  # Hard safety lock: no real orders are implemented.
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "").strip()
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()
BYBIT_RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000").strip()
SIMULATION_FEE_RATE = float(os.getenv("SIMULATION_FEE_RATE", "0.001"))
SIMULATION_SLIPPAGE_RATE = float(os.getenv("SIMULATION_SLIPPAGE_RATE", "0.0005"))
SIMULATION_TICK_SECONDS = max(5, int(os.getenv("SIMULATION_TICK_SECONDS", "15")))
DEFAULT_SIM_DEPOSIT_USDT = max(10.0, float(os.getenv("DEFAULT_SIM_DEPOSIT_USDT", "100")))
OWNER_TELEGRAM_IDS = {x.strip() for x in os.getenv("OWNER_TELEGRAM_IDS", "").split(",") if x.strip()}

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

# Trading state is persisted in PostgreSQL when DATABASE_URL is available.
# In RAM-only deployments it is intentionally marked non-persistent.
trading_state_lock = threading.RLock()
trading_state_ram = None
trading_last_tick = 0.0
trading_journal_ram = []

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
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cryptopulse_trading_state (
                        id SMALLINT PRIMARY KEY CHECK (id = 1),
                        data JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cryptopulse_trade_journal (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        event TEXT NOT NULL,
                        symbol TEXT,
                        side TEXT,
                        quantity DOUBLE PRECISION,
                        price_usdt DOUBLE PRECISION,
                        fee_usdt DOUBLE PRECISION,
                        pnl_usdt DOUBLE PRECISION,
                        details JSONB NOT NULL DEFAULT '{}'::jsonb
                    )
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

        if price > 0:
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

            if price <= 0:
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
    values = [float(value) for value in values if value is not None and float(value) > 0]
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
        if value <= 0:
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
                if value is not None and float(value) > 0:
                    prices[symbol] = float(value)

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

        if rate is not None and float(rate) > 0:
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
# SAFE SIMULATION ENGINE + BYBIT READ-ONLY BALANCE
# ============================================================

def _default_trading_state():
    return {
        "running": False,
        "mode": "simulation",
        "depositUsdt": DEFAULT_SIM_DEPOSIT_USDT,
        "cashUsdt": DEFAULT_SIM_DEPOSIT_USDT,
        "profitTargetRub": 500.0,
        "lossLimitRub": 50.0,
        "startEquityUsdt": DEFAULT_SIM_DEPOSIT_USDT,
        "realizedPnlUsdt": 0.0,
        "unrealizedPnlUsdt": 0.0,
        "totalPnlUsdt": 0.0,
        "totalPnlRub": 0.0,
        "positions": {},
        "tradesCount": 0,
        "statusMessage": "Симуляция остановлена. Настрой параметры и нажми «Запустить». ",
        "lastTickAt": None,
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "persistence": "postgresql" if DATABASE_URL else "ram-only",
    }


def _load_trading_state(cur=None):
    global trading_state_ram
    if DATABASE_URL:
        own = cur is None
        conn = get_db_connection() if own else None
        try:
            cursor = conn.cursor() if own else cur
            cursor.execute("SELECT data FROM cryptopulse_trading_state WHERE id=1")
            row = cursor.fetchone()
            if row:
                data = row["data"]
                if isinstance(data, str):
                    data = json.loads(data)
                data.setdefault("positions", {})
                data["persistence"] = "postgresql"
                return data
            data = _default_trading_state()
            cursor.execute(
                "INSERT INTO cryptopulse_trading_state (id, data) VALUES (1, %s::jsonb) ON CONFLICT (id) DO NOTHING",
                (json.dumps(data),),
            )
            if own:
                conn.commit()
            return data
        finally:
            if own and conn:
                conn.close()
    with trading_state_lock:
        if trading_state_ram is None:
            trading_state_ram = _default_trading_state()
        return json.loads(json.dumps(trading_state_ram))


def _save_trading_state(data, cur=None):
    global trading_state_ram
    data["updatedAt"] = datetime.now(timezone.utc).isoformat()
    data["persistence"] = "postgresql" if DATABASE_URL else "ram-only"
    if DATABASE_URL:
        own = cur is None
        conn = get_db_connection() if own else None
        try:
            cursor = conn.cursor() if own else cur
            cursor.execute("""
                INSERT INTO cryptopulse_trading_state (id, data, updated_at)
                VALUES (1, %s::jsonb, NOW())
                ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()
            """, (json.dumps(data),))
            if own:
                conn.commit()
        finally:
            if own and conn:
                conn.close()
    else:
        with trading_state_lock:
            trading_state_ram = json.loads(json.dumps(data))


def _journal(event, symbol=None, side=None, quantity=None, price=None, fee=0.0, pnl=0.0, details=None, cur=None):
    if not DATABASE_URL:
        with trading_state_lock:
            trading_journal_ram.insert(0, {
                "id": len(trading_journal_ram) + 1,
                "createdAt": datetime.now(timezone.utc).isoformat(),
                "event": event, "symbol": symbol, "side": side,
                "quantity": quantity, "priceUsdt": price, "feeUsdt": fee,
                "pnlUsdt": pnl, "details": details or {},
            })
            del trading_journal_ram[100:]
        return
    own = cur is None
    conn = get_db_connection() if own else None
    try:
        cursor = conn.cursor() if own else cur
        cursor.execute("""
            INSERT INTO cryptopulse_trade_journal
            (event, symbol, side, quantity, price_usdt, fee_usdt, pnl_usdt, details)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        """, (event, symbol, side, quantity, price, fee, pnl, json.dumps(details or {})))
        if own:
            conn.commit()
    finally:
        if own and conn:
            conn.close()


def _require_admin():
    # Fail closed: if ADMIN_TOKEN isn't configured, no browser control is allowed.
    supplied = request.headers.get("X-Admin-Token", "").strip()
    return bool(ADMIN_TOKEN and supplied and hmac.compare_digest(supplied, ADMIN_TOKEN))


def _bybit_signed_get(path, params):
    if not (BYBIT_API_KEY and BYBIT_API_SECRET):
        raise RuntimeError("BYBIT_API_KEY и BYBIT_API_SECRET не настроены")
    timestamp = str(int(time.time() * 1000))
    query = urlencode(sorted((k, str(v)) for k, v in params.items()))
    recv = BYBIT_RECV_WINDOW or "5000"
    payload = timestamp + BYBIT_API_KEY + recv + query
    signature = hmac.new(BYBIT_API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv,
        "X-BAPI-SIGN": signature,
    }
    response = http.get(BYBIT_BASE_URL + path, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    body = response.json()
    if body.get("retCode") != 0:
        raise RuntimeError("Bybit API error: " + str(body.get("retMsg", "unknown error")))
    return body.get("result", {})


def get_bybit_readonly_balance():
    if not (BYBIT_API_KEY and BYBIT_API_SECRET):
        return {"configured": False, "available": False, "error": "Ключи Bybit только для чтения пока не настроены."}
    try:
        result = _bybit_signed_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        accounts = result.get("list") or []
        account = accounts[0] if accounts else {}
        coins = account.get("coin") or []
        usdt = next((c for c in coins if c.get("coin") == "USDT"), {})
        return {
            "configured": True,
            "available": True,
            "accountType": account.get("accountType", "UNIFIED"),
            "totalEquityUsd": _safe_float(account.get("totalEquity")),
            "totalWalletBalanceUsdt": _safe_float(account.get("totalWalletBalance")),
            "usdtWalletBalance": _safe_float(usdt.get("walletBalance")),
            "usdtAvailableBalance": _safe_float(usdt.get("availableToWithdraw") or usdt.get("availableBalance") or usdt.get("walletBalance")),
            "updatedAt": datetime.now(timezone.utc).isoformat(),
            "note": "Только чтение. CryptoPulse не размещает реальные ордера.",
        }
    except Exception as exc:
        logger.warning("Bybit read-only balance unavailable: %s", exc)
        return {"configured": True, "available": False, "error": str(exc)}


def _safe_float(value, default=0.0):
    try:
        number = float(value)
        return number if number == number and abs(number) != float("inf") else default
    except (TypeError, ValueError):
        return default


def _journal_rows(limit=30):
    if not DATABASE_URL:
        with trading_state_lock:
            return json.loads(json.dumps(trading_journal_ram[:max(1, min(int(limit), 100))]))
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, created_at, event, symbol, side, quantity, price_usdt,
                           fee_usdt, pnl_usdt, details
                    FROM cryptopulse_trade_journal ORDER BY id DESC LIMIT %s
                """, (max(1, min(int(limit), 100)),))
                rows = cur.fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["createdAt"] = item.pop("created_at").isoformat() if item.get("created_at") else None
            for old, new in (("price_usdt", "priceUsdt"), ("fee_usdt", "feeUsdt"), ("pnl_usdt", "pnlUsdt")):
                item[new] = item.pop(old)
            item["details"] = item.get("details") or {}
            result.append(item)
        return result
    except Exception:
        logger.exception("Trade journal read failed")
        return []


def _tick_simulation():
    """Advance simulation once per interval. No real-order code exists here."""
    global trading_last_tick
    now = time.time()
    if now - trading_last_tick < SIMULATION_TICK_SECONDS:
        return
    with trading_state_lock:
        if now - trading_last_tick < SIMULATION_TICK_SECONDS:
            return
        trading_last_tick = now
    try:
        payload = build_market_payload()
        coins = {c.get("symbol"): c for c in payload.get("coins", []) if c.get("priceUsd")}
        if not coins:
            return
        rate = _safe_float(payload.get("usdRub"), 0.0)
        if rate <= 0:
            rate = 90.0  # Explicit fallback only for limit display; label in state warns.
        # Lock row in PostgreSQL so concurrent gunicorn workers don't mutate state simultaneously.
        conn = get_db_connection() if DATABASE_URL else None
        try:
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT pg_advisory_xact_lock(7142052026)")
                state = _load_trading_state(cur)
            else:
                cur = None
                state = _load_trading_state()
            if not state.get("running"):
                if conn:
                    conn.commit()
                return
            # A shared timestamp prevents duplicate simulation steps across gunicorn workers.
            last_tick_text = state.get("lastTickAt")
            if last_tick_text:
                try:
                    last_tick_dt = datetime.fromisoformat(str(last_tick_text).replace("Z", "+00:00"))
                    if (datetime.now(timezone.utc) - last_tick_dt).total_seconds() < SIMULATION_TICK_SECONDS:
                        if conn:
                            conn.commit()
                        return
                except (TypeError, ValueError):
                    pass
            positions = state.setdefault("positions", {})
            realized = _safe_float(state.get("realizedPnlUsdt"))
            cash = _safe_float(state.get("cashUsdt"))
            for symbol, pos in list(positions.items()):
                coin = coins.get(symbol)
                if not coin:
                    continue
                price = _safe_float(coin.get("priceUsd"))
                if price <= 0:
                    continue
                pos["lastPrice"] = price
                pos["unrealizedPnlUsdt"] = (price - _safe_float(pos.get("entryPrice"))) * _safe_float(pos.get("quantity"))
                analysis = analyze_coin(payload.get("source") or "unknown", symbol, price)
                rsi = _safe_float(analysis.get("rsi14"), 50)
                ema_fast = _safe_float(analysis.get("ema9"))
                ema_slow = _safe_float(analysis.get("ema21"))
                if (ema_fast and ema_slow and ema_fast < ema_slow) or rsi >= 72:
                    quantity = _safe_float(pos.get("quantity"))
                    gross = quantity * price
                    fee = gross * SIMULATION_FEE_RATE
                    slippage = gross * SIMULATION_SLIPPAGE_RATE
                    entry_cost = quantity * _safe_float(pos.get("entryPrice"))
                    pnl = gross - fee - slippage - entry_cost - _safe_float(pos.get("entryFeeUsdt"))
                    cash += gross - fee - slippage
                    realized += pnl
                    _journal("SELL", symbol, "SELL", quantity, price, fee + slippage, pnl,
                             {"reason": "EMA/RSI exit", "entryPrice": pos.get("entryPrice")}, cur)
                    positions.pop(symbol, None)
                    state["tradesCount"] = int(state.get("tradesCount", 0)) + 1
            # Open at most one position per tick; use 20% of initial deposit per position.
            if len(positions) < 3:
                candidates = []
                for symbol, coin in coins.items():
                    if symbol == "USDT" or symbol in positions:
                        continue
                    price = _safe_float(coin.get("priceUsd"))
                    if price <= 0:
                        continue
                    analysis = analyze_coin(payload.get("source") or "unknown", symbol, price)
                    rsi = _safe_float(analysis.get("rsi14"), 50)
                    fast, slow = _safe_float(analysis.get("ema9")), _safe_float(analysis.get("ema21"))
                    if fast > slow > 0 and 45 <= rsi <= 68 and _safe_float(analysis.get("sampleCount")) >= 30:
                        candidates.append((symbol, price, analysis))
                if candidates:
                    symbol, price, analysis = candidates[0]
                    gross = min(cash, _safe_float(state.get("depositUsdt")) * 0.20)
                    fee = gross * SIMULATION_FEE_RATE
                    slippage = gross * SIMULATION_SLIPPAGE_RATE
                    spend = gross + fee + slippage
                    if gross > 1 and spend <= cash:
                        quantity = gross / price
                        cash -= spend
                        positions[symbol] = {"symbol": symbol, "quantity": quantity, "entryPrice": price,
                                             "lastPrice": price, "entryFeeUsdt": fee + slippage,
                                             "openedAt": datetime.now(timezone.utc).isoformat(),
                                             "unrealizedPnlUsdt": 0.0}
                        _journal("BUY", symbol, "BUY", quantity, price, fee + slippage, 0,
                                 {"reason": "EMA9>EMA21 and RSI 45-68", "grossUsdt": gross}, cur)
                        state["tradesCount"] = int(state.get("tradesCount", 0)) + 1
            unrealized = sum(_safe_float(p.get("unrealizedPnlUsdt")) - _safe_float(p.get("entryFeeUsdt")) for p in positions.values())
            market_value = sum(_safe_float(p.get("lastPrice")) * _safe_float(p.get("quantity")) for p in positions.values())
            equity = cash + market_value
            total_pnl = equity - _safe_float(state.get("startEquityUsdt"), _safe_float(state.get("depositUsdt")))
            total_rub = total_pnl * rate
            state.update({"cashUsdt": cash, "realizedPnlUsdt": realized, "unrealizedPnlUsdt": unrealized,
                          "totalPnlUsdt": total_pnl, "totalPnlRub": total_rub,
                          "equityUsdt": equity, "usdRubRate": rate,
                          "lastTickAt": datetime.now(timezone.utc).isoformat()})
            target = _safe_float(state.get("profitTargetRub"), 500)
            loss = abs(_safe_float(state.get("lossLimitRub"), 50))
            if total_rub >= target:
                state["running"] = False
                state["statusMessage"] = f"Достигнута цель прибыли ({total_rub:.2f} ₽). Симуляция остановлена; открытые позиции сохранены виртуально."
                _journal("AUTO_STOP_PROFIT", details={"totalPnlRub": total_rub}, cur=cur)
            elif total_rub <= -loss:
                state["running"] = False
                state["statusMessage"] = f"Достигнут лимит убытка ({total_rub:.2f} ₽). Симуляция остановлена; открытые позиции сохранены виртуально."
                _journal("AUTO_STOP_LOSS", details={"totalPnlRub": total_rub}, cur=cur)
            else:
                state["statusMessage"] = "Симуляция работает. Стратегия экспериментальная; прибыль не гарантируется."
            _save_trading_state(state, cur)
            if conn:
                conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    except Exception:
        logger.exception("Simulation tick failed")


def _trading_snapshot(run_tick=True):
    if run_tick:
        _tick_simulation()
    state = _load_trading_state()
    positions = state.get("positions", {})
    return {"success": True, "mode": "simulation", "liveTradingEnabled": False,
            "running": bool(state.get("running")), "state": state,
            "positions": list(positions.values()), "journal": _journal_rows(30),
            "bybitBalance": get_bybit_readonly_balance(),
            "strategy": "EMA9/EMA21 + RSI14 (экспериментальная, не гарантия доходности)",
            "adminControlConfigured": bool(ADMIN_TOKEN),
            "persistence": "postgresql" if DATABASE_URL else "ram-only"}


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
    })


@app.route("/api/config")
def api_config():
    return jsonify({
        "success": True,
        "tradingMode": TRADING_MODE,
        "liveTradingEnabled": LIVE_TRADING_ENABLED,
        "adminControlConfigured": bool(ADMIN_TOKEN),
        "bybitBalanceConfigured": bool(BYBIT_API_KEY and BYBIT_API_SECRET),
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
        return jsonify({"success": False, "error": str(exc), "coins": []}), 503


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
        return jsonify({"success": False, "error": str(exc)}), 503


@app.route("/api/trading/status")
def trading_status():
    try:
        return jsonify(_trading_snapshot())
    except Exception as exc:
        logger.exception("Trading status failed")
        return jsonify({"success": False, "error": str(exc)}), 503


@app.route("/api/trading/balance")
def trading_balance():
    return jsonify({"success": True, "balance": get_bybit_readonly_balance()})


@app.route("/api/trading/journal")
def trading_journal():
    return jsonify({"success": True, "journal": _journal_rows(100), "persistence": "postgresql" if DATABASE_URL else "ram-only"})


@app.route("/api/trading/control", methods=["POST"])
def trading_control():
    if not _require_admin():
        return jsonify({"success": False, "error": "Управление закрыто. Настрой ADMIN_TOKEN в Render и введи его в панели."}), 401
    body = request.get_json(silent=True) or {}
    action = str(body.get("action", "")).lower()
    try:
        conn = get_db_connection() if DATABASE_URL else None
        try:
            cur = conn.cursor() if conn else None
            if conn:
                cur.execute("SELECT pg_advisory_xact_lock(7142052026)")
            state = _load_trading_state(cur)
            if action == "start":
                if state.get("running"):
                    return jsonify({"success": True, "message": "Симуляция уже запущена.", "state": state})
                deposit = _safe_float(body.get("depositUsdt"), _safe_float(state.get("depositUsdt"), DEFAULT_SIM_DEPOSIT_USDT))
                target = _safe_float(body.get("profitTargetRub"), _safe_float(state.get("profitTargetRub"), 500))
                loss = abs(_safe_float(body.get("lossLimitRub"), _safe_float(state.get("lossLimitRub"), 50)))
                if not (10 <= deposit <= 10000000):
                    return jsonify({"success": False, "error": "Виртуальный депозит должен быть от 10 до 10 000 000 USDT."}), 400
                if not (1 <= target <= 100000000 and 1 <= loss <= 100000000):
                    return jsonify({"success": False, "error": "Лимиты прибыли и убытка должны быть от 1 до 100 000 000 ₽."}), 400
                state.update({"running": True, "depositUsdt": deposit, "cashUsdt": deposit,
                              "profitTargetRub": target, "lossLimitRub": loss, "startEquityUsdt": deposit,
                              "realizedPnlUsdt": 0.0, "unrealizedPnlUsdt": 0.0, "totalPnlUsdt": 0.0,
                              "totalPnlRub": 0.0, "equityUsdt": deposit, "positions": {}, "tradesCount": 0,
                              "createdAt": datetime.now(timezone.utc).isoformat(),
                              "statusMessage": "Симуляция запущена. Реальные ордера отключены."})
                _journal("SIMULATION_START", details={"depositUsdt": deposit, "profitTargetRub": target, "lossLimitRub": loss}, cur=cur)
            elif action == "stop":
                state["running"] = False
                state["statusMessage"] = "Симуляция остановлена вручную. Виртуальные позиции сохранены."
                _journal("SIMULATION_STOP", details={"totalPnlRub": state.get("totalPnlRub", 0)}, cur=cur)
            elif action == "reset":
                state = _default_trading_state()
                _journal("SIMULATION_RESET", details={}, cur=cur)
            else:
                return jsonify({"success": False, "error": "Неизвестное действие."}), 400
            _save_trading_state(state, cur)
            if conn:
                conn.commit()
        finally:
            if conn:
                conn.close()
        return jsonify({"success": True, "message": state.get("statusMessage"), "state": state})
    except Exception as exc:
        logger.exception("Trading control failed")
        return jsonify({"success": False, "error": str(exc)}), 503


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
        return jsonify({"success": False, "error": str(exc), "signals": []}), 503


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
            [
                {"text": "🧪 Статус симуляции", "callback_data": "sim_status"},
                {"text": "▶️ Запуск симуляции", "callback_data": "sim_start"},
            ],
            [
                {"text": "⏹ Остановить симуляцию", "callback_data": "sim_stop"},
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

        if amount <= 0:
            raise ValueError("Количество должно быть больше нуля.")

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
        result = telegram_api(
            "setWebhook",
            {"url": TELEGRAM_WEBHOOK_URL},
        )
        if result.get("ok"):
            logger.info("Telegram webhook configured")
        else:
            logger.error("Webhook configuration failed: %s", result)
    except Exception:
        logger.exception("Could not configure Telegram webhook")


# ============================================================
# TELEGRAM CALLBACKS AND MESSAGES
# ============================================================

def _telegram_owner_allowed(chat_id, user_id=None):
    if not OWNER_TELEGRAM_IDS:
        return False
    return str(user_id or chat_id) in OWNER_TELEGRAM_IDS or str(chat_id) in OWNER_TELEGRAM_IDS


def _telegram_simulation_text():
    snapshot = _trading_snapshot()
    st = snapshot.get("state", {})
    return ("<b>CryptoPulse — симуляция</b>\n"
            f"Статус: {'работает' if st.get('running') else 'остановлена'}\n"
            f"Виртуальный капитал: {_safe_float(st.get('equityUsdt'), st.get('depositUsdt', 0)):.2f} USDT\n"
            f"Результат: {_safe_float(st.get('totalPnlRub')):.2f} ₽\n"
            f"Лимиты: +{_safe_float(st.get('profitTargetRub')):.0f} ₽ / -{abs(_safe_float(st.get('lossLimitRub'))):.0f} ₽\n"
            f"{html.escape(str(st.get('statusMessage', '')))}\n\n"
            "Реальные ордера отключены. Для управления ботом нужен OWNER_TELEGRAM_IDS.")


@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    if not TELEGRAM_TOKEN:
        return jsonify({
            "ok": False,
            "error": "Telegram token is not configured",
        }), 503

    update = request.get_json(silent=True) or {}

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

        elif data in ("sim_status", "sim_start", "sim_stop"):
            user_id = (callback.get("from") or {}).get("id")
            if not _telegram_owner_allowed(chat_id, user_id):
                telegram_send_message(chat_id, "Управление симуляцией не разрешено для этого Telegram-аккаунта.", reply_markup=main_keyboard())
            else:
                try:
                    if data == "sim_start":
                        conn = get_db_connection() if DATABASE_URL else None
                        try:
                            cur = conn.cursor() if conn else None
                            state = _load_trading_state(cur)
                            if not state.get("running"):
                                state.update({"running": True, "statusMessage": "Симуляция запущена через Telegram."})
                                _save_trading_state(state, cur)
                            if conn: conn.commit()
                        finally:
                            if conn: conn.close()
                    elif data == "sim_stop":
                        conn = get_db_connection() if DATABASE_URL else None
                        try:
                            cur = conn.cursor() if conn else None
                            state = _load_trading_state(cur)
                            state.update({"running": False, "statusMessage": "Симуляция остановлена через Telegram."})
                            _save_trading_state(state, cur)
                            if conn: conn.commit()
                        finally:
                            if conn: conn.close()
                    telegram_edit_message(chat_id, message_id, _telegram_simulation_text(), reply_markup=main_keyboard(), parse_mode="HTML")
                except Exception:
                    logger.exception("Telegram simulation control failed")
                    telegram_send_message(chat_id, "Не удалось изменить состояние симуляции. Проверь DATABASE_URL и логи.", reply_markup=main_keyboard())

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

    if command in ("/sim", "/simstatus", "/simstart", "/simstop"):
        user_id = (message.get("from") or {}).get("id")
        if not _telegram_owner_allowed(chat_id, user_id):
            telegram_send_message(chat_id, "Управление симуляцией не разрешено для этого Telegram-аккаунта.", reply_markup=main_keyboard())
        else:
            try:
                if command == "/simstart":
                    state = _load_trading_state()
                    state.update({"running": True, "statusMessage": "Симуляция запущена через Telegram."})
                    _save_trading_state(state)
                elif command == "/simstop":
                    state = _load_trading_state()
                    state.update({"running": False, "statusMessage": "Симуляция остановлена через Telegram."})
                    _save_trading_state(state)
                telegram_send_message(chat_id, _telegram_simulation_text(), reply_markup=main_keyboard(), parse_mode="HTML")
            except Exception:
                logger.exception("Telegram sim command failed")
                telegram_send_message(chat_id, "Ошибка управления симуляцией.", reply_markup=main_keyboard())
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


def simulation_worker():
    """Background ticking is enabled only with PostgreSQL-backed shared state."""
    if not DATABASE_URL:
        logger.warning("Simulation worker disabled: DATABASE_URL is required for safe shared state")
        return
    logger.info("Simulation worker started")
    while True:
        try:
            _tick_simulation()
        except Exception:
            logger.exception("Simulation worker iteration failed")
        time.sleep(SIMULATION_TICK_SECONDS)


# ============================================================
# STARTUP
# ============================================================

init_database()
configure_telegram()
start_price_history_worker()
threading.Thread(target=simulation_worker, name="simulation-worker", daemon=True).start()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
