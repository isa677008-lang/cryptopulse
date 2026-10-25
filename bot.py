import os
import time
import logging
import threading
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

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgres://",
        "postgresql://",
        1,
    )

# Real trading is deliberately disabled.
TRADING_MODE = "simulation"
LIVE_TRADING_ENABLED = False

REQUEST_TIMEOUT = 12
PRICE_CACHE_SECONDS = 30
HISTORY_RETENTION_HOURS = 72

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
BYBIT_BASE_URL = "https://api.bybit.com"

COINS = {
    "BTC": {
        "name": "Bitcoin",
        "coingecko_id": "bitcoin",
        "bybit_symbol": "BTCUSDT",
    },
    "ETH": {
        "name": "Ethereum",
        "coingecko_id": "ethereum",
        "bybit_symbol": "ETHUSDT",
    },
    "BNB": {
        "name": "BNB",
        "coingecko_id": "binancecoin",
        "bybit_symbol": "BNBUSDT",
    },
    "XRP": {
        "name": "XRP",
        "coingecko_id": "ripple",
        "bybit_symbol": "XRPUSDT",
    },
    "SOL": {
        "name": "Solana",
        "coingecko_id": "solana",
        "bybit_symbol": "SOLUSDT",
    },
    "DOGE": {
        "name": "Dogecoin",
        "coingecko_id": "dogecoin",
        "bybit_symbol": "DOGEUSDT",
    },
    "ADA": {
        "name": "Cardano",
        "coingecko_id": "cardano",
        "bybit_symbol": "ADAUSDT",
    },
    "TRX": {
        "name": "TRON",
        "coingecko_id": "tron",
        "bybit_symbol": "TRXUSDT",
    },
    "AVAX": {
        "name": "Avalanche",
        "coingecko_id": "avalanche-2",
        "bybit_symbol": "AVAXUSDT",
    },
    "LINK": {
        "name": "Chainlink",
        "coingecko_id": "chainlink",
        "bybit_symbol": "LINKUSDT",
    },
    "DOT": {
        "name": "Polkadot",
        "coingecko_id": "polkadot",
        "bybit_symbol": "DOTUSDT",
    },
    "LTC": {
        "name": "Litecoin",
        "coingecko_id": "litecoin",
        "bybit_symbol": "LTCUSDT",
    },
    "BCH": {
        "name": "Bitcoin Cash",
        "coingecko_id": "bitcoin-cash",
        "bybit_symbol": "BCHUSDT",
    },
    "SHIB": {
        "name": "Shiba Inu",
        "coingecko_id": "shiba-inu",
        "bybit_symbol": "SHIBUSDT",
    },
    "TON": {
        "name": "Toncoin",
        "coingecko_id": "the-open-network",
        "bybit_symbol": "TONUSDT",
    },
}


# ============================================================
# SHARED STATE AND LOCKS
# ============================================================

state_lock = threading.RLock()
database_lock = threading.RLock()

price_cache = {
    "updated_at": 0,
    "prices": {},
    "source": None,
    "error": None,
}

# Each item is:
# {
#     "timestamp": unix_timestamp,
#     "price": float
# }
price_history = {}

usd_rub_cache = {
    "updated_at": 0,
    "rate": None,
}

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


# ============================================================
# DATABASE
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
    """
    Create the persistent price-history table and indexes.
    Restore recent history into RAM after every application restart.
    """
    if not DATABASE_URL:
        logger.warning(
            "DATABASE_URL is missing. Price history will only be stored in RAM."
        )
        database_status["available"] = False
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS price_history (
                        id BIGSERIAL PRIMARY KEY,
                        source TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        price DOUBLE PRECISION NOT NULL
                            CHECK (price > 0),
                        observed_at TIMESTAMPTZ NOT NULL,
                        UNIQUE (source, symbol, observed_at)
                    )
                    """
                )

                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_price_history_symbol_time
                    ON price_history (symbol, observed_at DESC)
                    """
                )

                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_price_history_source_symbol_time
                    ON price_history (source, symbol, observed_at DESC)
                    """
                )

                conn.commit()

        database_status["available"] = True
        database_status["last_error"] = None

        logger.info("PostgreSQL database initialized")

        load_database_history()

    except Exception as exc:
        database_status["available"] = False
        database_status["last_error"] = str(exc)
        logger.exception("Could not initialize PostgreSQL")


def load_database_history():
    """
    Load recent observations from PostgreSQL into RAM.

    This is important after Render restarts the application:
    historical observations remain available for hourly calculations.
    """
    if not DATABASE_URL:
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        source,
                        symbol,
                        price,
                        EXTRACT(EPOCH FROM observed_at) AS timestamp
                    FROM price_history
                    WHERE observed_at >= NOW() - (%s * INTERVAL '1 hour')
                    ORDER BY observed_at ASC
                    """,
                    (HISTORY_RETENTION_HOURS,),
                )

                rows = cur.fetchall()

        with state_lock:
            for row in rows:
                key = (
                    str(row["source"]),
                    str(row["symbol"]),
                )

                price_history.setdefault(key, []).append(
                    {
                        "timestamp": float(row["timestamp"]),
                        "price": float(row["price"]),
                    }
                )

            # Remove duplicate points and sort by timestamp.
            for key, points in list(price_history.items()):
                unique = {}

                for point in points:
                    unique[point["timestamp"]] = point

                price_history[key] = sorted(
                    unique.values(),
                    key=lambda item: item["timestamp"],
                )

        database_status["available"] = True
        database_status["last_error"] = None

        logger.info("Loaded %s historical observations", len(rows))

    except Exception as exc:
        database_status["available"] = False
        database_status["last_error"] = str(exc)
        logger.exception("Could not load historical observations")


def save_history_to_database(source, prices, observed_at):
    """
    Persist the latest price observations.

    Database errors do not stop price collection or the web server.
    """
    if not DATABASE_URL or not prices:
        return

    try:
        records = [
            (
                source,
                symbol,
                float(price),
                observed_at,
            )
            for symbol, price in prices.items()
            if price is not None and float(price) > 0
        ]

        if not records:
            return

        with database_lock:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO price_history (
                            source,
                            symbol,
                            price,
                            observed_at
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (source, symbol, observed_at)
                        DO NOTHING
                        """,
                        records,
                    )

                    cur.execute(
                        """
                        DELETE FROM price_history
                        WHERE observed_at <
                            NOW() - (%s * INTERVAL '1 hour')
                        """,
                        (HISTORY_RETENTION_HOURS,),
                    )

                    conn.commit()

        database_status["available"] = True
        database_status["last_error"] = None

    except Exception as exc:
        database_status["available"] = False
        database_status["last_error"] = str(exc)
        logger.warning("Could not save price history: %s", exc)


def remember_prices(source, prices, observed_at=None):
    """
    Store observations in RAM and PostgreSQL.

    source is kept as part of the history key so that data from
    different providers is not accidentally mixed.
    """
    if observed_at is None:
        observed_at = datetime.now(timezone.utc)

    timestamp = observed_at.timestamp()

    with state_lock:
        for symbol, price in prices.items():
            if price is None:
                continue

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
                points.append(
                    {
                        "timestamp": timestamp,
                        "price": price,
                    }
                )

            cutoff = timestamp - HISTORY_RETENTION_HOURS * 3600

            price_history[key] = [
                point
                for point in points
                if point["timestamp"] >= cutoff
            ]

    save_history_to_database(source, prices, observed_at)


# ============================================================
# HTTP HELPERS
# ============================================================

def make_session():
    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": "CryptoPulse/1.0",
            "Accept": "application/json",
        }
    )

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
# COINGECKO PRICE PROVIDER
# ============================================================

def fetch_coingecko_prices():
    ids = ",".join(
        coin["coingecko_id"]
        for coin in COINS.values()
    )

    params = {
        "ids": ids,
        "vs_currencies": "usd",
        "include_24hr_change": "true",
    }

    headers = {}

    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    data = get_json(
        f"{COINGECKO_BASE_URL}/simple/price",
        params=params,
        headers=headers or None,
    )

    prices = {}
    changes_24h = {}

    for symbol, coin in COINS.items():
        item = data.get(coin["coingecko_id"], {})
        price = item.get("usd")

        if price is None:
            continue

        prices[symbol] = float(price)

        change = item.get("usd_24h_change")

        changes_24h[symbol] = (
            float(change)
            if change is not None
            else None
        )

    if not prices:
        raise RuntimeError("CoinGecko returned no usable prices")

    return prices, changes_24h


# ============================================================
# BYBIT PRICE PROVIDER
# ============================================================

def fetch_bybit_prices():
    prices = {}

    for symbol, coin in COINS.items():
        try:
            data = get_json(
                f"{BYBIT_BASE_URL}/v5/market/tickers",
                params={
                    "category": "spot",
                    "symbol": coin["bybit_symbol"],
                },
            )

            if data.get("retCode") != 0:
                continue

            items = data.get("result", {}).get("list", [])

            if not items:
                continue

            last_price = items[0].get("lastPrice")

            if last_price is not None:
                price = float(last_price)

                if price > 0:
                    prices[symbol] = price

        except Exception as exc:
            logger.warning(
                "Bybit request failed for %s: %s",
                symbol,
                exc,
            )

    bybit_status["available"] = bool(prices)
    bybit_status["last_check"] = datetime.now(
        timezone.utc
    ).isoformat()

    if not prices:
        bybit_status["last_error"] = (
            "Bybit did not return usable prices"
        )

        raise RuntimeError("Bybit returned no usable prices")

    bybit_status["last_error"] = None

    return prices


# ============================================================
# USD/RUB EXCHANGE RATE
# ============================================================

def fetch_usd_rub():
    now = time.time()

    with state_lock:
        cached_rate = usd_rub_cache["rate"]
        updated_at = usd_rub_cache["updated_at"]

    if (
        cached_rate is not None
        and now - updated_at < 900
    ):
        return cached_rate

    urls = [
        "https://open.er-api.com/v6/latest/USD",
    ]

    last_error = None

    for url in urls:
        try:
            data = get_json(url)

            rate = (
                data.get("rates", {})
                .get("RUB")
            )

            if rate is None:
                continue

            rate = float(rate)

            if rate <= 0:
                continue

            with state_lock:
                usd_rub_cache["rate"] = rate
                usd_rub_cache["updated_at"] = time.time()

            return rate

        except Exception as exc:
            last_error = str(exc)
            logger.warning(
                "USD/RUB rate request failed: %s",
                exc,
            )

    if cached_rate is not None:
        return cached_rate

    raise RuntimeError(
        f"Could not obtain USD/RUB exchange rate: {last_error}"
    )


# ============================================================
# PRICE CACHE
# ============================================================

def refresh_prices(force=False):
    now = time.time()

    with state_lock:
        cached_prices = dict(price_cache["prices"])
        cache_age = now - price_cache["updated_at"]

        if (
            cached_prices
            and cache_age < PRICE_CACHE_SECONDS
            and not force
        ):
            return {
                "prices": cached_prices,
                "source": price_cache["source"],
                "error": price_cache["error"],
            }

    errors = []

    # Primary provider.
    try:
        prices, changes_24h = fetch_coingecko_prices()

        observed_at = datetime.now(timezone.utc)

        remember_prices(
            "coingecko",
            prices,
            observed_at,
        )

        result = {
            "prices": prices,
            "changes24h": changes_24h,
            "source": "coingecko",
            "error": None,
        }

        with state_lock:
            price_cache["updated_at"] = time.time()
            price_cache["prices"] = dict(prices)
            price_cache["source"] = "coingecko"
            price_cache["error"] = None

        return result

    except Exception as exc:
        errors.append(f"CoinGecko: {exc}")

        logger.warning(
            "CoinGecko price retrieval failed: %s",
            exc,
        )

    # Secondary provider.
    try:
        prices = fetch_bybit_prices()

        observed_at = datetime.now(timezone.utc)

        remember_prices(
            "bybit",
            prices,
            observed_at,
        )

        result = {
            "prices": prices,
            "changes24h": {},
            "source": "bybit",
            "error": None,
        }

        with state_lock:
            price_cache["updated_at"] = time.time()
            price_cache["prices"] = dict(prices)
            price_cache["source"] = "bybit"
            price_cache["error"] = None

        return result

    except Exception as exc:
        errors.append(f"Bybit: {exc}")

        logger.warning(
            "Bybit price retrieval failed: %s",
            exc,
        )

    # If both providers fail, return the last cached prices.
    with state_lock:
        cached_prices = dict(price_cache["prices"])
        cached_source = price_cache["source"]

        price_cache["error"] = "; ".join(errors)

    if cached_prices:
        return {
            "prices": cached_prices,
            "source": cached_source,
            "error": "; ".join(errors),
        }

    raise RuntimeError("; ".join(errors))


# ============================================================
# HISTORICAL PRICE CALCULATIONS
# ============================================================

def get_history_points(source, symbol):
    """
    Return recent in-memory observations.

    If needed, retrieve the symbol's historical observations from
    PostgreSQL as a fallback.
    """
    key = (source, symbol)

    with state_lock:
        points = list(price_history.get(key, []))

    now = time.time()
    cutoff = now - HISTORY_RETENTION_HOURS * 3600

    points = [
        point
        for point in points
        if point["timestamp"] >= cutoff
    ]

    # If RAM contains an eligible observation around one hour ago,
    # no database query is necessary.
    has_hourly_candidate = any(
        45 * 60
        <= now - point["timestamp"]
        <= 75 * 60
        for point in points
    )

    if has_hourly_candidate or not DATABASE_URL:
        return points

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        price,
                        EXTRACT(EPOCH FROM observed_at) AS timestamp
                    FROM price_history
                    WHERE source = %s
                      AND symbol = %s
                      AND observed_at >= NOW() - INTERVAL '3 hours'
                    ORDER BY observed_at ASC
                    """,
                    (source, symbol),
                )

                rows = cur.fetchall()

        database_points = [
            {
                "timestamp": float(row["timestamp"]),
                "price": float(row["price"]),
            }
            for row in rows
        ]

        # Merge RAM and database history without duplicating timestamps.
        merged = {
            point["timestamp"]: point
            for point in points
        }

        for point in database_points:
            merged[point["timestamp"]] = point

        return sorted(
            merged.values(),
            key=lambda point: point["timestamp"],
        )

    except Exception as exc:
        logger.warning(
            "Historical lookup failed for %s: %s",
            symbol,
            exc,
        )

        return points


def calculate_hourly_change(source, symbol, current_price):
    """
    Calculate the percentage change relative to an observation
    between 45 and 75 minutes old.

    Return None if there is no suitable historical observation.
    """
    if current_price is None or current_price <= 0:
        return None

    points = get_history_points(source, symbol)
    now = time.time()

    candidates = [
        point
        for point in points
        if 45 * 60
        <= now - point["timestamp"]
        <= 75 * 60
        and point["price"] > 0
    ]

    if not candidates:
        return None

    # Prefer the observation closest to exactly 60 minutes ago.
    reference = min(
        candidates,
        key=lambda point: abs(
            (now - point["timestamp"]) - 3600
        ),
    )

    return (
        (current_price - reference["price"])
        / reference["price"]
    ) * 100


def calculate_change_24h(provider_change, source, symbol, current_price):
    """
    Prefer the provider's 24-hour percentage.

    If unavailable, attempt to calculate it from persisted history.
    """
    if provider_change is not None:
        return provider_change

    points = get_history_points(source, symbol)
    now = time.time()

    candidates = [
        point
        for point in points
        if 22 * 3600
        <= now - point["timestamp"]
        <= 26 * 3600
        and point["price"] > 0
    ]

    if not candidates:
        return None

    reference = min(
        candidates,
        key=lambda point: abs(
            (now - point["timestamp"]) - 86400
        ),
    )

    return (
        (current_price - reference["price"])
        / reference["price"]
    ) * 100


# ============================================================
# API RESPONSE BUILDERS
# ============================================================

def build_market_payload(force=False):
    result = refresh_prices(force=force)

    prices = result["prices"]
    source = result.get("source") or "unknown"

    try:
        usd_rub = fetch_usd_rub()
        usd_rub_error = None
    except Exception as exc:
        logger.warning("USD/RUB unavailable: %s", exc)
        usd_rub = None
        usd_rub_error = str(exc)

    changes_24h = result.get("changes24h", {})
    coins = []

    for symbol, info in COINS.items():
        price_usd = prices.get(symbol)

        if price_usd is None:
            continue

        price_usd = float(price_usd)

        price_rub = (
            price_usd * usd_rub
            if usd_rub is not None
            else None
        )

        change_1h = calculate_hourly_change(
            source,
            symbol,
            price_usd,
        )

        change_24h = calculate_change_24h(
            changes_24h.get(symbol),
            source,
            symbol,
            price_usd,
        )

        coins.append(
            {
                "symbol": symbol,
                "name": info["name"],
                "priceUsd": price_usd,
                "priceRub": price_rub,
                "change1h": change_1h,
                "change24h": change_24h,
                "source": source,
            }
        )

    return {
        "success": True,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "coins": coins,
        "usdRub": usd_rub,
        "usdRubError": usd_rub_error,
        "hourlyHistory": (
            "postgresql-persistent"
            if DATABASE_URL
            else "in-memory-only"
        ),
        "databaseAvailable": database_status["available"],
        "warning": result.get("error"),
    }


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def index():
    return send_from_directory(
        app.static_folder,
        "index.html",
    )


@app.route("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "service": "CryptoPulse",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )


@app.route("/api/config")
def api_config():
    return jsonify(
        {
            "success": True,
            "tradingMode": TRADING_MODE,
            "liveTradingEnabled": LIVE_TRADING_ENABLED,
            "telegramConfigured": telegram_configured,
            "databaseConfigured": bool(DATABASE_URL),
            "databaseAvailable": database_status["available"],
            "hourlyHistory": (
                "postgresql-persistent"
                if DATABASE_URL
                else "in-memory-only"
            ),
            "coins": [
                {
                    "symbol": symbol,
                    "name": coin["name"],
                }
                for symbol, coin in COINS.items()
            ],
        }
    )


@app.route("/api/prices")
def api_prices():
    try:
        payload = build_market_payload()

        return jsonify(payload)

    except Exception as exc:
        logger.exception("Price API failed")

        return jsonify(
            {
                "success": False,
                "error": str(exc),
                "coins": [],
            }
        ), 503


@app.route("/api/market-prices")
def api_market_prices():
    return api_prices()


@app.route("/api/bybit-status")
def api_bybit_status():
    return jsonify(
        {
            "success": True,
            "available": bybit_status["available"],
            "lastError": bybit_status["last_error"],
            "lastCheck": bybit_status["last_check"],
        }
    )


@app.route("/api/usd-rub")
def api_usd_rub():
    try:
        rate = fetch_usd_rub()

        return jsonify(
            {
                "success": True,
                "rate": rate,
                "currency": "RUB",
                "base": "USD",
            }
        )

    except Exception as exc:
        return jsonify(
            {
                "success": False,
                "error": str(exc),
            }
        ), 503


@app.route("/api/trading/status")
def trading_status():
    return jsonify(
        {
            "success": True,
            "mode": TRADING_MODE,
            "liveTradingEnabled": LIVE_TRADING_ENABLED,
            "message": (
                "Simulation only. Real trading is disabled."
            ),
        }
    )


@app.route("/api/trading/signals")
def trading_signals():
    """
    Informational market signals only.
    This endpoint never creates or submits orders.
    """
    try:
        payload = build_market_payload()

        signals = []

        for coin in payload["coins"]:
            change = coin["change1h"]

            if change is None:
                label = "insufficient_history"
            elif change >= 1.0:
                label = "positive_momentum"
            elif change <= -1.0:
                label = "negative_momentum"
            else:
                label = "neutral"

            signals.append(
                {
                    "symbol": coin["symbol"],
                    "signal": label,
                    "change1h": change,
                    "mode": "simulation",
                }
            )

        return jsonify(
            {
                "success": True,
                "mode": "simulation",
                "liveTradingEnabled": False,
                "signals": signals,
            }
        )

    except Exception as exc:
        logger.exception("Signal API failed")

        return jsonify(
            {
                "success": False,
                "error": str(exc),
                "signals": [],
            }
        ), 503


# ============================================================
# TELEGRAM
# ============================================================

def telegram_send_message(chat_id, text):
    if not TELEGRAM_TOKEN:
        return False

    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_TOKEN}/sendMessage"
    )

    try:
        response = http.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
            },
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()

        return True

    except Exception:
        logger.exception("Could not send Telegram message")
        return False


def configure_telegram():
    """
    Set a webhook only if the application has a public URL configured.

    Set TELEGRAM_WEBHOOK_URL in Render to the full webhook URL,
    for example: https://your-service.onrender.com/telegram-webhook
    """
    if not TELEGRAM_TOKEN:
        logger.info("Telegram token is not configured")
        return

    webhook_url = os.getenv(
        "TELEGRAM_WEBHOOK_URL",
        "",
    ).strip()

    if not webhook_url:
        logger.info(
            "TELEGRAM_WEBHOOK_URL is not configured; "
            "Telegram webhook was not changed"
        )
        return

    try:
        response = http.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
            json={"url": webhook_url},
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()

        logger.info("Telegram webhook configuration requested")

    except Exception:
        logger.exception("Could not configure Telegram webhook")


@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    if not TELEGRAM_TOKEN:
        return jsonify(
            {
                "ok": False,
                "error": "Telegram token is not configured",
            }
        ), 503

    update = request.get_json(silent=True) or {}

    message = (
        update.get("message")
        or update.get("edited_message")
        or {}
    )

    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return jsonify({"ok": True})

    if text.startswith("/start") or text.startswith("/help"):
        reply = (
            "CryptoPulse запущен.\n\n"
            "Команды:\n"
            "/prices — текущие цены\n"
            "/status — статус сервиса\n\n"
            "Торговля отключена: доступен только мониторинг."
        )

        telegram_send_message(chat_id, reply)

    elif text.startswith("/status"):
        reply = (
            "CryptoPulse работает.\n"
            f"База данных: "
            f"{'доступна' if database_status['available'] else 'не подтверждена'}\n"
            "Режим: simulation\n"
            "Реальные ордера: отключены"
        )

        telegram_send_message(chat_id, reply)

    elif text.startswith("/prices"):
        try:
            payload = build_market_payload()

            lines = ["CryptoPulse — цены:"]

            for coin in payload["coins"][:10]:
                change = coin["change1h"]

                if change is None:
                    change_text = "нет часовой истории"
                else:
                    change_text = f"{change:+.2f}% за час"

                lines.append(
                    f"{coin['symbol']}: "
                    f"${coin['priceUsd']:,.6f} — "
                    f"{change_text}"
                )

            telegram_send_message(
                chat_id,
                "\n".join(lines),
            )

        except Exception as exc:
            logger.exception("Telegram prices command failed")

            telegram_send_message(
                chat_id,
                f"Не удалось получить цены: {exc}",
            )

    else:
        telegram_send_message(
            chat_id,
            "Команды: /prices, /status, /help",
        )

    return jsonify({"ok": True})


# ============================================================
# STARTUP
# ============================================================

# Initialize PostgreSQL and restore saved history when this process starts.
init_database()

# Configure Telegram webhook only when TELEGRAM_WEBHOOK_URL is set.
configure_telegram()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
