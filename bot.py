import os
import time
import logging
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, send_from_directory, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
app = Flask(__name__, static_folder=STATIC_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cryptopulse")

TELEGRAM_TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
)
TELEGRAM_API = (
    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    if TELEGRAM_TOKEN else None
)
SITE_URL = os.getenv(
    "SITE_URL", "https://cryptopulse-lpc9.onrender.com"
)

REQUEST_TIMEOUT = 12
CACHE_TTL = 60
FX_CACHE_TTL = 900
FAILURE_COOLDOWN = 120
HISTORY_RETENTION = 3 * 60 * 60
HISTORY_TARGET_AGE = 60 * 60
HISTORY_MAX_AGE = 70 * 60

TRADING_MODE = "simulation"
LIVE_TRADING_ENABLED = False

BYBIT_HOSTS = ["https://api.bybit.eu"]
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()

COINS = [
    {"symbol": "BTC", "name": "Bitcoin", "id": "bitcoin"},
    {"symbol": "ETH", "name": "Ethereum", "id": "ethereum"},
    {"symbol": "USDT", "name": "Tether", "id": "tether"},
    {"symbol": "BNB", "name": "BNB", "id": "binancecoin"},
    {"symbol": "XRP", "name": "XRP", "id": "ripple"},
    {"symbol": "USDC", "name": "USD Coin", "id": "usd-coin"},
    {"symbol": "SOL", "name": "Solana", "id": "solana"},
    {"symbol": "TRX", "name": "TRON", "id": "tron"},
    {"symbol": "DOGE", "name": "Dogecoin", "id": "dogecoin"},
    {"symbol": "TON", "name": "Toncoin", "id": "the-open-network"},
    {"symbol": "ADA", "name": "Cardano", "id": "cardano"},
    {"symbol": "BCH", "name": "Bitcoin Cash", "id": "bitcoin-cash"},
    {"symbol": "LINK", "name": "Chainlink", "id": "chainlink"},
    {"symbol": "AVAX", "name": "Avalanche", "id": "avalanche-2"},
    {"symbol": "LTC", "name": "Litecoin", "id": "litecoin"},
]

COIN_IMAGES = {
    "BTC": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png",
    "ETH": "https://assets.coingecko.com/coins/images/279/large/ethereum.png",
    "USDT": "https://assets.coingecko.com/coins/images/325/large/Tether.png",
    "BNB": "https://assets.coingecko.com/coins/images/825/large/bnb-icon2_2x.png",
    "XRP": "https://assets.coingecko.com/coins/images/44/large/xrp-symbol-white-128.png",
    "USDC": "https://assets.coingecko.com/coins/images/6319/large/usdc.png",
    "SOL": "https://assets.coingecko.com/coins/images/4128/large/solana.png",
    "TRX": "https://assets.coingecko.com/coins/images/1094/large/tron-logo.png",
    "DOGE": "https://assets.coingecko.com/coins/images/5/large/dogecoin.png",
    "TON": "https://assets.coingecko.com/coins/images/17980/large/ton_symbol.png",
    "ADA": "https://assets.coingecko.com/coins/images/975/large/cardano.png",
    "BCH": "https://assets.coingecko.com/coins/images/780/large/bitcoin-cash-circle.png",
    "LINK": "https://assets.coingecko.com/coins/images/877/large/chainlink-new-logo.png",
    "AVAX": "https://assets.coingecko.com/coins/images/12559/large/Avalanche_Circle_RedWhite_Trans.png",
    "LTC": "https://assets.coingecko.com/coins/images/2/large/litecoin.png",
}

session = requests.Session()
session.headers.update({
    "User-Agent": "CryptoPulse/3.0",
    "Accept": "application/json",
})
if COINGECKO_API_KEY:
    session.headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

cache_lock = threading.RLock()

price_cache = {
    "bybit": {
        "prices": [], "updated_at": 0, "error": None,
        "retry_after": 0,
    },
    "market": {
        "prices": [], "updated_at": 0, "error": None,
        "retry_after": 0,
    },
}
usd_rub_cache = {"rate": None, "updated_at": 0}
price_history = {"bybit": {}, "CoinGecko": {}}

pending_calculations = set()
pending_lock = threading.Lock()


def utc_iso(timestamp=None):
    if not timestamp:
        return None
    return datetime.fromtimestamp(
        timestamp, timezone.utc
    ).isoformat()


def remember_prices(source, prices, observed_at):
    """Keep source-specific observations for hourly comparisons."""
    with cache_lock:
        history = price_history.setdefault(source, {})
        for coin in prices:
            symbol = coin["symbol"]
            value = float(coin["price"])
            points = history.setdefault(symbol, [])

            if points and observed_at <= points[-1][0]:
                continue

            points.append((observed_at, value))
            cutoff = observed_at - HISTORY_RETENTION
            history[symbol] = [
                point for point in points if point[0] >= cutoff
            ]


def hourly_change(source, symbol, current_price, now):
    """Compare with the closest stored observation around one hour ago."""
    with cache_lock:
        points = list(
            price_history.get(source, {}).get(symbol, [])
        )

    if not points:
        return None

    target = now - HISTORY_TARGET_AGE
    eligible = [
        point for point in points
        if now - HISTORY_MAX_AGE <= point[0] <= now - 45 * 60
    ]
    if not eligible:
        return None

    old_time, old_price = min(
        eligible, key=lambda point: abs(point[0] - target)
    )
    if old_price <= 0:
        return None

    return (current_price / old_price - 1) * 100


def enrich_prices(prices, source, observed_at):
    result = []
    for original in prices:
        coin = dict(original)
        coin["change1h"] = hourly_change(
            source, coin["symbol"], coin["price"], observed_at
        )
        coin["change1hAvailable"] = coin["change1h"] is not None
        coin["priceRub"] = None
        coin["historySource"] = source
        result.append(coin)
    return result


# ---------------------------------------------------------
# BYBIT — official exchange data when reachable
# ---------------------------------------------------------

def fetch_bybit_prices():
    errors = []

    for host in BYBIT_HOSTS:
        try:
            response = session.get(
                f"{host}/v5/market/tickers",
                params={"category": "spot"},
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code != 200:
                errors.append(
                    f"{host}: HTTP {response.status_code}: "
                    f"{response.text[:160].replace(chr(10), ' ')}"
                )
                continue

            payload = response.json()
            if payload.get("retCode") != 0:
                errors.append(
                    f"{host}: API code {payload.get('retCode')}: "
                    f"{payload.get('retMsg', 'unknown error')}"
                )
                continue

            rows = payload.get("result", {}).get("list", [])
            tickers = {
                row.get("symbol"): row for row in rows
                if row.get("symbol")
            }
            prices = []

            for coin in COINS:
                symbol = coin["symbol"]

                # USDT is the quote unit itself; this is a reference
                # value, not a USDTUSDT exchange pair.
                if symbol == "USDT":
                    prices.append({
                        "symbol": "USDT",
                        "name": "Tether",
                        "price": 1.0,
                        "change24h": 0.0,
                        "bid": None,
                        "ask": None,
                        "image": COIN_IMAGES.get("USDT", ""),
                        "source": "Reference",
                        "priceIsReference": True,
                    })
                    continue

                ticker = tickers.get(f"{symbol}USDT")
                if not ticker:
                    continue

                try:
                    price = float(ticker["lastPrice"])
                    change = float(ticker.get("price24hPcnt", 0)) * 100
                    bid = float(ticker.get("bid1Price", 0) or 0)
                    ask = float(ticker.get("ask1Price", 0) or 0)
                except (KeyError, TypeError, ValueError):
                    continue

                if price <= 0:
                    continue

                prices.append({
                    "symbol": symbol,
                    "name": coin["name"],
                    "price": price,
                    "change24h": change,
                    "bid": bid,
                    "ask": ask,
                    "image": COIN_IMAGES.get(symbol, ""),
                    "source": "Bybit",
                    "priceIsReference": False,
                })

            if prices:
                logger.info("Fetched %s Bybit/reference prices", len(prices))
                return prices

            errors.append(f"{host}: no matching spot pairs")

        except (requests.RequestException, ValueError, TypeError) as exc:
            errors.append(f"{host}: {type(exc).__name__}: {exc}")

    raise RuntimeError("Bybit unavailable. " + " | ".join(errors))


def get_bybit_prices(force_refresh=False):
    now = time.time()

    with cache_lock:
        entry = price_cache["bybit"]
        if entry["prices"] and now - entry["updated_at"] < CACHE_TTL:
            return list(entry["prices"]), entry["error"]
        if now < entry["retry_after"]:
            return list(entry["prices"]), entry["error"] or "Refresh cooldown"

    try:
        raw = fetch_bybit_prices()
        observed = time.time()
        remember_prices("bybit", raw, observed)
        prices = enrich_prices(raw, "bybit", observed)

        with cache_lock:
            price_cache["bybit"] = {
                "prices": prices,
                "updated_at": observed,
                "error": None,
                "retry_after": 0,
            }
        return prices, None

    except Exception as exc:
        message = str(exc)
        logger.error("Bybit refresh failed: %s", message)
        with cache_lock:
            entry = price_cache["bybit"]
            entry["error"] = message
            entry["retry_after"] = time.time() + FAILURE_COOLDOWN
            return list(entry["prices"]), message


# ---------------------------------------------------------
# COINGECKO — display fallback with Demo API key
# ---------------------------------------------------------

def fetch_market_prices():
    ids = ",".join(coin["id"] for coin in COINS)
    response = session.get(
        COINGECKO_URL,
        params={
            "ids": ids,
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        },
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code == 429:
        retry = response.headers.get("Retry-After", "")
        raise RuntimeError(
            "CoinGecko HTTP 429 rate limit. Retry-After="
            + (retry or "not provided")
        )

    response.raise_for_status()
    payload = response.json()
    prices = []

    for coin in COINS:
        row = payload.get(coin["id"], {})
        try:
            price = float(row["usd"])
            change = float(row.get("usd_24h_change") or 0)
        except (KeyError, TypeError, ValueError):
            continue

        if price <= 0:
            continue

        prices.append({
            "symbol": coin["symbol"],
            "name": coin["name"],
            "price": price,
            "change24h": change,
            "bid": None,
            "ask": None,
            "image": COIN_IMAGES.get(coin["symbol"], ""),
            "source": "CoinGecko",
            "priceIsReference": False,
        })

    if not prices:
        raise RuntimeError("CoinGecko returned no usable prices")

    return prices


def get_market_prices(force_refresh=False):
    now = time.time()

    with cache_lock:
        entry = price_cache["market"]
        if entry["prices"] and now - entry["updated_at"] < CACHE_TTL:
            return list(entry["prices"]), entry["error"]
        if now < entry["retry_after"]:
            return list(entry["prices"]), entry["error"] or "Refresh cooldown"

    try:
        raw = fetch_market_prices()
        observed = time.time()
        remember_prices("CoinGecko", raw, observed)
        prices = enrich_prices(raw, "CoinGecko", observed)

        with cache_lock:
            price_cache["market"] = {
                "prices": prices,
                "updated_at": observed,
                "error": None,
                "retry_after": 0,
            }
        return prices, None

    except Exception as exc:
        message = str(exc)
        logger.warning("CoinGecko refresh failed: %s", message)
        with cache_lock:
            entry = price_cache["market"]
            entry["error"] = message
            entry["retry_after"] = time.time() + FAILURE_COOLDOWN
            return list(entry["prices"]), message


# ---------------------------------------------------------
# USD/RUB reference exchange rate
# ---------------------------------------------------------

def fetch_usd_rub():
    sources = [
        "https://open.er-api.com/v6/latest/USD",
        "https://latest.currency-api.pages.dev/v1/currencies/usd.json",
    ]

    for url in sources:
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            rates = data.get("rates", {})
            if isinstance(rates, dict) and rates.get("RUB"):
                return float(rates["RUB"])

            usd = data.get("usd", {})
            if isinstance(usd, dict) and usd.get("rub"):
                return float(usd["rub"])
        except (requests.RequestException, ValueError, TypeError):
            logger.warning("USD/RUB source failed: %s", url)

    return None


def get_usd_rub():
    now = time.time()
    with cache_lock:
        rate = usd_rub_cache["rate"]
        updated = usd_rub_cache["updated_at"]

    if rate and now - updated < FX_CACHE_TTL:
        return rate

    new_rate = fetch_usd_rub()
    if new_rate and new_rate > 0:
        with cache_lock:
            usd_rub_cache["rate"] = new_rate
            usd_rub_cache["updated_at"] = time.time()
        return new_rate

    return rate


def add_ruble_prices(prices, currency, rate):
    result = []
    for original in prices:
        coin = dict(original)
        if rate and coin.get("price") is not None:
            # CoinGecko values are USD; Bybit spot pairs are quoted in USDT.
            # The conversion is indicative, not a RUB exchange-market quote.
            coin["priceRub"] = float(coin["price"]) * rate
        else:
            coin["priceRub"] = None
        coin["rubRate"] = rate
        coin["rubCurrencyBasis"] = currency
        result.append(coin)
    return result


def build_price_response(source, currency, prices, updated, error=None,
                         fallback_error=None, trading_data=False):
    rate = get_usd_rub()
    prices = add_ruble_prices(prices, currency, rate)
    now = time.time()

    return {
        "source": source,
        "currency": currency,
        "prices": prices,
        "updatedAt": utc_iso(updated),
        "serverTime": utc_iso(now),
        "usdRub": rate,
        "rubUpdatedAt": utc_iso(usd_rub_cache["updated_at"]),
        "cached": bool(error),
        "error": error,
        "fallbackError": fallback_error,
        "tradingData": trading_data,
        "hourlyHistory": "in-memory; starts collecting after deployment",
        "notice": (
            "Prices in RUB are indicative conversions using the reference "
            "USD/RUB rate. Fallback market data is not Bybit trading data."
        ),
    }


# ---------------------------------------------------------
# Simulation-only trading status
# ---------------------------------------------------------

def trading_status():
    prices, error = get_bybit_prices()
    return {
        "mode": TRADING_MODE,
        "liveTradingEnabled": LIVE_TRADING_ENABLED,
        "ordersEnabled": False,
        "exchange": "Bybit",
        "exchangeDataAvailable": bool(prices) and error is None,
        "priceCount": len(prices),
        "error": error,
        "message": "Simulation only. No real orders are sent.",
    }


def build_demo_signals():
    prices, error = get_bybit_prices()
    if not prices or error:
        return [], error or "Fresh Bybit data is unavailable"

    signals = []
    for coin in prices:
        change = coin["change24h"]
        signal = (
            "positive_momentum" if change >= 3 else
            "negative_momentum" if change <= -3 else "neutral"
        )
        signals.append({
            "symbol": coin["symbol"],
            "signal": signal,
            "change24h": change,
            "executable": False,
            "note": "Informational only; not a validated trading strategy.",
        })
    return signals, None


# ---------------------------------------------------------
# Telegram
# ---------------------------------------------------------

def telegram_request(method, data=None):
    if not TELEGRAM_API:
        logger.warning("Telegram token is missing in Render environment")
        return None
    try:
        response = session.post(
            f"{TELEGRAM_API}/{method}",
            json=data or {},
            timeout=REQUEST_TIMEOUT,
        )
        result = response.json()
        if not result.get("ok"):
            logger.warning(
                "Telegram %s error: %s",
                method, result.get("description", "unknown error")
            )
        return result
    except (requests.RequestException, ValueError):
        logger.exception("Telegram request failed: %s", method)
        return None


def main_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "🌐 Открыть CryptoPulse",
              "web_app": {"url": SITE_URL}}],
            [{"text": "📊 Крипторынок", "callback_data": "prices"},
             {"text": "💱 USD / RUB", "callback_data": "usd_rub"}],
            [{"text": "🧮 Калькулятор BTC", "callback_data": "calculator"},
             {"text": "🔄 Обновить", "callback_data": "refresh"}],
        ]
    }


def send_message(chat_id, text, keyboard=None):
    data = {"chat_id": chat_id, "text": text}
    if keyboard:
        data["reply_markup"] = keyboard
    return telegram_request("sendMessage", data)


def edit_message(chat_id, message_id, text, keyboard=None):
    data = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if keyboard:
        data["reply_markup"] = keyboard
    return telegram_request("editMessageText", data)


def answer_callback(callback_id, text=None):
    data = {"callback_query_id": callback_id}
    if text:
        data["text"] = text
    telegram_request("answerCallbackQuery", data)


def format_price(value, currency="USDT"):
    if value >= 1000:
        return f"{value:,.2f} {currency}"
    if value >= 1:
        return f"{value:,.4f} {currency}"
    return f"{value:.8f} {currency}"


def market_message():
    prices, error = get_bybit_prices()
    source = "Bybit"
    currency = "USDT"

    if not prices:
        prices, fallback_error = get_market_prices()
        source = "CoinGecko — справочные данные"
        currency = "USD"
        if not prices:
            return (
                "⚠️ Котировки временно недоступны.\n"
                f"Bybit: {error or 'недоступен'}\n"
                f"CoinGecko: {fallback_error or 'недоступен'}"
            )

    lines = [
        f"📊 CryptoPulse — {source}",
        f"Цены в {currency}; рублёвые значения — справочный пересчёт.",
        "Реальные сделки отключены.",
        "",
    ]
    for coin in prices:
        change = coin.get("change24h")
        change_text = (
            f"{change:+.2f}%" if change is not None else "нет данных"
        )
        lines.append(
            f"{coin['symbol']}: {format_price(coin['price'], currency)} "
            f"({change_text})"
        )

    return "\n".join(lines)


def usd_rub_message():
    rate = get_usd_rub()
    if not rate:
        return "⚠️ Справочный курс USD/RUB временно недоступен."
    return (
        "💱 Справочный курс USD / RUB\n\n"
        f"1 USD ≈ {rate:.2f} RUB\n\n"
        "Это не цена USDT на бирже."
    )


def handle_message(message):
    chat_id = (message.get("chat") or {}).get("id")
    text = (message.get("text") or "").strip()
    if not chat_id:
        return

    if text in ("/start", "/menu"):
        send_message(
            chat_id,
            "👋 Добро пожаловать в CryptoPulse!\n\n"
            "Открой Mini App или проверь котировки.\n"
            "Реальные сделки отключены.",
            main_keyboard(),
        )
        return
    if text == "/health":
        send_message(chat_id, "✅ CryptoPulse работает.")
        return
    if text == "/prices":
        send_message(chat_id, market_message(), main_keyboard())
        return
    if text == "/trading":
        status = trading_status()
        send_message(
            chat_id,
            "🛡 Статус торгового модуля\n\n"
            f"Режим: {status['mode']}\n"
            "Реальные ордера: отключены\n"
            f"Данные Bybit доступны: "
            f"{'да' if status['exchangeDataAvailable'] else 'нет'}\n"
            f"Пар получено: {status['priceCount']}\n\n"
            "Это не финансовая рекомендация.",
            main_keyboard(),
        )
        return

    with pending_lock:
        waiting = chat_id in pending_calculations

    if waiting:
        try:
            amount = float(text.replace(",", "."))
            if amount <= 0:
                raise ValueError("Amount must be positive")
            with pending_lock:
                pending_calculations.discard(chat_id)

            prices, error = get_bybit_prices()
            btc = next(
                (item for item in prices if item["symbol"] == "BTC"), None
            )
            if not btc or error:
                send_message(
                    chat_id,
                    "⚠️ Не удалось подтвердить свежую цену BTC на Bybit.",
                    main_keyboard(),
                )
                return
            send_message(
                chat_id,
                "🧮 Расчёт по цене BTC на Bybit\n\n"
                f"Сумма: {amount:,.2f} USDT\n"
                f"BTC: {amount / btc['price']:.8f}\n\n"
                "Расчёт без комиссий. Сделки не совершаются.",
                main_keyboard(),
            )
        except ValueError:
            send_message(
                chat_id, "Введи положительную сумму, например 100 или 25.50."
            )
        return

    send_message(chat_id, "Выбери действие в меню или отправь /start.",
                 main_keyboard())


def handle_callback(callback):
    callback_id = callback.get("id")
    data = callback.get("data", "")
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    answer_callback(callback_id)

    if not chat_id:
        return
    if data in ("prices", "refresh"):
        text = market_message()
    elif data == "usd_rub":
        text = usd_rub_message()
    elif data == "calculator":
        with pending_lock:
            pending_calculations.add(chat_id)
        send_message(
            chat_id,
            "🧮 Введи сумму в USDT, например 100.\n"
            "Расчёт BTC доступен только при подтверждённой цене Bybit.",
        )
        return
    else:
        text = "Выбери действие в меню."

    if message_id:
        result = edit_message(chat_id, message_id, text, main_keyboard())
        if not result or not result.get("ok"):
            description = (result or {}).get("description", "").lower()
            if "message is not modified" not in description:
                send_message(chat_id, text, main_keyboard())
    else:
        send_message(chat_id, text, main_keyboard())


# ---------------------------------------------------------
# Flask API
# ---------------------------------------------------------

@app.route("/")
def index():
    if os.path.isfile(os.path.join(STATIC_DIR, "index.html")):
        return send_from_directory(STATIC_DIR, "index.html")
    return "CryptoPulse is running, but static/index.html was not found.", 200


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "CryptoPulse",
        "time": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/config")
def api_config():
    return jsonify({
        "source": "Bybit",
        "currency": "USDT",
        "coins": [{"symbol": c["symbol"], "name": c["name"]} for c in COINS],
        "fallbackSource": "CoinGecko",
        "tradingMode": TRADING_MODE,
        "liveTradingEnabled": LIVE_TRADING_ENABLED,
    })


@app.route("/api/prices")
def api_prices():
    prices, error = get_bybit_prices()
    with cache_lock:
        updated = price_cache["bybit"]["updated_at"]

    if prices:
        return jsonify(build_price_response(
            "Bybit", "USDT", prices, updated,
            error=error, trading_data=not bool(error)
        ))

    fallback, fallback_error = get_market_prices()
    with cache_lock:
        fallback_updated = price_cache["market"]["updated_at"]

    return jsonify(build_price_response(
        "CoinGecko", "USD", fallback, fallback_updated,
        error=error,
        fallback_error=fallback_error,
        trading_data=False,
    ))


@app.route("/api/market-prices")
def api_market_prices():
    prices, error = get_market_prices()
    with cache_lock:
        updated = price_cache["market"]["updated_at"]
    return jsonify(build_price_response(
        "CoinGecko", "USD", prices, updated,
        error=error, trading_data=False
    ))


@app.route("/api/bybit-status")
def api_bybit_status():
    status = trading_status()
    return jsonify(status), (200 if status["exchangeDataAvailable"] else 503)


@app.route("/api/trading/status")
def api_trading_status():
    return jsonify(trading_status())


@app.route("/api/trading/signals")
def api_trading_signals():
    signals, error = build_demo_signals()
    return jsonify({
        "mode": "simulation",
        "executable": False,
        "signals": signals,
        "error": error,
        "notice": "Demo only; no orders are placed.",
    }), (200 if signals else 503)


@app.route("/api/usd-rub")
def api_usd_rub():
    rate = get_usd_rub()
    if not rate:
        return jsonify({
            "ok": False,
            "error": "Exchange rate temporarily unavailable",
        }), 503
    return jsonify({
        "ok": True,
        "source": "public exchange-rate API",
        "USD_RUB": rate,
        "updatedAt": utc_iso(usd_rub_cache["updated_at"]),
    })


@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True) or {}
    try:
        if "message" in update:
            handle_message(update["message"])
        elif "callback_query" in update:
            handle_callback(update["callback_query"])
    except Exception:
        logger.exception("Error processing Telegram update")
    return jsonify({"ok": True})


def configure_telegram():
    if not TELEGRAM_TOKEN:
        logger.warning("Telegram token missing. Set TELEGRAM_TOKEN in Render.")
        return

    hostname = os.getenv("RENDER_EXTERNAL_HOSTNAME")
    if hostname:
        result = telegram_request(
            "setWebhook",
            {"url": f"https://{hostname}/telegram-webhook"},
        )
        if result and result.get("ok"):
            logger.info("Telegram webhook configured")
        else:
            logger.error("Could not configure Telegram webhook")
    else:
        logger.warning("Render hostname unavailable; webhook not configured")

    result = telegram_request(
        "setChatMenuButton",
        {"menu_button": {
            "type": "web_app",
            "text": "CryptoPulse",
            "web_app": {"url": SITE_URL},
        }},
    )
    if result and result.get("ok"):
        logger.info("Telegram Mini App menu button configured")
    else:
        logger.warning("Could not configure Telegram Mini App menu button")


configure_telegram()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
