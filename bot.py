import os
import time
import logging
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, send_from_directory, request

# =========================================================
# CONFIGURATION
# =========================================================

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
    "SITE_URL",
    "https://cryptopulse-lpc9.onrender.com",
)

REQUEST_TIMEOUT = 12
CACHE_TTL = 30
FX_CACHE_TTL = 900

# Real trading is deliberately not implemented in this version.
TRADING_MODE = "simulation"
LIVE_TRADING_ENABLED = False

BYBIT_HOSTS = [
    "https://api.bybit.eu",
]

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

COINS = [
    {"symbol": "BTC", "name": "Bitcoin", "id": "bitcoin"},
    {"symbol": "ETH", "name": "Ethereum", "id": "ethereum"},
    {"symbol": "BNB", "name": "BNB", "id": "binancecoin"},
    {"symbol": "SOL", "name": "Solana", "id": "solana"},
    {"symbol": "XRP", "name": "XRP", "id": "ripple"},
    {"symbol": "ADA", "name": "Cardano", "id": "cardano"},
    {"symbol": "DOGE", "name": "Dogecoin", "id": "dogecoin"},
    {"symbol": "TRX", "name": "TRON", "id": "tron"},
    {"symbol": "TON", "name": "Toncoin", "id": "the-open-network"},
    {"symbol": "LINK", "name": "Chainlink", "id": "chainlink"},
    {"symbol": "AVAX", "name": "Avalanche", "id": "avalanche-2"},
    {"symbol": "DOT", "name": "Polkadot", "id": "polkadot"},
    {"symbol": "LTC", "name": "Litecoin", "id": "litecoin"},
    {"symbol": "BCH", "name": "Bitcoin Cash", "id": "bitcoin-cash"},
    {"symbol": "SUI", "name": "Sui", "id": "sui"},
]

COIN_IMAGES = {
    "BTC": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png",
    "ETH": "https://assets.coingecko.com/coins/images/279/large/ethereum.png",
    "BNB": "https://assets.coingecko.com/coins/images/825/large/bnb-icon2_2x.png",
    "SOL": "https://assets.coingecko.com/coins/images/4128/large/solana.png",
    "XRP": "https://assets.coingecko.com/coins/images/44/large/xrp-symbol-white-128.png",
    "ADA": "https://assets.coingecko.com/coins/images/975/large/cardano.png",
    "DOGE": "https://assets.coingecko.com/coins/images/5/large/dogecoin.png",
    "TRX": "https://assets.coingecko.com/coins/images/1094/large/tron-logo.png",
    "TON": "https://assets.coingecko.com/coins/images/17980/large/ton_symbol.png",
    "LINK": "https://assets.coingecko.com/coins/images/877/large/chainlink-new-logo.png",
    "AVAX": "https://assets.coingecko.com/coins/images/12559/large/Avalanche_Circle_RedWhite_Trans.png",
    "DOT": "https://assets.coingecko.com/coins/images/12171/large/polkadot.png",
    "LTC": "https://assets.coingecko.com/coins/images/2/large/litecoin.png",
    "BCH": "https://assets.coingecko.com/coins/images/780/large/bitcoin-cash-circle.png",
    "SUI": "https://assets.coingecko.com/coins/images/26375/large/sui_asset.jpeg",
}

session = requests.Session()
session.headers.update({
    "User-Agent": "CryptoPulse/2.0",
    "Accept": "application/json",
})

cache_lock = threading.Lock()

price_cache = {
    "bybit": {"prices": [], "updated_at": 0, "error": None},
    "market": {"prices": [], "updated_at": 0, "error": None},
}

usd_rub_cache = {"rate": None, "updated_at": 0}

pending_calculations = set()
pending_lock = threading.Lock()


def utc_iso(timestamp=None):
    if timestamp is None:
        return None
    return datetime.fromtimestamp(
        timestamp, timezone.utc
    ).isoformat()


# =========================================================
# BYBIT: OFFICIAL EXCHANGE DATA
# =========================================================

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
                    f"{response.text[:180].replace(chr(10), ' ')}"
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
                row.get("symbol"): row
                for row in rows
                if row.get("symbol")
            }

            prices = []

            for coin in COINS:
                ticker = tickers.get(f"{coin['symbol']}USDT")
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
                    "symbol": coin["symbol"],
                    "name": coin["name"],
                    "price": price,
                    "change24h": change,
                    "bid": bid,
                    "ask": ask,
                    "image": COIN_IMAGES.get(coin["symbol"], ""),
                    "source": "Bybit",
                })

            if prices:
                logger.info("Fetched %s Bybit spot pairs", len(prices))
                return prices

            errors.append(f"{host}: no matching USDT spot pairs")

        except (requests.RequestException, ValueError, TypeError) as exc:
            errors.append(f"{host}: {type(exc).__name__}: {exc}")

    raise RuntimeError("Bybit unavailable. " + " | ".join(errors))


def get_bybit_prices(force_refresh=False):
    now = time.time()

    with cache_lock:
        cached = list(price_cache["bybit"]["prices"])
        updated = price_cache["bybit"]["updated_at"]

    if cached and not force_refresh and now - updated < CACHE_TTL:
        return cached, None

    try:
        prices = fetch_bybit_prices()

        with cache_lock:
            price_cache["bybit"] = {
                "prices": prices,
                "updated_at": time.time(),
                "error": None,
            }

        return prices, None

    except Exception as exc:
        message = str(exc)
        logger.error("Bybit refresh failed: %s", message)

        with cache_lock:
            cached = list(price_cache["bybit"]["prices"])
            price_cache["bybit"]["error"] = message

        if cached:
            return cached, message

        return [], message


# =========================================================
# COINGECKO: DISPLAY-ONLY FALLBACK
# =========================================================

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
    response.raise_for_status()
    payload = response.json()

    prices = []

    for coin in COINS:
        row = payload.get(coin["id"], {})
        price = row.get("usd")

        if price is None:
            continue

        try:
            price = float(price)
            change = float(row.get("usd_24h_change") or 0)
        except (ValueError, TypeError):
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
        })

    if not prices:
        raise RuntimeError("CoinGecko returned no usable prices")

    return prices


def get_market_prices(force_refresh=False):
    now = time.time()

    with cache_lock:
        cached = list(price_cache["market"]["prices"])
        updated = price_cache["market"]["updated_at"]

    if cached and not force_refresh and now - updated < CACHE_TTL:
        return cached, None

    try:
        prices = fetch_market_prices()

        with cache_lock:
            price_cache["market"] = {
                "prices": prices,
                "updated_at": time.time(),
                "error": None,
            }

        return prices, None

    except Exception as exc:
        message = str(exc)
        logger.warning("CoinGecko refresh failed: %s", message)

        with cache_lock:
            cached = list(price_cache["market"]["prices"])
            price_cache["market"]["error"] = message

        if cached:
            return cached, message

        return [], message


# =========================================================
# USD / RUB REFERENCE RATE
# =========================================================

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


# =========================================================
# SAFE SIMULATION STATUS AND SIGNALS
# =========================================================

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
        "message": (
            "Simulation only. No real orders are sent."
            if not LIVE_TRADING_ENABLED
            else "Live trading flag is enabled."
        ),
    }


def build_demo_signals():
    # These are informational demonstrations, not trade instructions.
    # 24-hour change alone is not a sufficient trading strategy.
    prices, error = get_bybit_prices()

    if not prices or error:
        return [], error or "Fresh Bybit data is unavailable"

    signals = []

    for coin in prices:
        change = coin["change24h"]

        if change >= 3:
            signal = "positive_momentum"
        elif change <= -3:
            signal = "negative_momentum"
        else:
            signal = "neutral"

        signals.append({
            "symbol": coin["symbol"],
            "signal": signal,
            "change24h": change,
            "executable": False,
            "note": "Informational only; not a validated trading strategy.",
        })

    return signals, None


# =========================================================
# TELEGRAM API
# =========================================================

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
                method,
                result.get("description", "unknown error"),
            )

        return result

    except (requests.RequestException, ValueError):
        logger.exception("Telegram request failed: %s", method)
        return None


def main_keyboard():
    return {
        "inline_keyboard": [
            [{
                "text": "🌐 Открыть CryptoPulse",
                "web_app": {"url": SITE_URL},
            }],
            [
                {"text": "📊 Крипторынок", "callback_data": "prices"},
                {"text": "💱 USD / RUB", "callback_data": "usd_rub"},
            ],
            [
                {"text": "🧮 Калькулятор BTC", "callback_data": "calculator"},
                {"text": "🔄 Обновить", "callback_data": "refresh"},
            ],
        ]
    }


def send_message(chat_id, text, keyboard=None):
    data = {"chat_id": chat_id, "text": text}
    if keyboard:
        data["reply_markup"] = keyboard
    return telegram_request("sendMessage", data)


def edit_message(chat_id, message_id, text, keyboard=None):
    data = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }
    if keyboard:
        data["reply_markup"] = keyboard
    return telegram_request("editMessageText", data)


def answer_callback(callback_id, text=None):
    data = {"callback_query_id": callback_id}
    if text:
        data["text"] = text
    telegram_request("answerCallbackQuery", data)


# =========================================================
# TELEGRAM FORMATTING
# =========================================================

def format_price(value, currency="USDT"):
    if value >= 1000:
        return f"{value:,.2f} {currency}"
    if value >= 1:
        return f"{value:,.4f} {currency}"
    return f"{value:.8f} {currency}"


def market_message():
    prices, error = get_bybit_prices()

    if not prices:
        fallback, fallback_error = get_market_prices()

        if not fallback:
            return (
                "⚠️ Котировки временно недоступны.\n"
                "Bybit: данные не получены.\n"
                f"Резервный источник: {fallback_error or 'недоступен'}"
            )

        lines = [
            "📊 CryptoPulse — резервные рыночные котировки",
            "Источник: CoinGecko; цены в USD, не в USDT.",
            "Эти данные не используются для торговых ордеров.",
            "",
        ]

        for coin in fallback:
            sign = "+" if coin["change24h"] >= 0 else ""
            lines.append(
                f"{coin['symbol']}: {format_price(coin['price'], 'USD')} "
                f"({sign}{coin['change24h']:.2f}%)"
            )

        return "\n".join(lines)

    lines = [
        "📊 CryptoPulse — Bybit Spot",
        "Пары к USDT; данные биржи:",
        "",
    ]

    for coin in prices:
        change = coin["change24h"]
        sign = "+" if change >= 0 else ""
        lines.append(
            f"{coin['symbol']}: {format_price(coin['price'])} "
            f"({sign}{change:.2f}%)"
        )

    with cache_lock:
        updated = price_cache["bybit"]["updated_at"]

    if updated:
        lines.extend([
            "",
            "Обновлено: " + datetime.fromtimestamp(
                updated, timezone.utc
            ).strftime("%H:%M:%S UTC"),
        ])

    if error:
        lines.extend([
            "",
            "⚠️ API не обновился. Показаны последние сохранённые данные.",
        ])

    return "\n".join(lines)


def usd_rub_message():
    rate = get_usd_rub()

    if not rate:
        return "⚠️ Справочный курс USD/RUB временно недоступен."

    return (
        "💱 Справочный курс USD / RUB\n\n"
        f"1 USD ≈ {rate:.2f} RUB\n\n"
        "Это справочный валютный курс, не цена USDT на бирже."
    )


# =========================================================
# TELEGRAM UPDATE HANDLERS
# =========================================================

def handle_message(message):
    chat_id = (message.get("chat") or {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id:
        return

    if text in ("/start", "/menu"):
        send_message(
            chat_id,
            "👋 Добро пожаловать в CryptoPulse!\n\n"
            "Открой Mini App или проверь рыночные котировки.\n"
            "Реальные сделки в этой версии отключены.",
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
                (item for item in prices if item["symbol"] == "BTC"),
                None,
            )

            if not btc or error:
                send_message(
                    chat_id,
                    "⚠️ Не удалось подтвердить свежую цену BTC на Bybit. "
                    "Попробуй позже.",
                    main_keyboard(),
                )
                return

            send_message(
                chat_id,
                "🧮 Расчёт по подтверждённой цене BTC на Bybit\n\n"
                f"Сумма: {amount:,.2f} USDT\n"
                f"BTC: {amount / btc['price']:.8f}\n\n"
                "Ориентировочный расчёт без комиссий. "
                "Сделки не совершаются.",
                main_keyboard(),
            )

        except ValueError:
            send_message(
                chat_id,
                "Введи положительную сумму, например 100 или 25.50.",
            )
        return

    send_message(
        chat_id,
        "Выбери действие в меню или отправь /start.",
        main_keyboard(),
    )


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
            "Я рассчитаю BTC по свежей цене Bybit, если она доступна.",
        )
        return

    else:
        text = "Выбери действие в меню."

    if message_id:
        result = edit_message(
            chat_id, message_id, text, main_keyboard()
        )

        if not result or not result.get("ok"):
            description = (result or {}).get(
                "description", ""
            ).lower()

            if "message is not modified" not in description:
                send_message(chat_id, text, main_keyboard())
    else:
        send_message(chat_id, text, main_keyboard())


# =========================================================
# FLASK ROUTES
# =========================================================

@app.route("/")
def index():
    index_file = os.path.join(STATIC_DIR, "index.html")

    if os.path.isfile(index_file):
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
        "coins": [
            {"symbol": c["symbol"], "name": c["name"]}
            for c in COINS
        ],
        "fallbackSource": "CoinGecko",
        "tradingMode": TRADING_MODE,
        "liveTradingEnabled": LIVE_TRADING_ENABLED,
    })


@app.route("/api/prices")
def api_prices():
    # Bybit remains the primary source for the existing site contract.
    prices, error = get_bybit_prices()

    with cache_lock:
        updated = price_cache["bybit"]["updated_at"]

    if prices:
        return jsonify({
            "source": "Bybit",
            "currency": "USDT",
            "prices": prices,
            "updatedAt": utc_iso(updated),
            "cached": bool(error),
            "error": error,
            "tradingData": not bool(error),
        })

    # Display-only fallback; never used by the trading module.
    fallback, fallback_error = get_market_prices()

    with cache_lock:
        fallback_updated = price_cache["market"]["updated_at"]

    return jsonify({
        "source": "CoinGecko",
        "currency": "USD",
        "prices": fallback,
        "updatedAt": utc_iso(fallback_updated),
        "cached": bool(fallback and fallback_error),
        "error": error,
        "fallbackError": fallback_error,
        "tradingData": False,
        "notice": (
            "Fallback prices are indicative USD market prices. "
            "They are not Bybit USDT prices and cannot authorize trades."
        ),
    })


@app.route("/api/market-prices")
def api_market_prices():
    prices, error = get_market_prices()

    with cache_lock:
        updated = price_cache["market"]["updated_at"]

    return jsonify({
        "source": "CoinGecko",
        "currency": "USD",
        "prices": prices,
        "updatedAt": utc_iso(updated),
        "error": error,
        "tradingData": False,
    })


@app.route("/api/bybit-status")
def api_bybit_status():
    status = trading_status()
    return jsonify(status), (200 if status["exchangeDataAvailable"] else 503)


@app.route("/api/trading/status")
def api_trading_status():
    status = trading_status()
    return jsonify(status)


@app.route("/api/trading/signals")
def api_trading_signals():
    signals, error = build_demo_signals()

    return jsonify({
        "mode": "simulation",
        "executable": False,
        "signals": signals,
        "error": error,
        "notice": (
            "Demonstration signals only. No orders are placed. "
            "A 24-hour price change alone is not a validated strategy."
        ),
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


# =========================================================
# TELEGRAM WEBHOOK AND MINI APP MENU
# =========================================================

def configure_telegram():
    if not TELEGRAM_TOKEN:
        logger.warning(
            "Telegram token missing. Set TELEGRAM_TOKEN in Render."
        )
        return

    hostname = os.getenv("RENDER_EXTERNAL_HOSTNAME")

    if hostname:
        webhook_url = f"https://{hostname}/telegram-webhook"
        result = telegram_request("setWebhook", {"url": webhook_url})

        if result and result.get("ok"):
            logger.info("Telegram webhook configured")
        else:
            logger.error("Could not configure Telegram webhook")
    else:
        logger.warning("Render hostname unavailable; webhook not configured")

    menu_result = telegram_request(
        "setChatMenuButton",
        {
            "menu_button": {
                "type": "web_app",
                "text": "CryptoPulse",
                "web_app": {"url": SITE_URL},
            }
        },
    )

    if menu_result and menu_result.get("ok"):
        logger.info("Telegram Mini App menu button configured")
    else:
        logger.warning("Could not configure Telegram Mini App menu button")


configure_telegram()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
