import os
import time
import logging
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, send_from_directory, request

# ==================================================
# CONFIG
# ==================================================

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

# Official Bybit API hosts
BYBIT_HOSTS = [
    "https://api.bybit.com",
    "https://api.bytick.com",
]

REQUEST_TIMEOUT = 10
CACHE_TTL = 30

SITE_URL = "https://cryptopulse-lpc9.onrender.com"

COINS = [
    {"symbol": "BTC", "name": "Bitcoin"},
    {"symbol": "ETH", "name": "Ethereum"},
    {"symbol": "BNB", "name": "BNB"},
    {"symbol": "SOL", "name": "Solana"},
    {"symbol": "XRP", "name": "XRP"},
    {"symbol": "ADA", "name": "Cardano"},
    {"symbol": "DOGE", "name": "Dogecoin"},
    {"symbol": "TRX", "name": "TRON"},
    {"symbol": "TON", "name": "Toncoin"},
    {"symbol": "LINK", "name": "Chainlink"},
    {"symbol": "AVAX", "name": "Avalanche"},
    {"symbol": "DOT", "name": "Polkadot"},
    {"symbol": "LTC", "name": "Litecoin"},
    {"symbol": "BCH", "name": "Bitcoin Cash"},
    {"symbol": "SUI", "name": "Sui"},
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
session.headers.update({"User-Agent": "CryptoPulse/1.0"})

cache_lock = threading.Lock()

price_cache = {
    "prices": [],
    "updated_at": 0,
    "error": None,
}

usd_rub_cache = {
    "rate": None,
    "updated_at": 0,
}

pending_calculations = set()
pending_lock = threading.Lock()


# ==================================================
# BYBIT MARKET DATA
# ==================================================

def fetch_bybit_prices():
    errors = []

    for host in BYBIT_HOSTS:
        url = f"{host}/v5/market/tickers"

        try:
            response = session.get(
                url,
                params={"category": "spot"},
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 403:
                errors.append(f"{host}: HTTP 403 Forbidden")
                continue

            response.raise_for_status()
            payload = response.json()

            if payload.get("retCode") != 0:
                errors.append(
                    f"{host}: {payload.get('retMsg', 'Bybit API error')}"
                )
                continue

            rows = payload.get("result", {}).get("list", [])

            if not rows:
                errors.append(f"{host}: empty ticker list")
                continue

            tickers = {
                row.get("symbol"): row
                for row in rows
                if row.get("symbol")
            }

            prices = []

            for coin in COINS:
                symbol = coin["symbol"]
                ticker = tickers.get(f"{symbol}USDT")

                if not ticker:
                    continue

                try:
                    price = float(ticker["lastPrice"])
                    change = float(ticker.get("price24hPcnt", 0)) * 100
                except (KeyError, TypeError, ValueError):
                    continue

                if price <= 0:
                    continue

                prices.append({
                    "symbol": symbol,
                    "name": coin["name"],
                    "price": price,
                    "change24h": change,
                    "image": COIN_IMAGES.get(symbol, ""),
                })

            if not prices:
                errors.append(f"{host}: no matching USDT spot pairs")
                continue

            logger.info(
                "Successfully fetched %s Bybit spot prices from %s",
                len(prices),
                host,
            )

            return prices

        except Exception as exc:
            errors.append(f"{host}: {exc}")
            logger.warning("Bybit request failed: %s", host)

    raise RuntimeError("Bybit unavailable. " + " | ".join(errors))


def get_prices(force_refresh=False):
    now = time.time()

    with cache_lock:
        cached_prices = list(price_cache["prices"])
        cached_at = price_cache["updated_at"]

    if (
        not force_refresh
        and cached_prices
        and now - cached_at < CACHE_TTL
    ):
        return cached_prices, None

    try:
        prices = fetch_bybit_prices()

        with cache_lock:
            price_cache["prices"] = prices
            price_cache["updated_at"] = time.time()
            price_cache["error"] = None

        return prices, None

    except Exception as exc:
        logger.exception("Unable to refresh Bybit prices")

        with cache_lock:
            cached_prices = list(price_cache["prices"])
            price_cache["error"] = str(exc)

        if cached_prices:
            return cached_prices, str(exc)

        return [], str(exc)


# ==================================================
# USD / RUB
# ==================================================

def fetch_usd_rub():
    urls = [
        "https://open.er-api.com/v6/latest/USD",
        "https://latest.currency-api.pages.dev/v1/currencies/usd.json",
    ]

    for url in urls:
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

        except Exception:
            logger.warning("USD/RUB source failed: %s", url)

    return None


def get_usd_rub():
    now = time.time()

    with cache_lock:
        rate = usd_rub_cache["rate"]
        updated = usd_rub_cache["updated_at"]

    if rate and now - updated < 900:
        return rate

    new_rate = fetch_usd_rub()

    if new_rate and new_rate > 0:
        with cache_lock:
            usd_rub_cache["rate"] = new_rate
            usd_rub_cache["updated_at"] = time.time()
        return new_rate

    return rate


# ==================================================
# TELEGRAM
# ==================================================

def telegram_request(method, data=None):
    if not TELEGRAM_API:
        logger.error("Telegram token is missing in Render environment")
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

    except Exception:
        logger.exception("Telegram request failed: %s", method)
        return None


def main_keyboard():
    return {
        "inline_keyboard": [
            [
                {
                    "text": "🌐 Открыть CryptoPulse",
                    "url": SITE_URL,
                }
            ],
            [
                {
                    "text": "📊 Крипторынок",
                    "callback_data": "prices",
                },
                {
                    "text": "💱 USD / RUB",
                    "callback_data": "usd_rub",
                },
            ],
            [
                {
                    "text": "🧮 Калькулятор BTC",
                    "callback_data": "calculator",
                },
                {
                    "text": "🔄 Обновить",
                    "callback_data": "refresh",
                },
            ],
        ]
    }


def send_message(chat_id, text, keyboard=None):
    data = {
        "chat_id": chat_id,
        "text": text,
    }

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


def format_price(value):
    if value >= 1000:
        return f"${value:,.2f}"
    if value >= 1:
        return f"${value:,.4f}"
    return f"${value:.8f}"


def market_message():
    prices, error = get_prices()

    if not prices:
        return (
            "⚠️ Не удалось получить котировки непосредственно с Bybit.\n\n"
            "Сервер временно не имеет доступа к API. "
            "Попробуй позже."
        )

    lines = [
        "📊 CryptoPulse — Bybit Spot",
        "",
        "Цены торговых пар к USDT:",
        "",
    ]

    for coin in prices:
        change = coin["change24h"]
        sign = "+" if change >= 0 else ""

        lines.append(
            f"{coin['symbol']}: {format_price(coin['price'])} "
            f"({sign}{change:.2f}%)"
        )

    updated = datetime.fromtimestamp(
        price_cache["updated_at"],
        timezone.utc,
    ).strftime("%H:%M:%S UTC")

    lines.extend(["", f"Время обновления: {updated}"])

    if error:
        lines.append(
            "\n⚠️ API недоступен; отображаются последние сохранённые цены."
        )

    return "\n".join(lines)


def usd_rub_message():
    rate = get_usd_rub()

    if not rate:
        return "⚠️ Сейчас не удалось получить справочный курс USD/RUB."

    return (
        "💱 Справочный курс USD / RUB\n\n"
        f"1 USD ≈ {rate:.2f} RUB\n\n"
        "Курс справочный и может отличаться от биржевого."
    )


# ==================================================
# TELEGRAM UPDATE HANDLERS
# ==================================================

def handle_message(message):
    chat_id = (message.get("chat") or {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id:
        return

    if text in ("/start", "/menu"):
        send_message(
            chat_id,
            "👋 Добро пожаловать в CryptoPulse!\n\n"
            "Здесь можно посмотреть котировки Bybit и открыть сайт.",
            main_keyboard(),
        )
        return

    if text == "/health":
        send_message(chat_id, "✅ CryptoPulse работает.")
        return

    if text == "/prices":
        send_message(chat_id, market_message(), main_keyboard())
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

            prices, _ = get_prices()
            btc = next(
                (item for item in prices if item["symbol"] == "BTC"),
                None,
            )

            if not btc:
                send_message(
                    chat_id,
                    "⚠️ Цена BTC с Bybit сейчас недоступна.",
                    main_keyboard(),
                )
                return

            send_message(
                chat_id,
                "🧮 Расчёт по текущей цене BTC на Bybit\n\n"
                f"Сумма: ${amount:,.2f}\n"
                f"BTC: {amount / btc['price']:.8f}\n\n"
                "Расчёт ориентировочный, без комиссий.",
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
            "🧮 Введи сумму в долларах США, например 100.\n"
            "Я рассчитаю примерное количество BTC по цене Bybit.",
        )
        return

    else:
        text = "Выбери действие в меню."

    if message_id:
        result = edit_message(
            chat_id,
            message_id,
            text,
            main_keyboard(),
        )

        if not result or not result.get("ok"):
            description = (result or {}).get("description", "").lower()

            if "message is not modified" not in description:
                send_message(chat_id, text, main_keyboard())
    else:
        send_message(chat_id, text, main_keyboard())


# ==================================================
# FLASK ROUTES
# ==================================================

@app.route("/")
def index():
    index_file = os.path.join(STATIC_DIR, "index.html")

    if os.path.isfile(index_file):
        return send_from_directory(STATIC_DIR, "index.html")

    return (
        "CryptoPulse is running, but static/index.html was not found.",
        200,
    )


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
        "currency": "USD",
        "coins": COINS,
    })


@app.route("/api/prices")
def api_prices():
    prices, error = get_prices()

    with cache_lock:
        updated_at = price_cache["updated_at"]

    return jsonify({
        "source": "Bybit",
        "currency": "USD",
        "prices": prices,
        "updatedAt": (
            datetime.fromtimestamp(
                updated_at,
                timezone.utc,
            ).isoformat()
            if updated_at else None
        ),
        "cached": bool(error and prices),
        "error": error if not prices else None,
    })


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


# ==================================================
# WEBHOOK SETUP
# ==================================================

def configure_telegram():
    if not TELEGRAM_TOKEN:
        logger.warning(
            "Telegram token missing. Set TELEGRAM_TOKEN in Render environment."
        )
        return

    hostname = os.getenv("RENDER_EXTERNAL_HOSTNAME")

    if not hostname:
        logger.warning("Render hostname unavailable; webhook not configured.")
        return

    webhook_url = f"https://{hostname}/telegram-webhook"

    result = telegram_request(
        "setWebhook",
        {"url": webhook_url},
    )

    if result and result.get("ok"):
        logger.info("Telegram webhook configured successfully")


configure_telegram()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
