import os
import logging
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request, send_from_directory

# Render start command: gunicorn bot:app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("cryptopulse")

DEFAULT_URL = "https://cryptopulse-lpc9.onrender.com"
TIMEOUT = 12


def get_webapp_url():
    value = (os.getenv("WEBAPP_URL") or "").strip().strip("\"'").rstrip("/")
    parsed = urlparse(value)

    if parsed.scheme == "https" and parsed.netloc:
        return value

    log.warning("WEBAPP_URL is empty or invalid; using default site URL.")
    return DEFAULT_URL


WEBAPP_URL = get_webapp_url()

# Поддерживаем разные названия переменной токена.
TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or ""
).strip()

app = Flask(__name__)


# ---------- Telegram API ----------

def telegram(method, payload=None):
    if not TOKEN:
        log.error("Telegram token is missing in Render Environment.")
        return None

    url = f"https://api.telegram.org/bot{TOKEN}/{method}"

    try:
        response = requests.post(
            url,
            json=payload or {},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        if not data.get("ok"):
            log.error("Telegram API failed for method %s.", method)
            return None

        return data.get("result")

    except (requests.RequestException, ValueError):
        log.exception("Telegram API request failed for method %s.", method)
        return None


def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text}

    if reply_markup:
        payload["reply_markup"] = reply_markup

    return telegram("sendMessage", payload)


def make_keyboard():
    return {
        "inline_keyboard": [
            [
                {
                    "text": "📈 Открыть CryptoPulse",
                    "web_app": {"url": WEBAPP_URL},
                }
            ],
            [
                {"text": "₿ BTC", "callback_data": "price:BTC"},
                {"text": "Ξ ETH", "callback_data": "price:ETH"},
            ],
            [
                {"text": "◎ SOL", "callback_data": "price:SOL"},
                {"text": "💎 TON", "callback_data": "price:TON"},
            ],
            [
                {"text": "📊 Все цены", "callback_data": "prices:all"}
            ],
        ]
    }


# ---------- Cryptocurrency prices ----------

COINS = {
    "BTC": {
        "coingecko": "bitcoin",
        "paprika": "btc-bitcoin",
    },
    "ETH": {
        "coingecko": "ethereum",
        "paprika": "eth-ethereum",
    },
    "SOL": {
        "coingecko": "solana",
        "paprika": "sol-solana",
    },
    "TON": {
        "coingecko": "the-open-network",
        "paprika": "ton-toncoin",
    },
}


def get_prices():
    """Try CoinGecko first, then use CoinPaprika as a fallback."""

    # Source 1: CoinGecko, one request for all four coins.
    try:
        ids = ",".join(
            coin["coingecko"] for coin in COINS.values()
        )

        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": ids,
                "vs_currencies": "usd",
                "include_24hr_change": "true",
            },
            headers={"User-Agent": "CryptoPulse/1.0"},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        prices = {}
        for symbol, coin in COINS.items():
            item = data.get(coin["coingecko"], {})
            prices[symbol] = {
                "usd": item.get("usd"),
                "change_24h": item.get("usd_24h_change"),
            }

        if all(
            prices[symbol]["usd"] is not None
            for symbol in COINS
        ):
            return prices

        log.warning("CoinGecko returned incomplete prices.")

    except (requests.RequestException, ValueError):
        log.warning("CoinGecko unavailable; trying CoinPaprika.")

    # Source 2: CoinPaprika, one request per coin.
    prices = {}

    for symbol, coin in COINS.items():
        prices[symbol] = {
            "usd": None,
            "change_24h": None,
        }

        try:
            response = requests.get(
                "https://api.coinpaprika.com/v1/tickers/"
                + coin["paprika"],
                params={"quotes": "USD"},
                headers={"User-Agent": "CryptoPulse/1.0"},
                timeout=TIMEOUT,
            )
            response.raise_for_status()
            item = response.json().get("quotes", {}).get("USD", {})

            prices[symbol] = {
                "usd": item.get("price"),
                "change_24h": item.get("percent_change_24h"),
            }

        except (requests.RequestException, ValueError):
            log.warning("CoinPaprika unavailable for %s.", symbol)

    return prices


def format_price(symbol, item):
    price = item.get("usd")
    change = item.get("change_24h")

    if price is None:
        return f"{symbol}: цена временно недоступна."

    if isinstance(change, (int, float)):
        change_text = f"{change:+.2f}%"
    else:
        change_text = "нет данных"

    return (
        f"{symbol}: ${price:,.4f}"
        f" | изменение за 24 ч: {change_text}"
    )


def prices_text(symbol=None):
    prices = get_prices()

    if symbol:
        return format_price(symbol, prices.get(symbol, {}))

    lines = ["📊 CryptoPulse — цены в USD", ""]

    for ticker in ("BTC", "ETH", "SOL", "TON"):
        lines.append(format_price(ticker, prices.get(ticker, {})))

    return "\n".join(lines)


# ---------- Website routes ----------

@app.get("/")
def index():
    # Serve the existing website, not a plain text response.
    return send_from_directory("static", "index.html")


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "cryptopulse"}), 200


@app.get("/api/config")
def api_config():
    # Return only the public URL, never the bot token.
    return jsonify({"webapp_url": WEBAPP_URL}), 200


@app.get("/api/prices")
def api_prices():
    prices = get_prices()
    return jsonify({"ok": True, "prices": prices}), 200


# ---------- Telegram webhook ----------

@app.route("/telegram-webhook", methods=["GET", "POST"])
def telegram_webhook():
    if request.method == "GET":
        return jsonify({
            "ok": True,
            "message": "Telegram webhook endpoint is available.",
        }), 200

    update = request.get_json(silent=True) or {}

    try:
        message = update.get("message") or update.get("edited_message")

        if message:
            chat_id = (message.get("chat") or {}).get("id")
            text = (message.get("text") or "").strip()

            if chat_id is not None:
                command = text.split()[0].split("@")[0].lower() if text else ""

                if command in ("/start", "/help"):
                    send_message(
                        chat_id,
                        "Привет! 👋 Добро пожаловать в CryptoPulse!\n\n"
                        "Здесь ты можешь смотреть цены криптовалют "
                        "и открыть наш сайт.\n\n"
                        "Команды:\n"
                        "/start — главное меню\n"
                        "/prices — все цены\n"
                        "/btc — Bitcoin\n"
                        "/eth — Ethereum\n"
                        "/sol — Solana\n"
                        "/ton — Toncoin\n"
                        "/help — помощь",
                        make_keyboard(),
                    )

                elif command == "/prices":
                    send_message(
                        chat_id,
                        prices_text(),
                        make_keyboard(),
                    )

                elif command in ("/btc", "/eth", "/sol", "/ton"):
                    symbol = command[1:].upper()
                    send_message(
                        chat_id,
                        prices_text(symbol),
                        make_keyboard(),
                    )

                else:
                    send_message(
                        chat_id,
                        "Используй /start для меню или /prices для цен.",
                        make_keyboard(),
                    )

        callback = update.get("callback_query")

        if callback:
            callback_id = callback.get("id")
            data = callback.get("data", "")
            message = callback.get("message") or {}
            chat_id = (message.get("chat") or {}).get("id")

            if callback_id:
                telegram(
                    "answerCallbackQuery",
                    {"callback_query_id": callback_id},
                )

            if chat_id is not None:
                if data == "prices:all":
                    send_message(
                        chat_id,
                        prices_text(),
                        make_keyboard(),
                    )

                elif data.startswith("price:"):
                    symbol = data.split(":", 1)[1].upper()

                    if symbol in COINS:
                        send_message(
                            chat_id,
                            prices_text(symbol),
                            make_keyboard(),
                        )

    except Exception:
        log.exception("Error processing Telegram update.")

    # Return HTTP 200 to acknowledge the update to Telegram.
    return jsonify({"ok": True}), 200


# ---------- Configure Telegram webhook ----------

def configure_telegram():
    if not TOKEN:
        log.error(
            "Telegram token missing. Set TELEGRAM_TOKEN "
            "or TELEGRAM_BOT_TOKEN in Render Environment."
        )
        return

    webhook_url = f"{WEBAPP_URL}/telegram-webhook"

    result = telegram(
        "setWebhook",
        {"url": webhook_url},
    )

    if result is True:
        log.info("Telegram webhook configured successfully.")
    else:
        log.error("Telegram webhook configuration failed.")

    telegram(
        "setMyCommands",
        {
            "commands": [
                {"command": "start", "description": "Главное меню"},
                {"command": "prices", "description": "Все цены"},
                {"command": "btc", "description": "Цена Bitcoin"},
                {"command": "eth", "description": "Цена Ethereum"},
                {"command": "sol", "description": "Цена Solana"},
                {"command": "ton", "description": "Цена Toncoin"},
                {"command": "help", "description": "Помощь"},
            ]
        },
    )


# Configure the webhook when Gunicorn imports this module.
configure_telegram()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
