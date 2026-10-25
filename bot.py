import os
import logging
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("cryptopulse")

DEFAULT_URL = "https://cryptopulse-lpc9.onrender.com"


def get_webapp_url():
    value = (os.getenv("WEBAPP_URL") or "").strip().strip("\"'").rstrip("/")
    parsed = urlparse(value)

    if parsed.scheme == "https" and parsed.netloc:
        return value

    log.warning("WEBAPP_URL is empty or invalid; using the default site URL.")
    return DEFAULT_URL


WEBAPP_URL = get_webapp_url()
# Поддерживаем оба названия переменной токена.
TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or ""
).strip()

app = Flask(__name__)
TIMEOUT = 15


def telegram(method, payload=None):
    if not TOKEN:
        log.error(
            "Telegram token is missing. Set TELEGRAM_TOKEN in Render Environment."
        )
        return None

    url = f"https://api.telegram.org/bot{TOKEN}/{method}"

    try:
        response = requests.post(
            url,
            json=payload or {},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        result = response.json()

        if not result.get("ok"):
            log.error("Telegram API reported failure for method %s.", method)
            return None

        return result.get("result")

    except (requests.RequestException, ValueError):
        log.exception("Telegram API request failed for method %s.", method)
        return None


def send_message(chat_id, text, keyboard=None):
    payload = {"chat_id": chat_id, "text": text}
    if keyboard:
        payload["reply_markup"] = keyboard
    return telegram("sendMessage", payload)


def keyboard():
    return {
        "inline_keyboard": [
            [
                {
                    "text": "📈 Открыть CryptoPulse",
                    "web_app": {"url": WEBAPP_URL},
                }
            ],
            [
                {"text": "₿ BTC", "callback_data": "price:bitcoin"},
                {"text": "Ξ ETH", "callback_data": "price:ethereum"},
            ],
            [
                {"text": "◎ SOL", "callback_data": "price:solana"},
                {
                    "text": "💎 TON",
                    "callback_data": "price:the-open-network",
                },
            ],
            [{"text": "📊 Все цены", "callback_data": "prices:all"}],
        ]
    }


def get_prices():
    response = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": "bitcoin,ethereum,solana,the-open-network",
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        },
        headers={"User-Agent": "CryptoPulse/1.0"},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    symbols = {
        "bitcoin": "BTC",
        "ethereum": "ETH",
        "solana": "SOL",
        "the-open-network": "TON",
    }

    prices = {}
    for coin_id, symbol in symbols.items():
        coin = data.get(coin_id, {})
        prices[symbol] = {
            "usd": coin.get("usd"),
            "change_24h": coin.get("usd_24h_change"),
        }

    return prices


def format_price(symbol, item):
    price = item.get("usd")
    change = item.get("change_24h")

    if price is None:
        return f"{symbol}: цена временно недоступна."

    change_text = (
        f"{change:+.2f}%"
        if isinstance(change, (int, float))
        else "нет данных"
    )
    return f"{symbol}: ${price:,.4f} | 24 ч: {change_text}"


def prices_text(symbol=None):
    prices = get_prices()

    if symbol:
        return format_price(symbol, prices.get(symbol, {}))

    lines = ["📊 CryptoPulse — цены в USD", ""]
    for ticker in ("BTC", "ETH", "SOL", "TON"):
        lines.append(format_price(ticker, prices.get(ticker, {})))
    return "\n".join(lines)


@app.get("/")
def index():
    return (
        "CryptoPulse is running. "
        "Health: /health | Prices: /api/prices"
    ), 200


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "cryptopulse"}), 200


@app.get("/api/prices")
def api_prices():
    try:
        return jsonify({"ok": True, "prices": get_prices()}), 200
    except (requests.RequestException, ValueError):
        log.exception("Could not retrieve cryptocurrency prices.")
        return jsonify({
            "ok": False,
            "error": "price_provider_unavailable",
        }), 502


@app.get("/api/config")
def api_config():
    # Public URL only. Never return the Telegram token.
    return jsonify({"webapp_url": WEBAPP_URL}), 200


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
                if text.startswith("/start"):
                    send_message(
                        chat_id,
                        "Привет! 👋 Добро пожаловать в CryptoPulse!\n\n"
                        "Здесь можно смотреть цены криптовалют "
                        "и открывать наш сайт.\n\n"
                        "Выбери кнопку ниже или введи /prices.",
                        keyboard(),
                    )

                elif text.startswith("/prices"):
                    try:
                        send_message(chat_id, prices_text(), keyboard())
                    except (requests.RequestException, ValueError):
                        log.exception("Price lookup failed for /prices.")
                        send_message(
                            chat_id,
                            "Не удалось получить цены. Попробуй позже.",
                            keyboard(),
                        )

                elif text.startswith("/btc"):
                    send_single_price(chat_id, "BTC", "bitcoin")

                elif text.startswith("/eth"):
                    send_single_price(chat_id, "ETH", "ethereum")

                elif text.startswith("/sol"):
                    send_single_price(chat_id, "SOL", "solana")

                elif text.startswith("/ton"):
                    send_single_price(
                        chat_id, "TON", "the-open-network"
                    )

                elif text.startswith("/help"):
                    send_message(
                        chat_id,
                        "Команды:\n"
                        "/start — главное меню\n"
                        "/prices — все цены\n"
                        "/btc — Bitcoin\n"
                        "/eth — Ethereum\n"
                        "/sol — Solana\n"
                        "/ton — Toncoin",
                        keyboard(),
                    )

                else:
                    send_message(
                        chat_id,
                        "Используй /start для меню или /prices для цен.",
                        keyboard(),
                    )

        callback = update.get("callback_query")
        if callback:
            callback_id = callback.get("id")
            callback_data = callback.get("data", "")
            chat_id = (
                (callback.get("message") or {})
                .get("chat", {})
                .get("id")
            )

            if callback_id:
                telegram(
                    "answerCallbackQuery",
                    {"callback_query_id": callback_id},
                )

            if chat_id is not None:
                if callback_data == "prices:all":
                    try:
                        send_message(chat_id, prices_text(), keyboard())
                    except (requests.RequestException, ValueError):
                        log.exception("Price lookup failed for callback.")
                        send_message(
                            chat_id,
                            "Не удалось получить цены. Попробуй позже.",
                            keyboard(),
                        )

                elif callback_data.startswith("price:"):
                    coin_id = callback_data.split(":", 1)[1]
                    symbols = {
                        "bitcoin": "BTC",
                        "ethereum": "ETH",
                        "solana": "SOL",
                        "the-open-network": "TON",
                    }
                    symbol = symbols.get(coin_id)

                    if symbol:
                        send_single_price(chat_id, symbol, coin_id)

    except Exception:
        log.exception("Unexpected error processing Telegram update.")

    return jsonify({"ok": True}), 200


def send_single_price(chat_id, symbol, coin_id):
    try:
        prices = get_prices()
        send_message(
            chat_id,
            format_price(symbol, prices.get(symbol, {})),
            keyboard(),
        )
    except (requests.RequestException, ValueError):
        log.exception("Could not retrieve price for %s.", coin_id)
        send_message(
            chat_id,
            "Не удалось получить цену. Попробуй позже.",
            keyboard(),
        )


def configure_telegram():
    if not TOKEN:
        log.warning(
            "Telegram is not configured: set TELEGRAM_TOKEN in Render."
        )
        return

    webhook_url = f"{WEBAPP_URL}/telegram-webhook"

    result = telegram("setWebhook", {"url": webhook_url})
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


# Configure the webhook once when the module is imported.
configure_telegram()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
