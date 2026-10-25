import html
import logging
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request, send_from_directory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("cryptopulse")

DEFAULT_URL = "https://cryptopulse-lpc9.onrender.com"
TIMEOUT = 12
CACHE_SECONDS = 45

TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or ""
).strip()


def get_webapp_url():
    value = (os.getenv("WEBAPP_URL") or "").strip().strip("\"'").rstrip("/")
    parsed = urlparse(value)
    if parsed.scheme == "https" and parsed.netloc:
        return value
    log.warning("WEBAPP_URL invalid; using default site URL.")
    return DEFAULT_URL


WEBAPP_URL = get_webapp_url()
app = Flask(__name__)

COINS = [
    {"symbol": "BTC", "name": "Bitcoin", "cg": "bitcoin", "paprika": "btc-bitcoin"},
    {"symbol": "ETH", "name": "Ethereum", "cg": "ethereum", "paprika": "eth-ethereum"},
    {"symbol": "USDT", "name": "Tether", "cg": "tether", "paprika": "usdt-tether"},
    {"symbol": "BNB", "name": "BNB", "cg": "binancecoin", "paprika": "bnb-binance-coin"},
    {"symbol": "XRP", "name": "XRP", "cg": "ripple", "paprika": "xrp-xrp"},
    {"symbol": "USDC", "name": "USD Coin", "cg": "usd-coin", "paprika": "usdc-usd-coin"},
    {"symbol": "SOL", "name": "Solana", "cg": "solana", "paprika": "sol-solana"},
    {"symbol": "TRX", "name": "TRON", "cg": "tron", "paprika": "trx-tron"},
    {"symbol": "DOGE", "name": "Dogecoin", "cg": "dogecoin", "paprika": "doge-dogecoin"},
    {"symbol": "TON", "name": "Toncoin", "cg": "the-open-network", "paprika": "ton-toncoin"},
    {"symbol": "ADA", "name": "Cardano", "cg": "cardano", "paprika": "ada-cardano"},
    {"symbol": "BCH", "name": "Bitcoin Cash", "cg": "bitcoin-cash", "paprika": "bch-bitcoin-cash"},
    {"symbol": "LINK", "name": "Chainlink", "cg": "chainlink", "paprika": "link-chainlink"},
    {"symbol": "AVAX", "name": "Avalanche", "cg": "avalanche-2", "paprika": "avax-avalanche"},
    {"symbol": "LTC", "name": "Litecoin", "cg": "litecoin", "paprika": "ltc-litecoin"},
]

_cache = {"time": 0, "data": None}
_rub_cache = {"time": 0, "value": None}
_session = requests.Session()
_session.headers.update({"User-Agent": "CryptoPulse/2.1"})
_pending_calculations = set()


def get_prices(force=False):
    now = time.time()
    if not force and _cache["data"] and now - _cache["time"] < CACHE_SECONDS:
        return _cache["data"]

    values = {
        coin["symbol"]: {"price": None, "change24h": None}
        for coin in COINS
    }
    sources = []

    try:
        response = _session.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": ",".join(coin["cg"] for coin in COINS),
                "vs_currencies": "usd",
                "include_24hr_change": "true",
            },
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        for coin in COINS:
            quote = data.get(coin["cg"], {}) if isinstance(data, dict) else {}
            if isinstance(quote, dict) and quote.get("usd") is not None:
                values[coin["symbol"]] = {
                    "price": quote.get("usd"),
                    "change24h": quote.get("usd_24h_change"),
                }

        sources.append("CoinGecko")
    except (requests.RequestException, ValueError, TypeError) as exc:
        log.warning("CoinGecko price request failed: %s", exc)

    for coin in COINS:
        if values[coin["symbol"]]["price"] is not None:
            continue

        try:
            response = _session.get(
                f"https://api.coinpaprika.com/v1/tickers/{coin['paprika']}",
                params={"quotes": "USD"},
                timeout=TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            quote = (
                payload.get("quotes", {}).get("USD", {})
                if isinstance(payload, dict)
                else {}
            )

            if quote.get("price") is not None:
                values[coin["symbol"]] = {
                    "price": quote["price"],
                    "change24h": quote.get("percent_change_24h"),
                }
                if "CoinPaprika" not in sources:
                    sources.append("CoinPaprika")

        except (requests.RequestException, ValueError, TypeError) as exc:
            log.warning("CoinPaprika failed for %s: %s", coin["symbol"], exc)

    images = {}
    try:
        response = _session.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ",".join(coin["cg"] for coin in COINS),
                "per_page": 100,
                "page": 1,
            },
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()

        if isinstance(payload, list):
            images = {
                str(item.get("symbol", "")).upper(): item.get("image")
                for item in payload
                if isinstance(item, dict) and item.get("symbol")
            }

    except (requests.RequestException, ValueError, TypeError) as exc:
        log.warning("CoinGecko image request failed: %s", exc)

    prices = []
    for coin in COINS:
        quote = values[coin["symbol"]]
        prices.append({
            "symbol": coin["symbol"],
            "name": coin["name"],
            "price": quote["price"],
            "change24h": quote["change24h"],
            "image": images.get(coin["symbol"]),
            "available": quote["price"] is not None,
        })

    unavailable = sum(item["price"] is None for item in prices)

    result = {
        "ok": True,
        "prices": prices,
        "count": len(prices),
        "source": " + ".join(sources) if sources else "Временно недоступен",
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "stale": unavailable > 0,
        "warning": (
            f"Цены временно недоступны для {unavailable} монет."
            if unavailable else ""
        ),
    }

    _cache.update({"time": now, "data": result})
    return result


def get_usdt_rub(force=False):
    now = time.time()

    if (
        not force
        and _rub_cache["value"] is not None
        and now - _rub_cache["time"] < CACHE_SECONDS
    ):
        return _rub_cache["value"]

    sources = [
        (
            "https://api.coingecko.com/api/v3/simple/price",
            {"ids": "tether", "vs_currencies": "rub"},
            ("tether", "rub"),
        ),
        (
            "https://api.coinpaprika.com/v1/tickers/usdt-tether",
            {"quotes": "RUB"},
            ("quotes", "RUB", "price"),
        ),
    ]

    for url, params, path in sources:
        try:
            response = _session.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            for key in path:
                data = data[key]

            rate = float(data)
            if rate > 0:
                _rub_cache.update({"time": now, "value": rate})
                return rate

        except (
            requests.RequestException,
            ValueError,
            TypeError,
            KeyError,
        ) as exc:
            log.warning("USDT/RUB source failed: %s", exc)

    return None


def telegram(method, payload=None):
    if not TOKEN:
        log.error("Telegram token missing in Render Environment.")
        return None

    try:
        response = _session.post(
            f"https://api.telegram.org/bot{TOKEN}/{method}",
            json=payload or {},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        if not data.get("ok"):
            log.error("Telegram API error for %s", method)
            return None

        return data.get("result")

    except (requests.RequestException, ValueError):
        log.exception("Telegram request failed for %s", method)
        return None


def send_message(chat_id, text, markup=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if markup:
        payload["reply_markup"] = markup
    return telegram("sendMessage", payload)


def edit_message(chat_id, message_id, text, markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if markup:
        payload["reply_markup"] = markup
    return telegram("editMessageText", payload)


def main_keyboard():
    return {
        "inline_keyboard": [
            [{
                "text": "🌐 Открыть CryptoPulse",
                "web_app": {"url": WEBAPP_URL},
            }],
            [{
                "text": "📊 Все 15 монет",
                "callback_data": "menu:prices",
            }],
            [{
                "text": "💱 USDT → RUB",
                "callback_data": "menu:rate",
            }],
            [{
                "text": "🧮 Калькулятор",
                "callback_data": "menu:calc",
            }],
        ]
    }


def back_keyboard():
    return {
        "inline_keyboard": [[{
            "text": "⬅️ Главное меню",
            "callback_data": "menu:home",
        }]]
    }


def prices_keyboard():
    return {
        "inline_keyboard": [
            [{
                "text": "↻ Обновить цены",
                "callback_data": "menu:prices:refresh",
            }],
            [{
                "text": "⬅️ Главное меню",
                "callback_data": "menu:home",
            }],
        ]
    }


def fmt_price(value):
    if value is None:
        return "нет данных"

    try:
        number = float(value)
        if number >= 1000:
            return f"${number:,.2f}"
        if number >= 1:
            return f"${number:,.4f}".rstrip("0").rstrip(".")
        return f"${number:.8f}".rstrip("0").rstrip(".")

    except (TypeError, ValueError):
        return "нет данных"


def fmt_change(value):
    if not isinstance(value, (int, float)):
        return "нет данных"

    number = float(value)
    if number > 0:
        return f"🟢 +{number:.2f}%"
    if number < 0:
        return f"🔴 {number:.2f}%"
    return "⚪ 0.00%"


def menu_text():
    return (
        "<b>🌊 CryptoPulse</b>\n"
        "Криптовалюты и курсы в одном месте.\n\n"
        "Выбери нужный раздел:"
    )


def prices_text(force=False):
    data = get_prices(force)
    lines = [
        "<b>📊 CryptoPulse · 15 монет</b>",
        "<i>Цена в USD · изменение за 24 часа</i>",
        "",
    ]

    for coin in data["prices"]:
        lines.extend([
            f"<b>{html.escape(coin['name'])} ({coin['symbol']})</b>",
            f"  💵 {fmt_price(coin['price'])}  ·  "
            f"{fmt_change(coin.get('change24h'))}",
        ])

    if data.get("warning"):
        lines.extend(["", "⚠️ " + html.escape(data["warning"])])

    lines.extend([
        "",
        f"<i>Источник: {html.escape(str(data.get('source') or 'неизвестен'))}</i>",
    ])

    return "\n".join(lines)[:4096]


def rate_text(force=False):
    rate = get_usdt_rub(force)

    if rate is None:
        return (
            "<b>💱 USDT → RUB</b>\n\n"
            "Текущий курс временно недоступен. "
            "Попробуй ещё раз чуть позже."
        )

    return (
        "<b>💱 USDT → RUB</b>\n\n"
        f"1 USDT ≈ <b>{rate:,.2f} ₽</b>\n"
        f"10 USDT ≈ <b>{rate * 10:,.2f} ₽</b>\n"
        f"100 USDT ≈ <b>{rate * 100:,.2f} ₽</b>\n\n"
        "<i>Ориентировочная котировка; курс обменника "
        "и комиссии могут отличаться.</i>"
    )


def calc_prompt():
    return (
        "<b>🧮 Калькулятор USDT → RUB</b>\n\n"
        "Отправь сумму USDT одним сообщением, например: "
        "<code>125.50</code>."
    )


def calc_result(amount):
    rate = get_usdt_rub()

    if rate is None:
        return (
            "⚠️ Не удалось получить курс USDT/RUB. "
            "Попробуй позже."
        )

    return (
        f"<b>🧮 Результат</b>\n\n"
        f"{amount:,.8f} USDT ≈ <b>{amount * rate:,.2f} ₽</b>\n"
        f"Курс: 1 USDT ≈ {rate:,.2f} ₽\n\n"
        "<i>Без учёта комиссий обменника.</i>"
    )


@app.get("/")
def index():
    return send_from_directory("static", "index.html")


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "cryptopulse"}), 200


@app.get("/api/config")
def api_config():
    return jsonify({"webapp_url": WEBAPP_URL}), 200


@app.get("/api/prices")
def api_prices():
    force = request.args.get("refresh") == "1"
    return jsonify(get_prices(force)), 200


@app.route("/telegram-webhook", methods=["GET", "POST"])
def telegram_webhook():
    if request.method == "GET":
        return jsonify({
            "ok": True,
            "message": "Webhook is available.",
        }), 200

    update = request.get_json(silent=True) or {}

    try:
        message = update.get("message") or update.get("edited_message")

        if message:
            chat_id = (message.get("chat") or {}).get("id")
            text = (message.get("text") or "").strip()
            command = text.split()[0].split("@")[0].lower() if text else ""

            if chat_id is not None:
                if command in ("/start", "/help"):
                    _pending_calculations.discard(chat_id)
                    send_message(chat_id, menu_text(), main_keyboard())

                elif command == "/prices":
                    _pending_calculations.discard(chat_id)
                    send_message(chat_id, prices_text(), prices_keyboard())

                elif command == "/rate":
                    _pending_calculations.discard(chat_id)
                    send_message(chat_id, rate_text(), back_keyboard())

                elif chat_id in _pending_calculations:
                    raw = text.replace(",", ".").strip()

                    if re.fullmatch(r"\d+(?:\.\d{1,8})?", raw):
                        amount = float(raw)

                        if 0 < amount <= 1_000_000_000:
                            _pending_calculations.discard(chat_id)
                            send_message(
                                chat_id,
                                calc_result(amount),
                                main_keyboard(),
                            )
                        else:
                            send_message(
                                chat_id,
                                "Введи сумму больше нуля и не больше "
                                "1 000 000 000 USDT.",
                                back_keyboard(),
                            )
                    else:
                        send_message(
                            chat_id,
                            "Не распознал сумму. Пример: "
                            "<code>25</code> или <code>12,50</code>.",
                            back_keyboard(),
                        )

                else:
                    send_message(
                        chat_id,
                        "Нажми /start, чтобы открыть меню CryptoPulse.",
                        main_keyboard(),
                    )

        callback = update.get("callback_query")

        if callback:
            if callback.get("id"):
                telegram(
                    "answerCallbackQuery",
                    {"callback_query_id": callback["id"]},
                )

            message = callback.get("message") or {}
            chat_id = (message.get("chat") or {}).get("id")
            message_id = message.get("message_id")
            data = callback.get("data", "")

            if chat_id is not None and message_id is not None:
                if data == "menu:home":
                    _pending_calculations.discard(chat_id)
                    edit_message(
                        chat_id, message_id, menu_text(), main_keyboard()
                    )

                elif data == "menu:prices":
                    _pending_calculations.discard(chat_id)
                    edit_message(
                        chat_id, message_id, prices_text(), prices_keyboard()
                    )

                elif data == "menu:prices:refresh":
                    edit_message(
                        chat_id,
                        message_id,
                        prices_text(True),
                        prices_keyboard(),
                    )

                elif data == "menu:rate":
                    _pending_calculations.discard(chat_id)
                    edit_message(
                        chat_id, message_id, rate_text(True), back_keyboard()
                    )

                elif data == "menu:calc":
                    _pending_calculations.add(chat_id)
                    edit_message(
                        chat_id, message_id, calc_prompt(), back_keyboard()
                    )

    except Exception:
        log.exception("Error processing Telegram update.")

    return jsonify({"ok": True}), 200


def configure_telegram():
    if not TOKEN:
        log.warning("Telegram token missing; set it in Render Environment.")
        return

    result = telegram(
        "setWebhook",
        {"url": f"{WEBAPP_URL}/telegram-webhook"},
    )

    if result is not None:
        log.info("Telegram webhook configured.")
    else:
        log.error("Telegram webhook configuration failed.")

    telegram("setMyCommands", {
        "commands": [
            {"command": "start", "description": "Главное меню"},
            {"command": "prices", "description": "Цены 15 монет"},
            {"command": "rate", "description": "Курс USDT к рублю"},
            {"command": "help", "description": "Помощь"},
        ]
    })


configure_telegram()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
