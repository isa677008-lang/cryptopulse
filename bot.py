import os
import logging
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request, send_from_directory

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("cryptopulse")

BASE_URL = os.getenv(
    "WEBAPP_URL", "https://cryptopulse-lpc9.onrender.com"
).strip().rstrip("/")
TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()

app = Flask(__name__, static_folder="static")
http = requests.Session()
http.headers.update({"User-Agent": "CryptoPulse/2.0"})

COINS = [
    ("BTC", "Bitcoin", "btc-bitcoin", "BTCUSDT", "bitcoin"),
    ("ETH", "Ethereum", "eth-ethereum", "ETHUSDT", "ethereum"),
    ("BNB", "BNB", "bnb-binance-coin", "BNBUSDT", "binancecoin"),
    ("SOL", "Solana", "sol-solana", "SOLUSDT", "solana"),
    ("XRP", "XRP", "xrp-xrp", "XRPUSDT", "ripple"),
    ("ADA", "Cardano", "ada-cardano", "ADAUSDT", "cardano"),
    ("DOGE", "Dogecoin", "doge-dogecoin", "DOGEUSDT", "dogecoin"),
    ("TON", "Toncoin", "ton-toncoin", "TONUSDT", "the-open-network"),
    ("TRX", "TRON", "trx-tron", "TRXUSDT", "tron"),
    ("LTC", "Litecoin", "ltc-litecoin", "LTCUSDT", "litecoin"),
    ("DOT", "Polkadot", "dot-polkadot", "DOTUSDT", "polkadot"),
    ("AVAX", "Avalanche", "avax-avalanche", "AVAXUSDT", "avalanche-2"),
    ("LINK", "Chainlink", "link-chainlink", "LINKUSDT", "chainlink"),
    ("BCH", "Bitcoin Cash", "bch-bitcoin-cash", "BCHUSDT", "bitcoin-cash"),
    ("ETC", "Ethereum Classic", "etc-ethereum-classic", "ETCUSDT", "ethereum-classic"),
]

cache = {"data": None, "time": 0}
cache_lock = threading.Lock()


def get_prices():
    with cache_lock:
        if cache["data"] and time.time() - cache["time"] < 60:
            return cache["data"], False

    errors = []

    # Try CoinPaprika, then Binance, then CoinGecko.
    providers = [
        ("CoinPaprika", get_paprika),
        ("Binance", get_binance),
        ("CoinGecko", get_coingecko),
    ]

    for source, provider in providers:
        try:
            rows = provider()
            result = {
                "ok": True,
                "currency": "USD",
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "source": source,
                "count": len(rows),
                "prices": rows,
            }
            with cache_lock:
                cache["data"] = result
                cache["time"] = time.time()
            return result, False
        except Exception as exc:
            log.warning("%s prices failed: %s", source, exc)
            errors.append(source)

    with cache_lock:
        if cache["data"]:
            old = dict(cache["data"])
            old["stale"] = True
            return old, False

    return {
        "ok": False,
        "error": "Price providers are temporarily unavailable.",
        "details": errors,
        "prices": [],
    }, True


def get_paprika():
    r = http.get(
        "https://api.coinpaprika.com/v1/tickers",
        params={"quotes": "USD"},
        timeout=15,
    )
    r.raise_for_status()
    items = {x.get("id"): x for x in r.json() if x.get("id")}
    rows = []

    for symbol, name, coin_id, pair, cg_id in COINS:
        item = items.get(coin_id)
        q = (item or {}).get("quotes", {}).get("USD", {})
        if q.get("price") is not None:
            rows.append({
                "symbol": symbol,
                "name": name,
                "price": float(q["price"]),
                "change24h": float(q.get("percent_change_24h") or 0),
                "source": "CoinPaprika",
            })

    if len(rows) < 5:
        raise RuntimeError("Too few CoinPaprika prices")
    return rows


def get_binance():
    r = http.get("https://api.binance.com/api/v3/ticker/24hr", timeout=12)
    r.raise_for_status()
    items = {x.get("symbol"): x for x in r.json() if x.get("symbol")}
    rows = []

    for symbol, name, coin_id, pair, cg_id in COINS:
        item = items.get(pair)
        if item:
            rows.append({
                "symbol": symbol,
                "name": name,
                "price": float(item["lastPrice"]),
                "change24h": float(item.get("priceChangePercent") or 0),
                "source": "Binance",
            })

    if len(rows) < 5:
        raise RuntimeError("Too few Binance prices")
    return rows


def get_coingecko():
    ids = ",".join(x[4] for x in COINS)
    r = http.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd",
            "ids": ids,
            "price_change_percentage": "24h",
        },
        timeout=15,
    )
    r.raise_for_status()
    items = {x.get("id"): x for x in r.json() if x.get("id")}
    rows = []

    for symbol, name, coin_id, pair, cg_id in COINS:
        item = items.get(cg_id)
        if item and item.get("current_price") is not None:
            rows.append({
                "symbol": symbol,
                "name": name,
                "price": float(item["current_price"]),
                "change24h": float(item.get("price_change_percentage_24h") or 0),
                "source": "CoinGecko",
            })

    if not rows:
        raise RuntimeError("No CoinGecko prices")
    return rows


@app.get("/")
def home():
    return send_from_directory("static", "index.html")


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "CryptoPulse"})


@app.get("/api/prices")
def prices():
    data, failed = get_prices()
    return jsonify(data), 502 if failed else 200


def telegram(method, payload):
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is missing")

    r = http.post(
        f"https://api.telegram.org/bot{TOKEN}/{method}",
        json=payload,
        timeout=20,
    )
    r.raise_for_status()
    result = r.json()
    if not result.get("ok"):
        raise RuntimeError(f"Telegram {method} failed")
    return result


def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if markup:
        payload["reply_markup"] = markup
    telegram("sendMessage", payload)


def keyboard():
    return {
        "inline_keyboard": [
            [{
                "text": "📈 Открыть CryptoPulse",
                "web_app": {"url": BASE_URL + "/"},
            }],
            [
                {"text": "💰 Цены", "callback_data": "prices"},
                {"text": "💱 USDT/RUB", "callback_data": "rate"},
            ],
            [{"text": "ℹ️ Помощь", "callback_data": "help"}],
        ]
    }


def prices_text():
    data, failed = get_prices()
    if failed or not data.get("prices"):
        return "Котировки временно недоступны. Попробуй позже."

    lines = ["📈 CryptoPulse — котировки в USD", ""]
    for x in data["prices"]:
        sign = "+" if x["change24h"] >= 0 else ""
        lines.append(
            f'{x["symbol"]}: ${x["price"]:,.6f} '
            f'({sign}{x["change24h"]:.2f}%)'
        )
    lines.extend(["", "Источник: " + data["source"]])
    return "\n".join(lines)


def rate_text():
    try:
        r = http.get("https://open.er-api.com/v6/latest/USD", timeout=10)
        r.raise_for_status()
        rate = r.json().get("rates", {}).get("RUB")
        if rate is None:
            raise RuntimeError("RUB rate unavailable")
        return (
            f"💱 Справочный курс: 1 USD ≈ {rate:.2f} RUB.\n"
            "USDT обычно близок к 1 USD, но реальный курс обмена может отличаться."
        )
    except Exception:
        return "Не удалось получить курс USD/RUB. Попробуй позже."


def handle_message(message):
    chat_id = (message.get("chat") or {}).get("id")
    text = (message.get("text") or "").strip()
    if not chat_id or not text:
        return

    command = text.split()[0].split("@")[0].lower()

    if command == "/start":
        send(
            chat_id,
            "Привет! Добро пожаловать в CryptoPulse.\n\n"
            "Здесь можно смотреть котировки криптовалют и открывать наш сайт.",
            keyboard(),
        )
    elif command in ("/prices", "/price"):
        send(chat_id, prices_text(), keyboard())
    elif command == "/rate":
        send(chat_id, rate_text(), keyboard())
    elif command == "/help":
        send(
            chat_id,
            "Команды:\n/start — главное меню\n/prices — котировки\n"
            "/rate — справочный курс USD/RUB\n/help — помощь",
            keyboard(),
        )


@app.post("/telegram-webhook")
def telegram_webhook():
    update = request.get_json(silent=True) or {}

    try:
        message = update.get("message")
        if message:
            handle_message(message)

        callback = update.get("callback_query")
        if callback:
            telegram(
                "answerCallbackQuery",
                {"callback_query_id": callback["id"]},
            )
            chat_id = (
                (callback.get("message") or {}).get("chat") or {}
            ).get("id")
            action = callback.get("data")

            if chat_id and action == "prices":
                send(chat_id, prices_text(), keyboard())
            elif chat_id and action == "rate":
                send(chat_id, rate_text(), keyboard())
            elif chat_id and action == "help":
                send(
                    chat_id,
                    "Команды: /start, /prices, /rate, /help",
                    keyboard(),
                )

        log.info("Telegram update processed")
    except Exception:
        log.exception("Telegram update processing failed")

    return jsonify({"ok": True})


def configure_telegram():
    if not TOKEN:
        log.error("TELEGRAM_TOKEN is missing")
        return

    if not BASE_URL.startswith("https://"):
        log.error("WEBAPP_URL must start with https://")
        return

    try:
        webhook_url = BASE_URL + "/telegram-webhook"
        telegram("setWebhook", {
            "url": webhook_url,
            "allowed_updates": ["message", "callback_query"],
        })
        telegram("setMyCommands", {
            "commands": [
                {"command": "start", "description": "Главное меню"},
                {"command": "prices", "description": "Цены криптовалют"},
                {"command": "rate", "description": "Курс USD/RUB"},
                {"command": "help", "description": "Помощь"},
            ]
        })
        log.info("Telegram webhook configured: %s", webhook_url)
    except Exception:
        log.exception("Telegram webhook configuration failed")


configure_telegram()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
    )
