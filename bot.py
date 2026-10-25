import os
import time
import logging
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

# CoinGecko IDs and CoinPaprika IDs for the same 15 assets.
COINS = [
    {"symbol": "BTC",  "name": "Bitcoin",   "cg": "bitcoin",          "paprika": "btc-bitcoin"},
    {"symbol": "ETH",  "name": "Ethereum",  "cg": "ethereum",         "paprika": "eth-ethereum"},
    {"symbol": "USDT", "name": "Tether",    "cg": "tether",           "paprika": "usdt-tether"},
    {"symbol": "BNB",  "name": "BNB",       "cg": "binancecoin",      "paprika": "bnb-binance-coin"},
    {"symbol": "XRP",  "name": "XRP",       "cg": "ripple",           "paprika": "xrp-xrp"},
    {"symbol": "USDC", "name": "USD Coin",  "cg": "usd-coin",         "paprika": "usdc-usd-coin"},
    {"symbol": "SOL",  "name": "Solana",    "cg": "solana",           "paprika": "sol-solana"},
    {"symbol": "TRX",  "name": "TRON",      "cg": "tron",             "paprika": "trx-tron"},
    {"symbol": "DOGE", "name": "Dogecoin",  "cg": "dogecoin",         "paprika": "doge-dogecoin"},
    {"symbol": "TON",  "name": "Toncoin",   "cg": "the-open-network", "paprika": "ton-toncoin"},
    {"symbol": "ADA",  "name": "Cardano",   "cg": "cardano",          "paprika": "ada-cardano"},
    {"symbol": "BCH",  "name": "Bitcoin Cash", "cg": "bitcoin-cash",  "paprika": "bch-bitcoin-cash"},
    {"symbol": "LINK", "name": "Chainlink", "cg": "chainlink",        "paprika": "link-chainlink"},
    {"symbol": "AVAX", "name": "Avalanche", "cg": "avalanche-2",      "paprika": "avax-avalanche"},
    {"symbol": "LTC",  "name": "Litecoin",  "cg": "litecoin",         "paprika": "ltc-litecoin"},
]

COIN_BY_SYMBOL = {coin["symbol"]: coin for coin in COINS}
_cache = {"time": 0, "data": None}
_session = requests.Session()
_session.headers.update({"User-Agent": "CryptoPulse/2.0"})

def empty_price():
    return {"price": None, "change24h": None}

def get_prices(force=False):
    """Always return all 15 coins; use cached values briefly to limit API calls."""
    now = time.time()
    if not force and _cache["data"] and now - _cache["time"] < CACHE_SECONDS:
        return _cache["data"]

    values = {coin["symbol"]: empty_price() for coin in COINS}
    source_names = []
    warnings = []

    # Primary source: one batch request for all coins.
    try:
        ids = ",".join(coin["cg"] for coin in COINS)
        response = _session.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": ids,
                "vs_currencies": "usd",
                "include_24hr_change": "true",
            },
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        for coin in COINS:
            item = data.get(coin["cg"], {})
            if item.get("usd") is not None:
                values[coin["symbol"]] = {
                    "price": item.get("usd"),
                    "change24h": item.get("usd_24h_change"),
                }
        source_names.append("CoinGecko")
    except (requests.RequestException, ValueError) as exc:
        log.warning("CoinGecko request failed: %s", exc)
        warnings.append("CoinGecko временно недоступен")

    # Fallback source: only fill missing prices.
    missing = [
        coin for coin in COINS
        if values[coin["symbol"]]["price"] is None
    ]
    for coin in missing:
        try:
            response = _session.get(
                f"https://api.coinpaprika.com/v1/tickers/{coin['paprika']}",
                params={"quotes": "USD"},
                timeout=TIMEOUT,
            )
            response.raise_for_status()
            item = response.json().get("quotes", {}).get("USD", {})
            if item.get("price") is not None:
                values[coin["symbol"]] = {
                    "price": item.get("price"),
                    "change24h": item.get("percent_change_24h"),
                }
                if "CoinPaprika" not in source_names:
                    source_names.append("CoinPaprika")
        except (requests.RequestException, ValueError) as exc:
            log.warning("CoinPaprika failed for %s: %s", coin["symbol"], exc)

    results = []
    unavailable = 0
    for coin in COINS:
        quote = values[coin["symbol"]]
        price = quote["price"]
        if price is None:
            unavailable += 1

        results.append({
            "symbol": coin["symbol"],
            "name": coin["name"],
            "price": price,
            "change24h": quote["change24h"],
            "image": f"https://assets.coingecko.com/coins/images/1/large/bitcoin.png"
                     if coin["symbol"] == "BTC" else None,
            "available": price is not None,
        })

    # Fetch actual CoinGecko image URLs separately; failure must not hide coins.
    try:
        image_response = _session.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ",".join(coin["cg"] for coin in COINS),
                "per_page": 100,
                "page": 1,
            },
            timeout=TIMEOUT,
        )
        image_response.raise_for_status()
        image_data = image_response.json()
        images = {
            item.get("symbol", "").upper(): item.get("image")
            for item in image_data if item.get("symbol")
        }
        for item in results:
            if images.get(item["symbol"]):
                item["image"] = images[item["symbol"]]
    except (requests.RequestException, ValueError) as exc:
        log.warning("CoinGecko image request failed: %s", exc)

    result = {
        "ok": True,
        "prices": results,
        "count": len(results),
        "source": " + ".join(source_names) if source_names else "Временно недоступен",
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "stale": unavailable > 0,
        "warning": (
            f"Цены временно недоступны для {unavailable} монет. "
            "Список монет сохранён; попробуйте обновить позже."
            if unavailable else ""
        ),
    }

    _cache["time"] = now
    _cache["data"] = result
    return result

# ---------------- Telegram ----------------

def telegram(method, payload=None):
    if not TOKEN:
        log.error("Telegram token is missing in Render Environment.")
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
            log.error("Telegram API returned an error for %s.", method)
            return None
        return data.get("result")
    except (requests.RequestException, ValueError):
        log.exception("Telegram API request failed for %s.", method)
        return None

def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return telegram("sendMessage", payload)

def make_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "📈 Открыть CryptoPulse", "web_app": {"url": WEBAPP_URL}}],
            [
                {"text": "₿ BTC", "callback_data": "price:BTC"},
                {"text": "Ξ ETH", "callback_data": "price:ETH"},
            ],
            [
                {"text": "◎ SOL", "callback_data": "price:SOL"},
                {"text": "💎 TON", "callback_data": "price:TON"},
            ],
            [{"text": "📊 Все 15 монет", "callback_data": "prices:all"}],
        ]
    }

def fmt_price(value):
    if value is None:
        return "Цена временно недоступна"
    try:
        value = float(value)
        if value >= 1:
            return f"${value:,.2f}"
        return f"${value:.8f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "Цена временно недоступна"

def prices_text(symbol=None):
    data = get_prices()
    prices = {item["symbol"]: item for item in data["prices"]}

    if symbol:
        item = prices.get(symbol)
        if not item:
            return f"Неизвестная монета: {symbol}"
        change = item.get("change24h")
        change_text = f"{float(change):+.2f}%" if isinstance(change, (int, float)) else "нет данных"
        return (
            f"🪙 {item['name']} ({item['symbol']})\n"
            f"💵 Цена: {fmt_price(item['price'])}\n"
            f"📈 Изменение за 24 ч.: {change_text}"
        )

    lines = ["📊 CryptoPulse — цены в USD", ""]
    for item in data["prices"]:
        change = item.get("change24h")
        change_text = f"{float(change):+.2f}%" if isinstance(change, (int, float)) else "нет данных"
        lines.append(f"• {item['name']} ({item['symbol']})")
        lines.append(f"  💵 {fmt_price(item['price'])} | 24 ч.: {change_text}")
    if data.get("warning"):
        lines.extend(["", "⚠️ " + data["warning"]])
    return "\n".join(lines)

# ---------------- Website routes ----------------

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
    return jsonify(get_prices(force=force)), 200

# ---------------- Telegram webhook ----------------

@app.route("/telegram-webhook", methods=["GET", "POST"])
def telegram_webhook():
    if request.method == "GET":
        return jsonify({"ok": True, "message": "Webhook is available."}), 200

    update = request.get_json(silent=True) or {}
    try:
        message = update.get("message") or update.get("edited_message")
        if message:
            chat_id = (message.get("chat") or {}).get("id")
            text = (message.get("text") or "").strip()
            command = text.split()[0].split("@")[0].lower() if text else ""

            if chat_id is not None:
                if command in ("/start", "/help"):
                    send_message(
                        chat_id,
                        "Привет! 👋 Это CryptoPulse.\n\n"
                        "Команды:\n/start — меню\n/prices — цены всех 15 монет\n"
                        "/btc /eth /sol /ton — отдельные монеты\n/help — помощь",
                        make_keyboard(),
                    )
                elif command == "/prices":
                    send_message(chat_id, prices_text(), make_keyboard())
                elif command in ("/btc", "/eth", "/sol", "/ton"):
                    send_message(chat_id, prices_text(command[1:].upper()), make_keyboard())
                else:
                    send_message(chat_id, "Используй /start или /prices.", make_keyboard())

        callback = update.get("callback_query")
        if callback:
            callback_id = callback.get("id")
            if callback_id:
                telegram("answerCallbackQuery", {"callback_query_id": callback_id})
            message = callback.get("message") or {}
            chat_id = (message.get("chat") or {}).get("id")
            data = callback.get("data", "")
            if chat_id is not None:
                if data == "prices:all":
                    send_message(chat_id, prices_text(), make_keyboard())
                elif data.startswith("price:"):
                    symbol = data.split(":", 1)[1].upper()
                    if symbol in COIN_BY_SYMBOL:
                        send_message(chat_id, prices_text(symbol), make_keyboard())
    except Exception:
        log.exception("Error processing Telegram update.")

    return jsonify({"ok": True}), 200

def configure_telegram():
    if not TOKEN:
        log.error("Telegram token missing; set it in Render Environment.")
        return
    result = telegram("setWebhook", {"url": f"{WEBAPP_URL}/telegram-webhook"})
    if result is True:
        log.info("Telegram webhook configured successfully.")
    else:
        log.error("Telegram webhook configuration failed.")

    telegram("setMyCommands", {"commands": [
        {"command": "start", "description": "Главное меню"},
        {"command": "prices", "description": "Цены 15 монет"},
        {"command": "btc", "description": "Цена Bitcoin"},
        {"command": "eth", "description": "Цена Ethereum"},
        {"command": "sol", "description": "Цена Solana"},
        {"command": "ton", "description": "Цена Toncoin"},
        {"command": "help", "description": "Помощь"},
    ]})

configure_telegram()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
