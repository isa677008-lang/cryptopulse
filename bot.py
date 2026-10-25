import os
import logging
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request, send_from_directory

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("cryptopulse")
app = Flask(__name__, static_folder="static")
session = requests.Session()
session.headers.update({"User-Agent": "CryptoPulse/1.1 (CryptoPulse dashboard)"})

# CoinPaprika IDs. This provider is tried first; Binance and CoinGecko are fallbacks.
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

_cache = {"data": None, "at": 0.0}
_cache_lock = threading.Lock()
CACHE_SECONDS = 60


def paprika_prices():
    # One request fetches all supported tickers; avoids 15 separate calls.
    response = session.get("https://api.coinpaprika.com/v1/tickers",
                           params={"quotes": "USD"}, timeout=15)
    response.raise_for_status()
    items = {x.get("id"): x for x in response.json() if x.get("id")}
    rows = []
    for symbol, name, coin_id, pair, cg_id in COINS:
        item = items.get(coin_id)
        quote = (item or {}).get("quotes", {}).get("USD", {})
        price = quote.get("price")
        if price is None:
            continue
        rows.append({
            "symbol": symbol,
            "name": name,
            "price": float(price),
            "change24h": float(quote.get("percent_change_24h") or 0),
            "source": "CoinPaprika",
        })
    if len(rows) < 5:
        raise RuntimeError("CoinPaprika returned too few prices")
    return rows


def binance_prices():
    response = session.get("https://api.binance.com/api/v3/ticker/24hr", timeout=12)
    response.raise_for_status()
    items = {x.get("symbol"): x for x in response.json() if x.get("symbol")}
    rows = []
    for symbol, name, coin_id, pair, cg_id in COINS:
        item = items.get(pair)
        if item:
            rows.append({"symbol": symbol, "name": name, "price": float(item["lastPrice"]),
                         "change24h": float(item.get("priceChangePercent") or 0), "source": "Binance"})
    if len(rows) < 5:
        raise RuntimeError("Binance returned too few prices")
    return rows


def coingecko_prices():
    ids = ",".join(x[4] for x in COINS)
    response = session.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "ids": ids, "price_change_percentage": "24h"},
        timeout=15,
    )
    response.raise_for_status()
    items = {x.get("id"): x for x in response.json() if x.get("id")}
    rows = []
    for symbol, name, coin_id, pair, cg_id in COINS:
        item = items.get(cg_id)
        if item and item.get("current_price") is not None:
            rows.append({"symbol": symbol, "name": name, "price": float(item["current_price"]),
                         "change24h": float(item.get("price_change_percentage_24h") or 0), "source": "CoinGecko"})
    if not rows:
        raise RuntimeError("CoinGecko returned no prices")
    return rows


def get_prices():
    now = time.time()
    with _cache_lock:
        if _cache["data"] and now - _cache["at"] < CACHE_SECONDS:
            return _cache["data"], False

    errors = []
    for provider in (paprika_prices, binance_prices, coingecko_prices):
        try:
            rows = provider()
            result = {
                "ok": True,
                "currency": "USD",
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "source": rows[0]["source"],
                "count": len(rows),
                "prices": rows,
            }
            with _cache_lock:
                _cache["data"], _cache["at"] = result, time.time()
            return result, False
        except Exception as exc:
            log.warning("%s failed: %s", provider.__name__, exc)
            errors.append(f"{provider.__name__}: {exc}")

    with _cache_lock:
        if _cache["data"]:
            stale = dict(_cache["data"])
            stale["stale"] = True
            stale["warning"] = "Показаны последние успешно полученные котировки."
            return stale, False
    return {"ok": False, "error": "Все источники котировок временно недоступны. Попробуйте позже.",
            "details": errors, "prices": []}, True


@app.get("/")
def home():
    return send_from_directory("static", "index.html")


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "CryptoPulse"})


@app.get("/api/prices")
def prices():
    data, failed = get_prices()
    return jsonify(data), (502 if failed else 200)


def telegram_api(method, payload):
    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    if not token:
        return None
    response = session.post(f"https://api.telegram.org/bot{token}/{method}", json=payload, timeout=10)
    response.raise_for_status()
    result = response.json()
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API {method} failed")
    return result


@app.post("/telegram-webhook")
def telegram_webhook():
    update = request.get_json(silent=True) or {}
    message = update.get("message") or {}
    text = (message.get("text") or "").strip()
    chat_id = (message.get("chat") or {}).get("id")
    if chat_id and text.startswith("/start"):
        url = os.getenv("WEBAPP_URL", "").strip().rstrip("/")
        payload = {"chat_id": chat_id, "text": "Добро пожаловать в CryptoPulse!"}
        if url:
            payload["text"] += " Нажми кнопку ниже, чтобы открыть котировки."
            payload["reply_markup"] = {"inline_keyboard": [[
                {"text": "📈 Открыть CryptoPulse", "web_app": {"url": url + "/"}}
            ]]}
        try:
            telegram_api("sendMessage", payload)
        except Exception:
            log.exception("Telegram response failed")
    return jsonify({"ok": True})


def configure_telegram():
    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    url = os.getenv("WEBAPP_URL", "").strip().rstrip("/")
    if token and url:
        try:
            telegram_api("setWebhook", {"url": url + "/telegram-webhook"})
            log.info("Telegram webhook configured")
        except Exception:
            log.exception("Telegram webhook setup failed; website can still serve prices")


# Gunicorn imports this module, so webhook configuration is done on import.
configure_telegram()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
