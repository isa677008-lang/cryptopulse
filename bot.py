import os
import time
import threading
import logging
from html import escape

import requests
from flask import Flask, jsonify, send_from_directory

# Set these in the hosting provider's Environment settings.
TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip().rstrip("/")

TELEGRAM_API = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""
BYBIT_API = "https://api.bybit.com/v5/market/tickers"
COINGECKO_API = "https://api.coingecko.com/api/v3/simple/price"

COINS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "TONUSDT", "TRXUSDT", "LTCUSDT",
    "DOTUSDT", "AVAXUSDT", "LINKUSDT", "BCHUSDT", "ETCUSDT",
]

app = Flask(__name__, static_folder="static")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def get_market_data():
    """Fetch spot tickers and USDT/RUB rate for the Mini App."""
    response = requests.get(
        BYBIT_API, params={"category": "spot"}, timeout=15
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("retCode") != 0:
        raise RuntimeError(f"Bybit returned an error: {payload.get('retMsg', 'unknown')}")

    by_symbol = {
        item.get("symbol"): item
        for item in payload.get("result", {}).get("list", [])
        if item.get("symbol") in COINS
    }

    coins = []
    for symbol in COINS:
        ticker = by_symbol.get(symbol)
        if not ticker:
            continue
        try:
            price = float(ticker["lastPrice"])
            change = float(ticker["price24hPcnt"]) * 100
        except (KeyError, TypeError, ValueError):
            continue

        coins.append({
            "symbol": symbol.replace("USDT", ""),
            "pair": symbol,
            "price": price,
            "change": change,
        })

    coins.sort(key=lambda item: item["change"], reverse=True)

    rate = None
    try:
        rate_response = requests.get(
            COINGECKO_API,
            params={"ids": "tether", "vs_currencies": "rub"},
            timeout=10,
        )
        rate_response.raise_for_status()
        rate = float(rate_response.json()["tether"]["rub"])
    except (requests.RequestException, KeyError, TypeError, ValueError) as exc:
        logging.warning("Could not fetch USDT/RUB rate: %s", exc)

    return {
        "coins": coins,
        "usdt_rub": rate,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "source": "Bybit Spot",
    }


@app.get("/")
def home():
    return send_from_directory("static", "index.html")


@app.get("/api/prices")
def api_prices():
    try:
        return jsonify(get_market_data())
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        logging.exception("Market data request failed")
        return jsonify({"error": "Не удалось получить котировки. Попробуйте ещё раз.", "details": str(exc)}), 502


@app.get("/health")
def health():
    return jsonify({"ok": True})


def telegram_call(method, payload=None, timeout=25):
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is not configured")
    response = requests.post(
        f"{TELEGRAM_API}/{method}",
        json=payload or {},
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API {method}: {data}")
    return data["result"]


def send_message(chat_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        telegram_call("sendMessage", payload, timeout=20)
    except (requests.RequestException, RuntimeError) as exc:
        logging.exception("Failed to send Telegram message: %s", exc)


def set_webapp_menu_button():
    if not WEBAPP_URL:
        logging.warning("WEBAPP_URL is not set; Mini App menu button is not configured.")
        return
    try:
        telegram_call("setChatMenuButton", {
            "menu_button": {
                "type": "web_app",
                "text": "Открыть CryptoPulse",
                "web_app": {"url": WEBAPP_URL},
            }
        })
        logging.info("Telegram menu button configured.")
    except (requests.RequestException, RuntimeError) as exc:
        logging.warning("Could not configure Telegram menu button: %s", exc)


def handle_message(message):
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    text = (message.get("text") or "").strip()
    if not chat_id:
        return

    command = text.split()[0].lower().split("@")[0] if text else ""

    if command in ("/start", "/help"):
        if WEBAPP_URL:
            markup = {
                "inline_keyboard": [[{
                    "text": "🚀 Открыть CryptoPulse",
                    "web_app": {"url": WEBAPP_URL},
                }]]
            }
            send_message(
                chat_id,
                "Привет! 👋\n\n"
                "Открой CryptoPulse, чтобы смотреть цены криптовалют, "
                "изменение за 24 часа и курс USDT/RUB.\n\n"
                "Котировки предоставляются Bybit Spot. Это не финансовая рекомендация.",
                markup,
            )
        else:
            send_message(
                chat_id,
                "Привет! CryptoPulse ещё не опубликован.\n"
                "После размещения Mini App добавьте WEBAPP_URL в настройки сервера."
            )
    elif command in ("/prices", "/rate"):
        # Keep old commands working while the Mini App becomes the main interface.
        try:
            market = get_market_data()
            if command == "/rate":
                rate = market["usdt_rub"]
                answer = "Курс USDT/RUB временно недоступен." if rate is None else f"1 USDT ≈ {rate:.2f} RUB"
            else:
                rows = ["📊 <b>КРИПТОРЫНОК BYBIT SPOT</b>", "<pre>", f"{'Монета':<11} {'Цена USDT':>14} {'Изм. 24ч':>12}", "─" * 39]
                for coin in market["coins"]:
                    change = coin["change"]
                    if change > 0:
                        marker = "🟢 ↑"
                        change_text = f"+{change:.2f}%"
                    elif change < 0:
                        marker = "🔴 ↓"
                        change_text = f"{change:.2f}%"
                    else:
                        marker = "⚪ →"
                        change_text = "0.00%"
                    price = coin["price"]
                    price_text = f"{price:.4f}" if price >= 1 else f"{price:.8f}".rstrip("0").rstrip(".")
                    rows.append(f"{coin['symbol']:<11} {price_text:>14} {marker} {change_text}")
                rows.append("</pre>")
                rate = market["usdt_rub"]
                rows.append("💱 USDT/RUB: " + ("курс временно недоступен" if rate is None else f"1 USDT ≈ {rate:.2f} RUB"))
                answer = "\n".join(rows)
            send_message(chat_id, answer)
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            logging.exception("Could not answer command: %s", exc)
            send_message(chat_id, "⚠️ Не удалось получить данные рынка. Попробуйте позже.")


def telegram_polling():
    if not TOKEN:
        logging.error("TELEGRAM_TOKEN is missing. Telegram bot polling will not start.")
        return

    # Drop old updates once at startup so stale messages aren't replayed.
    offset = None
    try:
        result = telegram_call("getUpdates", {"timeout": 1}, timeout=5)
        if result:
            offset = result[-1]["update_id"] + 1
    except (requests.RequestException, RuntimeError) as exc:
        logging.warning("Initial getUpdates failed: %s", exc)

    while True:
        try:
            payload = {"timeout": 25}
            if offset is not None:
                payload["offset"] = offset
            updates = telegram_call("getUpdates", payload, timeout=30)
            for update in updates:
                offset = update["update_id"] + 1
                message = update.get("message")
                if message:
                    handle_message(message)
        except (requests.RequestException, RuntimeError, KeyError, ValueError) as exc:
            logging.exception("Telegram polling error: %s", exc)
            time.sleep(5)


if __name__ == "__main__":
    if not TOKEN:
        logging.warning("TELEGRAM_TOKEN is empty. Website/API will work, but Telegram bot won't.")
    else:
        set_webapp_menu_button()
        threading.Thread(target=telegram_polling, daemon=True, name="telegram-polling").start()

    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
