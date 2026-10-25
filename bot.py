import os
import time
import logging
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request, send_from_directory


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

log = logging.getLogger("cryptopulse")


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

DEFAULT_URL = "https://cryptopulse-lpc9.onrender.com"

raw_url = os.getenv("WEBAPP_URL", "")
BASE_URL = raw_url.strip().strip("\"'").rstrip("/")

if not BASE_URL:
    log.warning(
        "WEBAPP_URL is empty or missing. Using default URL."
    )
    BASE_URL = DEFAULT_URL

parsed_url = urlparse(BASE_URL)

if (
    parsed_url.scheme.lower() != "https"
    or not parsed_url.netloc
):
    log.error(
        "Invalid WEBAPP_URL: repr=%r, length=%d. "
        "Using default URL.",
        BASE_URL,
        len(BASE_URL),
    )
    BASE_URL = DEFAULT_URL

log.info(
    "Web app URL validated: scheme=%s, host=%s, length=%d",
    urlparse(BASE_URL).scheme,
    urlparse(BASE_URL).netloc,
    len(BASE_URL),
)

TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()

app = Flask(__name__, static_folder="static")

http = requests.Session()
http.headers.update({
    "User-Agent": "CryptoPulse/3.0"
})


# --------------------------------------------------
# CRYPTOCURRENCIES
# --------------------------------------------------

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
    ("BCH", "Bitcoin Cash", "b
