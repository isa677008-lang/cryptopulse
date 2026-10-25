import os, logging, threading, time
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify, request, send_from_directory

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("cryptopulse")
app = Flask(__name__, static_folder="static")
session = requests.Session()
session.headers.update({"User-Agent": "CryptoPulse/1.0"})
COINS = [
("BTC","Bitcoin","BTCUSDT","bitcoin"),("ETH","Ethereum","ETHUSDT","ethereum"),
("BNB","BNB","BNBUSDT","binancecoin"),("SOL","Solana","SOLUSDT","solana"),
("XRP","XRP","XRPUSDT","ripple"),("ADA","Cardano","ADAUSDT","cardano"),
("DOGE","Dogecoin","DOGEUSDT","dogecoin"),("TON","Toncoin","TONUSDT","the-open-network"),
("TRX","TRON","TRXUSDT","tron"),("LTC","Litecoin","LTCUSDT","litecoin"),
("DOT","Polkadot","DOTUSDT","polkadot"),("AVAX","Avalanche","AVAXUSDT","avalanche-2"),
("LINK","Chainlink","LINKUSDT","chainlink"),("BCH","Bitcoin Cash","BCHUSDT","bitcoin-cash"),
("ETC","Ethereum Classic","ETCUSDT","ethereum-classic")]
cache, cache_at, lock = None, 0, threading.Lock()

def binance_prices():
    r=session.get("https://api.binance.com/api/v3/ticker/24hr",timeout=12); r.raise_for_status()
    items={x.get("symbol"):x for x in r.json() if x.get("symbol")}
    rows=[]
    for sym,name,pair,cgid in COINS:
        x=items.get(pair)
        if x:
            try: rows.append({"symbol":sym,"name":name,"price":float(x["lastPrice"]),"change24h":float(x.get("priceChangePercent",0)),"source":"Binance"})
            except (ValueError,TypeError,KeyError): pass
    if len(rows)<5: raise RuntimeError("Binance returned too few prices")
    return rows

def coingecko_prices():
    ids=",".join(x[3] for x in COINS)
    r=session.get("https://api.coingecko.com/api/v3/coins/markets",params={"vs_currency":"usd","ids":ids,"price_change_percentage":"24h"},timeout=15)
    r.raise_for_status(); items={x.get("id"):x for x in r.json() if x.get("id")}; rows=[]
    for sym,name,pair,cgid in COINS:
        x=items.get(cgid)
        if x and x.get("current_price") is not None:
            rows.append({"symbol":sym,"name":name,"price":float(x["current_price"]),"change24h":float(x.get("price_change_percentage_24h") or 0),"source":"CoinGecko"})
    if not rows: raise RuntimeError("CoinGecko returned no prices")
    return rows

def get_prices():
    global cache, cache_at
    with lock:
        if cache and time.time()-cache_at<20: return cache, False
    errors=[]
    for fn in (binance_prices,coingecko_prices):
        try:
            rows=fn(); data={"ok":True,"currency":"USD","updatedAt":datetime.now(timezone.utc).isoformat(),"source":rows[0]["source"],"count":len(rows),"prices":rows}
            with lock: cache,cache_at=data,time.time()
            return data,False
        except Exception as e:
            log.warning("%s failed: %s",fn.__name__,e); errors.append(str(e))
    with lock:
        if cache:
            old=dict(cache); old["stale"]=True; old["warning"]="Показаны последние успешно полученные котировки."
            return old,False
    return {"ok":False,"error":"Не удалось получить котировки от публичных API. Попробуйте позже.","details":errors,"prices":[]},True

@app.get("/")
def home(): return send_from_directory("static","index.html")
@app.get("/health")
def health(): return jsonify({"ok":True,"service":"CryptoPulse"})
@app.get("/api/prices")
def prices():
    data,failed=get_prices()
    return jsonify(data),(502 if failed else 200)

def tg(method,payload):
    token=os.getenv("TELEGRAM_TOKEN","").strip()
    if not token: return
    r=session.post(f"https://api.telegram.org/bot{token}/{method}",json=payload,timeout=10); r.raise_for_status()
    if not r.json().get("ok"): raise RuntimeError("Telegram API returned ok=false")

@app.post("/telegram-webhook")
def webhook():
    update=request.get_json(silent=True) or {}
    message=update.get("message") or {}
    text=(message.get("text") or "").strip()
    chat_id=(message.get("chat") or {}).get("id")
    if chat_id and text.startswith("/start"):
        url=os.getenv("WEBAPP_URL","").strip().rstrip("/")
        payload={"chat_id":chat_id,"text":"Добро пожаловать в CryptoPulse! Нажми кнопку ниже, чтобы открыть котировки."}
        if url: payload["reply_markup"]={"inline_keyboard":[[{"text":"📈 Открыть CryptoPulse","web_app":{"url":url+"/"}}]]}
        try: tg("sendMessage",payload)
        except Exception: log.exception("Telegram response failed")
    return jsonify({"ok":True})

def configure_telegram():
    token=os.getenv("TELEGRAM_TOKEN","").strip(); url=os.getenv("WEBAPP_URL","").strip().rstrip("/")
    if token and url:
        try:
            tg("setWebhook",{"url":url+"/telegram-webhook"})
            log.info("Telegram webhook configured")
        except Exception: log.exception("Telegram webhook setup failed")

configure_telegram()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")))
