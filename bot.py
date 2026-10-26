import os, time, hmac, math, logging, threading, json, csv, io
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, send_from_directory, request, Response

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

app = Flask(__name__, static_folder="static")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cryptopulse")

BASE = os.getenv("BYBIT_BASE_URL","https://api.bybit.com").rstrip("/")
PUBLIC_APP_URL = os.getenv("PUBLIC_APP_URL","https://cryptopulse-1-chtf.onrender.com").rstrip("/")
WEBAPP_URL = os.getenv("TELEGRAM_WEBAPP_URL", PUBLIC_APP_URL + "/")
WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", PUBLIC_APP_URL + "/telegram-webhook")

TG_TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
TG_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET","").strip()
TG_AUTO = os.getenv("TELEGRAM_AUTO_CONFIGURE","true").lower() in ("1","true","yes","on")
BG_WORKER = os.getenv("ENABLE_BACKGROUND_WORKER","true").lower() in ("1","true","yes","on")

API_KEY = os.getenv("BYBIT_API_KEY","").strip()
API_SECRET = os.getenv("BYBIT_API_SECRET","").strip()

DATABASE_URL = os.getenv("DATABASE_URL","").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://","postgresql://",1)

TRADING_MODE = "simulation"
LIVE_TRADING_ENABLED = False

TIMEOUT = 12
CACHE_SECONDS = 30
HISTORY_HOURS = 72
REFRESH_SECONDS = 60

BOT_STATE_FILE = os.getenv("BOT_STATE_FILE","bot_state.json")
BOT_TICK_SECONDS = 15
BOT_START_BALANCE = 100.0

# ТУТ ТЕ САМЫЕ 15 МОНЕТ
DEFAULT_STRATEGY = {
    "emaFast": 9, "emaSlow": 21, "rsiPeriod": 14,
    "rsiBuyMin": 50, "rsiBuyMax": 68,
    "rsiSellMin": 32, "rsiSellMax": 50,
    "tpPercent": 2.0, "slPercent": 1.5,
    "maxPositions": 3,
    "topCoins": ["BTC","ETH","SOL","BNB","XRP","DOGE","ADA","AVAX","TON","LINK","DOT","LTC","TRX","NEAR","SUI"]
}

session = requests.Session()
session.headers.update({"User-Agent":"CryptoPulse/3.0","Accept":"application/json"})

lock = threading.RLock()
refresh_lock = threading.Lock()
db_lock = threading.Lock()

cache = {"updated":0,"prices":{},"changes":{},"source":"bybit","error":None}
history = {}
usd_rub_cache = {"updated":0,"rate":None}
bybit_status = {"available":False,"lastError":None,"lastCheck":None}
db_status = {"configured":bool(DATABASE_URL),"available":False,"lastError":None}
wallet_cache = {"updated":0,"data":None}

bot_state = {
    "running":False,"emergency":False,"reason":"Не запущен",
    "settings":{"profitTarget":100.0,"maxLoss":10.0,"tradeSize":20.0,"startBalance":BOT_START_BALANCE},
    "strategy":dict(DEFAULT_STRATEGY),
    "balance":BOT_START_BALANCE,"equityCurve":[],
    "positions":[],"closedTrades":[],"logs":[],
    "startedAt":None,"stoppedAt":None,
    "lastAction":None,"lastSignalScan":None,"scanResults":[]
}
bot_state_lock = threading.RLock()

def now_iso(): return datetime.now(timezone.utc).isoformat()

def get_json(url, params=None, headers=None):
    r = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
    r.raise_for_status(); return r.json()

def bot_save():
    try:
        with bot_state_lock:
            data = {k: bot_state[k] for k in ("running","emergency","reason","settings","strategy",
                    "balance","positions","closedTrades","logs","startedAt","stoppedAt",
                    "lastAction","lastSignalScan","scanResults")}
            data["equityCurve"] = bot_state["equityCurve"][-2000:]
            data["closedTrades"] = bot_state["closedTrades"][-1000:]
            data["logs"] = bot_state["logs"][-500:]
        with open(BOT_STATE_FILE,"w",encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as exc: log.warning("save fail: %s", exc)

def bot_load():
    try:
        if not os.path.exists(BOT_STATE_FILE): return
        with open(BOT_STATE_FILE,"r",encoding="utf-8") as f: data = json.load(f)
        with bot_state_lock:
            for k in data:
                if k in bot_state: bot_state[k] = data[k]
            merged = dict(DEFAULT_STRATEGY)
            if bot_state.get("strategy"): merged.update(bot_state["strategy"])
            bot_state["strategy"] = merged
    except Exception as exc: log.warning("load fail: %s", exc)

def bot_log(text, color="blue"):
    entry = {"time":datetime.now().strftime("%H:%M:%S"),"text":text,"color":color,"ts":time.time()}
    with bot_state_lock:
        bot_state["logs"].insert(0, entry)
        bot_state["logs"] = bot_state["logs"][:500]
    bot_save()

def push_equity_point():
    with bot_state_lock:
        bot_state["equityCurve"].append({"ts":time.time(),"balance":bot_state["balance"]})
        bot_state["equityCurve"] = bot_state["equityCurve"][-2000:]

def db_connect():
    if not DATABASE_URL: raise RuntimeError("no db")
    if psycopg is None: raise RuntimeError("no psycopg")
    return psycopg.connect(DATABASE_URL, connect_timeout=8, row_factory=dict_row)

def init_db():
    if not DATABASE_URL: return
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS price_history (
                    id BIGSERIAL PRIMARY KEY, source TEXT NOT NULL, symbol TEXT NOT NULL,
                    price DOUBLE PRECISION NOT NULL CHECK(price > 0),
                    observed_at TIMESTAMPTZ NOT NULL, UNIQUE(source, symbol, observed_at))""")
                cur.execute("""CREATE INDEX IF NOT EXISTS idx_price_history_symbol_time
                    ON price_history(symbol, observed_at DESC)""")
                conn.commit()
        db_status.update(available=True, lastError=None)
    except Exception as exc:
        db_status.update(available=False, lastError=str(exc))
        log.exception("db init failed")

def remember(prices, source="bybit"):
    timestamp = time.time()
    cutoff = timestamp - HISTORY_HOURS * 3600
    valid = {}
    with lock:
        for symbol, value in prices.items():
            try: value = float(value)
            except: continue
            if not math.isfinite(value) or value <= 0: continue
            valid[symbol] = value
            pts = history.setdefault((source, symbol), [])
            pts.append({"timestamp":timestamp,"price":value})
            history[(source, symbol)] = [p for p in pts if p["timestamp"] >= cutoff]
    if not DATABASE_URL or not valid: return
    try:
        with db_lock:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""INSERT INTO price_history (source,symbol,price,observed_at)
                        VALUES (%s,%s,%s,to_timestamp(%s))
                        ON CONFLICT (source,symbol,observed_at) DO NOTHING""",
                        [(source,s,p,timestamp) for s,p in valid.items()])
                    cur.execute("""DELETE FROM price_history WHERE observed_at < NOW() - (%s * INTERVAL '1 hour')""",
                                (HISTORY_HOURS,))
                    conn.commit()
        db_status.update(available=True, lastError=None)
    except Exception as exc:
        db_status.update(available=False, lastError=str(exc))

def get_points(symbol):
    with lock: pts = list(history.get(("bybit",symbol), []))
    cutoff = time.time() - HISTORY_HOURS * 3600
    return [p for p in pts if p["timestamp"] >= cutoff]

def ema(values, period):
    values = [float(v) for v in values if v and float(v) > 0]
    if len(values) < period: return None
    r = sum(values[:period]) / period; m = 2 / (period+1)
    for v in values[period:]: r = (v-r)*m + r
    return r

def rsi(values, period=14):
    values = [float(v) for v in values if v and float(v) > 0]
    if len(values) < period+1: return None
    ch = [values[i]-values[i-1] for i in range(1,len(values))][-period:]
    g = sum(max(v,0) for v in ch)/period; l = sum(max(-v,0) for v in ch)/period
    if l == 0: return 100.0 if g else 50.0
    return 100 - 100/(1 + g/l)

def analyze(symbol, price, strategy=None):
    st = strategy or bot_state["strategy"]
    pts = sorted(get_points(symbol), key=lambda p: p["timestamp"])
    values = [p["price"] for p in pts]
    if not values or values[-1] != price: values.append(price)
    e9 = ema(values, int(st["emaFast"])); e21 = ema(values, int(st["emaSlow"]))
    r14 = rsi(values, int(st["rsiPeriod"]))
    signal = "insufficient_history"; expl = "Нужно больше истории."
    if len(values) >= 30 and e9 is not None and e21 is not None and r14 is not None:
        if price > e9 > e21 and st["rsiBuyMin"] <= r14 < st["rsiBuyMax"]:
            signal = "buy_candidate"; expl = "Бычий сигнал."
        elif price < e9 < e21 and st["rsiSellMin"] < r14 <= st["rsiSellMax"]:
            signal = "sell_candidate"; expl = "Медвежий сигнал."
        else:
            signal = "neutral"; expl = "Условия не совпали."
    return {"symbol":symbol,"price":price,"ema9":e9,"ema21":e21,"rsi14":r14,
            "sampleCount":len(values),"signal":signal,"explanation":expl,"mode":"simulation"}

def fetch_prices():
    data = get_json(BASE + "/v5/market/tickers", {"category":"spot"})
    if data.get("retCode") != 0: raise RuntimeError(data.get("retMsg","err"))
    prices, changes = {}, {}
    for item in (data.get("result") or {}).get("list", []):
        pair = str(item.get("symbol","")).upper()
        if not pair.endswith("USDT"): continue
        sym = pair[:-4]
        try: val = float(item.get("lastPrice"))
        except: continue
        if not math.isfinite(val) or val <= 0: continue
        prices[sym] = val
        try: changes[sym] = float(item.get("price24hPcnt"))*100
        except: changes[sym] = None
    if not prices: raise RuntimeError("no prices")
    bybit_status.update(available=True, lastError=None, lastCheck=now_iso())
    return prices, changes

def refresh(force=False):
    if cache["prices"] and time.time() - cache["updated"] < CACHE_SECONDS and not force:
        return dict(cache["prices"]), dict(cache["changes"]), cache["error"]
    with refresh_lock:
        if cache["prices"] and time.time() - cache["updated"] < CACHE_SECONDS and not force:
            return dict(cache["prices"]), dict(cache["changes"]), cache["error"]
        try:
            prices, changes = fetch_prices(); remember(prices)
            cache.update(updated=time.time(), prices=prices, changes=changes, error=None)
            return dict(prices), dict(changes), None
        except Exception as exc:
            bybit_status.update(available=False, lastError=str(exc), lastCheck=now_iso())
            cache["error"] = str(exc)
            if cache["prices"]: return dict(cache["prices"]), dict(cache["changes"]), str(exc)
            raise

def usd_rub():
    if usd_rub_cache["rate"] and time.time() - usd_rub_cache["updated"] < 900:
        return usd_rub_cache["rate"]
    data = get_json("https://open.er-api.com/v6/latest/USD")
    rate = float(data["rates"]["RUB"])
    if not math.isfinite(rate) or rate <= 0: raise RuntimeError("bad rate")
    usd_rub_cache.update(updated=time.time(), rate=rate)
    return rate

def market_payload(force=False):
    prices, changes, warning = refresh(force)
    try: rate = usd_rub(); rate_err = None
    except Exception as exc: rate = None; rate_err = str(exc)
    coins = []
    for sym, price in prices.items():
        pts = [p for p in get_points(sym) if 2700 <= time.time()-p["timestamp"] <= 4500]
        c1h = None
        if pts:
            ref = min(pts, key=lambda p: abs(time.time()-p["timestamp"]-3600))["price"]
            c1h = (price/ref - 1)*100
        coins.append({"symbol":sym,"name":sym,"priceUsd":price,
                      "priceRub":price*rate if rate else None,"change1h":c1h,
                      "change24h":changes.get(sym),"source":"bybit"})
    coins.sort(key=lambda c: c["symbol"])
    return {"success":True,"updatedAt":now_iso(),"source":"bybit","coins":coins,
            "usdRub":rate,"usdRubError":rate_err,
            "databaseAvailable":db_status["available"],"warning":warning}

def signed_get(path, params):
    if not API_KEY or not API_SECRET: raise RuntimeError("no key")
    ts = str(int(time.time()*1000)); rw = "5000"
    q = urlencode(sorted((k,str(v)) for k,v in params.items()))
    sig = hmac.new(API_SECRET.encode(), (ts+API_KEY+rw+q).encode(), "sha256").hexdigest()
    h = {"X-BAPI-API-KEY":API_KEY,"X-BAPI-TIMESTAMP":ts,"X-BAPI-RECV-WINDOW":rw,
         "X-BAPI-SIGN":sig,"X-BAPI-SIGN-TYPE":"2"}
    data = get_json(BASE+path, params=params, headers=h)
    if data.get("retCode") != 0: raise RuntimeError(f"err {data.get('retCode')}: {data.get('retMsg')}")
    return data.get("result") or {}

def wallet_balance(force=False):
    if wallet_cache["data"] and time.time()-wallet_cache["updated"]<60 and not force:
        return wallet_cache["data"]
    result = signed_get("/v5/account/wallet-balance", {"accountType":"UNIFIED"})
    assets, total = [], 0.0
    for acc in result.get("list", []):
        for coin in acc.get("coin", []):
            try: bal = float(coin.get("walletBalance") or 0); usd = float(coin.get("usdValue") or 0)
            except: continue
            if bal == 0 and usd == 0: continue
            assets.append({"coin":coin.get("coin",""),"walletBalance":bal,"usdValue":usd,
                           "availableToWithdraw":coin.get("availableToWithdraw")})
            total += usd
    assets.sort(key=lambda a: a["usdValue"], reverse=True)
    data = {"success":True,"available":True,"accountType":"UNIFIED","totalUsd":total,
            "assets":assets,"updatedAt":now_iso()}
    wallet_cache.update(updated=time.time(), data=data)
    return data

def bot_open_position(symbol, price, side, reason=""):
    with bot_state_lock:
        st = bot_state["strategy"]; settings = bot_state["settings"]
        size = float(settings["tradeSize"])
        if bot_state["balance"] < size: return None
        for p in bot_state["positions"]:
            if p["symbol"] == symbol: return None
        if len(bot_state["positions"]) >= int(st["maxPositions"]): return None
        qty = size/price
        tp_pct = float(st["tpPercent"])/100; sl_pct = float(st["slPercent"])/100
        if side == "Long": tp = price*(1+tp_pct); sl = price*(1-sl_pct)
        else: tp = price*(1-tp_pct); sl = price*(1+sl_pct)
        pos = {"id":int(time.time()*1000)+len(bot_state["positions"]),
               "symbol":symbol,"side":side,"sizeUsd":size,"entryPrice":price,"qty":qty,
               "openedAt":now_iso(),"openedTs":time.time(),"tp":tp,"sl":sl,"reason":reason}
        bot_state["positions"].append(pos)
        bot_state["lastAction"] = f"Открыта {side} {symbol}/USDT @ ${price:,.4f}"
    bot_log(f"Открыта {side} {symbol}/USDT. Вход ${price:,.4f}, TP ${tp:,.4f}, SL ${sl:,.4f}", "blue")
    bot_save(); return pos

def bot_close_position(pos, price, reason="manual"):
    with bot_state_lock:
        if pos not in bot_state["positions"]: return None
        bot_state["positions"].remove(pos)
        pnl = (price - pos["entryPrice"])*pos["qty"] if pos["side"]=="Long" else (pos["entryPrice"]-price)*pos["qty"]
        pnl_pct = (pnl/pos["sizeUsd"])*100 if pos["sizeUsd"] else 0
        bot_state["balance"] += pnl
        trade = {"symbol":pos["symbol"],"side":pos["side"],"sizeUsd":pos["sizeUsd"],
                 "entryPrice":pos["entryPrice"],"exitPrice":price,"pnl":pnl,"pnlPct":pnl_pct,
                 "reason":reason,"openedAt":pos["openedAt"],"closedAt":now_iso(),"closedTs":time.time()}
        bot_state["closedTrades"].insert(0, trade)
        bot_state["closedTrades"] = bot_state["closedTrades"][:1000]
        bot_state["lastAction"] = f"Закрыта {pos['side']} {pos['symbol']}/USDT: {pnl:+.2f}$ [{reason}]"
    push_equity_point()
    bot_log(f"Закрыта {pos['side']} {pos['symbol']}/USDT @ ${price:,.4f}. P&L {pnl:+.2f}$ ({pnl_pct:+.2f}%) [{reason}]",
            "green" if pnl >= 0 else "red")
    bot_save(); return trade

def bot_check_limits():
    with bot_state_lock:
        s = bot_state["settings"]; start = float(s["startBalance"]); bal = float(bot_state["balance"])
        diff = bal - start
        if diff >= float(s["profitTarget"]): return ("profit", f"Достигнут лимит прибыли (+${diff:.2f})")
        if diff <= -float(s["maxLoss"]): return ("loss", f"Достигнут лимит убытка (-${abs(diff):.2f})")
    return (None, None)

def bot_close_all(reason="manual"):
    try: prices, _, _ = refresh()
    except: prices = {}
    with bot_state_lock: positions = list(bot_state["positions"])
    for pos in positions:
        bot_close_position(pos, prices.get(pos["symbol"], pos["entryPrice"]), reason)

def bot_tick():
    with bot_state_lock:
        if not bot_state["running"]: return
    try: prices, _, _ = refresh(force=True)
    except Exception as exc: bot_log(f"Цены: {exc}", "red"); return
    with bot_state_lock: positions = list(bot_state["positions"])
    for pos in positions:
        price = prices.get(pos["symbol"])
        if price is None: continue
        if pos["side"] == "Long":
            if price >= pos["tp"]: bot_close_position(pos, price, "TP"); continue
            if price <= pos["sl"]: bot_close_position(pos, price, "SL"); continue
        else:
            if price <= pos["tp"]: bot_close_position(pos, price, "TP"); continue
            if price >= pos["sl"]: bot_close_position(pos, price, "SL"); continue
    kind, msg = bot_check_limits()
    if kind:
        bot_log(msg + ". Останавливаем.", "gold" if kind=="profit" else "red")
        bot_close_all("limits")
        with bot_state_lock:
            bot_state["running"] = False; bot_state["reason"] = msg
            bot_state["stoppedAt"] = now_iso(); bot_state["lastAction"] = msg
        bot_save(); return
    st = bot_state["strategy"]; top = st.get("topCoins", DEFAULT_STRATEGY["topCoins"])
    scan = []
    for sym in top:
        price = prices.get(sym)
        if price is None: continue
        a = analyze(sym, price, st)
        scan.append({"symbol":sym,"price":price,"rsi14":a["rsi14"],"ema9":a["ema9"],
                     "ema21":a["ema21"],"signal":a["signal"],"explanation":a["explanation"],
                     "sampleCount":a["sampleCount"]})
    with bot_state_lock:
        bot_state["scanResults"] = scan
        bot_state["lastSignalScan"] = now_iso()
    bot_save()
    with bot_state_lock:
        if len(bot_state["positions"]) >= int(st["maxPositions"]): return
        current = {p["symbol"] for p in bot_state["positions"]}
        max_new = int(st["maxPositions"]) - len(bot_state["positions"])
    opened = 0
    for item in scan:
        if opened >= max_new: break
        if item["symbol"] in current: continue
        if item["signal"] == "buy_candidate":
            if bot_open_position(item["symbol"], item["price"], "Long", "EMA/RSI"): opened += 1
        elif item["signal"] == "sell_candidate":
            if bot_open_position(item["symbol"], item["price"], "Short", "EMA/RSI"): opened += 1
    with bot_state_lock:
        if not bot_state["positions"]: bot_state["lastAction"] = "Сканирует — сигналов нет."
        else: bot_state["lastAction"] = f"Держит {len(bot_state['positions'])} позиций. Ждёт TP/SL."

def bot_worker():
    while True:
        try: bot_tick()
        except Exception: log.exception("tick fail")
        time.sleep(BOT_TICK_SECONDS)

def compute_stats():
    with bot_state_lock:
        trades = list(bot_state["closedTrades"]); positions = list(bot_state["positions"])
        balance = float(bot_state["balance"]); start = float(bot_state["settings"]["startBalance"])
        equity = list(bot_state["equityCurve"])
    wins = [t for t in trades if t["pnl"] > 0]; losses = [t for t in trades if t["pnl"] < 0]
    total = len(trades); winrate = (len(wins)/total*100) if total else 0
    sp = sum(t["pnl"] for t in wins) if wins else 0
    sl = abs(sum(t["pnl"] for t in losses)) if losses else 0
    pf = (sp/sl) if sl > 0 else (float('inf') if sp > 0 else 0)
    aw = (sp/len(wins)) if wins else 0
    al = (-sl/len(losses)) if losses else 0
    best = max((t["pnl"] for t in trades), default=0)
    worst = min((t["pnl"] for t in trades), default=0)
    max_dd = 0.0; peak = -1e18
    for p in equity:
        peak = max(peak, p["balance"]); dd = peak - p["balance"]
        if dd > max_dd: max_dd = dd
    by_sym = {}
    for t in trades:
        s = t["symbol"]
        by_sym.setdefault(s, {"symbol":s,"trades":0,"pnl":0.0,"wins":0,"losses":0})
        by_sym[s]["trades"] += 1; by_sym[s]["pnl"] += t["pnl"]
        if t["pnl"] > 0: by_sym[s]["wins"] += 1
        elif t["pnl"] < 0: by_sym[s]["losses"] += 1
    try: prices, _, _ = refresh()
    except: prices = {}
    positions_live = []; open_pnl = 0.0
    for p in positions:
        cur = prices.get(p["symbol"], p["entryPrice"])
        pnl = (cur-p["entryPrice"])*p["qty"] if p["side"]=="Long" else (p["entryPrice"]-cur)*p["qty"]
        open_pnl += pnl
        positions_live.append({**p,"currentPrice":cur,"pnl":pnl,
                               "pnlPct":(pnl/p["sizeUsd"])*100 if p["sizeUsd"] else 0})
    return {"trades":total,"wins":len(wins),"losses":len(losses),"winrate":winrate,
            "sumProfit":sp,"sumLoss":sl,"profitFactor":pf,"avgWin":aw,"avgLoss":al,
            "best":best,"worst":worst,"maxDrawdown":max_dd,
            "bySymbol":sorted(by_sym.values(), key=lambda x: x["pnl"], reverse=True),
            "balance":balance,"startBalance":start,
            "totalPnl":balance-start+open_pnl,"openPnl":open_pnl,"closedPnl":balance-start,
            "openPositions":len(positions_live),"positionsLive":positions_live,
            "equityCurve":equity}

@app.after_request
def headers(response):
    response.headers.setdefault("X-Content-Type-Options","nosniff")
    response.headers.setdefault("X-Frame-Options","SAMEORIGIN")
    return response

@app.route("/")
def index(): return send_from_directory(app.static_folder, "index.html")

@app.route("/health")
def health(): return jsonify({"status":"ok","service":"CryptoPulse","timestamp":now_iso()})

@app.route("/api/config")
def config():
    return jsonify({"success":True,"tradingMode":"simulation","liveTradingEnabled":False,
                    "databaseConfigured":bool(DATABASE_URL),"databaseAvailable":db_status["available"]})

@app.route("/api/prices")
@app.route("/api/market-prices")
def prices_api():
    try: return jsonify(market_payload())
    except: return jsonify({"success":False,"error":"Рынок недоступен","coins":[]}), 503

@app.route("/api/usd-rub")
def rub_api():
    try: return jsonify({"success":True,"rate":usd_rub(),"base":"USD","currency":"RUB"})
    except: return jsonify({"success":False,"error":"Курс недоступен"}), 503

@app.route("/api/account/balance")
@app.route("/api/bybit/balance")
def balance_api():
    try: return jsonify(wallet_balance())
    except Exception as exc:
        log.warning("bal: %s", exc)
        return jsonify({"success":False,"available":False,"error":"Баланс недоступен"}), 503

@app.route("/api/trading/signals")
def signals_api():
    try:
        payload = market_payload(); st = bot_state["strategy"]; out = []
        for coin in payload["coins"]:
            item = analyze(coin["symbol"], coin["priceUsd"], st)
            item["change1h"] = coin["change1h"]; item["change24h"] = coin["change24h"]
            out.append(item)
        return jsonify({"success":True,"signals":out})
    except: return jsonify({"success":False,"signals":[]}), 503

@app.route("/api/klines")
def klines_api():
    sym = (request.args.get("symbol") or "BTCUSDT").upper().strip()
    iv = (request.args.get("interval") or "5m").strip()
    bi = {"1m":"1","3m":"3","5m":"5","15m":"15","30m":"30","1h":"60","2h":"120",
          "4h":"240","6h":"360","12h":"720","1d":"D","1w":"W"}.get(iv, iv)
    try: lim = max(1, min(int(request.args.get("limit","200")), 1000))
    except: lim = 200
    try:
        data = get_json(BASE+"/v5/market/kline", {"category":"spot","symbol":sym,"interval":bi,"limit":lim})
        if data.get("retCode") != 0: raise RuntimeError(data.get("retMsg"))
        raw = (data.get("result") or {}).get("list", [])
        candles = []
        for it in reversed(raw):
            try: candles.append({"time":int(it[0]),"open":float(it[1]),"high":float(it[2]),
                                 "low":float(it[3]),"close":float(it[4]),
                                 "volume":float(it[5]) if len(it)>5 else 0})
            except: continue
        return jsonify({"success":True,"symbol":sym,"interval":iv,"candles":candles})
    except Exception as exc:
        return jsonify({"success":False,"error":"Свечи недоступны","details":str(exc)}), 503

@app.route("/api/bot/state")
def bot_state_api():
    stats = compute_stats()
    with bot_state_lock:
        return jsonify({"success":True,"running":bot_state["running"],
                        "emergency":bot_state["emergency"],"reason":bot_state["reason"],
                        "settings":bot_state["settings"],"strategy":bot_state["strategy"],
                        "balance":bot_state["balance"]+stats["openPnl"],
                        "startBalance":stats["startBalance"],
                        "totalPnl":stats["totalPnl"],
                        "totalPnlPct":(stats["totalPnl"]/stats["startBalance"]*100) if stats["startBalance"] else 0,
                        "positions":stats["positionsLive"],
                        "closedTrades":bot_state["closedTrades"][:100],
                        "logs":bot_state["logs"][:100],
                        "startedAt":bot_state["startedAt"],"stoppedAt":bot_state["stoppedAt"],
                        "lastAction":bot_state["lastAction"],
                        "lastSignalScan":bot_state["lastSignalScan"],
                        "scanResults":bot_state["scanResults"],
                        "stats":{"trades":stats["trades"],"wins":stats["wins"],"losses":stats["losses"],
                                 "winrate":stats["winrate"],
                                 "profitFactor":stats["profitFactor"] if math.isfinite(stats["profitFactor"]) else None,
                                 "avgWin":stats["avgWin"],"avgLoss":stats["avgLoss"],
                                 "best":stats["best"],"worst":stats["worst"],
                                 "maxDrawdown":stats["maxDrawdown"],"bySymbol":stats["bySymbol"],
                                 "equityCurve":stats["equityCurve"][-500:]}})

@app.route("/api/bot/start", methods=["POST"])
def bot_start_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        if "settings" in data and isinstance(data["settings"], dict):
            for k in ("profitTarget","maxLoss","tradeSize"):
                if k in data["settings"]:
                    try: bot_state["settings"][k] = float(data["settings"][k])
                    except: pass
        bot_state["running"] = True; bot_state["emergency"] = False
        bot_state["reason"] = "Бот запущен"; bot_state["startedAt"] = now_iso()
        bot_state["stoppedAt"] = None; bot_state["lastAction"] = "Запущен. Сканирует…"
    push_equity_point(); bot_log("Бот запущен.", "green")
    return jsonify({"success":True})

@app.route("/api/bot/stop", methods=["POST"])
def bot_stop_api():
    with bot_state_lock:
        bot_state["running"] = False; bot_state["reason"] = "Остановлен вручную"
        bot_state["stoppedAt"] = now_iso(); bot_state["lastAction"] = "Остановлен вручную"
    bot_log("Бот остановлен.", "red"); return jsonify({"success":True})

@app.route("/api/bot/emergency", methods=["POST"])
def bot_emergency_api():
    bot_close_all("emergency")
    with bot_state_lock:
        bot_state["running"] = False; bot_state["emergency"] = True
        bot_state["reason"] = "Аварийная остановка"; bot_state["stoppedAt"] = now_iso()
        bot_state["lastAction"] = "Аварийная остановка"
    bot_log("АВАРИЙНАЯ ОСТАНОВКА.", "red"); return jsonify({"success":True})

@app.route("/api/bot/settings", methods=["POST"])
def bot_settings_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        for k in ("profitTarget","maxLoss","tradeSize"):
            if k in data:
                try: bot_state["settings"][k] = float(data[k])
                except: pass
    bot_log("Настройки сохранены.", "blue")
    return jsonify({"success":True,"settings":bot_state["settings"]})

@app.route("/api/bot/strategy", methods=["POST"])
def bot_strategy_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        for k in ("emaFast","emaSlow","rsiPeriod","rsiBuyMin","rsiBuyMax",
                  "rsiSellMin","rsiSellMax","maxPositions"):
            if k in data:
                try: bot_state["strategy"][k] = int(data[k])
                except: pass
        for k in ("tpPercent","slPercent"):
            if k in data:
                try: bot_state["strategy"][k] = float(data[k])
                except: pass
    bot_log("Стратегия обновлена.", "blue")
    return jsonify({"success":True,"strategy":bot_state["strategy"]})

@app.route("/api/bot/reset", methods=["POST"])
def bot_reset_api():
    with bot_state_lock:
        bot_state.update({"running":False,"emergency":False,"reason":"Сброшено",
                          "balance":float(bot_state["settings"].get("startBalance",100.0)),
                          "positions":[],"closedTrades":[],"logs":[],"equityCurve":[],
                          "startedAt":None,"stoppedAt":None,"lastAction":"Сброшено","scanResults":[]})
    bot_log("Бот сброшен.", "gold"); return jsonify({"success":True})

@app.route("/api/bot/export.csv")
def bot_export_csv():
    with bot_state_lock: trades = list(bot_state["closedTrades"])
    out = io.StringIO(); w = csv.writer(out)
    w.writerow(["Time","Symbol","Side","SizeUSD","EntryPrice","ExitPrice","PnL","PnL%","Reason"])
    for t in trades:
        w.writerow([t.get("closedAt",""),t.get("symbol",""),t.get("side",""),
                    f"{t.get('sizeUsd',0):.2f}",f"{t.get('entryPrice',0):.8f}",
                    f"{t.get('exitPrice',0):.8f}",f"{t.get('pnl',0):.4f}",
                    f"{t.get('pnlPct',0):.4f}",t.get("reason","")])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=cryptopulse_trades.csv"})

def tg(method, payload):
    if not TG_TOKEN: raise RuntimeError("no token")
    r = session.post(f"https://api.telegram.org/bot{TG_TOKEN}/{method}", json=payload, timeout=TIMEOUT)
    r.raise_for_status(); return r.json()

def tg_send(chat, text, markup=None):
    p = {"chat_id":chat,"text":text}
    if markup: p["reply_markup"] = markup
    return tg("sendMessage", p)

def keyboard():
    return {"inline_keyboard":[
        [{"text":"🌐 CryptoPulse","web_app":{"url":WEBAPP_URL}},
         {"text":"📊 Цены","callback_data":"prices"}],
        [{"text":"💱 USD/RUB","callback_data":"usdrub"},
         {"text":"🧮 Калькулятор","web_app":{"url":WEBAPP_URL+"?page=calculator"}}]
    ]}

def telegram_prices():
    payload = market_payload()
    lines = ["CryptoPulse — Bybit Spot", ""]
    for c in payload["coins"][:40]:
        ch = "—" if c["change24h"] is None else f'{c["change24h"]:+.2f}%'
        lines.append(f'{c["symbol"]}: ${c["priceUsd"]:,.8f} | 24ч {ch}')
    return "\n".join(lines)

@app.route("/telegram-webhook", methods=["POST"])
@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    if TG_SECRET:
        s = request.headers.get("X-Telegram-Bot-Api-Secret-Token","")
        if not hmac.compare_digest(s, TG_SECRET): return jsonify({"ok":False}), 401
    if not TG_TOKEN: return jsonify({"ok":False}), 503
    update = request.get_json(silent=True) or {}
    cb = update.get("callback_query")
    if cb:
        chat = ((cb.get("message") or {}).get("chat") or {}).get("id")
        try: tg("answerCallbackQuery", {"callback_query_id":cb.get("id")})
        except: pass
        if chat:
            data = cb.get("data")
            try:
                if data == "prices": tg_send(chat, telegram_prices(), keyboard())
                elif data == "usdrub":
                    rate = usd_rub()
                    tg_send(chat, f"💱 USD/RUB\n\n1 USD = ₽{rate:.2f}\n\nИсточник: open.er-api.com", keyboard())
            except: tg_send(chat, "Недоступно.", keyboard())
        return jsonify({"ok":True})
    msg = update.get("message") or {}
    chat = (msg.get("chat") or {}).get("id"); text = (msg.get("text") or "").strip()
    if chat and text.startswith(("/start","/help")):
        tg_send(chat, "CryptoPulse\nЦены, сигналы, курс USD/RUB.\nРеальная торговля отключена.", keyboard())
    elif chat and text.startswith("/prices"):
        try: tg_send(chat, telegram_prices(), keyboard())
        except: tg_send(chat, "Недоступно.", keyboard())
    return jsonify({"ok":True})

def configure_telegram():
    if not TG_TOKEN: return
    try:
        tg("setChatMenuButton", {"menu_button":{"type":"web_app","text":"Открыть CryptoPulse",
                                                  "web_app":{"url":WEBAPP_URL}}})
        p = {"url":WEBHOOK_URL}
        if TG_SECRET: p["secret_token"] = TG_SECRET
        tg("setWebhook", p)
    except Exception: log.exception("tg setup fail")

def worker():
    while True:
        try: refresh(force=True)
        except Exception: log.exception("bg fail")
        time.sleep(REFRESH_SECONDS)

init_db(); bot_load()
if TG_AUTO: configure_telegram()
if BG_WORKER: threading.Thread(target=worker, daemon=True).start()
threading.Thread(target=bot_worker, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","10000")))
