import os, time, hmac, math, logging, threading, json, csv, io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlencode
import requests

try:
    from groq import Groq
except ImportError:
    Groq = None

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
GROQ_API_KEY = os.getenv("GROQ_API_KEY","").strip()

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

DEFAULT_STRATEGY = {
    "emaFast": 9, "emaSlow": 21, "rsiPeriod": 14,
    "rsiBuyMin": 50, "rsiBuyMax": 68,
    "rsiSellMin": 32, "rsiSellMax": 50,
    "tpPercent": 2.0, "slPercent": 1.5,
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
    "lastAction":None,"lastSignalScan":None,"scanResults":[],
    "scanStatus":"Ожидание запуска"
}
bot_state_lock = threading.RLock()

strategy_history = {}
strategy_history_updated = 0.0
strategy_history_lock = threading.Lock()
SCAN_CANDLE_INTERVAL = "5"
SCAN_CANDLE_LIMIT = 200
SCAN_HISTORY_REFRESH_SECONDS = 60

history_loading_flag = threading.Event()

groq_client = None
if Groq and GROQ_API_KEY:
    try:
        groq_client = Groq(api_key=GROQ_API_KEY)
    except Exception as e:
        log.error("Groq init failed: %s", e)

def ask_ai_advisor(symbol, price, signal, rsi_val, ema_fast, ema_slow):
    """Спрашиваем у ИИ, в какую сторону торговать."""
    if not groq_client:
        return ("Long" if "buy" in signal else "Short"), "ИИ не настроен (берем сигнал)"
    
    with strategy_history_lock:
        pts = list(strategy_history.get(symbol, []))
    
    recent_data = []
    if len(pts) >= 10:
        for p in pts[-10:]:
            dt = datetime.fromtimestamp(p["timestamp"], tz=timezone.utc)
            recent_data.append(f"{dt.strftime('%H:%M')}: ${p['price']:.4f}")
    else:
        recent_data.append("Мало данных")

    trend_text = "\n".join(recent_data)

    try:
        prompt = (
            f"Ты профессиональный крипто-трейдер. Проанализируй ситуацию и прими решение.\n"
            f"Монета: {symbol}/USDT\n"
            f"Текущая цена: ${price:.4f}\n"
            f"RSI(14): {rsi_val:.1f}\n"
            f"EMA9: ${ema_fast:.4f}, EMA21: ${ema_slow:.4f}\n"
            f"Сигнал стратегии: {signal}\n\n"
            f"Последние 10 свечей (5м):\n{trend_text}\n\n"
            f"Правила:\n"
            f"1. Если цена резко упала и RSI низкий — это может быть отскок (BUY) или продолжение падения (SELL).\n"
            f"2. Если цена растет и RSI высокий — может быть коррекция (SELL) или продолжение роста (BUY).\n"
            f"3. Если сигнал неясный — выбери направление, которое совпадает с последними 3-4 свечами.\n"
            f"ЗАПРЕЩЕНО отвечать SKIP. Ты ОБЯЗАН выбрать BUY или SELL.\n"
            f"Ответь строго одним словом: BUY или SELL."
        )
        
        completion = groq_client.chat.completions.create(
            model="openai/gpt-oss-20b",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=5
        )
        
        raw_answer = completion.choices[0].message.content.strip().upper()
        first_word = raw_answer.split()[0] if raw_answer.split() else ""
        
        if "BUY" in first_word:
            return "Long", "ИИ: BUY (Long)"
        elif "SELL" in first_word:
            return "Short", "ИИ: SELL (Short)"
        else:
            return ("Long" if "buy" in signal else "Short"), "ИИ неясен (берем сигнал)"
            
    except Exception as e:
        log.warning("Groq error: %s", e)
        return ("Long" if "buy" in signal else "Short"), f"Ошибка ИИ: {str(e)[:40]}"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def get_json(url, params=None, headers=None):
    r = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def bot_save():
    try:
        with bot_state_lock:
            data = {k: bot_state[k] for k in (
                "running","emergency","reason","settings","strategy",
                "balance","positions","closedTrades","logs","startedAt",
                "stoppedAt","lastAction","lastSignalScan","scanResults"
            )}
            data["equityCurve"] = bot_state["equityCurve"][-2000:]
            data["closedTrades"] = bot_state["closedTrades"][-1000:]
            data["logs"] = bot_state["logs"][-500:]
        with open(BOT_STATE_FILE,"w",encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        log.warning("save fail: %s", exc)

def bot_load():
    try:
        if not os.path.exists(BOT_STATE_FILE):
            return
        with open(BOT_STATE_FILE,"r",encoding="utf-8") as f:
            data = json.load(f)
        with bot_state_lock:
            for k in data:
                if k in bot_state:
                    bot_state[k] = data[k]
            merged = dict(DEFAULT_STRATEGY)
            if bot_state.get("strategy"):
                merged.update(bot_state["strategy"])
            bot_state["strategy"] = merged
    except Exception as exc:
        log.warning("load fail: %s", exc)

def bot_log(text, color="blue"):
    entry = {
        "time":datetime.now().strftime("%H:%M:%S"),
        "text":text,
        "color":color,
        "ts":time.time()
    }
    with bot_state_lock:
        bot_state["logs"].insert(0, entry)
        bot_state["logs"] = bot_state["logs"][:500]
    bot_save()

def push_equity_point():
    with bot_state_lock:
        free = bot_state["balance"]
        pos_size = sum(p["sizeUsd"] for p in bot_state["positions"])
        bot_state["equityCurve"].append({
            "ts":time.time(),
            "balance": free + pos_size
        })
        bot_state["equityCurve"] = bot_state["equityCurve"][-2000:]

def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("no db")
    if psycopg is None:
        raise RuntimeError("no psycopg")
    return psycopg.connect(DATABASE_URL, connect_timeout=8, row_factory=dict_row)

def init_db():
    if not DATABASE_URL:
        return
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
            try:
                value = float(value)
            except Exception:
                continue
            if not math.isfinite(value) or value <= 0:
                continue
            valid[symbol] = value
            pts = history.setdefault((source, symbol), [])
            pts.append({"timestamp":timestamp,"price":value})
            history[(source, symbol)] = [
                p for p in pts if p["timestamp"] >= cutoff
            ]
    if not DATABASE_URL or not valid:
        return
    try:
        with db_lock:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""INSERT INTO price_history (source,symbol,price,observed_at)
                        VALUES (%s,%s,%s,to_timestamp(%s))
                        ON CONFLICT (source,symbol,observed_at) DO NOTHING""",
                        [(source,s,p,timestamp) for s,p in valid.items()])
                    cur.execute("""DELETE FROM price_history WHERE observed_at < NOW() - (%s * INTERVAL '1 hour')""", (HISTORY_HOURS,))
                    conn.commit()
        db_status.update(available=True, lastError=None)
    except Exception as exc:
        db_status.update(available=False, lastError=str(exc))

def get_points(symbol):
    with lock:
        pts = list(history.get(("bybit",symbol), []))
    cutoff = time.time() - HISTORY_HOURS * 3600
    return [p for p in pts if p["timestamp"] >= cutoff]

def ema(values, period):
    values = [float(v) for v in values if v and float(v) > 0]
    if len(values) < period:
        return None
    result = sum(values[:period]) / period
    multiplier = 2 / (period + 1)
    for value in values[period:]:
        result = (value - result) * multiplier + result
    return result

def rsi(values, period=14):
    values = [float(v) for v in values if v and float(v) > 0]
    if len(values) < period + 1:
        return None
    changes = [values[i] - values[i-1] for i in range(1,len(values))][-period:]
    gains = sum(max(v,0) for v in changes) / period
    losses = sum(max(-v,0) for v in changes) / period
    if losses == 0:
        return 100.0 if gains else 50.0
    return 100 - 100 / (1 + gains / losses)

def fetch_strategy_candles(symbol):
    for attempt in range(5):
        try:
            response = requests.get(
                BASE + "/v5/market/kline",
                params={"category":"spot","symbol":symbol+"USDT","interval":SCAN_CANDLE_INTERVAL,"limit":SCAN_CANDLE_LIMIT},
                headers={"User-Agent":"CryptoPulse/3.0","Accept":"application/json"},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            if data.get("retCode") == 0:
                raw = (data.get("result") or {}).get("list", [])
                points = []
                for item in reversed(raw):
                    try:
                        timestamp = int(item[0]) / 1000
                        price = float(item[4])
                    except (TypeError, ValueError, IndexError):
                        continue
                    if math.isfinite(price) and price > 0:
                        points.append({"timestamp":timestamp,"price":price})
                return symbol, points
            else:
                time.sleep(2)
        except Exception:
            time.sleep(2)
    return symbol, []

def refresh_strategy_history_background(symbols):
    global strategy_history_updated
    if history_loading_flag.is_set():
        return
    history_loading_flag.set()
    try:
        with bot_state_lock:
            bot_state["scanStatus"] = f"Загружаю 5m-свечи: 0/{len(symbols)}"
        
        completed = 0
        successful = 0
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {pool.submit(fetch_strategy_candles, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    _, points = future.result()
                    if len(points) >= 30:
                        with strategy_history_lock:
                            strategy_history[symbol] = points
                        successful += 1
                except Exception:
                    pass
                completed += 1
                with bot_state_lock:
                    bot_state["scanStatus"] = f"Загружаю 5m-свечи: {completed}/{len(symbols)}"
                time.sleep(0.5)
        
        with strategy_history_lock:
            strategy_history_updated = time.time()
        with bot_state_lock:
            bot_state["scanStatus"] = f"Готово: {successful}/{len(symbols)}"
    except Exception as e:
        with bot_state_lock:
            bot_state["scanStatus"] = f"Ошибка: {e}"
    finally:
        history_loading_flag.clear()

def analyze(symbol, price, strategy=None):
    st = strategy or bot_state["strategy"]
    with strategy_history_lock:
        pts = list(strategy_history.get(symbol, []))
    if not pts:
        pts = sorted(get_points(symbol), key=lambda p: p["timestamp"])
    values = [p["price"] for p in pts]
    if not values or values[-1] != price:
        values.append(price)
    ema_fast = ema(values, int(st["emaFast"]))
    ema_slow = ema(values, int(st["emaSlow"]))
    rsi_value = rsi(values, int(st["rsiPeriod"]))
    signal = "insufficient_history"
    explanation = "Нужно больше истории."
    if len(values) >= 30 and ema_fast is not None and ema_slow is not None and rsi_value is not None:
        if price > ema_fast > ema_slow and st["rsiBuyMin"] <= rsi_value < st["rsiBuyMax"]:
            signal = "buy_candidate"
            explanation = "Бычий сигнал."
        elif price < ema_fast < ema_slow and st["rsiSellMin"] < rsi_value <= st["rsiSellMax"]:
            signal = "sell_candidate"
            explanation = "Медвежий сигнал."
        else:
            signal = "neutral"
            explanation = "Условия не совпали."
    return {
        "symbol":symbol,"price":price,"ema9":ema_fast,"ema21":ema_slow,"rsi14":rsi_value,
        "sampleCount":len(values),"signal":signal,"explanation":explanation,"mode":"simulation"
    }

def fetch_prices():
    data = get_json(BASE + "/v5/market/tickers", {"category":"spot"})
    if data.get("retCode") != 0:
        raise RuntimeError(data.get("retMsg","err"))
    prices = {}
    changes = {}
    for item in (data.get("result") or {}).get("list", []):
        pair = str(item.get("symbol","")).upper()
        if not pair.endswith("USDT"):
            continue
        symbol = pair[:-4]
        try:
            value = float(item.get("lastPrice"))
        except Exception:
            continue
        if not math.isfinite(value) or value <= 0:
            continue
        prices[symbol] = value
        try:
            changes[symbol] = float(item.get("price24hPcnt")) * 100
        except Exception:
            changes[symbol] = None
    if not prices:
        raise RuntimeError("no prices")
    bybit_status.update(available=True,lastError=None,lastCheck=now_iso())
    return prices, changes

def refresh(force=False):
    if cache["prices"] and time.time() - cache["updated"] < CACHE_SECONDS and not force:
        return dict(cache["prices"]),dict(cache["changes"]),cache["error"]
    with refresh_lock:
        if cache["prices"] and time.time() - cache["updated"] < CACHE_SECONDS and not force:
            return dict(cache["prices"]),dict(cache["changes"]),cache["error"]
        try:
            prices,changes = fetch_prices()
            remember(prices)
            cache.update(updated=time.time(), prices=prices, changes=changes, error=None)
            return dict(prices),dict(changes),None
        except Exception as exc:
            bybit_status.update(available=False, lastError=str(exc), lastCheck=now_iso())
            cache["error"] = str(exc)
            if cache["prices"]:
                return dict(cache["prices"]),dict(cache["changes"]),str(exc)
            raise

def usd_rub():
    if usd_rub_cache["rate"] and time.time() - usd_rub_cache["updated"] < 900:
        return usd_rub_cache["rate"]
    data = get_json("https://open.er-api.com/v6/latest/USD")
    rate = float(data["rates"]["RUB"])
    if not math.isfinite(rate) or rate <= 0:
        raise RuntimeError("bad rate")
    usd_rub_cache.update(updated=time.time(),rate=rate)
    return rate

def market_payload(force=False):
    prices,changes,warning = refresh(force)
    try:
        rate = usd_rub()
        rate_err = None
    except Exception as exc:
        rate = None
        rate_err = str(exc)
    coins = []
    for symbol,price in prices.items():
        pts = [p for p in get_points(symbol) if 2700 <= time.time() - p["timestamp"] <= 4500]
        change_1h = None
        if pts:
            ref = min(pts, key=lambda p:abs(time.time() - p["timestamp"] - 3600))["price"]
            change_1h = (price / ref - 1) * 100
        coins.append({
            "symbol":symbol,"name":symbol,"priceUsd":price,
            "priceRub":price * rate if rate else None,
            "change1h":change_1h,"change24h":changes.get(symbol),"source":"bybit"
        })
    coins.sort(key=lambda c:c["symbol"])
    return {
        "success":True,"updatedAt":now_iso(),"source":"bybit","coins":coins,
        "usdRub":rate,"usdRubError":rate_err,"databaseAvailable":db_status["available"],"warning":warning
    }

def signed_get(path,params):
    if not API_KEY or not API_SECRET:
        raise RuntimeError("no key")
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    query = urlencode(sorted((k,str(v)) for k,v in params.items()))
    signature = hmac.new(API_SECRET.encode(), (ts + API_KEY + recv_window + query).encode(), "sha256").hexdigest()
    headers = {
        "X-BAPI-API-KEY":API_KEY,"X-BAPI-TIMESTAMP":ts,"X-BAPI-RECV-WINDOW":recv_window,
        "X-BAPI-SIGN":signature,"X-BAPI-SIGN-TYPE":"2"
    }
    data = get_json(BASE + path,params=params,headers=headers)
    if data.get("retCode") != 0:
        raise RuntimeError(f"err {data.get('retCode')}: {data.get('retMsg')}")
    return data.get("result") or {}

def wallet_balance(force=False):
    if wallet_cache["data"] and time.time() - wallet_cache["updated"] < 60 and not force:
        return wallet_cache["data"]
    result = signed_get("/v5/account/wallet-balance", {"accountType":"UNIFIED"})
    assets = []
    total = 0.0
    for account in result.get("list", []):
        for coin in account.get("coin", []):
            try:
                balance = float(coin.get("walletBalance") or 0)
                usd_value = float(coin.get("usdValue") or 0)
            except Exception:
                continue
            if balance == 0 and usd_value == 0:
                continue
            assets.append({
                "coin":coin.get("coin",""),"walletBalance":balance,"usdValue":usd_value,
                "availableToWithdraw":coin.get("availableToWithdraw")
            })
            total += usd_value
    assets.sort(key=lambda a:a["usdValue"],reverse=True)
    data = {
        "success":True,"available":True,"accountType":"UNIFIED",
        "totalUsd":total,"assets":assets,"updatedAt":now_iso()
    }
    wallet_cache.update(updated=time.time(),data=data)
    return data

def bot_open_position(symbol,price,side,reason=""):
    with bot_state_lock:
        st = bot_state["strategy"]
        settings = bot_state["settings"]
        size = float(settings["tradeSize"])
        
        if bot_state["balance"] < size:
            return None
        for position in bot_state["positions"]:
            if position["symbol"] == symbol:
                return None
        qty = size / price
        tp_pct = float(st["tpPercent"]) / 100
        sl_pct = float(st["slPercent"]) / 100
        if side == "Long":
            tp = price * (1 + tp_pct)
            sl = price * (1 - sl_pct)
        else:
            tp = price * (1 - tp_pct)
            sl = price * (1 + sl_pct)
        position = {
            "id":int(time.time() * 1000) + len(bot_state["positions"]),
            "symbol":symbol,"side":side,"sizeUsd":size,"entryPrice":price,"qty":qty,
            "openedAt":now_iso(),"openedTs":time.time(),"tp":tp,"sl":sl,"reason":reason
        }
        bot_state["positions"].append(position)
        bot_state["balance"] -= size
        bot_state["lastAction"] = f"Открыта {side} {symbol}/USDT @ ${price:,.4f}"
    bot_log(f"Открыта {side} {symbol}/USDT. Вход ${price:,.4f}, TP ${tp:,.4f}, SL ${sl:,.4f}", "blue")
    bot_save()
    return position

def bot_close_position(position,price,reason="manual"):
    with bot_state_lock:
        if position not in bot_state["positions"]:
            return None
        bot_state["positions"].remove(position)
        if position["side"] == "Long":
            pnl = (price - position["entryPrice"]) * position["qty"]
        else:
            pnl = (position["entryPrice"] - price) * position["qty"]
        pnl_pct = (pnl / position["sizeUsd"]) * 100 if position["sizeUsd"] else 0
        bot_state["balance"] += (position["sizeUsd"] + pnl)
        trade = {
            "symbol":position["symbol"],"side":position["side"],"sizeUsd":position["sizeUsd"],
            "entryPrice":position["entryPrice"],"exitPrice":price,"pnl":pnl,"pnlPct":pnl_pct,
            "reason":reason,"openedAt":position["openedAt"],"closedAt":now_iso(),"closedTs":time.time()
        }
        bot_state["closedTrades"].insert(0,trade)
        bot_state["closedTrades"] = bot_state["closedTrades"][:1000]
        bot_state["lastAction"] = f"Закрыта {position['side']} {position['symbol']}/USDT: {pnl:+.2f}$ [{reason}]"
    push_equity_point()
    bot_log(f"Закрыта {position['side']} {position['symbol']}/USDT @ ${price:,.4f}. P&L ${pnl:+.2f} ({pnl_pct:+.2f}%) [{reason}]", "green" if pnl >= 0 else "red")
    bot_save()
    return trade

def bot_check_limits():
    with bot_state_lock:
        settings = bot_state["settings"]
        start = float(settings["startBalance"])
        free_balance = float(bot_state["balance"])
        positions_value = sum(p["sizeUsd"] for p in bot_state["positions"])
        open_pnl = 0.0
        try:
            prices, _, _ = refresh()
            for p in bot_state["positions"]:
                current = prices.get(p["symbol"], p["entryPrice"])
                if p["side"] == "Long":
                    open_pnl += (current - p["entryPrice"]) * p["qty"]
                else:
                    open_pnl += (p["entryPrice"] - current) * p["qty"]
        except Exception:
            pass
        total_equity = free_balance + positions_value + open_pnl
        diff = total_equity - start
        profit_target = float(settings["profitTarget"])
        max_loss = float(settings["maxLoss"])
        if diff >= profit_target:
            return "profit", f"Достигнут лимит прибыли (+${diff:.2f})"
        if diff <= -max_loss:
            return "loss", f"Достигнут лимит убытка (-${abs(diff):.2f})"
    return None, None

def bot_close_all(reason="manual"):
    try:
        prices,_,_ = refresh()
    except Exception:
        prices = {}
    with bot_state_lock:
        positions = list(bot_state["positions"])
    for position in positions:
        bot_close_position(position, prices.get(position["symbol"],position["entryPrice"]), reason)

def bot_tick():
    with bot_state_lock:
        if not bot_state["running"]:
            return
    try:
        prices,_,_ = refresh(force=True)
    except Exception as exc:
        with bot_state_lock:
            bot_state["scanStatus"] = f"Ошибка получения цен: {exc}"
        bot_log(f"Цены: {exc}","red")
        return

    with bot_state_lock:
        positions = list(bot_state["positions"])
    for position in positions:
        price = prices.get(position["symbol"])
        if price is None:
            continue
        if position["side"] == "Long":
            if price >= position["tp"]:
                bot_close_position(position,price,"TP")
                continue
            if price <= position["sl"]:
                bot_close_position(position,price,"SL")
                continue
        else:
            if price <= position["tp"]:
                bot_close_position(position,price,"TP")
                continue
            if price >= position["sl"]:
                bot_close_position(position,price,"SL")
                continue

    kind,message = bot_check_limits()
    if kind:
        bot_log(message + ". Останавливаем.", "gold" if kind == "profit" else "red")
        bot_close_all("limits")
        with bot_state_lock:
            bot_state["running"] = False
            bot_state["reason"] = message
            bot_state["stoppedAt"] = now_iso()
            bot_state["lastAction"] = message
            bot_state["scanStatus"] = f"Остановлено: {message}"
        bot_save()
        return

    st = bot_state["strategy"]
    top = st.get("topCoins",DEFAULT_STRATEGY["topCoins"])
    
    if not history_loading_flag.is_set() and time.time() - strategy_history_updated > SCAN_HISTORY_REFRESH_SECONDS:
        threading.Thread(target=refresh_strategy_history_background, args=(top,), daemon=True).start()

    scan = []
    for symbol in top:
        price = prices.get(symbol)
        if price is None:
            continue
        result = analyze(symbol,price,st)
        scan.append({
            "symbol":symbol,"price":price,"rsi14":result["rsi14"],"ema9":result["ema9"],
            "ema21":result["ema21"],"signal":result["signal"],"explanation":result["explanation"],
            "sampleCount":result["sampleCount"]
        })
    with bot_state_lock:
        bot_state["scanResults"] = scan
        bot_state["lastSignalScan"] = now_iso()
        signal_count = sum(1 for item in scan if item["signal"] in ("buy_candidate","sell_candidate"))
        insufficient = sum(1 for item in scan if item["signal"] == "insufficient_history")
        bot_state["scanStatus"] = f"Проверено {len(scan)}/{len(top)} монет; сигналов: {signal_count}"
        if insufficient:
            bot_state["scanStatus"] += f"; мало истории: {insufficient}"
    bot_save()

    with bot_state_lock:
        current = {p["symbol"] for p in bot_state["positions"]}
        
    opened = 0
    for item in scan:
        if item["symbol"] in current:
            continue
        
        if item["signal"] in ("buy_candidate", "sell_candidate"):
            ai_side, ai_reason = ask_ai_advisor(
                item["symbol"], item["price"], item["signal"], 
                item["rsi14"] or 50, item["ema9"] or item["price"], item["ema21"] or item["price"]
            )
            log_color = "green" if ai_side else "red"
            bot_log(f"ИИ по {item['symbol']}: {ai_reason}", log_color)
            
            if ai_side == "Long":
                if bot_open_position(item["symbol"],item["price"],"Long","ИИ: Тренд вверх"):
                    opened += 1
            elif ai_side == "Short":
                if bot_open_position(item["symbol"],item["price"],"Short","ИИ: Тренд вниз"):
                    opened += 1
        
    with bot_state_lock:
        if not bot_state["positions"]:
            bot_state["lastAction"] = "Сканирует — сигналов нет."
        else:
            bot_state["lastAction"] = f"Держит {len(bot_state['positions'])} позиций. Ждёт TP/SL."

def bot_worker():
    while True:
        try:
            bot_tick()
        except Exception:
            log.exception("tick fail")
        time.sleep(BOT_TICK_SECONDS)

def compute_stats():
    with bot_state_lock:
        trades = list(bot_state["closedTrades"])
        positions = list(bot_state["positions"])
        balance = float(bot_state["balance"])
        start = float(bot_state["settings"]["startBalance"])
        equity = list(bot_state["equityCurve"])
        
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] < 0]
    total = len(trades)
    winrate = len(wins) / total * 100 if total else 0
    sum_profit = sum(t["pnl"] for t in wins) if wins else 0
    sum_loss = abs(sum(t["pnl"] for t in losses)) if losses else 0
    profit_factor = (sum_profit / sum_loss) if sum_loss > 0 else (float("inf") if sum_profit > 0 else 0)
    avg_win = sum_profit / len(wins) if wins else 0
    avg_loss = -sum_loss / len(losses) if losses else 0
    best = max((t["pnl"] for t in trades),default=0)
    worst = min((t["pnl"] for t in trades),default=0)
    
    max_drawdown = 0.0
    peak = -1e18
    for point in equity:
        peak = max(peak,point["balance"])
        drawdown = peak - point["balance"]
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            
    by_symbol = {}
    for trade in trades:
        symbol = trade["symbol"]
        by_symbol.setdefault(symbol,{"symbol":symbol,"trades":0,"pnl":0.0,"wins":0,"losses":0})
        by_symbol[symbol]["trades"] += 1
        by_symbol[symbol]["pnl"] += trade["pnl"]
        if trade["pnl"] > 0: by_symbol[symbol]["wins"] += 1
        elif trade["pnl"] < 0: by_symbol[symbol]["losses"] += 1
            
    try:
        prices,_,_ = refresh()
    except Exception:
        prices = {}
        
    positions_live = []
    open_pnl = 0.0
    for position in positions:
        current = prices.get(position["symbol"],position["entryPrice"])
        if position["side"] == "Long":
            pnl = (current - position["entryPrice"]) * position["qty"]
        else:
            pnl = (position["entryPrice"] - current) * position["qty"]
        open_pnl += pnl
        positions_live.append({
            **position, "currentPrice":current, "pnl":pnl,
            "pnlPct":(pnl / position["sizeUsd"] * 100 if position["sizeUsd"] else 0)
        })
        
    positions_size = sum(p["sizeUsd"] for p in positions)
    total_equity = balance + positions_size + open_pnl 
    total_pnl = total_equity - start

    return {
        "trades":total,"wins":len(wins),"losses":len(losses),"winrate":winrate,
        "sumProfit":sum_profit,"sumLoss":sum_loss,"profitFactor":profit_factor,
        "avgWin":avg_win,"avgLoss":avg_loss,"best":best,"worst":worst,
        "maxDrawdown":max_drawdown,
        "bySymbol":sorted(by_symbol.values(), key=lambda item:item["pnl"], reverse=True),
        "balance":total_equity,"startBalance":start,"totalPnl":total_pnl,
        "openPnl":open_pnl,"closedPnl":balance - start,
        "openPositions":len(positions_live),"positionsLive":positions_live,"equityCurve":equity
    }

@app.after_request
def headers(response):
    response.headers.setdefault("X-Content-Type-Options","nosniff")
    response.headers.setdefault("X-Frame-Options","SAMEORIGIN")
    return response

@app.route("/")
def index():
    return send_from_directory(app.static_folder,"index.html")

@app.route("/health")
def health():
    return jsonify({"status":"ok","service":"CryptoPulse","timestamp":now_iso()})

@app.route("/api/config")
def config():
    return jsonify({"success":True,"tradingMode":"simulation","liveTradingEnabled":False,
                    "databaseConfigured":bool(DATABASE_URL),"databaseAvailable":db_status["available"]})

@app.route("/api/prices")
@app.route("/api/market-prices")
def prices_api():
    try:
        return jsonify(market_payload())
    except Exception:
        return jsonify({"success":False,"error":"Рынок недоступен","coins":[]}), 503

@app.route("/api/usd-rub")
def rub_api():
    try:
        return jsonify({"success":True,"rate":usd_rub(),"base":"USD","currency":"RUB"})
    except Exception:
        return jsonify({"success":False,"error":"Курс недоступен"}), 503

@app.route("/api/account/balance")
@app.route("/api/bybit/balance")
def balance_api():
    try:
        return jsonify(wallet_balance())
    except Exception as exc:
        log.warning("bal: %s",exc)
        return jsonify({"success":False,"available":False,"error":"Баланс недоступен"}), 503

@app.route("/api/trading/signals")
def signals_api():
    try:
        payload = market_payload()
        st = bot_state["strategy"]
        out = []
        for coin in payload["coins"]:
            item = analyze(coin["symbol"],coin["priceUsd"],st)
            item["change1h"] = coin["change1h"]
            item["change24h"] = coin["change24h"]
            out.append(item)
        return jsonify({"success":True,"signals":out})
    except Exception:
        return jsonify({"success":False,"signals":[]}), 503

@app.route("/api/klines")
def klines_api():
    sym = (request.args.get("symbol") or "BTCUSDT").upper().strip()
    iv = (request.args.get("interval") or "5m").strip()
    bi = {"1m":"1","3m":"3","5m":"5","15m":"15","30m":"30","1h":"60","2h":"120","4h":"240","6h":"360","12h":"720","1d":"D","1w":"W"}.get(iv,iv)
    try:
        lim = max(1,min(int(request.args.get("limit","200")),1000))
    except Exception:
        lim = 200
    try:
        data = get_json(BASE + "/v5/market/kline", {"category":"spot","symbol":sym,"interval":bi,"limit":lim})
        if data.get("retCode") != 0:
            raise RuntimeError(data.get("retMsg"))
        raw = (data.get("result") or {}).get("list", [])
        candles = []
        for item in reversed(raw):
            try:
                candles.append({
                    "time":int(item[0]),"open":float(item[1]),"high":float(item[2]),
                    "low":float(item[3]),"close":float(item[4]),"volume":float(item[5]) if len(item) > 5 else 0
                })
            except Exception:
                continue
        return jsonify({"success":True,"symbol":sym,"interval":iv,"candles":candles})
    except Exception as exc:
        return jsonify({"success":False,"error":"Свечи недоступны","details":str(exc)}), 503

@app.route("/api/bot/state")
def bot_state_api():
    stats = compute_stats()
    with bot_state_lock:
        return jsonify({
            "success":True,
            "running":bot_state["running"],
            "emergency":bot_state["emergency"],
            "reason":bot_state["reason"],
            "settings":bot_state["settings"],
            "strategy":bot_state["strategy"],
            "balance":stats["balance"],
            "startBalance":stats["startBalance"],
            "totalPnl":stats["totalPnl"],
            "totalPnlPct":(stats["totalPnl"] / stats["startBalance"] * 100 if stats["startBalance"] else 0),
            "positions":stats["positionsLive"],
            "closedTrades":bot_state["closedTrades"][:100],
            "logs":bot_state["logs"][:100],
            "startedAt":bot_state["startedAt"],
            "stoppedAt":bot_state["stoppedAt"],
            "lastAction":bot_state["lastAction"],
            "lastSignalScan":bot_state["lastSignalScan"],
            "scanStatus":bot_state["scanStatus"],
            "scanResults":bot_state["scanResults"],
            "stats":{
                "trades":stats["trades"],"wins":stats["wins"],"losses":stats["losses"],
                "winrate":stats["winrate"],
                "profitFactor":(stats["profitFactor"] if math.isfinite(stats["profitFactor"]) else None),
                "avgWin":stats["avgWin"],"avgLoss":stats["avgLoss"],
                "best":stats["best"],"worst":stats["worst"],
                "maxDrawdown":stats["maxDrawdown"],"bySymbol":stats["bySymbol"],
                "equityCurve":stats["equityCurve"][-500:]
            }
        })

@app.route("/api/bot/start", methods=["POST"])
def bot_start_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        if "settings" in data and isinstance(data["settings"],dict):
            for key in ("profitTarget","maxLoss","tradeSize"):
                if key in data["settings"]:
                    try:
                        bot_state["settings"][key] = float(data["settings"][key])
                    except Exception:
                        pass
        free = float(bot_state["balance"])
        pos_value = sum(p["sizeUsd"] for p in bot_state["positions"])
        bot_state["settings"]["startBalance"] = free + pos_value
        bot_state["running"] = True
        bot_state["emergency"] = False
        bot_state["reason"] = "Бот запущен"
        bot_state["startedAt"] = now_iso()
        bot_state["stoppedAt"] = None
        bot_state["lastAction"] = "Запущен. Сканирует…"
        bot_state["scanStatus"] = "Подготовка сканера…"
    push_equity_point()
    bot_log("Бот запущен.","green")
    return jsonify({"success":True})

@app.route("/api/bot/stop", methods=["POST"])
def bot_stop_api():
    with bot_state_lock:
        bot_state["running"] = False
        bot_state["reason"] = "Остановлен вручную"
        bot_state["stoppedAt"] = now_iso()
        bot_state["lastAction"] = "Остановлен вручную"
        bot_state["scanStatus"] = "Сканер остановлен"
    bot_log("Бот остановлен.","red")
    return jsonify({"success":True})

@app.route("/api/bot/emergency", methods=["POST"])
def bot_emergency_api():
    bot_close_all("emergency")
    with bot_state_lock:
        bot_state["running"] = False
        bot_state["emergency"] = True
        bot_state["reason"] = "Аварийная остановка"
        bot_state["stoppedAt"] = now_iso()
        bot_state["lastAction"] = "Аварийная остановка"
        bot_state["scanStatus"] = "Аварийная остановка"
    bot_log("АВАРИЙНАЯ ОСТАНОВКА.","red")
    return jsonify({"success":True})

@app.route("/api/bot/settings", methods=["POST"])
def bot_settings_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        for key in ("profitTarget","maxLoss","tradeSize"):
            if key in data:
                try:
                    bot_state["settings"][key] = float(data[key])
                except Exception:
                    pass
    bot_log("Настройки сохранены.","blue")
    return jsonify({"success":True,"settings":bot_state["settings"]})

@app.route("/api/bot/strategy", methods=["POST"])
def bot_strategy_api():
    data = request.get_json(silent=True) or {}
    with bot_state_lock:
        for key in ("emaFast","emaSlow","rsiPeriod","rsiBuyMin","rsiBuyMax","rsiSellMin","rsiSellMax"):
            if key in data:
                try:
                    bot_state["strategy"][key] = int(data[key])
                except Exception:
                    pass
        for key in ("tpPercent","slPercent"):
            if key in data:
                try:
                    bot_state["strategy"][key] = float(data[key])
                except Exception:
                    pass
    bot_log("Стратегия обновлена.","blue")
    return jsonify({"success":True,"strategy":bot_state["strategy"]})

@app.route("/api/bot/reset", methods=["POST"])
def bot_reset_api():
    with bot_state_lock:
        bot_state.update({
            "running":False,"emergency":False,"reason":"Сброшено",
            "balance":float(bot_state["settings"].get("startBalance",100.0)),
            "positions":[],"closedTrades":[],"logs":[],"equityCurve":[],
            "startedAt":None,"stoppedAt":None,"lastAction":"Сброшено",
            "scanResults":[],"scanStatus":"Сброшено; ожидание запуска"
        })
    bot_log("Бот сброшен.","gold")
    return jsonify({"success":True})

@app.route("/api/bot/export.csv")
def bot_export_csv():
    with bot_state_lock:
        trades = list(bot_state["closedTrades"])
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["Time","Symbol","Side","SizeUSD","EntryPrice","ExitPrice","PnL","PnL%","Reason"])
    for trade in trades:
        writer.writerow([
            trade.get("closedAt",""),trade.get("symbol",""),trade.get("side",""),
            f"{trade.get('sizeUsd',0):.2f}",f"{trade.get('entryPrice',0):.8f}",
            f"{trade.get('exitPrice',0):.8f}",f"{trade.get('pnl',0):.4f}",
            f"{trade.get('pnlPct',0):.4f}",trade.get("reason","")
        ])
    return Response(out.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment;filename=cryptopulse_trades.csv"})

def tg(method,payload):
    if not TG_TOKEN:
        raise RuntimeError("no token")
    r = session.post(f"https://api.telegram.org/bot{TG_TOKEN}/{method}", json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def tg_send(chat,text,markup=None):
    payload = {"chat_id":chat,"text":text}
    if markup:
        payload["reply_markup"] = markup
    return tg("sendMessage",payload)

def keyboard():
    return {"inline_keyboard":[[{"text":"🌐 CryptoPulse","web_app":{"url":WEBAPP_URL}},{"text":"📊 Цены","callback_data":"prices"}],[{"text":"💱 USD/RUB","callback_data":"usdrub"},{"text":"🧮 Калькулятор","web_app":{"url":WEBAPP_URL+"?page=calculator"}}]]}

def telegram_prices():
    payload = market_payload()
    lines = ["CryptoPulse — Bybit Spot",""]
    for coin in payload["coins"][:40]:
        change = "—" if coin["change24h"] is None else f'{coin["change24h"]:+.2f}%'
        lines.append(f'{coin["symbol"]}: ${coin["priceUsd"]:,.8f} | 24ч {change}')
    return "\n".join(lines)

@app.route("/telegram-webhook", methods=["POST"])
@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    if TG_SECRET:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token","")
        if not hmac.compare_digest(secret,TG_SECRET):
            return jsonify({"ok":False}),401
    if not TG_TOKEN:
        return jsonify({"ok":False}),503
    update = request.get_json(silent=True) or {}
    callback = update.get("callback_query")
    if callback:
        chat = ((callback.get("message") or {}).get("chat") or {}).get("id")
        try:
            tg("answerCallbackQuery",{"callback_query_id":callback.get("id")})
        except Exception:
            pass
        if chat:
            data = callback.get("data")
            try:
                if data == "prices":
                    tg_send(chat,telegram_prices(),keyboard())
                elif data == "usdrub":
                    rate = usd_rub()
                    tg_send(chat,f"💱 USD/RUB\n\n1 USD = ₽{rate:.2f}\n\nИсточник: open.er-api.com",keyboard())
            except Exception:
                tg_send(chat,"Недоступно.",keyboard())
        return jsonify({"ok":True})
    message = update.get("message") or {}
    chat = (message.get("chat") or {}).get("id")
    text = (message.get("text") or "").strip()
    if chat and text.startswith(("/start","/help")):
        tg_send(chat,"CryptoPulse\nЦены, сигналы, курс USD/RUB.\nРеальная торговля отключена.",keyboard())
    elif chat and text.startswith("/prices"):
        try:
            tg_send(chat,telegram_prices(),keyboard())
        except Exception:
            tg_send(chat,"Недоступно.",keyboard())
    return jsonify({"ok":True})

def configure_telegram():
    if not TG_TOKEN:
        return
    try:
        tg("setChatMenuButton",{"menu_button":{"type":"web_app","text":"Открыть CryptoPulse","web_app":{"url":WEBAPP_URL}}})
        payload = {"url":WEBHOOK_URL}
        if TG_SECRET:
            payload["secret_token"] = TG_SECRET
        tg("setWebhook",payload)
    except Exception:
        log.exception("tg setup fail")

def worker():
    while True:
        try:
            refresh(force=True)
        except Exception:
            log.exception("bg fail")
        time.sleep(REFRESH_SECONDS)

init_db()
bot_load()

if TG_AUTO:
    configure_telegram()

if BG_WORKER:
    threading.Thread(target=worker,daemon=True).start()

threading.Thread(target=bot_worker,daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")))
