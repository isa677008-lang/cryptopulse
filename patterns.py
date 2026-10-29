"""
CryptoPulse — детектор свечных паттернов.
Работает через TA-Lib (61 паттерн). Если TA-Lib не установлен — fallback на
упрощённый встроенный детектор (только 6 базовых паттернов).
"""
import logging

log = logging.getLogger("cryptopulse.patterns")

try:
    import numpy as np
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    talib = None
    np = None
    TALIB_AVAILABLE = False
    log.warning("TA-Lib не установлен — используется упрощённый детектор паттернов")


# ============================================================
# СПИСОК ПАТТЕРНОВ (TA-Lib имена)
# ============================================================

BULLISH_PATTERNS = {
    "CDLHAMMER": "Молот",
    "CDLINVERTEDHAMMER": "Перевёрнутый молот",
    "CDLENGULFING": "Поглощение",
    "CDLMORNINGSTAR": "Утренняя звезда",
    "CDLPIERCING": "Просвет в облаках",
    "CDL3WHITESOLDIERS": "Три белых солдата",
    "CDLHARAMI": "Харами",
    "CDLDRAGONFLYDOJI": "Доджи-стрекоза",
    "CDL3INSIDEUP": "Три внутренних вверх",
    "CDL3OUTSIDEUP": "Три внешних вверх",
    "CDLBELTHOLD": "Захват за пояс",
    "CDLMEETINGLINES": "Встреча линий",
}

BEARISH_PATTERNS = {
    "CDLSHOOTINGSTAR": "Падающая звезда",
    "CDLEVENINGSTAR": "Вечерняя звезда",
    "CDLDARKCLOUDCOVER": "Завеса из тёмных облаков",
    "CDL3BLACKCROWS": "Три чёрные вороны",
    "CDLHARAMI": "Харами (медвежье)",
    "CDLGRAVESTONEDOJI": "Доджи-надгробие",
    "CDL3INSIDEDOWN": "Три внутренних вниз",
    "CDL3OUTSIDEDOWN": "Три внешних вниз",
    "CDLENGULFING": "Поглощение (медвежье)",
    "CDLHANGINGMAN": "Повешенный",
    "CDLIDENTICAL3CROWS": "Три идентичные вороны",
    "CDLUPSIDEGAP2CROWS": "Разрыв вверх + 2 вороны",
}

NEUTRAL_PATTERNS = {
    "CDLDOJI": "Доджи",
    "CDLSPINNINGTOP": "Волчок",
    "CDLHIGHWAVE": "Высокая волна",
    "CDLLONGLEGGEDDOJI": "Длинноногий доджи",
    "CDLSHORTLINE": "Короткая линия",
}


# ============================================================
# УПРОЩЁННЫЙ FALLBACK (если TA-Lib нет)
# ============================================================

def _fallback_pattern(o, h, l, c, po, pc):
    """Возвращает (name, direction) или None. Один паттерн по последней свече."""
    body = abs(c - o)
    rng = h - l
    upper_shadow = h - max(o, c)
    lower_shadow = min(o, c) - l
    if rng == 0:
        return None
    body_pct = body / rng
    # Молот
    if lower_shadow > body * 2 and upper_shadow < body and body_pct < 0.35:
        return ("Молот (fallback)", "bullish")
    # Падающая звезда
    if upper_shadow > body * 2 and lower_shadow < body and body_pct < 0.35:
        return ("Падающая звезда (fallback)", "bearish")
    # Бычье поглощение
    if pc < po and c > o and c >= po and o <= pc:
        return ("Поглощение (fallback)", "bullish")
    # Медвежье поглощение
    if pc > po and c < o and c <= po and o >= pc:
        return ("Поглощение медвежье (fallback)", "bearish")
    # Доджи
    if body_pct < 0.1:
        return ("Доджи (fallback)", "neutral")
    return None


# ============================================================
# ПУБЛИЧНЫЙ API
# ============================================================

def detect_patterns(candles, lookback=3):
    """
    candles: список dict с ключами open/high/low/close (в хронологическом порядке,
             последняя — самая свежая, уже закрытая).
    lookback: сколько последних свечей проверить (обычно 1-3).

    Возвращает:
        {
          "enabled": bool,        # TA-Lib доступен
          "method": "talib" | "fallback",
          "found": [ {"name": str, "direction": "bullish"|"bearish"|"neutral", "index": int} ],
          "has_bullish": bool,
          "has_bearish": bool,
          "has_neutral": bool,
        }
    """
    result = {
        "enabled": bool(candles) and len(candles) >= 5,
        "method": "talib" if TALIB_AVAILABLE else "fallback",
        "found": [],
        "has_bullish": False,
        "has_bearish": False,
        "has_neutral": False,
    }
    if not result["enabled"]:
        return result

    o = [c["open"] for c in candles]
    h = [c["high"] for c in candles]
    l = [c["low"] for c in candles]
    c = [c["close"] for c in candles]

    # Проверяем последние `lookback` свечей (самые свежие)
    start_idx = max(0, len(candles) - lookback)

    if TALIB_AVAILABLE:
        o_arr = np.array(o, dtype=float)
        h_arr = np.array(h, dtype=float)
        l_arr = np.array(l, dtype=float)
        c_arr = np.array(c, dtype=float)

        for func_name, human_name in BULLISH_PATTERNS.items():
            try:
                out = getattr(talib, func_name)(o_arr, h_arr, l_arr, c_arr)
            except Exception:
                continue
            if out is None:
                continue
            for i in range(start_idx, len(out)):
                if out[i] > 0:
                    result["found"].append({"name": human_name, "direction": "bullish", "index": i})
                    result["has_bullish"] = True
                    break

        for func_name, human_name in BEARISH_PATTERNS.items():
            try:
                out = getattr(talib, func_name)(o_arr, h_arr, l_arr, c_arr)
            except Exception:
                continue
            if out is None:
                continue
            for i in range(start_idx, len(out)):
                if out[i] < 0:
                    result["found"].append({"name": human_name, "direction": "bearish", "index": i})
                    result["has_bearish"] = True
                    break

        for func_name, human_name in NEUTRAL_PATTERNS.items():
            try:
                out = getattr(talib, func_name)(o_arr, h_arr, l_arr, c_arr)
            except Exception:
                continue
            if out is None:
                continue
            for i in range(start_idx, len(out)):
                if out[i] != 0:
                    result["found"].append({"name": human_name, "direction": "neutral", "index": i})
                    result["has_neutral"] = True
                    break
    else:
        # Fallback: только последняя свеча
        if len(candles) >= 2:
            p = _fallback_pattern(o[-1], h[-1], l[-1], c[-1], o[-2], c[-2])
            if p:
                name, direction = p
                result["found"].append({"name": name, "direction": direction, "index": len(candles) - 1})
                if direction == "bullish":
                    result["has_bullish"] = True
                elif direction == "bearish":
                    result["has_bearish"] = True
                else:
                    result["has_neutral"] = True

    return result


def pattern_strength(patterns_result):
    """0..1 — насколько сильный сигнал дают паттерны.
    1 бычий + ничего медвежьего = 1.0
    2+ бычьих = 1.0
    только бычий без медвежьего = 0.6
    смешанные = 0.3
    """
    if not patterns_result["enabled"]:
        return 0.0
    bull = patterns_result["has_bullish"]
    bear = patterns_result["has_bearish"]
    if bull and not bear:
        n = sum(1 for f in patterns_result["found"] if f["direction"] == "bullish")
        return min(1.0, 0.6 + 0.2 * n)
    if bull and bear:
        return 0.3
    return 0.0
