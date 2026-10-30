"""Единый расчёт equity, P&L, комиссий, funding и двух кошельков (spot/futures)."""
from __future__ import annotations

import time

FEE_RATE = 0.001

# Funding: в симуляции берём константу.
# Точка расширения: если захочешь реальный funding с Bybit — переопредели
# FUNDING_RATE_8H через env или подмени функцию funding_rate_for_symbol().
FUNDING_RATE_8H = 0.0001  # 0.01% за 8ч (нейтральный рынок)

WALLET_SPOT = "spot"
WALLET_FUTURES = "futures"


def position_value(position: dict, price: float) -> float:
    return position["qty"] * price


def position_unrealized_pnl(position: dict, price: float) -> float:
    """P&L без funding. Long: (price-entry)*qty. Short: (entry-price)*qty."""
    entry = position["entryPrice"]
    qty = position["qty"]
    if position["side"] == "Long":
        return (price - entry) * qty
    return (entry - price) * qty


def funding_pnl(position: dict, rate_8h: float = FUNDING_RATE_8H,
                hours_held: float = 0.0) -> float:
    """Funding за время удержания в USD (знаковая величина).

    Long: rate>0 → платит (pnl<0). Short: rate>0 → получает (pnl>0).
    """
    if hours_held <= 0:
        return 0.0
    periods = hours_held / 8.0
    notional = position["sizeUsd"]
    if position["side"] == "Long":
        return -notional * rate_8h * periods
    return notional * rate_8h * periods


def compute_equity(balance_free: float, positions: list, prices: dict,
                   wallet: str = WALLET_SPOT,
                   funding_rate: float = FUNDING_RATE_8H) -> dict:
    """Equity для одного кошелька. Позиции другого кошелька игнорируются.

    Позиции без поля 'wallet' считаются spot (обратная совместимость).
    """
    pos_value = 0.0
    unrealized = 0.0
    funding_total = 0.0
    missing = []
    now = time.time()
    for p in positions:
        p_wallet = p.get("wallet", WALLET_SPOT)
        if p_wallet != wallet:
            continue
        price = prices.get(p["symbol"])
        if price is None:
            missing.append(p["symbol"])
            price = p["entryPrice"]
        pos_value += position_value(p, price)
        unrealized += position_unrealized_pnl(p, price)
        # Funding по открытой позиции — от openedTs до now.
        opened_ts = p.get("openedTs")
        if opened_ts:
            hours = max(0.0, (now - opened_ts) / 3600.0)
            funding_total += funding_pnl(p, funding_rate, hours)
    equity = balance_free + pos_value + funding_total
    return {
        "wallet": wallet,
        "equity": equity,
        "free_cash": balance_free,
        "positions_value": pos_value,
        "unrealized_pnl": unrealized,
        "funding_pnl": funding_total,
        "missing_prices": missing,
    }


def open_position_costs(size_usd: float) -> dict:
    fee = size_usd * FEE_RATE
    return {"size_usd": size_usd, "entry_fee": fee, "cash_out": size_usd + fee}


def close_position_costs(size_usd: float, gross_pnl: float,
                         funding: float = 0.0) -> dict:
    """funding — уже в USD (знаковая), вычитается из net_pnl."""
    exit_fee = size_usd * FEE_RATE
    net_pnl = gross_pnl + funding - exit_fee
    cash_in = size_usd + net_pnl
    return {
        "exit_fee": exit_fee,
        "funding": funding,
        "net_pnl": net_pnl,
        "cash_in": cash_in,
    }


def size_from_risk(balance: float, risk_pct: float, sl_distance_pct: float,
                   max_position_pct: float = 0.2) -> float:
    if sl_distance_pct <= 0:
        raise ValueError("sl_distance_pct must be > 0")
    risk_amount = balance * (risk_pct / 100.0)
    raw = risk_amount / (sl_distance_pct / 100.0)
    cap = balance * (max_position_pct / 100.0)
    return max(0.0, min(raw, cap))
