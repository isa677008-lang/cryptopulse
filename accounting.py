"""Единый расчёт equity, P&L, комиссий."""

FEE_RATE = 0.001


def position_value(position: dict, price: float) -> float:
    return position["qty"] * price


def position_unrealized_pnl(position: dict, price: float) -> float:
    entry = position["entryPrice"]
    qty = position["qty"]
    if position["side"] == "Long":
        return (price - entry) * qty
    return (entry - price) * qty


def compute_equity(balance_free: float, positions: list, prices: dict) -> dict:
    pos_value = 0.0
    unrealized = 0.0
    missing = []
    for p in positions:
        price = prices.get(p["symbol"])
        if price is None:
            missing.append(p["symbol"])
            price = p["entryPrice"]
        pos_value += position_value(p, price)
        unrealized += position_unrealized_pnl(p, price)
    equity = balance_free + pos_value
    return {
        "equity": equity,
        "free_cash": balance_free,
        "positions_value": pos_value,
        "unrealized_pnl": unrealized,
        "missing_prices": missing,
    }


def open_position_costs(size_usd: float) -> dict:
    fee = size_usd * FEE_RATE
    return {"size_usd": size_usd, "entry_fee": fee, "cash_out": size_usd + fee}


def close_position_costs(size_usd: float, gross_pnl: float) -> dict:
    exit_fee = size_usd * FEE_RATE
    net_pnl = gross_pnl - exit_fee
    cash_in = size_usd + net_pnl
    return {"exit_fee": exit_fee, "net_pnl": net_pnl, "cash_in": cash_in}


def size_from_risk(balance: float, risk_pct: float, sl_distance_pct: float,
                   max_position_pct: float = 0.2) -> float:
    if sl_distance_pct <= 0:
        raise ValueError("sl_distance_pct must be > 0")
    risk_amount = balance * (risk_pct / 100.0)
    raw = risk_amount / (sl_distance_pct / 100.0)
    cap = balance * (max_position_pct / 100.0)
    return max(0.0, min(raw, cap))
