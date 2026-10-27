def bot_check_limits():
    with bot_state_lock:
        settings = bot_state["settings"]
        start = float(settings["startBalance"])
        
        # Считаем ТОЛЬКО реальный P&L: 
        # (Свободный баланс + Деньги в позициях) - Стартовый баланс
        free_balance = float(bot_state["balance"])
        positions_value = sum(p["sizeUsd"] for p in bot_state["positions"])
        
        # Плавающий P&L (текущая прибыль/убыток по открытым сделкам)
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
