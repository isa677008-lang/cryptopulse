# CryptoPulse Mini App

A Telegram Mini App + Telegram bot for Bybit Spot prices and an indicative USDT/RUB rate.

## Files
- `bot.py` — Flask API and Telegram polling bot
- `static/index.html` — responsive dark Mini App interface
- `requirements.txt` — Python dependencies
- `render.yaml` — optional Render deployment blueprint

## 1. Keep your Telegram token private
Do not paste your real token into chat or commit it to a public repository. If the token was ever exposed, revoke it with `@BotFather` and use a new one.

## 2. Deploy on Render
1. Put these files in a GitHub repository (keep the repository private if you prefer).
2. In Render, create a new Web Service from that repository, or use the Blueprint flow for `render.yaml`.
3. Add environment variables:
   - `TELEGRAM_TOKEN` = your bot token from BotFather
   - `WEBAPP_URL` = the public HTTPS URL of this Render service, e.g. `https://your-service-name.onrender.com`
4. Deploy. Wait for the service to become healthy.
5. Open `https://your-service-name.onrender.com/health`; it should return `{"ok":true}`.
6. Open your Telegram bot and send `/start`. It should show the “🚀 Открыть CryptoPulse” button. The bot also attempts to set the Telegram chat menu button.

## 3. Important: only one getUpdates poller
Stop the old local `python bot.py` process before using the deployed bot. Telegram long polling should have only one active instance; multiple instances can cause `409 Conflict`.

## 4. Local test (optional)
In Windows CMD, from this folder:
```bat
python -m pip install -r requirements.txt
set TELEGRAM_TOKEN=YOUR_TOKEN_HERE
set WEBAPP_URL=https://example.com
python bot.py
```
For local website-only testing, `TELEGRAM_TOKEN` can be omitted, then open `http://127.0.0.1:10000`. Telegram's Mini App button needs a public HTTPS URL, so `example.com` is only a placeholder and won't open your app.

## Notes
- Bybit Spot provides crypto prices and 24-hour percentage changes.
- CoinGecko provides the indicative USDT/RUB conversion rate. The rate may be unavailable if that provider limits requests.
- The page refreshes automatically every 60 seconds; use the refresh button for a manual update.
- This is market information, not investment advice.
