# CryptoPulse

## Структура
- `bot.py` — Flask API и Telegram webhook.
- `static/index.html` — сайт котировок.
- `requirements.txt` — зависимости.
- `render.yaml` — настройки Render.

## Render
Build Command: `pip install -r requirements.txt`
Start Command: `gunicorn --workers 1 --threads 4 --timeout 60 bot:app`

В Environment задайте `TELEGRAM_TOKEN` и `WEBAPP_URL` (публичный адрес сайта Render, без `/api/prices`). Никогда не публикуйте токен в GitHub.

Проверки:
- `/health` — `{"ok":true,"service":"CryptoPulse"}`
- `/api/prices` — JSON с `ok: true` и `prices`.
- `/` — сайт котировок.

Источники в порядке проверки: CoinPaprika, Binance, CoinGecko. Публичные API могут ограничивать запросы или быть недоступны из отдельных регионов; стабильность не гарантируется. Кэш уменьшает количество запросов. Render Free может засыпать после простоя.
