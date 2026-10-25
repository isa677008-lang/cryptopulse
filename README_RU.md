# CryptoPulse — установка

В архиве: `bot.py`, `static/index.html`, `requirements.txt`, `render.yaml`, `README_RU.md`.

## GitHub
Загрузи содержимое архива в корень репозитория, сохраняя структуру папки `static/index.html`.

## Render
Build Command: `pip install -r requirements.txt`
Start Command: `gunicorn --workers 1 --threads 4 --timeout 60 bot:app`

В Environment добавь:
- `TELEGRAM_TOKEN` — токен BotFather. Не публикуй и никому не отправляй его.
- `WEBAPP_URL` — публичный URL сервиса Render, например `https://имя-сервиса.onrender.com`, без `/api/prices`.

После коммита запусти Manual Deploy → Deploy latest commit.

## Проверка
- `/health` — ожидается `{"ok":true,"service":"CryptoPulse"}`.
- `/api/prices` — JSON с `ok: true` и массивом `prices`.
- `/` — страница котировок.

API сначала пробует Binance, затем CoinGecko. Публичные API могут быть временно недоступны или ограничивать запросы, поэтому абсолютную бесперебойность обещать нельзя. Если обе службы недоступны, приложение покажет ошибку, а не выдуманные цены.

Telegram: при наличии `TELEGRAM_TOKEN` и `WEBAPP_URL` приложение настроит webhook. Команда `/start` отправляет кнопку открытия сайта. Render Free может засыпать после простоя, из-за чего первый запрос иногда медленный.
