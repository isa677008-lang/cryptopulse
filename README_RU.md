# CryptoPulse — единый проект

В архиве уже собраны сайт котировок, API, автообновление страницы и Telegram Mini App webhook.

## Что уже настроено
- Котировки через CoinPaprika, резервные источники Binance и CoinGecko.
- Кэш котировок на сервере на 60 секунд.
- Автообновление страницы каждые 60 секунд и кнопка ручного обновления.
- Telegram-команда `/start` присылает кнопку «Открыть CryptoPulse».
- Render health check: `/health`.

## Настройка Render (обязательна один раз)
1. В Render откройте существующий сервис `cryptopulse` → **Environment**.
2. Добавьте `TELEGRAM_TOKEN` — токен, который выдал официальный бот `@BotFather`.
3. Добавьте `WEBAPP_URL` — публичный HTTPS-адрес сайта Render, например `https://ВАШ-СЕРВИС.onrender.com` (без `/api/prices` и без завершающего слеша).
4. Сохраните изменения и запустите redeploy. Если сервис уже связан с GitHub, замените в репозитории файлы содержимым архива и отправьте commit; Render сам выполнит деплой.

**Важно:** не вставляйте токен в `bot.py`, HTML, README или GitHub. Вставляйте его только в Render → Environment. Не отправляйте токен в чат.

## Команды Render
Build Command:
`pip install -r requirements.txt`

Start Command:
`gunicorn --workers 1 --threads 4 --timeout 60 bot:app`

## Проверка после деплоя
- Откройте `https://ВАШ-СЕРВИС.onrender.com/` — должна открыться страница.
- Откройте `https://ВАШ-СЕРВИС.onrender.com/health` — ответ должен содержать `"ok":true`.
- Напишите своему боту в Telegram `/start` — он должен прислать кнопку открытия сайта.

Если котировки не загрузились, это может быть временным ограничением внешних API. Источники проверяются по очереди: CoinPaprika → Binance → CoinGecko.
