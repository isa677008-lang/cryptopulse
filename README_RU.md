CryptoPulse — улучшенная сборка
Что в комплекте
- app.py — Flask API, источники CoinGecko/Bybit, PostgreSQL-история, калькулятор и Telegram webhook.
- static/index.html — лёгкая адаптивная панель с котировками.
- requirements.txt, Procfile, render.yaml — запуск и развёртывание.
- .env.example — список переменных окружения без секретов.
Важные ограничения
- Реальные ордера намеренно отключены (LIVE_TRADING_ENABLED = False). Эта сборка не размещает сделки.
- Сигналы EMA/RSI экспериментальны: точки истории снимаются примерно раз в минуту, а не из биржевых свечей. Не используйте их как готовую торговую систему без бэктеста и проверки на демо-счёте.
- Без DATABASE_URL история хранится только в памяти процесса и пропадает при перезапуске.
- Для устойчивой истории подключите PostgreSQL.
Развёртывание на Render
1. Загрузите содержимое папки в свой Git-репозиторий.
2. Создайте Web Service из репозитория. Можно использовать render.yaml или команды из Procfile.
3. Добавьте переменные окружения из .env.example в панели Render. В PUBLIC_APP_URL укажите реальный HTTPS-адрес сервиса.
4. Сгенерируйте случайный TELEGRAM_WEBHOOK_SECRET (не токен бота) и задайте его в Render. При включённой автонастройке приложение передаст его Telegram и проверит заголовок webhook.
5. Для постоянной истории подключите PostgreSQL и установите DATABASE_URL из панели базы данных.
6. После деплоя проверьте /health, затем /api/config и /api/prices.
Локальный запуск
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
# Скопируйте .env.example в .env и выставьте переменные в оболочке/IDE.
python app.py
Переменные
- TELEGRAM_TOKEN — токен Telegram-бота. Не вставляйте его в код или Git.
- TELEGRAM_WEBHOOK_SECRET — секрет для проверки входящих webhook-запросов.
- PUBLIC_APP_URL — публичный HTTPS URL сервиса.
- TELEGRAM_WEBAPP_URL — HTTPS URL Mini App.
- TELEGRAM_WEBHOOK_URL — HTTPS URL /telegram-webhook.
- DATABASE_URL — PostgreSQL connection string.
- COINGECKO_API_KEY — необязательный ключ CoinGecko.
- TELEGRAM_AUTO_CONFIGURE — автоматически задавать Telegram menu button и webhook (по умолчанию true).
- ENABLE_BACKGROUND_WORKER — собирать историю в фоне (по умолчанию true). Используйте один Gunicorn worker, как в Procfile, чтобы не плодить одинаковые потоки.
- LOG_LEVEL — уровень журналирования.
Что улучшено
- добавлена проверка секретного заголовка Telegram webhook, если задан TELEGRAM_WEBHOOK_SECRET;
- ограничен размер тела входящего запроса;
- добавлены базовые HTTP security headers;
- публичные API-ошибки больше не раскрывают внутренние исключения;
- проверяются нечисловые и бесконечные значения котировок и калькулятора;
- добавлена полноценная стартовая страница, чтобы / не возвращал ошибку при отсутствии static/index.html;
- конфигурация Render и запуск Gunicorn закреплены на одном worker для совместимости с in-memory состоянием и фоновым сборщиком.
Перед публикацией проверьте, что ни токен Telegram, ни пароль БД не попали в историю Git или логи. Если секрет уже публиковался, замените его у провайдера.
