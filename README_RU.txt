CryptoPulse — исправленная версия

1. Замените bot.py в GitHub на этот файл.
2. В Render оставьте TELEGRAM_TOKEN как уже настроенную секретную переменную.
3. WEBAPP_URL должен быть адресом вашего Render-сайта, например https://ВАШ-СЕРВИС.onrender.com
4. После сохранения bot.py Render должен автоматически выполнить новый deploy.
5. Проверьте https://ВАШ-СЕРВИС.onrender.com/api/prices

В коде Bybit используется первым, а при ошибке — Binance как резервный источник.
