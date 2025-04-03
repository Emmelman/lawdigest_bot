import os
from dotenv import load_dotenv
from telethon import TelegramClient
import asyncio

# Загрузка переменных окружения из .env файла
load_dotenv()

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")

# Проверка наличия необходимых переменных
if not API_ID or not API_HASH:
    print("Ошибка: Не найдены TELEGRAM_API_ID или TELEGRAM_API_HASH в .env файле")
    exit(1)

print(f"Используем API_ID: {API_ID}")
print(f"Используем API_HASH: {API_HASH[:4]}...{API_HASH[-4:]}")  # Показываем только часть хэша для безопасности

async def test_telegram_connection():
    print("Создаем соединение с Telegram...")
    
    # Создаем клиент с именем сессии 'test_session'
    client = TelegramClient('test_session', API_ID, API_HASH)
    
    # Запускаем клиент и проверяем авторизацию
    await client.start()
    
    if await client.is_user_authorized():
        print("Успешная авторизация!")
        
        # Получаем информацию о текущем пользователе
        me = await client.get_me()
        print(f"Подключено как: {me.first_name} (@{me.username})")
        
        # Проверяем доступ к каналам (попробуем получить информацию о нескольких каналах)
        test_channels = ['@dumainfo', '@sovfedinfo', '@vsrf_ru']
        
        for channel_name in test_channels:
            try:
                channel = await client.get_entity(channel_name)
                print(f"Успешно получена информация о канале {channel_name}: {channel.title}")
            except Exception as e:
                print(f"Ошибка при получении информации о канале {channel_name}: {str(e)}")
    else:
        print("Ошибка авторизации. Возможно, требуется интерактивный вход.")
    
    # Отключаемся
    await client.disconnect()
    print("Соединение закрыто")

# Запускаем тестовую функцию
if __name__ == "__main__":
    asyncio.run(test_telegram_connection())