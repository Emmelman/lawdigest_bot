# test_simple_collector.py
import os
from dotenv import load_dotenv
import asyncio
from telethon import TelegramClient
from config.settings import TELEGRAM_CHANNELS

# Загрузка переменных окружения
load_dotenv()

async def test_telegram_connection():
    """Проверка подключения к Telegram и доступности каналов"""
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    
    print(f"Подключение к Telegram с использованием API ID: {api_id}")
    
    client = TelegramClient('test_session', api_id, api_hash)
    await client.start()
    
    print("Успешное подключение к Telegram!")
    
    for channel in TELEGRAM_CHANNELS:
        try:
            entity = await client.get_entity(channel)
            print(f"Канал {channel} доступен: {entity.title}")
            
            # Получаем 5 последних сообщений
            messages = await client.get_messages(entity, limit=5)
            print(f"Получено {len(messages)} сообщений из канала {channel}")
            
            for msg in messages:
                if msg.message:
                    preview = msg.message[:50] + "..." if len(msg.message) > 50 else msg.message
                    print(f"  - {msg.date}: {preview}")
            
            print("---")
        
        except Exception as e:
            print(f"Ошибка при доступе к каналу {channel}: {str(e)}")
    
    await client.disconnect()
    print("Отключение от Telegram завершено")

if __name__ == "__main__":
    asyncio.run(test_telegram_connection())