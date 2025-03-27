# simple_telegram_bot.py
import os
import asyncio
import logging
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import InputPeerUser

from database.db_manager import DatabaseManager

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Настройки Telegram
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# URL базы данных
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///lawdigest.db")

# Максимальный размер сообщения в Telegram (символов)
MAX_MESSAGE_LENGTH = 4000

def split_text(text, max_length=MAX_MESSAGE_LENGTH):
    """
    Разбивает длинный текст на части для отправки в Telegram
    """
    if len(text) <= max_length:
        return [text]
    
    parts = []
    current_part = ""
    
    for paragraph in text.split("\n\n"):
        # Если абзац сам по себе слишком длинный
        if len(paragraph) > max_length:
            # Добавляем текущую часть, если она есть
            if current_part:
                parts.append(current_part)
                current_part = ""
            
            # Разбиваем длинный абзац на предложения
            sentences = paragraph.split(". ")
            sentence_part = ""
            
            for sentence in sentences:
                if len(sentence_part) + len(sentence) + 2 <= max_length:
                    if sentence_part:
                        sentence_part += ". " + sentence
                    else:
                        sentence_part = sentence
                else:
                    parts.append(sentence_part.strip())
                    sentence_part = sentence
            
            if sentence_part:
                parts.append(sentence_part.strip())
        
        # Обычный случай - проверяем, можно ли добавить абзац к текущей части
        elif len(current_part) + len(paragraph) + 2 <= max_length:
            if current_part:
                current_part += "\n\n" + paragraph
            else:
                current_part = paragraph
        else:
            parts.append(current_part)
            current_part = paragraph
    
    if current_part:
        parts.append(current_part)
    
    return parts

async def main():
    """Главная функция бота"""
    logger.info("Запуск простого Telegram-бота для показа дайджеста...")
    
    # Инициализация менеджера БД
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Проверяем, есть ли дайджест в БД
    digest = db_manager.get_latest_digest()
    if not digest:
        logger.error("В базе данных нет дайджестов. Сначала сгенерируйте дайджест.")
        return
    
    logger.info(f"Найден дайджест от {digest.date.strftime('%d.%m.%Y')} (ID: {digest.id})")
    
    # Создаем клиент Telegram
    client = TelegramClient('bot_session', API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    
    logger.info("Бот запущен!")
    logger.info("Бот будет отвечать на команды /start, /help и /digest")
    
    # Обработчик команды /start
    @client.on(events.NewMessage(pattern='/start'))
    async def start_handler(event):
        await event.respond(
            "Здравствуйте! Я бот для дайджеста правовых новостей.\n\n"
            "Доступные команды:\n"
            "/digest - получить последний дайджест\n"
            "/help - получить справку"
        )
    
    # Обработчик команды /help
    @client.on(events.NewMessage(pattern='/help'))
    async def help_handler(event):
        await event.respond(
            "Я могу предоставить вам дайджест правовых новостей.\n\n"
            "Доступные команды:\n"
            "/digest - получить последний дайджест\n"
            "/help - получить справку"
        )
    
    # Обработчик команды /digest
    @client.on(events.NewMessage(pattern='/digest'))
    async def digest_handler(event):
        digest = db_manager.get_latest_digest()
        
        if not digest:
            await event.respond("К сожалению, дайджест еще не сформирован.")
            return
        
        # Разбиваем дайджест на части, чтобы не превысить ограничение Telegram
        chunks = split_text(digest.text)
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                await event.respond(f"Дайджест за {digest.date.strftime('%d.%m.%Y')}:\n\n{chunk}")
            else:
                await event.respond(chunk)
        
        logger.info(f"Отправлен дайджест пользователю {event.sender_id}")
    
    # Запускаем обработку сообщений
    try:
        logger.info("Бот готов к использованию. Нажмите Ctrl+C для остановки.")
        await client.run_until_disconnected()
    finally:
        await client.disconnect()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")