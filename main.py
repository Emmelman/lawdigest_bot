"""
Главный файл приложения
"""
import logging
import threading
import os
from dotenv import load_dotenv

from config.logging_config import setup_logging
from config.settings import DATABASE_URL
from database.db_manager import DatabaseManager
from telegram_bot.bot import TelegramBot
from scheduler.jobs import JobScheduler

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger = setup_logging()

def run_scheduler(scheduler):
    """Запуск планировщика в отдельном потоке"""
    scheduler.start()

def main():
    """Точка входа в приложение"""
    logger.info("Запуск приложения")
    
    # Инициализация менеджера БД
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Инициализация и запуск планировщика в отдельном потоке
    scheduler = JobScheduler(db_manager)
    scheduler_thread = threading.Thread(target=run_scheduler, args=(scheduler,))
    scheduler_thread.daemon = True
    scheduler_thread.start()
    logger.info("Планировщик запущен в отдельном потоке")
    
    # Инициализация и запуск Telegram-бота
    bot = TelegramBot(db_manager)
    bot.run()
    
    # Этот код не будет достигнут, пока бот работает
    logger.info("Приложение завершает работу")
    scheduler.stop()

if __name__ == "__main__":
    main()