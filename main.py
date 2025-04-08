"""
Главный файл приложения
"""
import logging
import threading
import asyncio
import argparse
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from config.logging_config import setup_logging
from config.settings import DATABASE_URL, TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS
from database.db_manager import DatabaseManager
from telegram_bot.bot import TelegramBot
from scheduler.jobs import JobScheduler
from telethon import TelegramClient

# Импорт компонентов workflow
from llm.qwen_model import QwenLLM
from llm.gemma_model import GemmaLLM
from agents.critic import CriticAgent

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger = setup_logging()

def run_scheduler(scheduler):
    """Запуск планировщика в отдельном потоке"""
    scheduler.start()

async def run_full_workflow(days_back=1):
    """Запуск полного рабочего процесса с использованием общего движка"""
    logger.info(f"Запуск оптимизированного рабочего процесса за последние {days_back} дней...")
    
    # Инициализация компонентов
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Определяем период
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back-1)
    
    # Используем общий движок генерации дайджестов
    from utils.digest_engine import generate_digest
    
    result = await generate_digest(
        db_manager=db_manager,
        start_date=start_date,
        end_date=end_date
    )
    
    # Записываем в лог подробные результаты
    if result["status"] == "success":
        logger.info(f"Дайджест успешно создан: краткий={bool(result.get('brief_digest_id'))}, подробный={bool(result.get('detailed_digest_id'))}")
        
        # Логируем идентификаторы для отладки
        if result.get("brief_digest_id"):
            logger.info(f"ID краткого дайджеста: {result['brief_digest_id']}")
        if result.get("detailed_digest_id"):
            logger.info(f"ID подробного дайджеста: {result['detailed_digest_id']}")
            
        return True
    else:
        logger.error(f"Ошибка при создании дайджеста: {result.get('message', result.get('error', 'Неизвестная ошибка'))}")
        return False
    
async def shutdown(signal, loop, client=None, scheduler=None, bot=None):
    """Корректное завершение приложения с закрытием всех подключений"""
    logger.info(f"Получен сигнал {signal.name}, завершение работы...")
    
    # Сначала останавливаем планировщик если он существует
    if scheduler:
        logger.info("Останавливаем планировщик...")
        scheduler.stop()
    
    # Останавливаем бота если он существует
    if bot and hasattr(bot, 'application'):
        logger.info("Останавливаем Telegram бота...")
        await bot.application.stop()
    
    # Корректно закрываем Telethon клиент
    if client:
        logger.info("Закрываем подключение к Telegram API...")
        # Важно использовать await для корректного закрытия
        await client.disconnect()
    
    # Отмена всех задач
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    logger.info(f"Отмена {len(tasks)} задач...")
    for task in tasks:
        task.cancel()
    
    # Ожидаем завершения задач с обработкой исключений
    await asyncio.gather(*tasks, return_exceptions=True)
    
    logger.info("Закрываем event loop...")
    loop.stop()
    
def run_bot_with_scheduler():
    """Запуск бота с планировщиком задач"""
    logger.info("Запуск приложения в режиме бота с планировщиком")
    
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

def main():
    """Точка входа в приложение"""
    parser = argparse.ArgumentParser(description='Запуск приложения в различных режимах')
    parser.add_argument('--mode', choices=['bot', 'workflow', 'digest'], default='bot',
                        help='Режим работы: bot - запуск бота и планировщика, '
                             'workflow - запуск полного рабочего процесса, '
                             'digest - только формирование дайджеста')
    parser.add_argument('--days', type=int, default=1, 
                        help='Количество дней для сбора сообщений (режимы workflow и digest)')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Начальная дата для дайджеста в формате ДД.ММ.ГГГГ (режим digest)')
    parser.add_argument('--end-date', type=str, default=None,
                        help='Конечная дата для дайджеста в формате ДД.ММ.ГГГГ (режим digest)')
    parser.add_argument('--category', type=str, default=None,
                        help='Фокусная категория для дайджеста (режим digest)')
    
    args = parser.parse_args()
    
    logger.info(f"Запуск приложения в режиме: {args.mode}")
    
    if args.mode == 'bot':
        run_bot_with_scheduler()
    elif args.mode == 'workflow':
        asyncio.run(run_full_workflow(days_back=args.days))
    elif args.mode == 'digest':
        # Преобразуем строки дат в datetime, если они указаны
        start_date = None
        end_date = None
        
        if args.start_date:
            try:
                # Парсим из формата ДД.ММ.ГГГГ
                day, month, year = map(int, args.start_date.split('.'))
                start_date = datetime(year, month, day)
                logger.info(f"Указана начальная дата: {start_date.strftime('%d.%m.%Y')}")
            except (ValueError, TypeError) as e:
                logger.error(f"Ошибка при разборе начальной даты: {str(e)}")
                return
                
        if args.end_date:
            try:
                day, month, year = map(int, args.end_date.split('.'))
                end_date = datetime(year, month, day)
                logger.info(f"Указана конечная дата: {end_date.strftime('%d.%m.%Y')}")
            except (ValueError, TypeError) as e:
                logger.error(f"Ошибка при разборе конечной даты: {str(e)}")
                return
        
        # Если указана только начальная дата, используем её и для конечной
        if start_date and not end_date:
            end_date = start_date
            logger.info(f"Конечная дата не указана, используем начальную: {end_date.strftime('%d.%m.%Y')}")
        
        # Если не указаны даты, используем логику на основе days_back
        if not start_date and not end_date:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=args.days-1)
            logger.info(f"Используем период {args.days} дн.: с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}")
            
        # Инициализируем БД менеджер
        db_manager = DatabaseManager(DATABASE_URL)
        
        # Импортируем новый движок дайджестов
        from utils.digest_engine import generate_digest
        
        # Запускаем генерацию с учетом фокусной категории
        result = asyncio.run(generate_digest(
            db_manager=db_manager,
            start_date=start_date,
            end_date=end_date,
            focus_category=args.category
        ))
        
        if result.get("status") == "success":
            logger.info("Дайджест успешно сформирован")
            
            # Получаем и показываем краткий дайджест
            if result.get("brief_digest_id"):
                brief_digest = db_manager.get_digest_by_id_with_sections(result["brief_digest_id"])
                if brief_digest:
                    logger.info("КРАТКИЙ ДАЙДЖЕСТ:")
                    logger.info(brief_digest.get("text", ""))
            
            # Получаем и показываем подробный дайджест, если нужно
            if result.get("detailed_digest_id"):
                detailed_digest = db_manager.get_digest_by_id_with_sections(result["detailed_digest_id"])
                if detailed_digest:
                    logger.info("ПОДРОБНЫЙ ДАЙДЖЕСТ ТАКЖЕ СФОРМИРОВАН")
                    # Раскомментируйте строку ниже, если хотите видеть и подробный дайджест в логах
                    # logger.info(detailed_digest.get("text", ""))
        else:
            logger.error(f"Не удалось сформировать дайджест: {result.get('message', result.get('error', 'Неизвестная ошибка'))}")

if __name__ == "__main__":
    main()