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

async def collect_messages(client, db_manager, channel, days_back=1, limit_per_request=100):
    """Сбор сообщений из канала и сохранение в БД"""
    logger.info(f"Сбор сообщений из канала {channel} за последние {days_back} дней...")
    
    try:
        entity = await client.get_entity(channel)
        
        # Определение дат для фильтрации
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Период сбора: с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")
        
        # Получаем сообщения с пагинацией
        offset_id = 0
        all_messages = []
        total_messages = 0
        
        while True:
            messages = await client.get_messages(
                entity, 
                limit=limit_per_request,
                offset_id=offset_id
            )
            
            if not messages:
                break
                
            total_messages += len(messages)
            
            # Фильтруем сообщения по дате - важно привести даты к одному формату!
            filtered_messages = []
            for msg in messages:
                # Преобразуем дату из Telegram (aware) в naive datetime
                msg_date = msg.date.replace(tzinfo=None)
                if start_date <= msg_date <= end_date:
                    filtered_messages.append(msg)
            
            all_messages.extend(filtered_messages)
            
            # Проверяем, нужно ли продолжать пагинацию
            if len(messages) < limit_per_request:
                # Получили меньше сообщений, чем запрашивали (конец списка)
                break
                
            # Проверяем дату последнего сообщения
            last_date = messages[-1].date.replace(tzinfo=None)
            if last_date < start_date:
                # Последнее сообщение старше начальной даты, прекращаем сбор
                break
                
            # Устанавливаем смещение для следующего запроса
            offset_id = messages[-1].id
            
            logger.debug(f"Получено {len(filtered_messages)} сообщений из {len(messages)}. "
                         f"Продолжаем пагинацию с ID {offset_id}")
        
        logger.info(f"Всего получено {total_messages} сообщений, отфильтровано {len(all_messages)} "
                    f"за указанный период")
        
        # Сохраняем отфильтрованные сообщения
        saved_count = 0
        for msg in all_messages:
            if msg.message:  # Проверяем, что сообщение содержит текст
                try:
                    db_manager.save_message(
                        channel=channel,
                        message_id=msg.id,
                        text=msg.message,
                        date=msg.date.replace(tzinfo=None)  # Убираем информацию о часовом поясе
                    )
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Ошибка при сохранении сообщения {msg.id}: {str(e)}")
        
        logger.info(f"Сохранено {saved_count} сообщений из канала {channel}")
        return saved_count
    except Exception as e:
        logger.error(f"Ошибка при сборе сообщений из канала {channel}: {str(e)}")
        return 0

async def analyze_messages(db_manager, llm_model, limit=50):
    """Анализ и классификация сообщений"""
    logger.info(f"Анализ сообщений (лимит: {limit})...")
    
    from agents.analyzer import AnalyzerAgent
    analyzer = AnalyzerAgent(db_manager, llm_model)
    result = analyzer.analyze_messages(limit=limit)
    
    logger.info(f"Анализ завершен: {result}")
    return result

async def review_categorization(db_manager, limit=20):
    """Проверка и исправление категоризации"""
    logger.info(f"Проверка категоризации последних {limit} сообщений...")
    
    critic = CriticAgent(db_manager)
    results = critic.review_recent_categorizations(limit=limit)
    
    logger.info(f"Проверка завершена. Всего: {results['total']}, обновлено: {results['updated']}, "
                f"без изменений: {results['unchanged']}")
    
    return results

async def create_digest(db_manager, llm_model, days_back=1):
    """Создание дайджеста"""
    logger.info(f"Создание дайджеста за последние {days_back} дней...")
    
    from agents.digester import DigesterAgent
    digester = DigesterAgent(db_manager, llm_model)
    digest = digester.create_digest(days_back=days_back)
    
    logger.info(f"Дайджест создан: {digest.get('status', 'unknown')}")
    return digest

# Обновление в main.py

async def run_full_workflow(days_back=1):
    """Запуск полного рабочего процесса с уверенностью и оптимизацией"""
    logger.info(f"Запуск оптимизированного рабочего процесса за последние {days_back} дней...")
    
    # Инициализация компонентов
    db_manager = DatabaseManager(DATABASE_URL)
    qwen_model = QwenLLM()
    gemma_model = GemmaLLM()
    
    # Создаем клиент Telegram
    client = TelegramClient('workflow_session', TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start()
    
    try:
        # Шаг 1: Параллельный сбор данных
        logger.info("Шаг 1: Параллельный сбор данных")
        from agents.data_collector import DataCollectorAgent
        collector = DataCollectorAgent(db_manager)
        
        # Прямой вызов асинхронного метода
        collect_result = await collector.collect_data(days_back=days_back)
        total_messages = collect_result.get("total_new_messages", 0)
        
        logger.info(f"Всего собрано {total_messages} новых сообщений")
        
        if total_messages == 0:
            logger.info("Нет новых сообщений для анализа. Проверка на существующие сообщения с категориями...")
            # Проверяем, есть ли уже сообщения с категориями
            existing_messages = db_manager.get_recently_categorized_messages(1)
            if not existing_messages:
                logger.info("Нет сообщений с категориями. Завершение работы.")
                return False
        
        # Шаг 2: Анализ сообщений
        logger.info("Шаг 2: Анализ сообщений с оценкой уверенности")
        from agents.analyzer import AnalyzerAgent
        analyzer = AnalyzerAgent(db_manager, qwen_model)
        analyzer.fast_check = True  # Включаем быструю проверку
        analyze_result = analyzer.analyze_messages(
            limit=max(total_messages, 30), 
            batch_size=5
        )
        
        analyzed_count = analyze_result.get("analyzed_count", 0)
        confidence_stats = analyze_result.get("confidence_stats", {})
        
        logger.info(f"Проанализировано {analyzed_count} сообщений")
        logger.info(f"Распределение по уровням уверенности: {confidence_stats}")
        
        # Шаг 3: Проверка категоризации
        logger.info("Шаг 3: Проверка категоризации сообщений с низкой уверенностью")
        critic = CriticAgent(db_manager)
        review_result = critic.review_recent_categorizations(
            confidence_threshold=2,  # Проверять только сообщения с уверенностью 1-2
            limit=50,
            batch_size=5,
            max_workers=3
        )
        
        updated_count = review_result.get("updated", 0)
        logger.info(f"Проверка категоризации: обновлено {updated_count} сообщений")
        
        # Шаг 4: Создание дайджеста
        logger.info("Шаг 4: Создание дайджеста")
        from agents.digester import DigesterAgent
        digester = DigesterAgent(db_manager, gemma_model)
        digest_result = digester.create_digest(days_back=days_back)
        
        has_brief = "brief_digest_id" in digest_result
        has_detailed = "detailed_digest_id" in digest_result
        
        if has_brief or has_detailed:
            logger.info(f"Дайджест успешно создан: краткий={has_brief}, подробный={has_detailed}")
            return True
        else:
            logger.error("Не удалось создать дайджест")
            return False
    
    finally:
        # Закрываем соединение с Telegram
        await client.disconnect()

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
    
    args = parser.parse_args()
    
    logger.info(f"Запуск приложения в режиме: {args.mode}")
    
    if args.mode == 'bot':
        run_bot_with_scheduler()
    elif args.mode == 'workflow':
        asyncio.run(run_full_workflow(days_back=args.days))
    elif args.mode == 'digest':
        db_manager = DatabaseManager(DATABASE_URL)
        gemma_model = GemmaLLM()
        digest = asyncio.run(create_digest(db_manager, gemma_model, days_back=args.days))
        
        if digest and digest.get('status') == 'success':
            logger.info("Дайджест успешно сформирован")
            logger.info(digest.get('digest_text', ''))
        else:
            logger.error("Не удалось сформировать дайджест")

if __name__ == "__main__":
    main()