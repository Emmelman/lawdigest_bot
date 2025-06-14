"""
Главный файл приложения
"""
import logging # Keep logging import
import threading
import asyncio
import argparse
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from config.logging_config import setup_logging
from config.settings import DATABASE_URL, TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS
from database.db_manager import DatabaseManager
from utils.telegram_session_manager import TelegramSessionManager # Import the session manager
from telegram_bot.bot import TelegramBot
from scheduler.jobs import JobScheduler
from telethon import TelegramClient

# Импорт компонентов workflow
from llm.qwen_model import QwenLLM
from llm.gemma_model import GemmaLLM
from agents.orchestrator import OrchestratorAgent
from agents.agent_registry import AgentRegistry
from agents.task_queue import TaskQueue
from agents.critic import CriticAgent

# Загрузка переменных окружения
logger = logging.getLogger(__name__) # Added logger definition

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
async def run_orchestrated_workflow(scenario: str = "daily_workflow", **kwargs):
    """Запуск рабочего процесса через оркестратор"""
    logger.info(f"Запуск оркестрированного рабочего процесса: {scenario}")
    
    # Инициализация компонентов
    db_manager = DatabaseManager(DATABASE_URL)
    agent_registry = AgentRegistry(db_manager)
    orchestrator = OrchestratorAgent(db_manager, agent_registry)
    
    try:
        # Запускаем планирование и выполнение
        result = await orchestrator.plan_and_execute(scenario, **kwargs)
        
        # Выводим результаты
        logger.info("=== РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ ===")
        logger.info(f"Статус: {result.get('status')}")
        logger.info(f"Сценарий: {result.get('metrics', {}).get('scenario')}")
        logger.info(f"Успешность: {result.get('metrics', {}).get('success_rate', 0):.1%}")
        logger.info(f"Время выполнения: {result.get('metrics', {}).get('total_execution_time', 0):.1f}с")
        
        summary = result.get('summary', {})
        logger.info(f"Собрано сообщений: {summary.get('collected_messages', 0)}")
        logger.info(f"Проанализировано: {summary.get('analyzed_messages', 0)}")
        logger.info(f"Улучшено критиком: {summary.get('reviewed_messages', 0)}")
        logger.info(f"Создано дайджестов: {len(summary.get('created_digests', []))}")
        logger.info(f"Обновлено дайджестов: {len(summary.get('updated_digests', []))}")
        
        # Выводим рекомендации
        recommendations = result.get('recommendations', [])
        if recommendations:
            logger.info("=== РЕКОМЕНДАЦИИ ===")
            for rec in recommendations:
                logger.info(f"- {rec.get('description')}")
        
        return result.get('status') == 'success'
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении оркестрированного процесса: {str(e)}")
        return False
    finally:
        # Закрываем соединения
        session_manager = TelegramSessionManager(api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH)
        await session_manager.close_all_clients()

async def run_full_workflow(days_back=1, force_update=False):
    """Запуск полного рабочего процесса с уверенностью и оптимизацией"""
    logger.info(f"Запуск оптимизированного рабочего процесса за последние {days_back} дней...")
    
    # Инициализация компонентов
    db_manager = DatabaseManager(DATABASE_URL) # Initialize DB Manager
    qwen_model = QwenLLM()
    gemma_model = GemmaLLM()
    
    # Create DataCollectorAgent with the db_manager
    try:
        # Шаг 1: Параллельный сбор данных
        logger.info("Шаг 1: Параллельный сбор данных")
        from agents.data_collector import DataCollectorAgent
        collector = DataCollectorAgent(db_manager)
        
        # Прямой вызов асинхронного метода
        collect_result = await collector.collect_data(days_back=days_back, force_update=force_update)
        total_messages = collect_result.get("total_new_messages", 0)
        
        logger.info(f"Всего собрано {total_messages} новых сообщений")
        
        # If no new messages, check existing ones for uncategorized
        if total_messages == 0:
            logger.info("Нет новых сообщений. Проверка существующих сообщений за указанный период...")
            
            # Определяем даты для поиска
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Проверяем все сообщения за период (не только категоризированные)
            existing_messages = db_manager.get_messages_by_date_range(start_date, end_date)
            
            if not existing_messages:
                logger.info(f"Нет сообщений за указанный период ({start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}). Завершение работы.")
                return False
            
            logger.info(f"Найдено {len(existing_messages)} существующих сообщений за указанный период. Проверяем необходимость категоризации.")
            
            # Проверяем, сколько из них уже категоризировано
            uncategorized = [msg for msg in existing_messages if msg.category is None]
            
            if uncategorized:
                logger.info(f"Найдено {len(uncategorized)} некатегоризированных сообщений. Запускаем анализ...")
                
                # Запускаем категоризацию для некатегоризированных сообщений
                from agents.analyzer import AnalyzerAgent # Import AnalyzerAgent for this specific use
                analyzer = AnalyzerAgent(db_manager, qwen_model)
                analyzer.fast_check = True
                analyze_result = analyzer.analyze_messages(limit=len(uncategorized))
                
                logger.info(f"Завершена категоризация сообщений: {analyze_result.get('analyzed_count', 0)} обработано.")
            else:
                logger.info("Все существующие сообщения уже категоризированы.")
        
        # Шаг 2: Анализ сообщений
        logger.info("Шаг 2: Анализ сообщений с оценкой уверенности")
        from agents.analyzer import AnalyzerAgent # Ensure AnalyzerAgent is imported for regular use
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
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        from agents.critic import CriticAgent # Import CriticAgent for this specific use
        critic = CriticAgent(db_manager)
        review_result = critic.review_recent_categorizations(
            confidence_threshold=2,  # Проверять только сообщения с уверенностью 1-2
            limit=50,
            batch_size=5,
            max_workers=3,
            start_date=start_date,  # Передаем фильтр по дате
            end_date=end_date
        )
        
        updated_count = review_result.get("updated", 0)
        logger.info(f"Проверка категоризации: обновлено {updated_count} сообщений")
        
        # Шаг 4: Создание дайджеста
        logger.info("Шаг 4: Создание дайджеста")
        from agents.digester import DigesterAgent # Import DigesterAgent for this specific use
        digester = DigesterAgent(db_manager, gemma_model)
        # This method is now async
        digest_result = digester.create_digest(days_back=days_back)
        
        has_brief = "brief_digest_id" in digest_result
        has_detailed = "detailed_digest_id" in digest_result
        
        if has_brief or has_detailed:
            # Определяем период на основе days_back
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back-1)

            logger.info(f"Дайджест успешно создан: краткий={has_brief}, подробный={has_detailed}")
            if has_brief or has_detailed:
                # Сохраняем информацию о генерации дайджеста
                digest_ids = {}
                if "brief_digest_id" in digest_result:
                    digest_ids["brief"] = digest_result["brief_digest_id"]
                if "detailed_digest_id" in digest_result:
                    digest_ids["detailed"] = digest_result["detailed_digest_id"]
                
                db_manager.save_digest_generation(
                source="workflow",
                messages_count=total_messages,
                digest_ids=digest_ids,
                start_date=start_date,  # Добавить эти параметры
                end_date=end_date       # из существующих переменных
                )
            return True
        else:
            logger.error("Не удалось создать дайджест")
            return False
        
        
    finally:
        # Ensure all Telegram clients are released by the session manager
        # This relies on the global singleton TelegramSessionManager instance
        session_manager = TelegramSessionManager(api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH)
        await session_manager.close_all_clients() # Call the new method

async def shutdown(signal, loop, scheduler=None, bot=None): # Removed client=None as it's managed by session_manager
    """Корректное завершение приложения с закрытием всех подключений"""
    logger.info(f"Получен сигнал {signal.name}, завершение работы...")
    
    # Сначала останавливаем планировщик если он существует
    if scheduler:
        logger.info("Останавливаем планировщик...")
        scheduler.stop() # This calls scheduler.shutdown()
    
    # Останавливаем бота если он существует
    if bot and hasattr(bot, 'application'):
        logger.info("Останавливаем Telegram бота...")
        await bot.application.stop()
    
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
    logger.info("Запуск приложения в режиме Telegram бота с планировщиком")
    
    # Инициализация менеджера БД
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Инициализация и запуск планировщика в отдельном потоке
    scheduler = JobScheduler(db_manager)
    scheduler_thread = threading.Thread(target=run_scheduler, args=(scheduler,))
    scheduler_thread.daemon = True
    scheduler_thread.start()
    logger.info("Планировщик запущен в отдельном потоке")
    
    # Инициализация и запуск Telegram-бота
    bot = TelegramBot(db_manager) # Pass db_manager
    bot.run()
    
    # Этот код не будет достигнут, пока бот работает
    logger.info("Приложение завершает работу")
    scheduler.stop()
    
    # Clean up Telethon session file on shutdown
    session_manager = TelegramSessionManager(api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH)
    asyncio.run(session_manager.disconnect_client()) # Ensure the client is disconnected
    

def main():
    """Точка входа в приложение"""
    parser = argparse.ArgumentParser(description='Запуск приложения в различных режимах')
    parser.add_argument('--mode', choices=['bot', 'workflow', 'digest'], default='bot',
                        help='Режим работы: bot - запуск бота и планировщика, '
                              'workflow - запуск полного рабочего процесса, '
                              'digest - только формирование дайджеста')
    parser.add_argument('--orchestrator', action='store_true', 
                        help='Использовать оркестратор для режима workflow')
    parser.add_argument('--scenario', default='daily_workflow',
                        choices=['daily_workflow', 'urgent_update', 'full_analysis', 'digest_only'],
                        help='Сценарий выполнения для оркестратора')
    parser.add_argument('--days', type=int, default=1, 
                        help='Количество дней для сбора сообщений (режимы workflow и digest)')
    parser.add_argument('--force-update', action='store_true',
                        help='Принудительное обновление данных')
    
    args = parser.parse_args()
    
    logger.info(f"Запуск приложения в режиме: {args.mode}")
    
    if args.mode == 'bot':
        run_bot_with_scheduler()
    elif args.mode == 'workflow':
        if args.orchestrator:
            asyncio.run(run_orchestrated_workflow(
                scenario=args.scenario, 
                days_back=args.days,
                force_update=args.force_update
            ))
        else:
            asyncio.run(run_full_workflow(days_back=args.days, force_update=args.force_update))
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
