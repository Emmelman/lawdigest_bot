"""
Обновленный main.py с поддержкой Intelligent Orchestrator
Сохраняет всю оригинальную функциональность + добавляет новые возможности
"""
import asyncio
import logging
import argparse
import threading
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Добавляем корневую директорию в path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Загрузка переменных окружения
load_dotenv()

from config.logging_config import setup_logging
from config.settings import (
    DATABASE_URL, 
    TELEGRAM_API_ID, 
    TELEGRAM_API_HASH, 
    TELEGRAM_CHANNELS,
    TELEGRAM_BOT_TOKEN
)
from database.db_manager import DatabaseManager
from utils.telegram_session_manager import TelegramSessionManager
from telegram_bot.bot import TelegramBot
from scheduler.jobs import JobScheduler
from telethon import TelegramClient

# Импорт компонентов workflow
from llm.qwen_model import QwenLLM
from llm.gemma_model import GemmaLLM
from agents.orchestrator import OrchestratorAgent  # Оригинальный оркестратор
from agents.orchestrator import IntelligentOrchestratorAgent  # Новый intelligent оркестратор
from agents.agent_registry import AgentRegistry
from agents.task_queue import TaskQueue
from agents.critic import CriticAgent

# Настройка логирования
logger = setup_logging()

def enable_detailed_reasoning_logs():
    """Включает детальное логирование reasoning для агентов"""
    
    # Устанавливаем уровень логирования для агентов
    logging.getLogger('agents.analyzer').setLevel(logging.INFO)
    logging.getLogger('agents.critic').setLevel(logging.INFO)
    
    # Создаем специальный форматтер для reasoning логов
    reasoning_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Получаем логгеры агентов
    analyzer_logger = logging.getLogger('agents.analyzer')
    critic_logger = logging.getLogger('agents.critic')
    
    # Проверяем, есть ли уже handlers (чтобы не дублировать)
    if not analyzer_logger.handlers:
        analyzer_handler = logging.StreamHandler()
        analyzer_handler.setFormatter(reasoning_formatter)
        analyzer_logger.addHandler(analyzer_handler)
    
    if not critic_logger.handlers:
        critic_handler = logging.StreamHandler()
        critic_handler.setFormatter(reasoning_formatter)
        critic_logger.addHandler(critic_handler)
    
    # Отключаем propagation чтобы избежать дублирования логов
    analyzer_logger.propagate = False
    critic_logger.propagate = False
    
    print("🧠 Детальное логирование reasoning ВКЛЮЧЕНО")

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
            
            # Фильтруем сообщения по дате
            filtered_messages = []
            for msg in messages:
                if msg.date.replace(tzinfo=None) >= start_date:
                    filtered_messages.append(msg)
                else:
                    # Достигли сообщений старше нужной даты, прекращаем сбор
                    break
            
            if not filtered_messages:
                break
                
            all_messages.extend(filtered_messages)
            
            # Если сообщений меньше лимита, значит достигли конца
            if len(messages) < limit_per_request:
                break
                
            offset_id = messages[-1].id
            
            # Проверяем, не достигли ли мы старых сообщений
            if messages[-1].date.replace(tzinfo=None) < start_date:
                break
        
        logger.info(f"Собрано {len(all_messages)} сообщений из {total_messages} просмотренных")
        
        # Сохраняем сообщения в БД
        saved_count = 0
        for msg in all_messages:
            if msg.text:  # Сохраняем только текстовые сообщения
                try:
                    db_manager.save_message(
                        channel=channel,
                        message_id=msg.id,
                        text=msg.text,
                        date=msg.date.replace(tzinfo=None)
                    )
                    saved_count += 1
                except Exception as e:
                    logger.warning(f"Ошибка сохранения сообщения {msg.id}: {str(e)}")
        
        logger.info(f"Сохранено {saved_count} новых сообщений из канала {channel}")
        return {"channel": channel, "collected": len(all_messages), "saved": saved_count}
        
    except Exception as e:
        logger.error(f"Ошибка при сборе сообщений из {channel}: {str(e)}")
        return {"channel": channel, "collected": 0, "saved": 0, "error": str(e)}

async def run_data_collection(db_manager, days_back=1, force_update=False):
    """Запуск сбора данных из всех каналов"""
    logger.info(f"Запуск сбора данных за последние {days_back} дней...")
    
    session_manager = TelegramSessionManager(api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH)
    client = await session_manager.get_client()
    
    try:
        results = []
        for channel in TELEGRAM_CHANNELS:
            result = await collect_messages(client, db_manager, channel, days_back)
            results.append(result)
        
        total_collected = sum(r['collected'] for r in results)
        total_saved = sum(r['saved'] for r in results)
        
        logger.info(f"Сбор данных завершен. Собрано: {total_collected}, сохранено: {total_saved}")
        return {"status": "success", "total_collected": total_collected, "total_saved": total_saved, "results": results}
        
    except Exception as e:
        logger.error(f"Ошибка при сборе данных: {str(e)}")
        return {"status": "error", "error": str(e)}
    finally:
        await session_manager.disconnect_client()

async def run_message_analysis(db_manager, llm_model):
    """Запуск анализа сообщений"""
    logger.info("Запуск анализа неанализированных сообщений...")
    
    from agents.analyzer import AnalyzerAgent
    analyzer = AnalyzerAgent(db_manager, llm_model)
    
    try:
        results = analyzer.analyze_messages()
        logger.info(f"Анализ завершен. Результат: {results}")
        return results
    except Exception as e:
        logger.error(f"Ошибка при анализе сообщений: {str(e)}")
        return {"status": "error", "error": str(e)}

async def run_categorization_review(db_manager, llm_model):
    """Запуск проверки категоризации критиком"""
    logger.info("Запуск проверки категоризации критиком...")
    
    try:
        critic = CriticAgent(db_manager, llm_model)
        results = critic.review_recent_categorizations(
            confidence_threshold=3,  # Проверяем сообщения с уверенностью <= 3
            limit=50,               # Максимум 50 сообщений
            batch_size=5,           # По 5 в пакете
            max_workers=3
        )
        logger.info(f"Проверка категоризации завершена. Обновлено: {results.get('updated', 0)}, "
                   f"всего: {results.get('total', 0)}")
        return results
    except Exception as e:
        logger.error(f"Ошибка при проверке категоризации: {str(e)}")
        return {"status": "error", "error": str(e)}

async def create_digest(db_manager, llm_model, days_back=1):
    """Создание дайджеста"""
    logger.info(f"Создание дайджеста за последние {days_back} дней...")
    
    from agents.digester import DigesterAgent
    digester = DigesterAgent(db_manager, llm_model)
    digest = digester.create_digest(days_back=days_back)
    
    logger.info(f"Дайджест создан: {digest.get('status', 'unknown')}")
    return digest

async def run_full_workflow(days_back=1, force_update=False):
    """Запуск полного рабочего процесса (legacy версия)"""
    logger.info(f"Запуск полного рабочего процесса за {days_back} дней...")
    
    # Инициализация компонентов
    db_manager = DatabaseManager(DATABASE_URL)
    llm_model = QwenLLM()
    
    try:
        # Этап 1: Сбор данных
        collection_result = await run_data_collection(db_manager, days_back, force_update)
        if collection_result['status'] != 'success':
            logger.error("Сбор данных завершился с ошибкой")
            return False
        
        # Этап 2: Анализ сообщений
        analysis_result = await run_message_analysis(db_manager, llm_model)
        if analysis_result.get('status') == 'error':
            logger.error("Анализ сообщений завершился с ошибкой")
            return False
        
        # Этап 3: Проверка категоризации
        review_result = await run_categorization_review(db_manager, llm_model)
        if review_result.get('status') == 'error':
            logger.error("Проверка категоризации завершилась с ошибкой")
            return False
        
        # Этап 4: Создание дайджеста
        digest_result = await create_digest(db_manager, llm_model, days_back)
        if digest_result.get('status') != 'success':
            logger.error("Создание дайджеста завершилось с ошибкой")
            return False
        
        logger.info("Полный рабочий процесс успешно завершен!")
        return True
        
    except Exception as e:
        logger.error(f"Критическая ошибка в рабочем процессе: {str(e)}")
        return False
    finally:
        # Закрываем соединения
        from utils.telegram_session_manager import TelegramSessionManager
        session_manager = TelegramSessionManager()
        await session_manager.close_all_clients()

async def run_orchestrated_workflow(scenario: str = "daily_workflow", **kwargs):
    """Запуск рабочего процесса через оригинальный оркестратор"""
    logger.info(f"Запуск оркестрированного рабочего процесса: {scenario}")
    
    # Инициализация компонентов
    db_manager = DatabaseManager(DATABASE_URL)
    agent_registry = AgentRegistry(db_manager)
    orchestrator = OrchestratorAgent(db_manager, agent_registry)  # Оригинальный оркестратор
    
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
        from utils.telegram_session_manager import TelegramSessionManager
        session_manager = TelegramSessionManager()
        await session_manager.close_all_clients()

async def run_intelligent_workflow(scenario: str = "daily_workflow", **kwargs):
    """
    Запуск intelligent workflow через новый оркестратор
    
    Args:
        scenario: Сценарий выполнения
        **kwargs: Дополнительные параметры
    """
    try:
        logger.info(f"Запуск приложения в режиме: intelligent workflow")
        logger.info(f"Запуск intelligent оркестрированного рабочего процесса: {scenario}")
        
        # Инициализация компонентов
        db_manager = DatabaseManager(DATABASE_URL)
        agent_registry = AgentRegistry(db_manager)
        orchestrator = IntelligentOrchestratorAgent(db_manager, agent_registry)
        
        # Проверка здоровья агентов перед запуском
        health_check = await agent_registry.health_check()
        logger.info(f"Проверка здоровья агентов: {health_check['overall_status']}")
        
        if health_check['overall_status'] == 'critical':
            logger.error("Критические проблемы с агентами, прерываем выполнение")
            return {"status": "error", "reason": "critical_agent_failures"}
        
        # Запуск intelligent планирования и выполнения
        result = await orchestrator.plan_and_execute(scenario=scenario, **kwargs)
        
        # Детальное логирование результатов
        _log_execution_results(result)
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении intelligent workflow: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}
    
    finally:
        # Закрытие соединений
        try:
            from utils.telegram_session_manager import TelegramSessionManager
            session_manager = TelegramSessionManager()
            await session_manager.close_all_clients()
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединений: {str(e)}")

def _log_execution_results(result: dict):
    """Детальное логирование результатов выполнения"""
    try:
        logger.info("=== РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ ===")
        logger.info(f"Статус: {result.get('status')}")
        
        metrics = result.get('metrics', {})
        if metrics:
            logger.info(f"Сценарий: {metrics.get('scenario')}")
            logger.info(f"Успешность: {metrics.get('success_rate', 0)*100:.1f}%")
            logger.info(f"Время выполнения: {metrics.get('total_execution_time', 0):.1f}с")
            logger.info(f"Intelligent планирование: {metrics.get('intelligent_planning', False)}")
        
        summary = result.get('summary', {})
        if summary:
            logger.info(f"Собрано сообщений: {summary.get('collected_messages', 0)}")
            logger.info(f"Проанализировано: {summary.get('analyzed_messages', 0)}")
            logger.info(f"Улучшено критиком: {summary.get('reviewed_messages', 0)}")
            
            created_digests = summary.get('created_digests', [])
            if created_digests:
                logger.info(f"Создано дайджестов: {len(created_digests)}")
                for digest in created_digests:
                    logger.info(f"  - {digest}")
            
            updated_digests = summary.get('updated_digests', [])
            if updated_digests:
                logger.info(f"Обновлено дайджестов: {len(updated_digests)}")
        
        # Логируем context планирования
        planning_context = result.get('planning_context', {})
        if planning_context:
            logger.info("=== КОНТЕКСТ ПЛАНИРОВАНИЯ ===")
            logger.info(f"Изначально неанализированных: {planning_context.get('original_unanalyzed', 0)}")
            logger.info(f"С низкой уверенностью: {planning_context.get('original_low_confidence', 0)}")
            logger.info(f"Дайджестов за сегодня: {planning_context.get('original_digests_count', 0)}")
        
        # Логируем рекомендации
        recommendations = result.get('recommendations', [])
        if recommendations:
            logger.info("=== РЕКОМЕНДАЦИИ ===")
            for rec in recommendations:
                logger.info(f"  - {rec.get('description')}")
        
        # Логируем детали выполнения задач
        task_results = result.get('task_results', [])
        if task_results:
            logger.info("=== ДЕТАЛИ ВЫПОЛНЕНИЯ ЗАДАЧ ===")
            for task_result in task_results:
                status_icon = "✅" if task_result['status'] == 'completed' else "❌"
                logger.info(f"  {status_icon} {task_result['task']}: {task_result['status']} "
                           f"({task_result['execution_time']:.2f}с)")
                if task_result.get('error'):
                    logger.info(f"    Ошибка: {task_result['error']}")
                    
    except Exception as e:
        logger.error(f"Ошибка при логировании результатов: {str(e)}")

async def cleanup_on_shutdown(loop, scheduler=None, bot=None):
    """Очистка ресурсов при завершении работы"""
    logger.info("Начинаем процедуру завершения работы...")
    
    # Останавливаем планировщик если он существует
    if scheduler:
        logger.info("Останавливаем планировщик...")
        scheduler.stop()
    
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
    bot = TelegramBot(db_manager)
    bot.run()
    
    # Этот код не будет достигнут, пока бот работает
    logger.info("Приложение завершает работу")
    scheduler.stop()
    
    # Очистка сессии Telethon при завершении
    session_manager = TelegramSessionManager(api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH)
    asyncio.run(session_manager.disconnect_client())

def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='LawDigest Bot - Intelligent News Processing System')
    
    parser.add_argument('--mode', 
                       choices=['bot', 'workflow', 'legacy', 'digest'], 
                       default='bot',
                       help='Режим работы: bot - запуск бота и планировщика, '
                            'workflow - запуск полного рабочего процесса, '
                            'legacy - legacy workflow без оркестратора, '
                            'digest - только формирование дайджеста')
    
    parser.add_argument('--orchestrator', 
                       action='store_true',
                       help='Использовать оркестратор для режима workflow')
    
    parser.add_argument('--intelligent', 
                       action='store_true',
                       help='Использовать intelligent оркестратор (новая функция)')
    
    parser.add_argument('--scenario', 
                       choices=['daily_workflow', 'urgent_update', 'full_analysis', 'digest_only'],
                       default='daily_workflow',
                       help='Сценарий выполнения для оркестратора')
    
    parser.add_argument('--days', 
                       type=int, 
                       default=1,
                       help='Количество дней для обработки')
    
    parser.add_argument('--force-update', 
                       action='store_true',
                       help='Принудительное обновление данных')
    
    parser.add_argument('--debug', 
                       action='store_true',
                       help='Включить режим отладки')
    
    return parser.parse_args()

def main():
    """Точка входа в приложение"""
    args = parse_arguments()
    enable_detailed_reasoning_logs()
    
    # Дополнительное логирование если включен verbose или debug
    # Настройка уровня логирования
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger('agents.analyzer').setLevel(logging.DEBUG)
        logging.getLogger('agents.critic').setLevel(logging.DEBUG)
        logging.getLogger('agents.orchestrator').setLevel(logging.DEBUG)
        logger.debug("Режим отладки включен")
        print("🔍 Включен debug режим с детальными логами")
    
    logger.info(f"Запуск приложения в режиме: {args.mode}")
    logger.info(f"LawDigest Bot v2.0 с поддержкой intelligent оркестратора")
    
    try:
        if args.mode == 'bot':
            run_bot_with_scheduler()
            
        elif args.mode == 'workflow':
            if args.intelligent:
                # Используем новый intelligent оркестратор
                logger.info("Используется intelligent оркестратор")
                result = asyncio.run(run_intelligent_workflow(
                    scenario=args.scenario,
                    days_back=args.days,
                    force_update=args.force_update
                ))
                
                # Выводим финальный статус
                if result.get('status') == 'success':
                    logger.info("🎉 Intelligent выполнение завершено успешно!")
                else:
                    logger.error(f"❌ Intelligent выполнение завершено с ошибками: {result.get('error', 'Unknown error')}")
                    
            elif args.orchestrator:
                # Используем оригинальный оркестратор
                logger.info("Используется оригинальный оркестратор")
                success = asyncio.run(run_orchestrated_workflow(
                    scenario=args.scenario, 
                    days_back=args.days,
                    force_update=args.force_update
                ))
                
                if success:
                    logger.info("🎉 Оркестрированное выполнение завершено успешно!")
                else:
                    logger.error("❌ Оркестрированное выполнение завершено с ошибками")
            else:
                # Legacy режим
                logger.info("Используется legacy режим")
                success = asyncio.run(run_full_workflow(
                    days_back=args.days, 
                    force_update=args.force_update
                ))
                
                if success:
                    logger.info("🎉 Legacy выполнение завершено успешно!")
                else:
                    logger.error("❌ Legacy выполнение завершено с ошибками")
                    
        elif args.mode == 'legacy':
            # Принудительно legacy режим
            logger.info("Принудительный legacy режим")
            success = asyncio.run(run_full_workflow(
                days_back=args.days,
                force_update=args.force_update
            ))
            
        elif args.mode == 'digest':
            # Только создание дайджеста
            if args.intelligent:
                # Через intelligent оркестратор
                result = asyncio.run(run_intelligent_workflow(
                    scenario='digest_only',
                    days_back=args.days
                ))
            else:
                # Legacy создание дайджеста
                db_manager = DatabaseManager(DATABASE_URL)
                gemma_model = GemmaLLM()
                digest = asyncio.run(create_digest(db_manager, gemma_model, days_back=args.days))
                
                if digest and digest.get('status') == 'success':
                    logger.info("Дайджест успешно сформирован")
                    logger.info(digest.get('digest_text', ''))
                else:
                    logger.error("Не удалось сформировать дайджест")
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания, завершаем работу...")
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()