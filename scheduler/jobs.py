"""
Настройка и управление задачами по расписанию
"""
import logging
from datetime import datetime, time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
#from apscheduler.executors.pool import ThreadPoolExecutor
import asyncio
from config.settings import (
    COLLECT_INTERVAL_MINUTES,
    ANALYZE_INTERVAL_MINUTES,
    DIGEST_TIME_HOUR,
    DIGEST_TIME_MINUTE
)
from agents.data_collector import DataCollectorAgent
from agents.analyzer import AnalyzerAgent
from agents.digester import DigesterAgent
from apscheduler.executors.asyncio import AsyncIOExecutor # Added import for AsyncIOExecutor
from agents.orchestrator import OrchestratorAgent
from agents.agent_registry import AgentRegistry
from agents.task_queue import TaskQueue
from crewai import Crew


logger = logging.getLogger(__name__)

class JobScheduler:
    """Планировщик задач"""
    
    def __init__(self, db_manager, crew=None):
        """
        Инициализация планировщика с поддержкой оркестратора
        
        Args:
            db_manager (DatabaseManager): Менеджер БД
            crew (Crew, optional): Экземпляр CrewAI для выполнения задач
        """
        self.db_manager = db_manager # The db_manager is passed to agents, not crew itself
        self.scheduler = BackgroundScheduler()
        
        # Инициализация оркестратора
        self.agent_registry = AgentRegistry(db_manager)
        self.orchestrator = OrchestratorAgent(db_manager, self.agent_registry)
        self.task_queue = TaskQueue(max_concurrent_tasks=2)
        self.use_orchestrator = True  # Флаг использования оркестратора
        
        # Создаем агентов
        self.data_collector = DataCollectorAgent(db_manager)
        self.analyzer = AnalyzerAgent(db_manager)
        self.digester = DigesterAgent(db_manager)
        
        # For CrewAI, agents need to be initialized with their roles and tools
        # В scheduler/jobs.py, замените секцию инициализации Crew на:

# For CrewAI, agents need to be initialized with their roles and tools
        from crewai import Crew, Task # Local import to avoid circular dependency if agents used CrewAI
        if crew:
            self.crew = crew
        else:
            # Создаем Crew только если все агенты правильно инициализированы
            try:
                # Проверяем, что у агентов есть необходимые атрибуты
                if (hasattr(self.data_collector, 'agent') and 
                    hasattr(self.analyzer, 'agent') and 
                    hasattr(self.digester, 'agent') and
                    hasattr(self.data_collector, 'create_task') and
                    hasattr(self.analyzer, 'create_task') and
                    hasattr(self.digester, 'create_task')):
                    
                    self.crew = Crew(
                        agents=[
                            self.data_collector.agent,
                            self.analyzer.agent,
                            self.digester.agent
                        ],
                        tasks=[
                            self.data_collector.create_task(),
                            self.analyzer.create_task(),
                            self.digester.create_task()
                        ],
                        verbose=True
                    )
                else:
                    # Если агенты не поддерживают CrewAI, используем None
                    logger.warning("Агенты не поддерживают CrewAI интерфейс, Crew не инициализирована")
                    self.crew = None
                    
            except Exception as e:
                logger.warning(f"Ошибка при создании Crew: {str(e)}, используем legacy режим")
                self.crew = None
    
    async def orchestrated_collect_data_job(self):
        """Задача сбора данных через оркестратор"""
        logger.info("Запуск оркестрированной задачи сбора данных")
        try:
            result = await self.orchestrator.plan_and_execute(
                scenario="urgent_update",
                days_back=1,
                force_update=False
            )
            logger.info(f"Оркестрированная задача сбора данных завершена: {result.get('status')}")
            
            # Логируем основные метрики
            metrics = result.get('metrics', {})
            summary = result.get('summary', {})
            logger.info(f"Успешность: {metrics.get('success_rate', 0):.1%}, "
                       f"собрано: {summary.get('collected_messages', 0)}, "
                       f"проанализировано: {summary.get('analyzed_messages', 0)}")
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении оркестрированной задачи сбора данных: {str(e)}")
    
    async def collect_data_job(self):
        """Задача сбора данных"""
        logger.info("Запуск задачи сбора данных")
        try:
            result = await self.data_collector.collect_data() # Now it's an async call
            logger.info(f"Задача сбора данных завершена: {result}")
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи сбора данных: {str(e)}")
    
    def analyze_messages_job(self): # This method is synchronous, no change needed for its own definition
        """Задача анализа сообщений"""
        logger.info("Запуск задачи анализа сообщений")
        try:
            self.analyzer.analyze_messages()
            logger.info("Задача анализа сообщений завершена")
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи анализа сообщений: {str(e)}")
    
    async def orchestrated_daily_workflow_job(self):
        """Ежедневная задача полного рабочего процесса через оркестратор"""
        logger.info("Запуск ежедневного оркестрированного рабочего процесса")
        try:
            result = await self.orchestrator.plan_and_execute(
                scenario="daily_workflow",
                days_back=1,
                force_update=True
            )
            
            logger.info(f"Ежедневный рабочий процесс завершен: {result.get('status')}")
            
            # Детальное логирование результатов
            self._log_workflow_results(result)
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении ежедневного рабочего процесса: {str(e)}")
    
    def _log_workflow_results(self, result: dict):
        """Детальное логирование результатов рабочего процесса"""
        try:
            metrics = result.get('metrics', {})
            summary = result.get('summary', {})
            
            logger.info("=== РЕЗУЛЬТАТЫ ЕЖЕДНЕВНОГО ПРОЦЕССА ===")
            logger.info(f"Общий статус: {result.get('status')}")
            logger.info(f"Время выполнения: {metrics.get('total_execution_time', 0):.1f}с")
            logger.info(f"Успешных задач: {metrics.get('successful_tasks', 0)}/{metrics.get('total_tasks', 0)}")
            
            if summary.get('collected_messages', 0) > 0:
                logger.info(f"Собрано новых сообщений: {summary['collected_messages']}")
            
            if summary.get('analyzed_messages', 0) > 0:
                logger.info(f"Проанализировано сообщений: {summary['analyzed_messages']}")
            
            if summary.get('reviewed_messages', 0) > 0:
                logger.info(f"Улучшено критиком: {summary['reviewed_messages']}")
            
            created_digests = summary.get('created_digests', [])
            if created_digests:
                logger.info(f"Созданы дайджесты: {', '.join(created_digests)}")
            
            updated_digests = summary.get('updated_digests', [])
            if updated_digests:
                logger.info(f"Обновлены дайджесты: {', '.join(updated_digests)}")
            
            # Логируем рекомендации
            recommendations = result.get('recommendations', [])
            if recommendations:
                logger.info("Рекомендации:")
                for rec in recommendations:
                    logger.info(f"  - {rec.get('description')}")
                    
        except Exception as e:
            logger.error(f"Ошибка при логировании результатов: {str(e)}")
    
    async def create_digest_job(self): # Made async because it calls an async method
        """Задача создания дайджеста"""
        logger.info("Запуск задачи создания дайджеста")
        try: 
            result = self.digester.create_digest() # This is now awaited
            logger.info(f"Задача создания дайджеста завершена: {result}")
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи создания дайджеста: {str(e)}")
    
    def run_crew_job(self):
        """Выполнение всех задач в рамках Crew"""
        logger.info("Запуск задач Crew")
        try:
            result = self.crew.kickoff()
            logger.info(f"Задачи Crew завершены: {result}")
        except Exception as e:
            logger.error(f"Ошибка при выполнении задач Crew: {str(e)}")
    
    def setup_jobs(self):
        """Настройка расписания задач"""
        logger.info("Настройка задач планировщика...")
        
        # УБИРАЕМ полностью AsyncIOExecutor - BackgroundScheduler его не поддерживает
        # Все асинхронные задачи будем запускать через lambda с asyncio.run_coroutine_threadsafe
        
        # Выбираем версию задач в зависимости от настройки
        if self.use_orchestrator:
            self._setup_orchestrated_jobs()
        else:
            self._setup_legacy_jobs()
    
        logger.info(f"Задачи настроены ({'оркестратор' if self.use_orchestrator else 'стандартные'})")
    
    def _setup_legacy_jobs(self):
        """Настройка стандартных задач (без оркестратора)"""
        self.scheduler.add_job(
            self.collect_data_job, # Now an async method
            IntervalTrigger(minutes=COLLECT_INTERVAL_MINUTES),
            id='collect_data'
        )
        
        self.scheduler.add_job(
            self.analyze_messages_job,
            IntervalTrigger(minutes=ANALYZE_INTERVAL_MINUTES),
            id='analyze_messages'
        )
        
        # Задача создания дайджеста (ежедневно в указанное время)
        from apscheduler.triggers.cron import CronTrigger # Moved import here
        digest_time = time(hour=DIGEST_TIME_HOUR, minute=DIGEST_TIME_MINUTE)
        self.scheduler.add_job(
            self.create_digest_job, # This should also be async or wrap async call
            CronTrigger(hour=digest_time.hour, minute=digest_time.minute),
            id='create_digest'
        )
        
        # Задача обновления дайджестов (после анализа сообщений)
        self.scheduler.add_job(
            self.update_digests_job,
            IntervalTrigger(minutes=ANALYZE_INTERVAL_MINUTES + 5),  # Запускаем чуть позже анализа
            id='update_digests'
        )
    
    def _setup_orchestrated_jobs(self):
        """Настройка задач с использованием оркестратора"""
        logger.info("Настройка оркестрированных задач")
        
        # Функция-обертка для запуска async функций в BackgroundScheduler
        def run_async_job(coro_func, *args, **kwargs):
            """Запуск асинхронной функции в синхронном контексте"""
            try:
                # Создаем новый event loop для этого потока
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(coro_func(*args, **kwargs))
                    return result
                finally:
                    loop.close()
            except Exception as e:
                logger.error(f"Ошибка при выполнении async задачи: {str(e)}")
        
        # Быстрые обновления каждые 30 минут
        self.scheduler.add_job(
            func=lambda: run_async_job(self.orchestrated_collect_data_job),
            trigger=IntervalTrigger(minutes=COLLECT_INTERVAL_MINUTES),
            id='orchestrated_collect_data',
            name='Сбор данных через оркестратор',
            max_instances=1,
            coalesce=True
        )
        
        # Полный ежедневный процесс
        self.scheduler.add_job(
            func=lambda: run_async_job(self.orchestrated_daily_workflow_job),
            trigger=CronTrigger(hour=DIGEST_TIME_HOUR, minute=DIGEST_TIME_MINUTE),
            id='orchestrated_daily_workflow',
            name='Ежедневный оркестрированный процесс',
            max_instances=1,
            coalesce=True
        )
        
        # Дополнительный полный анализ в середине дня
        self.scheduler.add_job(
            func=lambda: run_async_job(
                self.orchestrator.plan_and_execute,
                scenario="full_analysis", 
                days_back=1, 
                force_update=False
            ),
            trigger=CronTrigger(hour=14, minute=0),
            id='orchestrated_midday_analysis',
            name='Анализ в середине дня',
            max_instances=1,
            coalesce=True
        )
        
        # Задача обновления дайджестов (синхронная)
        self.scheduler.add_job(
            func=self.update_digests_job,
            trigger=IntervalTrigger(minutes=ANALYZE_INTERVAL_MINUTES + 5),
            id='update_digests',
            name='Обновление дайджестов',
            max_instances=1,
            coalesce=True
        )
        
        logger.info("Оркестрированные задачи настроены успешно")

    def update_today_flags_job(self):
        """Задача обновления флагов is_today"""
        logger.info("Запуск задачи обновления флагов is_today")
        try:
            result = self.db_manager.update_today_flags()
            if "error" in result:
                logger.error(f"Задача обновления флагов is_today завершилась с ошибкой: {result['error']}")
            else:
                logger.info(f"Задача обновления флагов is_today успешно выполнена. Обновлено: {result['updated']}")
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи обновления флагов is_today: {str(e)}")
    
    def start(self):
        """Запуск планировщика"""
        self.setup_jobs()
    
        # ПРОСТОЕ РЕШЕНИЕ: Временно убираем запуск очереди задач
        # Оркестратор будет работать через обычные scheduled задачи
        if self.use_orchestrator:
            logger.info("Оркестратор настроен, очередь задач будет запускаться по требованию")
        
        self.scheduler.start()
        logger.info("Планировщик запущен")
    
    def stop(self):
        """Остановка планировщика"""
        self.scheduler.shutdown()
        logger.info("Планировщик остановлен")
    
    def toggle_orchestrator(self, enabled: bool):
        """Переключение режима оркестратора"""
        if self.use_orchestrator != enabled:
            logger.info(f"Переключение оркестратора: {self.use_orchestrator} -> {enabled}")
            self.use_orchestrator = enabled
            
            # Пересоздаем задачи
            self.scheduler.remove_all_jobs()
            self.setup_jobs()
    
    def update_digests_job(self):
        """Задача обновления дайджестов при получении новых сообщений"""
        logger.info("Запуск задачи обновления дайджестов")
        try:
            # Определяем дату для обновления (обычно сегодня)
            today = datetime.now()
            
            # Agents are already initialized in __init__
            digester = DigesterAgent(self.db_manager)
            
            # Обновляем все дайджесты, содержащие сегодняшнюю дату
            result = digester.update_digests_for_date(today)
            
            logger.info(f"Задача обновления дайджестов завершена: {result}")
            return result
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи обновления дайджестов: {str(e)}")
            return {"status": "error", "error": str(e)}
    async def get_orchestrator_status(self):
        """Получение статуса оркестратора"""
        try:
            if not self.use_orchestrator:
                return {
                    "enabled": False,
                    "orchestrator": None,
                    "agent_registry": None,
                    "task_queue": None
                }
            
            # Получаем статус агентов
            agent_status = self.agent_registry.get_status() if self.agent_registry else {}
            
            # Получаем статус очереди задач
            queue_status = await self.task_queue.get_status() if self.task_queue else {}
            
            return {
                "enabled": True,
                "orchestrator": {
                    "initialized": self.orchestrator is not None,
                    "agent_count": len(self.orchestrator.agent_registry.agents) if self.orchestrator and self.orchestrator.agent_registry else 0
                },
                "agent_registry": agent_status,
                "task_queue": queue_status
            }
            
        except Exception as e:
            return {
                "enabled": self.use_orchestrator,
                "error": str(e)
            }