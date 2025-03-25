"""
Настройка и управление задачами по расписанию
"""
import logging
from datetime import datetime, time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import (
    COLLECT_INTERVAL_MINUTES,
    ANALYZE_INTERVAL_MINUTES,
    DIGEST_TIME_HOUR,
    DIGEST_TIME_MINUTE
)
from agents.data_collector import DataCollectorAgent
from agents.analyzer import AnalyzerAgent
from agents.digester import DigesterAgent
from crewai import Crew

logger = logging.getLogger(__name__)

class JobScheduler:
    """Планировщик задач"""
    
    def __init__(self, db_manager, crew=None):
        """
        Инициализация планировщика
        
        Args:
            db_manager (DatabaseManager): Менеджер БД
            crew (Crew, optional): Экземпляр CrewAI для выполнения задач
        """
        self.db_manager = db_manager
        self.scheduler = BackgroundScheduler()
        
        # Создаем агентов
        self.data_collector = DataCollectorAgent(db_manager)
        self.analyzer = AnalyzerAgent(db_manager)
        self.digester = DigesterAgent(db_manager)
        
        # Создаем или используем переданный экземпляр Crew
        if crew:
            self.crew = crew
        else:
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
    
    def collect_data_job(self):
        """Задача сбора данных"""
        logger.info("Запуск задачи сбора данных")
        try:
            result = self.data_collector.collect_data()
            logger.info(f"Задача сбора данных завершена: {result}")
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи сбора данных: {str(e)}")
    
    def analyze_messages_job(self):
        """Задача анализа сообщений"""
        logger.info("Запуск задачи анализа сообщений")
        try:
            result = self.analyzer.analyze_messages()
            logger.info(f"Задача анализа сообщений завершена: {result}")
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи анализа сообщений: {str(e)}")
    
    def create_digest_job(self):
        """Задача создания дайджеста"""
        logger.info("Запуск задачи создания дайджеста")
        try:
            result = self.digester.create_digest()
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
        # Задача сбора данных (каждые N минут)
        self.scheduler.add_job(
            self.collect_data_job,
            IntervalTrigger(minutes=COLLECT_INTERVAL_MINUTES),
            id='collect_data'
        )
        
        # Задача анализа сообщений (каждые N минут)
        self.scheduler.add_job(
            self.analyze_messages_job,
            IntervalTrigger(minutes=ANALYZE_INTERVAL_MINUTES),
            id='analyze_messages'
        )
        
        # Задача создания дайджеста (ежедневно в указанное время)
        digest_time = time(hour=DIGEST_TIME_HOUR, minute=DIGEST_TIME_MINUTE)
        self.scheduler.add_job(
            self.create_digest_job,
            CronTrigger(hour=digest_time.hour, minute=digest_time.minute),
            id='create_digest'
        )
        
        logger.info("Задачи настроены")
    
    def start(self):
        """Запуск планировщика"""
        self.setup_jobs()
        self.scheduler.start()
        logger.info("Планировщик запущен")
    
    def stop(self):
        """Остановка планировщика"""
        self.scheduler.shutdown()
        logger.info("Планировщик остановлен")