"""
Агент для сбора данных из Telegram-каналов
"""
import logging
import asyncio
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from langchain.tools import Tool
from crewai import Agent, Task

from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS

logger = logging.getLogger(__name__)

class DataCollectorAgent:
    """Агент для сбора данных из Telegram-каналов"""
    
    def __init__(self, db_manager, api_id=None, api_hash=None):
        """
        Инициализация агента
        
        Args:
            db_manager (DatabaseManager): Менеджер БД
            api_id (str, optional): Telegram API ID
            api_hash (str, optional): Telegram API Hash
        """
        self.db_manager = db_manager
        self.api_id = api_id or TELEGRAM_API_ID
        self.api_hash = api_hash or TELEGRAM_API_HASH
        self.client = None
        
        # Создаем инструмент для сбора данных
        collect_data_tool = Tool(
            name="collect_data",
            func=self.collect_data,
            description="Собирает сообщения из правительственных Telegram-каналов"
        )
        
        # Создаем агента CrewAI
        self.agent = Agent(
            name="Data Collector",
            role="Сборщик данных",
            goal="Собирать сообщения из правительственных Telegram-каналов",
            backstory="Я собираю актуальную информацию из официальных правительственных каналов для последующего анализа и формирования дайджеста.",
            verbose=True,
            tools=[collect_data_tool]
        )
    
    async def _init_client(self):
        """Инициализация клиента Telegram"""
        if not self.client:
            self.client = TelegramClient('session_name', self.api_id, self.api_hash)
            await self.client.start()
    
    async def _get_channel_messages(self, channel, limit=20):
        """
        Получение сообщений из канала
        
        Args:
            channel (str): Название канала
            limit (int): Максимальное количество сообщений
            
        Returns:
            list: Список сообщений
        """
        await self._init_client()
        
        try:
            entity = await self.client.get_entity(channel)
            
            # Получаем историю сообщений
            history = await self.client(GetHistoryRequest(
                peer=entity,
                limit=limit,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            
            return history.messages
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений из канала {channel}: {str(e)}")
            return []
    
    async def _process_channel(self, channel):
        """
        Обработка канала: получение и сохранение сообщений
        
        Args:
            channel (str): Название канала
            
        Returns:
            int: Количество новых сообщений
        """
        messages = await self._get_channel_messages(channel)
        new_messages_count = 0
        
        for message in messages:
            # Пропускаем сообщения без текста
            if not message.message:
                continue
            
            # Сохраняем сообщение в БД
            try:
                self.db_manager.save_message(
                    channel=channel,
                    message_id=message.id,
                    text=message.message,
                    date=message.date
                )
                new_messages_count += 1
            except Exception as e:
                logger.error(f"Ошибка при сохранении сообщения из канала {channel}: {str(e)}")
        
        return new_messages_count
    
    async def _collect_all_channels(self):
        """
        Сбор данных со всех каналов
        
        Returns:
            dict: Словарь с результатами {канал: количество новых сообщений}
        """
        results = {}
        
        for channel in TELEGRAM_CHANNELS:
            try:
                count = await self._process_channel(channel)
                results[channel] = count
                logger.info(f"Собрано {count} новых сообщений из канала {channel}")
            except Exception as e:
                logger.error(f"Ошибка при обработке канала {channel}: {str(e)}")
                results[channel] = 0
        
        return results
    
    def collect_data(self, days_ago=1):
        """
        Инструмент для сбора данных из каналов
        
        Args:
            days_ago (int): За сколько дней назад собирать данные
            
        Returns:
            dict: Результаты сбора данных
        """
        logger.info(f"Запуск сбора данных из каналов: {', '.join(TELEGRAM_CHANNELS)}")
        
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self._collect_all_channels())
        
        total_messages = sum(results.values())
        logger.info(f"Сбор данных завершен. Всего собрано {total_messages} новых сообщений")
        
        return {
            "status": "success",
            "total_new_messages": total_messages,
            "channels_stats": results
        }
    
    def create_task(self):
        """
        Создание задачи для агента
        
        Returns:
            Task: Задача CrewAI
        """
        return Task(
            description="Собрать сообщения из правительственных Telegram-каналов",
            agent=self.agent,
            expected_output="Результаты сбора данных с информацией о количестве собранных сообщений"
        )