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
    
    async def _get_channel_messages(self, channel, days_back=1, limit_per_request=100):
        """
        Получение сообщений из канала за указанный период
        
        Args:
            channel (str): Название канала
            days_back (int): За сколько дней назад собирать сообщения
            limit_per_request (int): Максимальное количество сообщений в одном запросе (для пагинации)
            
        Returns:
            list: Список сообщений
        """
        await self._init_client()
        
        try:
            entity = await self.client.get_entity(channel)
            
            # Определение дат для фильтрации
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            logger.info(f"Получение сообщений из канала {channel} с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")
            
            # Получаем сообщения с пагинацией
            offset_id = 0
            all_messages = []
            total_messages = 0
            
            while True:
                messages = await self.client(GetHistoryRequest(
                    peer=entity,
                    limit=limit_per_request,
                    offset_date=None,
                    offset_id=offset_id,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0
                ))
                
                if not messages.messages:
                    break
                    
                messages_list = messages.messages
                total_messages += len(messages_list)
                
                # Фильтруем сообщения по дате
                
                filtered_messages = []
                for msg in messages_list:
                    # Преобразуем дату из Telegram (aware) в naive datetime
                    msg_date = msg.date.replace(tzinfo=None)
                    if start_date <= msg_date <= end_date:
                        filtered_messages.append(msg)
                all_messages.extend(filtered_messages)
                
                # Проверяем, нужно ли продолжать пагинацию
                if len(messages_list) < limit_per_request or not messages_list[-1].date >= start_date:
                    # Либо получили меньше сообщений, чем запрашивали (конец списка),
                    # либо последнее сообщение старше начальной даты
                    break
                    
                # Устанавливаем смещение для следующего запроса
                offset_id = messages_list[-1].id
                
                logger.debug(f"Получено {len(filtered_messages)} сообщений из {len(messages_list)}. "
                           f"Продолжаем пагинацию с ID {offset_id}")
            
            logger.info(f"Всего получено {total_messages} сообщений, отфильтровано {len(all_messages)} "
                      f"за указанный период из канала {channel}")
            
            return all_messages
            
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений из канала {channel}: {str(e)}")
            return []
    
    async def _process_channel(self, channel, days_back=1):
        """
        Обработка канала: получение и сохранение сообщений
        
        Args:
            channel (str): Название канала
            days_back (int): За сколько дней назад собирать сообщения
            
        Returns:
            int: Количество новых сообщений
        """
        messages = await self._get_channel_messages(channel, days_back=days_back)
        new_messages_count = 0
        duplicates_count = 0
        
        for message in messages:
            # Пропускаем сообщения без текста
            if not message.message:
                continue
            
            # Сохраняем сообщение в БД
            try:
                # Проверяем, существует ли уже такое сообщение
                existing_message = self.db_manager.get_message_by_channel_and_id(channel, message.id)
                
                if existing_message:
                    duplicates_count += 1
                    continue
                
                self.db_manager.save_message(
                    channel=channel,
                    message_id=message.id,
                    text=message.message,
                    date=message.date.replace(tzinfo=None)
                )
                new_messages_count += 1
            except Exception as e:
                logger.error(f"Ошибка при сохранении сообщения из канала {channel}: {str(e)}")
        
        logger.info(f"Канал {channel}: сохранено {new_messages_count} новых сообщений, "
                   f"пропущено {duplicates_count} дубликатов")
        
        return new_messages_count
    
    async def _collect_all_channels(self, days_back=1):
        """
        Сбор данных со всех каналов
        
        Args:
            days_back (int): За сколько дней назад собирать сообщения
            
        Returns:
            dict: Словарь с результатами {канал: количество новых сообщений}
        """
        results = {}
        
        for channel in TELEGRAM_CHANNELS:
            try:
                count = await self._process_channel(channel, days_back=days_back)
                results[channel] = count
                logger.info(f"Собрано {count} новых сообщений из канала {channel}")
            except Exception as e:
                logger.error(f"Ошибка при обработке канала {channel}: {str(e)}")
                results[channel] = 0
        
        return results
    
    def collect_data(self, days_back=1):
        """
        Инструмент для сбора данных из каналов
        
        Args:
            days_back (int): За сколько дней назад собирать данные
            
        Returns:
            dict: Результаты сбора данных
        """
        logger.info(f"Запуск сбора данных из каналов за последние {days_back} дней: {', '.join(TELEGRAM_CHANNELS)}")
        
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self._collect_all_channels(days_back=days_back))
        
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
            description="Собрать сообщения из правительственных Telegram-каналов за последние 1-3 дня",
            agent=self.agent,
            expected_output="Результаты сбора данных с информацией о количестве собранных сообщений"
        )