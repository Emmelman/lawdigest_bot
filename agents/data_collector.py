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
import time
from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS
from utils.telegram_session_manager import TelegramSessionManager

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
        """Инициализация клиента Telegram с использованием менеджера сессий"""
        if not self.client:
            # Используем менеджер сессий для получения клиента
            session_manager = TelegramSessionManager(self.api_id, self.api_hash)
            self.client = await session_manager.get_client()
            self.session_manager = session_manager  # Сохраняем ссылку на менеджер
    
    async def _release_client(self):
        """Корректное освобождение клиента после использования"""
        if hasattr(self, 'session_manager') and hasattr(self, 'client') and self.client:
            await self.session_manager.release_client(self.client)
            self.client = None

    async def _get_channel_messages(self, channel, days_back=1, limit_per_request=100, start_date=None, end_date=None):
        """
        Получение сообщений из канала за указанный период
        """
        await self._init_client()
        
        try:
            entity = await self.client.get_entity(channel)
            # Определение дат для фильтрации
            if start_date is None or end_date is None:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days_back)
            # Определение дат для фильтрации - используем datetime без timezone
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
                    # Всегда преобразуем дату из Telegram (aware) в naive datetime
                    msg_date = msg.date.replace(tzinfo=None)
                    # Теперь обе даты naive, можно сравнивать
                    if start_date <= msg_date <= end_date:
                        filtered_messages.append(msg)
                all_messages.extend(filtered_messages)
                
                # Проверяем, нужно ли продолжать пагинацию
                if len(messages_list) < limit_per_request:
                    # Получили меньше сообщений, чем запрашивали (конец списка)
                    break
                    
                # Проверяем дату последнего сообщения
                if messages_list:
                    last_date = messages_list[-1].date.replace(tzinfo=None)
                    if last_date < start_date:
                        # Последнее сообщение старше начальной даты
                        break
                    
                    # Устанавливаем смещение для следующего запроса
                    offset_id = messages_list[-1].id
                    
                    logger.debug(f"Получено {len(filtered_messages)} сообщений из {len(messages_list)}. "
                            f"Продолжаем пагинацию с ID {offset_id}")
                else:
                    break
            
            logger.info(f"Всего получено {total_messages} сообщений, отфильтровано {len(all_messages)} "
                    f"за указанный период из канала {channel}")
            
            return all_messages
            
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений из канала {channel}: {str(e)}")
            return []
      
    
    async def _process_channel(self, channel, start_date=None, end_date=None, days_back=1, force_update=False):
        """
        Обработка канала: получение и сохранение сообщений
        
        Args:
            channel (str): Имя канала
            start_date (datetime, optional): Начальная дата для сбора
            end_date (datetime, optional): Конечная дата для сбора
            days_back (int): За сколько дней назад собирать сообщения 
            force_update (bool): Принудительно обрабатывать все сообщения
        """
        try:
            # Инициализируем клиент для каждого канала, чтобы избежать конфликтов
            await self._init_client()
            
            # Получаем сообщения с учетом дат
            if start_date is None or end_date is None:
                messages = await self._get_channel_messages(channel, days_back=days_back)
            else:
                messages = await self._get_channel_messages(
                    channel, days_back=days_back, start_date=start_date, end_date=end_date
                )
            
            # Если нет принудительного обновления, фильтруем существующие сообщения
            existing_ids = []
            if not force_update:
                # Получаем существующие ID - используем имеющийся метод
                existing_messages = []
                for msg in messages:
                    existing = self.db_manager.get_message_by_channel_and_id(channel, msg.id)
                    if existing:
                        existing_ids.append(msg.id)
            
            # Фильтруем сообщения - оставляем только новые или все при force_update
            filtered_messages = [msg for msg in messages if force_update or msg.id not in existing_ids]
            
            logger.info(f"Канал {channel}: получено {len(messages)} сообщений, "
                    f"для обработки отобрано {len(filtered_messages)} (режим force_update={force_update})")
            
            # Подготавливаем данные для пакетного сохранения
            messages_to_save = []
            for message in filtered_messages:
                # Пропускаем сообщения без текста
                if not message.message:
                    continue
                
                # Нормализуем дату
                message_date = message.date
                if message_date.tzinfo is not None:
                    message_date = message_date.replace(tzinfo=None)
                
                messages_to_save.append({
                    'channel': channel,
                    'message_id': message.id,
                    'text': message.message,
                    'date': message_date
                })
            
            # Сохраняем сообщения - либо пакетно, либо по одному
            new_messages_count = 0
            
            # Проверяем, реализован ли метод batch_save_messages
            if hasattr(self.db_manager, 'batch_save_messages') and callable(getattr(self.db_manager, 'batch_save_messages')):
                # Используем пакетное сохранение
                if messages_to_save:
                    try:
                        new_messages_count = self.db_manager.batch_save_messages(messages_to_save)
                    except Exception as e:
                        logger.error(f"Ошибка при пакетном сохранении сообщений из канала {channel}: {str(e)}")
                        # Если пакетное сохранение не удалось, пробуем по одному
                        new_messages_count = self._save_messages_one_by_one(messages_to_save, channel)
            else:
                # Если метода batch_save_messages нет, сохраняем по одному
                new_messages_count = self._save_messages_one_by_one(messages_to_save, channel)
            
            logger.info(f"Канал {channel}: сохранено {new_messages_count} новых сообщений, "
                    f"пропущено {len(existing_ids)} существующих")
                    
            return new_messages_count
        finally:
            # Освобождаем клиент в любом случае
            await self._release_client()

    def _save_messages_one_by_one(self, messages_to_save, channel):
        """Вспомогательный метод для сохранения сообщений по одному"""
        saved_count = 0
        for msg_data in messages_to_save:
            try:
                self.db_manager.save_message(
                    channel=msg_data['channel'],
                    message_id=msg_data['message_id'],
                    text=msg_data['text'],
                    date=msg_data['date']
                )
                saved_count += 1
            except Exception as e:
                logger.error(f"Ошибка при сохранении сообщения {msg_data['message_id']} из канала {channel}: {str(e)}")
        
        return saved_count
    
    """
    Оптимизированный метод для параллельного сбора данных из каналов
    """
    async def _collect_all_channels_parallel(self, days_back=1, channels=None, specific_date=None, start_date=None, end_date=None, force_update=False):
        """
        Параллельный сбор данных со всех или указанных каналов
        
        Args:
            days_back (int): За сколько дней назад собирать сообщения
            channels (list, optional): Список каналов для сбора данных
            specific_date (datetime, optional): Конкретная дата для сбора данных 
            start_date (datetime, optional): Начальная дата для сбора данных
            end_date (datetime, optional): Конечная дата для сбора данных
        """
        # Убираем вызов self._init_client()
        results = {}
        
        # Используем переданные каналы или берем из настроек
        channels_to_process = channels or TELEGRAM_CHANNELS
        
        # Определяем даты для сбора с приоритетом на явно заданные даты
        if start_date and end_date:
            # Если обе даты указаны явно, используем их
            logger.info(f"Использую явно указанный период: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
        elif specific_date:
            # Если указана конкретная дата, используем ее как диапазон целого дня
            start_date = datetime.combine(specific_date.date(), time(0, 0, 0))
            end_date = datetime.combine(specific_date.date(), time(23, 59, 59))
            logger.info(f"Использую конкретную дату: {specific_date.strftime('%Y-%m-%d')}")
        else:
            # Иначе рассчитываем от текущей даты
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            logger.info(f"Рассчитываю период от текущей даты: последние {days_back} дней")
        
        # Создаем задачи для всех каналов
        tasks = []
        for channel in channels_to_process:
            task = self._process_channel(channel, start_date=start_date, end_date=end_date, force_update=force_update)
            tasks.append(task)
        
        # Запускаем все задачи параллельно
        channel_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for i, channel in enumerate(channels_to_process):
            result = channel_results[i]
            if isinstance(result, Exception):
                logger.error(f"Ошибка при обработке канала {channel}: {str(result)}")
                results[channel] = 0
            else:
                results[channel] = result
                logger.info(f"Собрано {result} новых сообщений из канала {channel}")
        
        return results

    # Изменения в agents/data_collector.py 

    # В файле agents/data_collector.py модифицировать метод collect_data:

async def collect_data(self, days_back=1, force_update=False):
    """
    Асинхронный метод для сбора данных из каналов
    
    Args:
        days_back (int): За сколько дней назад собирать данные
        force_update (bool): Принудительно обновлять существующие сообщения
                
    Returns:
        dict: Результаты сбора данных
    """
    logger.info(f"Запуск асинхронного сбора данных за последние {days_back} дней: {', '.join(TELEGRAM_CHANNELS)}")
    
    # Добавляем error handling с повторными попытками для сбора данных
    max_attempts = 3
    attempt = 0
    backoff_delay = 2  # начальная задержка в секундах
    
    while attempt < max_attempts:
        try:
            # Прямой вызов асинхронного метода
            results = await self._collect_all_channels_parallel(days_back=days_back, force_update=force_update)
            
            total_messages = sum(results.values())
            logger.info(f"Сбор данных завершен. Всего собрано {total_messages} новых сообщений")
            
            if total_messages > 0:
                # Вызываем хук обновления после сбора
                update_result = await self.after_collect_hook({
                    "status": "success",
                    "total_new_messages": total_messages,
                    "channels_stats": results
                })
                
                return {
                    "status": "success",
                    "total_new_messages": total_messages,
                    "channels_stats": results,
                    "update_result": update_result
                }
            
            return {
                "status": "success",
                "total_new_messages": total_messages,
                "channels_stats": results
            }
            
        except Exception as e:
            attempt += 1
            # Проверяем, связана ли ошибка с блокировкой базы данных
            if "database is locked" in str(e).lower():
                if attempt < max_attempts:
                    # Используем экспоненциальную задержку между попытками
                    delay = backoff_delay * (2 ** (attempt - 1))
                    logger.warning(f"Ошибка блокировки БД при сборе данных, повторная попытка {attempt}/{max_attempts} через {delay}с: {str(e)}")
                    await asyncio.sleep(delay)
                    
                    # Очищаем состояние клиента перед следующей попыткой
                    self.client = None
                else:
                    logger.error(f"Не удалось собрать данные после {max_attempts} попыток из-за блокировки БД: {str(e)}")
                    return {
                        "status": "error",
                        "error": f"Ошибка блокировки базы данных: {str(e)}",
                        "total_new_messages": 0,
                        "channels_stats": {}
                    }
            else:
                # Если ошибка не связана с блокировкой, просто логируем и возвращаем результат
                logger.error(f"Ошибка при сборе данных: {str(e)}")
                return {
                    "status": "error",
                    "error": str(e),
                    "total_new_messages": 0,
                    "channels_stats": {}
                }
                
    # Если все попытки не удались
    return {
        "status": "error",
        "error": "Превышено максимальное количество попыток сбора данных",
        "total_new_messages": 0,
        "channels_stats": {}
    }

    async def after_collect_hook(self, collect_result):
        """
        Асинхронный хук, вызываемый после успешного сбора данных
        
        Args:
            collect_result (dict): Результаты сбора данных
        """
        if collect_result.get("total_new_messages", 0) > 0:
            # Если собраны новые сообщения, запускаем обновление дайджестов
            logger.info(f"Собрано {collect_result['total_new_messages']} новых сообщений, запускаем обновление дайджестов")
            
            try:
                # Получаем текущую дату
                today = datetime.now()
                
                # Запускаем обновление дайджестов, которые включают сегодняшнюю дату
                from agents.digester import DigesterAgent
                digester = DigesterAgent(self.db_manager)
                update_result = digester.update_digests_for_date(today)
                
                logger.info(f"Результат обновления дайджестов: {update_result}")
                return update_result
            except Exception as e:
                logger.error(f"Ошибка при обновлении дайджестов после сбора данных: {str(e)}")
                return {"status": "error", "error": str(e)}
        
        return {"status": "no_update_needed"}
    # В DataCollectorAgent:
    def collect_from_multiple_sources(self, channels, days_back=1, batch_size=10):
        """
        Оптимизированный сбор данных из множества каналов с батчингом
        """
        logger.info(f"Запуск оптимизированного сбора данных из {len(channels)} каналов")
        
        # Разбиваем каналы на группы
        channel_groups = [channels[i:i+batch_size] for i in range(0, len(channels), batch_size)]
        
        all_results = {}
        total_messages = 0
        
        for group_idx, group in enumerate(channel_groups):
            logger.info(f"Обработка группы каналов {group_idx+1}/{len(channel_groups)}: {', '.join(group)}")
            
            try:
                # Собираем данные из этой группы каналов параллельно
                group_results = self._collect_all_channels_parallel(
                    channels=group,
                    days_back=days_back
                )
                
                # Обрабатываем результаты
                group_total = sum(group_results.values())
                total_messages += group_total
                all_results.update(group_results)
                
                logger.info(f"Группа {group_idx+1}: собрано {group_total} сообщений")
                
                # Делаем небольшую паузу между группами, чтобы не перегружать API
                if group_idx < len(channel_groups) - 1:
                    time.sleep(2)
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке группы каналов {group_idx+1}: {str(e)}")
        
        logger.info(f"Всего собрано {total_messages} сообщений из {len(channels)} каналов")
        return all_results
    # В файле agents/data_collector.py добавим подробное логирование дат

    async def _get_channel_messages(self, channel, days_back=1, limit_per_request=100, start_date=None, end_date=None):
        """
        Получение сообщений из канала за указанный период   
        
        Args:
            channel (str): Имя канала
            days_back (int): За сколько дней назад собирать сообщения
            limit_per_request (int): Лимит сообщений на запрос
            start_date (datetime, optional): Начальная дата периода
            end_date (datetime, optional): Конечная дата периода
        """
        await self._init_client()
        
        try:
            entity = await self.client.get_entity(channel)
            
            # Определение дат для фильтрации - используем datetime без timezone
            if start_date is None or end_date is None:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days_back)
            
            # Добавляем подробное логирование
            logger.info(f"Получение сообщений из канала {channel} с {start_date.strftime('%Y-%m-%d %H:%M')} по {end_date.strftime('%Y-%m-%d %H:%M')}")
            
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
                
                # Фильтруем сообщения по дате с логированием
                filtered_messages = []
                for msg in messages_list:
                    # Преобразуем дату из Telegram (aware) в naive datetime
                    msg_date = msg.date.replace(tzinfo=None)
                    
                    if start_date <= msg_date <= end_date:
                        filtered_messages.append(msg)
                        logger.debug(f"Сообщение {msg.id} от {msg_date.strftime('%Y-%m-%d %H:%M')} в диапазоне дат")
                    else:
                        logger.debug(f"Сообщение {msg.id} от {msg_date.strftime('%Y-%m-%d %H:%M')} вне диапазона дат")
                        
                all_messages.extend(filtered_messages)
                
                # Проверяем, нужно ли продолжать пагинацию
                if len(messages_list) < limit_per_request:
                    # Получили меньше сообщений, чем запрашивали (конец списка)
                    break
                    
                # Проверяем дату последнего сообщения
                if messages_list:
                    last_date = messages_list[-1].date.replace(tzinfo=None)
                    if last_date < start_date:
                        # Последнее сообщение старше начальной даты
                        break
                    
                    # Устанавливаем смещение для следующего запроса
                    offset_id = messages_list[-1].id
                    
                    logger.debug(f"Получено {len(filtered_messages)} сообщений из {len(messages_list)}. "
                            f"Продолжаем пагинацию с ID {offset_id}")
                else:
                    break
            
            logger.info(f"Всего получено {total_messages} сообщений, отфильтровано {len(all_messages)} "
                    f"за указанный период из канала {channel}")
            
            return all_messages
            
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений из канала {channel}: {str(e)}")
            return []

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