"""
Улучшенный агент для сбора данных из Telegram-каналов
с поддержкой глубокого исторического сбора
"""
import logging
import asyncio
from datetime import datetime, timedelta
import random
import time
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.errors import FloodWaitError, SlowModeWaitError
from langchain.tools import Tool
from crewai import Agent, Task

from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS
from utils.telegram_session_manager import TelegramSessionManager

logger = logging.getLogger(__name__)

class DataCollectorAgent:
    """Улучшенный агент для сбора данных из Telegram-каналов"""
    
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

    async def get_historical_messages(self, client, channel, start_date, end_date, max_messages=1000):
        """
        Улучшенный метод для сбора исторических сообщений с использованием дат-якорей
        и оптимизированной стратегией пагинации
        
        Args:
            client (TelegramClient): Клиент Telegram
            channel (str): Имя канала
            start_date (datetime): Начальная дата (включительно)
            end_date (datetime): Конечная дата (включительно)
            max_messages (int): Максимальное количество сообщений для сбора
            
        Returns:
            list: Список сообщений
        """
        try:
            entity = await client.get_entity(channel)
            logger.info(f"Получаем исторические сообщения из {channel} с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")
            
            # Для очень длинных периодов используем стратегию разбиения на подпериоды
            total_days = (end_date - start_date).days
            
            if total_days > 30:
                logger.info(f"Длинный период ({total_days} дней), разбиваем на подпериоды")
                return await self._get_messages_by_chunks(client, entity, channel, start_date, end_date, max_messages)
            
            # Для периодов средней длины используем оптимизированный подход с смещением дат
            all_messages = []
            offset_id = 0
            limit = 100  # Оптимальное значение для исторических запросов
            
            # Начинаем с конца периода и двигаемся назад
            current_offset_date = end_date
            
            # Для надежности ограничиваем количество запросов
            max_iterations = 50  # Увеличенное число для глубоких запросов
            
            for iteration in range(max_iterations):
                try:
                    logger.debug(f"Запрос #{iteration+1}: channel={channel}, offset_date={current_offset_date.strftime('%Y-%m-%d %H:%M')}, offset_id={offset_id}")
                    
                    # Используем GetHistoryRequest для получения сообщений с указанным смещением
                    messages = await client(GetHistoryRequest(
                        peer=entity,
                        limit=limit,
                        offset_date=current_offset_date,
                        offset_id=offset_id,
                        max_id=0,
                        min_id=0,
                        add_offset=0,
                        hash=0
                    ))
                    
                    if not messages.messages:
                        logger.debug(f"Больше сообщений нет, завершаем сбор")
                        break
                    
                    # Фильтруем сообщения по датам
                    filtered_batch = []
                    reached_start_date = False
                    
                    for msg in messages.messages:
                        msg_date = msg.date.replace(tzinfo=None)
                        
                        # Проверяем, входит ли сообщение в наш диапазон дат
                        if start_date <= msg_date <= end_date:
                            filtered_batch.append(msg)
                        
                        # Если дата сообщения раньше начальной даты, отмечаем это
                        if msg_date < start_date:
                            reached_start_date = True
                    
                    # Логируем результаты для отладки
                    if messages.messages:
                        first_date = messages.messages[0].date.replace(tzinfo=None)
                        last_date = messages.messages[-1].date.replace(tzinfo=None)
                        logger.debug(f"Получено {len(messages.messages)} сообщений, даты: {first_date.strftime('%Y-%m-%d')} - {last_date.strftime('%Y-%m-%d')}")
                        logger.debug(f"Отфильтровано {len(filtered_batch)} сообщений в диапазоне дат")
                    
                    # Добавляем отфильтрованные сообщения к общему списку
                    all_messages.extend(filtered_batch)
                    
                    # Если мы достигли начальной даты или собрали достаточно сообщений, завершаем сбор
                    if reached_start_date or len(all_messages) >= max_messages:
                        logger.info(f"Завершаем сбор: reached_start_date={reached_start_date}, total_messages={len(all_messages)}")
                        break
                    
                    # Если получили меньше сообщений, чем запросили, значит достигли конца истории
                    if len(messages.messages) < limit:
                        logger.debug(f"Получено меньше сообщений ({len(messages.messages)}) чем запрошено ({limit}), достигнут конец истории")
                        break
                    
                    # Устанавливаем смещение для следующего запроса на основе последнего сообщения
                    if messages.messages:
                        current_offset_date = messages.messages[-1].date
                        offset_id = messages.messages[-1].id
                    else:
                        break
                    
                    # Добавляем случайную паузу между запросами чтобы снизить вероятность ограничений API
                    delay = 1 + random.random()  # 1-2 секунды
                    await asyncio.sleep(delay)
                    
                except FloodWaitError as e:
                    wait_time = e.seconds
                    logger.warning(f"Получен FloodWaitError, ожидаем {wait_time} секунд")
                    await asyncio.sleep(wait_time + 1)  # +1 для надежности
                except SlowModeWaitError as e:
                    wait_time = e.seconds
                    logger.warning(f"Получен SlowModeWaitError, ожидаем {wait_time} секунд")
                    await asyncio.sleep(wait_time + 1)
                except Exception as e:
                    logger.error(f"Ошибка при получении сообщений из канала {channel}: {str(e)}")
                    # Делаем паузу в случае неизвестной ошибки
                    await asyncio.sleep(3)
            
            logger.info(f"Всего собрано {len(all_messages)} сообщений из канала {channel}")
            return all_messages
        
        except Exception as e:
            logger.error(f"Общая ошибка при сборе сообщений из канала {channel}: {str(e)}")
            return []

    async def _get_messages_by_chunks(self, client, entity, channel, start_date, end_date, max_messages):
        """
        Получение сообщений путем разбиения длинного периода на более короткие чанки
        """
        all_messages = []
        total_days = (end_date - start_date).days
        
        # Определяем оптимальный размер чанка в зависимости от длины периода
        chunk_size_days = 7  # Неделя - оптимальный период для запросов
        
        # Создаем список дат-якорей
        anchor_dates = []
        current_date = start_date
        while current_date <= end_date:
            anchor_dates.append(current_date)
            current_date += timedelta(days=chunk_size_days)
        
        # Добавляем конечную дату, если она еще не добавлена
        if anchor_dates[-1] < end_date:
            anchor_dates.append(end_date)
        
        logger.info(f"Разбиваем период на {len(anchor_dates) - 1} чанков с якорными датами")
        
        # Обрабатываем каждый чанк
        for i in range(len(anchor_dates) - 1):
            chunk_start = anchor_dates[i]
            chunk_end = anchor_dates[i + 1]
            logger.info(f"Обработка чанка {i+1}/{len(anchor_dates)-1}: {chunk_start.strftime('%Y-%m-%d')} - {chunk_end.strftime('%Y-%m-%d')}")
            
            # Получаем сообщения для текущего чанка
            chunk_messages = []
            offset_id = 0
            limit = 100
            
            # Начинаем с конца чанка
            current_offset_date = chunk_end
            
            # Для надежности ограничиваем количество запросов для каждого чанка
            max_iterations = 20
            
            for iteration in range(max_iterations):
                try:
                    logger.debug(f"Запрос #{iteration+1} для чанка {i+1}: offset_date={current_offset_date.strftime('%Y-%m-%d')}")
                    
                    # Запрашиваем сообщения
                    messages = await client(GetHistoryRequest(
                        peer=entity,
                        limit=limit,
                        offset_date=current_offset_date,
                        offset_id=offset_id,
                        max_id=0,
                        min_id=0,
                        add_offset=0,
                        hash=0
                    ))
                    
                    if not messages.messages:
                        break
                    
                    # Фильтруем сообщения, входящие в текущий чанк
                    filtered_messages = []
                    reached_chunk_start = False
                    
                    for msg in messages.messages:
                        msg_date = msg.date.replace(tzinfo=None)
                        
                        if chunk_start <= msg_date <= chunk_end:
                            filtered_messages.append(msg)
                        
                        if msg_date < chunk_start:
                            reached_chunk_start = True
                    
                    # Добавляем отфильтрованные сообщения
                    chunk_messages.extend(filtered_messages)
                    
                    # Если достигли начала чанка или получили мало сообщений, завершаем
                    if reached_chunk_start or len(messages.messages) < limit:
                        break
                    
                    # Обновляем смещение
                    if messages.messages:
                        current_offset_date = messages.messages[-1].date
                        offset_id = messages.messages[-1].id
                    else:
                        break
                    
                    # Пауза между запросами
                    await asyncio.sleep(1 + random.random())
                    
                except FloodWaitError as e:
                    wait_time = e.seconds
                    logger.warning(f"Получен FloodWaitError, ожидаем {wait_time} секунд")
                    await asyncio.sleep(wait_time + 1)
                except Exception as e:
                    logger.error(f"Ошибка при обработке чанка {i+1}: {str(e)}")
                    await asyncio.sleep(3)
            
            # Добавляем сообщения из чанка в общий список
            all_messages.extend(chunk_messages)
            logger.info(f"Собрано {len(chunk_messages)} сообщений из чанка {i+1}")
            
            # Делаем более длительную паузу между чанками
            await asyncio.sleep(3 + random.random() * 2)
        
        # Обрезаем список, если он слишком большой
        if len(all_messages) > max_messages:
            logger.info(f"Ограничиваем количество сообщений до {max_messages} (собрано {len(all_messages)})")
            all_messages = all_messages[:max_messages]
        
        logger.info(f"Всего собрано {len(all_messages)} сообщений из канала {channel} методом чанков")
        return all_messages

    async def collect_with_smart_filtering(self, client, channel, start_date, end_date):
        """
        Интеллектуальный сбор данных с фильтрацией и оптимизацией для существующей БД
        
        Args:
            client (TelegramClient): Клиент Telegram
            channel (str): Имя канала
            start_date (datetime): Начальная дата
            end_date (datetime): Конечная дата
            
        Returns:
            dict: Результаты сбора с дополнительной информацией
        """
        # Проверяем существующие данные для определения оптимальной стратегии сбора
        try:
            # Проверяем, есть ли у нас уже сообщения из этого канала за указанный период
            existing_messages = self.db_manager.get_messages_by_date_range(
                start_date=start_date,
                end_date=end_date,
                channels=[channel]
            )
            
            # Выводим информацию о существующих сообщениях
            logger.info(f"Найдено {len(existing_messages)} существующих сообщений из канала {channel} за период")
            
            # Если у нас уже есть некоторые сообщения, определяем пробелы в данных
            if existing_messages:
                # Сортируем по дате
                existing_messages.sort(key=lambda msg: msg.date)
                
                # Находим даты первого и последнего сообщения
                first_date = existing_messages[0].date
                last_date = existing_messages[-1].date
                
                logger.info(f"Существующие сообщения: с {first_date.strftime('%Y-%m-%d')} по {last_date.strftime('%Y-%m-%d')}")
                
                # Проверяем, есть ли пробел в начале периода
                if first_date > start_date:
                    logger.info(f"Обнаружен пробел в начале периода: с {start_date.strftime('%Y-%m-%d')} по {first_date.strftime('%Y-%m-%d')}")
                    
                    # Собираем данные за этот пробел
                    messages = await self.get_historical_messages(client, channel, start_date, first_date)
                    
                    # Сохраняем новые сообщения
                    new_count = 0
                    for msg in messages:
                        if msg.message:  # Проверяем, что сообщение содержит текст
                            if self.db_manager.save_message(
                                channel=channel,
                                message_id=msg.id,
                                text=msg.message,
                                date=msg.date.replace(tzinfo=None)
                            ):
                                new_count += 1
                    
                    logger.info(f"Заполнен пробел в начале периода: добавлено {new_count} сообщений")
                
                # Проверяем, есть ли пробел в конце периода
                if last_date < end_date:
                    logger.info(f"Обнаружен пробел в конце периода: с {last_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")
                    
                    # Собираем данные за этот пробел
                    messages = await self.get_historical_messages(client, channel, last_date, end_date)
                    
                    # Сохраняем новые сообщения
                    new_count = 0
                    for msg in messages:
                        if msg.message:
                            if self.db_manager.save_message(
                                channel=channel,
                                message_id=msg.id,
                                text=msg.message,
                                date=msg.date.replace(tzinfo=None)
                            ):
                                new_count += 1
                    
                    logger.info(f"Заполнен пробел в конце периода: добавлено {new_count} сообщений")
                
                # Ищем пробелы внутри периода (более 1 дня между сообщениями)
                gaps = []
                for i in range(1, len(existing_messages)):
                    prev_date = existing_messages[i-1].date
                    curr_date = existing_messages[i].date
                    
                    # Если разница более 1 дня, считаем это пробелом
                    if (curr_date - prev_date).days > 1:
                        # Добавляем пробел с запасом в 1 час
                        gap_start = prev_date + timedelta(hours=1)
                        gap_end = curr_date - timedelta(hours=1)
                        
                        if gap_start < gap_end:  # Проверка на корректность пробела
                            gaps.append((gap_start, gap_end))
                
                # Обрабатываем найденные пробелы
                for gap_idx, (gap_start, gap_end) in enumerate(gaps):
                    logger.info(f"Обнаружен пробел #{gap_idx+1}: с {gap_start.strftime('%Y-%m-%d %H:%M')} "
                              f"по {gap_end.strftime('%Y-%m-%d %H:%M')} ({(gap_end-gap_start).days} дней)")
                    
                    # Собираем данные за этот пробел
                    messages = await self.get_historical_messages(client, channel, gap_start, gap_end)
                    
                    # Сохраняем новые сообщения
                    new_count = 0
                    for msg in messages:
                        if msg.message:
                            if self.db_manager.save_message(
                                channel=channel,
                                message_id=msg.id,
                                text=msg.message,
                                date=msg.date.replace(tzinfo=None)
                            ):
                                new_count += 1
                    
                    logger.info(f"Заполнен пробел #{gap_idx+1}: добавлено {new_count} сообщений")
                
                # Возвращаем информацию о заполненных пробелах
                return {
                    "status": "filled_gaps",
                    "existing_count": len(existing_messages),
                    "gaps_filled": len(gaps) + (1 if first_date > start_date else 0) + (1 if last_date < end_date else 0)
                }
            else:
                # Если нет существующих сообщений, собираем все заново
                logger.info(f"Нет существующих сообщений из канала {channel} за период. Собираем всё заново.")
                
                # Собираем данные за весь период
                messages = await self.get_historical_messages(client, channel, start_date, end_date)
                
                # Сохраняем новые сообщения
                new_count = 0
                for msg in messages:
                    if msg.message:
                        if self.db_manager.save_message(
                            channel=channel,
                            message_id=msg.id,
                            text=msg.message,
                            date=msg.date.replace(tzinfo=None)
                        ):
                            new_count += 1
                
                logger.info(f"Собрано с нуля {new_count} сообщений из канала {channel}")
                
                return {
                    "status": "collected_all",
                    "new_count": new_count
                }
        
        except Exception as e:
            logger.error(f"Ошибка при интеллектуальном сборе данных из канала {channel}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def collect_deep_history(self, channel, start_date, end_date, force_update=False):
        """
        Метод для глубокого сбора исторических данных
        
        Args:
            channel (str): Имя канала
            start_date (datetime): Начальная дата
            end_date (datetime): Конечная дата
            force_update (bool): Принудительное обновление существующих сообщений
            
        Returns:
            dict: Результаты сбора
        """
        logger.info(f"Глубокий сбор данных из канала {channel} за период с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")
        
        try:
            # Используем менеджер сессий для получения клиента
            session_manager = TelegramSessionManager(self.api_id, self.api_hash)
            client = await session_manager.get_client()
            
            try:
                # Определяем длительность периода
                period_days = (end_date - start_date).days + 1
                
                # Выбираем соответствующий метод сбора
                if period_days > 30:
                    # Для длительных периодов используем умный сбор с анализом пробелов
                    result = await self.collect_with_smart_filtering(
                        client, channel, start_date, end_date
                    )
                else:
                    # Для более коротких периодов используем прямой сбор
                    messages = await self.get_historical_messages(
                        client, channel, start_date, end_date
                    )
                    
                    # Сохраняем полученные сообщения
                    saved_count = 0
                    for msg in messages:
                        if msg.message:  # Проверяем, что сообщение содержит текст
                            if self.db_manager.save_message(
                                channel=channel,
                                message_id=msg.id,
                                text=msg.message,
                                date=msg.date.replace(tzinfo=None)
                            ):
                                saved_count += 1
                    
                    result = {
                        "status": "success",
                        "saved_count": saved_count,
                        "total_messages": len(messages)
                    }
                
                logger.info(f"Завершен глубокий сбор данных из канала {channel}: {result}")
                return result
                
            finally:
                # Освобождаем клиента
                await session_manager.release_client(client)
        
        except Exception as e:
            logger.error(f"Ошибка при глубоком сборе данных из канала {channel}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def collect_data(self, days_back=1, force_update=False, start_date=None, end_date=None):
        """
        Асинхронный метод для сбора данных из каналов с поддержкой исторических периодов
        
        Args:
            days_back (int): За сколько дней назад собирать данные
            force_update (bool): Принудительно обновлять существующие сообщения
            start_date (datetime, optional): Начальная дата для сбора
            end_date (datetime, optional): Конечная дата для сбора
                    
        Returns:
            dict: Результаты сбора данных
        """
        # Определяем эффективные даты сбора
        use_date_range = start_date is not None and end_date is not None
        
        if use_date_range:
            logger.info(f"Запуск сбора данных за период: с {start_date.strftime('%Y-%m-%d')} "
                      f"по {end_date.strftime('%Y-%m-%d')}")
        else:
            logger.info(f"Запуск сбора данных за последние {days_back} дней: {', '.join(TELEGRAM_CHANNELS)}")
            # Только если даты не указаны явно, рассчитываем от текущей даты
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back-1)
        
        # Проверяем длительность периода для выбора стратегии сбора
        period_days = (end_date - start_date).days + 1
        
        # Добавляем error handling с повторными попытками для сбора данных
        max_attempts = 3
        attempt = 0
        backoff_delay = 2  # начальная задержка в секундах
        
        try:
            while attempt < max_attempts:
                try:
                    # Создаем одного менеджера сессий на весь процесс
                    session_manager = TelegramSessionManager(self.api_id, self.api_hash)
                    
                    # Используем переданные каналы или берем из настроек
                    channels_to_process = TELEGRAM_CHANNELS
                    results = {}
                    total_messages = 0
                    
                    # Для длительных периодов используем специальный режим для глубокого сбора
                    if period_days > 7:
                        logger.info(f"Обнаружен длительный период сбора данных ({period_days} дней). Использую глубокий сбор.")
                        
                        for channel in channels_to_process:
                            channel_result = await self.collect_deep_history(
                                channel, 
                                start_date, 
                                end_date, 
                                force_update
                            )
                            
                            # Сохраняем результат для канала
                            if channel_result["status"] == "success":
                                saved_count = channel_result.get("saved_count", 0)
                                results[channel] = saved_count
                                total_messages += saved_count
                                logger.info(f"Канал {channel}: собрано {saved_count} сообщений")
                            elif channel_result["status"] == "filled_gaps":
                                # Учитываем существующие сообщения
                                existing_count = channel_result.get("existing_count", 0)
                                results[channel] = existing_count
                                total_messages += existing_count
                                logger.info(f"Канал {channel}: найдено {existing_count} сообщений")
                            elif channel_result["status"] == "collected_all":
                                new_count = channel_result.get("new_count", 0)
                                results[channel] = new_count
                                total_messages += new_count
                                logger.info(f"Канал {channel}: собрано {new_count} сообщений")
                            else:
                                results[channel] = 0
                                logger.warning(f"Канал {channel}: ошибка сбора - {channel_result.get('error', 'unknown')}")
                            
                            # Делаем паузу между каналами
                            await asyncio.sleep(2)
                    else:
                        # Для коротких периодов используем стандартный сбор
                        client = await session_manager.get_client()
                        
                        try:
                            # Обрабатываем каждый канал последовательно
                            for channel in channels_to_process:
                                try:
                                    logger.info(f"Обработка канала {channel}...")
                                    
                                    # Получаем сообщения с использованием методов оптимизированного сбора
                                    messages = await self.get_historical_messages(
                                        client, 
                                        channel, 
                                        start_date, 
                                        end_date
                                    )
                                    
                                    # Дополнительная проверка на сегодняшние сообщения
                                    today = datetime.now().date()
                                    if end_date.date() >= today:
                                        logger.info(f"Выполняю дополнительный запрос для получения самых свежих сообщений из канала {channel}")
                                        
                                        # Получаем последние сообщения для актуализации данных
                                        latest_messages = await self._get_newest_messages(client, channel, 20)
                                        
                                        # Фильтруем и добавляем к результатам
                                        if latest_messages:
                                            added_count = 0
                                            for msg in latest_messages:
                                                msg_date = msg.date.replace(tzinfo=None)
                                                if start_date <= msg_date <= end_date and msg.id not in [m.id for m in messages]:
                                                    logger.info(f"Найдено новое сообщение в канале {channel} от {msg_date.strftime('%Y-%m-%d %H:%M:%S')}")
                                                    messages.append(msg)
                                                    added_count += 1
                                            
                                            if added_count > 0:
                                                logger.info(f"Добавлено {added_count} свежих сообщений из канала {channel}")
                                    
                                    # Если нет принудительного обновления, фильтруем существующие сообщения
                                    existing_ids = []
                                    if not force_update:
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
                                    
                                    # Используем пакетное сохранение, если метод доступен
                                    if hasattr(self.db_manager, 'batch_save_messages'):
                                        if messages_to_save:
                                            try:
                                                new_messages_count = self.db_manager.batch_save_messages(messages_to_save)
                                            except Exception as e:
                                                logger.error(f"Ошибка при пакетном сохранении: {str(e)}")
                                                new_messages_count = self._save_messages_one_by_one(messages_to_save, channel)
                                    else:
                                        # Если метода batch_save_messages нет, сохраняем по одному
                                        new_messages_count = self._save_messages_one_by_one(messages_to_save, channel)
                                    
                                    logger.info(f"Канал {channel}: сохранено {new_messages_count} новых сообщений")
                                    results[channel] = new_messages_count
                                    total_messages += new_messages_count
                                    
                                    # Добавляем небольшую задержку между запросами к разным каналам
                                    await asyncio.sleep(1)
                                    
                                except Exception as e:
                                    logger.error(f"Ошибка при обработке канала {channel}: {str(e)}")
                                    results[channel] = 0
                        finally:
                            # Освобождаем клиент
                            await session_manager.release_client(client)
                    
                    # Формируем результаты сбора
                    logger.info(f"Сбор данных завершен. Всего собрано {total_messages} новых сообщений")
                    
                    # Вызываем хук обновления после сбора
                    if total_messages > 0:
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
        finally:
            # Корректное завершение работы с клиентом, если он был создан
            if hasattr(self, 'client') and self.client:
                try:
                    await self._release_client()
                except Exception as e:
                    logger.error(f"Ошибка при освобождении клиента Telegram: {str(e)}")
    
    async def _get_newest_messages(self, client, channel, limit=20):
        """Получение самых новых сообщений из канала"""
        try:
            entity = await client.get_entity(channel)
            messages = await client(GetHistoryRequest(
                peer=entity,
                limit=limit,
                offset_date=None,  # Без смещения для получения самых новых
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            
            if not messages or not messages.messages:
                return []
                
            logger.info(f"Получено {len(messages.messages)} новых сообщений из канала {channel}")
            return messages.messages
        except Exception as e:
            logger.error(f"Ошибка при получении новых сообщений: {str(e)}")
            return []

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
        
    async def collect_for_specific_period(self, start_date_str, end_date_str, channels=None):
        """
        Сбор данных за указанный период (формат дат YYYY-MM-DD)
        
        Args:
            start_date_str (str): Начальная дата в формате YYYY-MM-DD
            end_date_str (str): Конечная дата в формате YYYY-MM-DD
            channels (list, optional): Список каналов для сбора (по умолчанию все каналы)
            
        Returns:
            dict: Результаты сбора
        """
        try:
            # Преобразуем строки дат в datetime объекты
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            # Для конечной даты устанавливаем время 23:59:59
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            
            # Используем каналы из параметра или все каналы по умолчанию
            target_channels = channels or TELEGRAM_CHANNELS
            
            logger.info(f"Запуск сбора данных за период {start_date_str} - {end_date_str} из каналов: {target_channels}")
            
            # Вызываем основной метод сбора с указанными датами
            result = await self.collect_data(
                start_date=start_date,
                end_date=end_date,
                force_update=False  # Не обновляем существующие сообщения
            )
            
            return result
            
        except ValueError as e:
            logger.error(f"Ошибка формата даты: {str(e)}")
            return {
                "status": "error",
                "error": f"Некорректный формат даты: {str(e)}",
                "total_new_messages": 0
            }
        except Exception as e:
            logger.error(f"Ошибка при сборе данных за период: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "total_new_messages": 0
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