# utils/telegram_session_manager.py
"""
Менеджер сессий Telegram для предотвращения конкурентного доступа
"""
import logging
import asyncio
import time
from telethon import TelegramClient
import os

logger = logging.getLogger(__name__)

class TelegramSessionManager:
    """
    Синглтон для управления подключениями к Telegram API
    и предотвращения блокировок базы данных сессий
    """
    _instance = None
    _client = None
    _lock = asyncio.Lock()
    _active_operations = 0
    _last_operation_time = 0
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(TelegramSessionManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, api_id=None, api_hash=None, session_name='session_name'):
        if self._initialized:
            return
            
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self._initialized = True
    
    async def get_client(self, api_id=None, api_hash=None):
        """
        Получение клиента с контролем доступа.
        При необходимости инициализирует новый клиент.
        
        Args:
            api_id (str, optional): Telegram API ID (если не указан при инициализации)
            api_hash (str, optional): Telegram API Hash (если не указан при инициализации)
            
        Returns:
            TelegramClient: Клиент Telegram
        """
        async with self._lock:
            # Используем переданные значения или значения из инициализации
            api_id = api_id or self.api_id
            api_hash = api_hash or self.api_hash
            
            # Генерируем уникальное имя сессии, основанное на текущем времени и PID
            # чтобы избежать конфликтов между разными запросами
            unique_session = f"{self.session_name}_{int(time.time())}_{os.getpid()}"
            
            # Создаем нового клиента при каждом запросе для надежности
            client = TelegramClient(unique_session, api_id, api_hash)
            
            # Ожидаем между запросами, чтобы избежать слишком частых подключений
            current_time = time.time()
            if self._last_operation_time > 0:
                elapsed = current_time - self._last_operation_time
                if elapsed < 1.5:  # Минимум 1.5 секунды между операциями
                    await asyncio.sleep(1.5 - elapsed)
            
            # Обновляем время последней операции
            self._last_operation_time = time.time()
            
            try:
                # Запускаем клиента
                await client.start()
                self._active_operations += 1
                logger.debug(f"Создан новый клиент Telegram (активно: {self._active_operations})")
                return client
            except Exception as e:
                logger.error(f"Ошибка при создании клиента Telegram: {str(e)}")
                # Если не удалось создать клиента, делаем паузу подольше
                await asyncio.sleep(3)
                raise
    
    async def release_client(self, client):
        """
        Освобождение клиента после использования
        
        Args:
            client (TelegramClient): Клиент для освобождения
        """
        async with self._lock:
            if client:
                try:
                    await client.disconnect()
                    # Удаляем файл сессии после использования
                    session_file = f"{client.session.filename}.session"
                    if os.path.exists(session_file):
                        try:
                            os.remove(session_file)
                        except Exception as e:
                            logger.warning(f"Не удалось удалить файл сессии {session_file}: {str(e)}")
                except Exception as e:
                    logger.error(f"Ошибка при закрытии клиента Telegram: {str(e)}")
                finally:
                    self._active_operations -= 1
                    logger.debug(f"Клиент Telegram освобожден (активно: {self._active_operations})")