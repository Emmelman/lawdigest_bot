"""
Управление базой данных
"""
import logging
import functools
import time
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime, timedelta
import json
from sqlalchemy import or_, and_
from sqlalchemy import extract
from .models import Base, Message, Digest, DigestSection, DigestGeneration, init_db

logger = logging.getLogger(__name__) 

class DatabaseManager:
    """Менеджер для работы с базой данных"""
    
    # В database/db_manager.py

    def __init__(self, db_url):
        """
        Инициализация менеджера БД с улучшенной обработкой блокировок
        """
        # Добавляем параметры для улучшения работы с SQLite
        if 'sqlite' in db_url.lower():
            # Увеличиваем таймаут ожидания и добавляем параметры для оптимизации конкурентного доступа
            if '?' not in db_url:
                db_url += '?timeout=60&busy_timeout=60000&journal_mode=WAL&synchronous=NORMAL'
            else:
                db_url += '&timeout=60&busy_timeout=60000&journal_mode=WAL&synchronous=NORMAL'
        
        init_db(db_url) 
        
        # Создаем движок с улучшенными параметрами для многопоточного доступа
        self.engine = create_engine(
            db_url,
            connect_args={
                'check_same_thread': False,  # Разрешаем доступ из разных потоков
            },
            pool_size=10,  # Размер пула соединений
            max_overflow=20,  # Максимальное количество дополнительных соединений
            pool_timeout=60,  # Увеличиваем время ожидания соединения из пула
            pool_recycle=1800,  # Переиспользовать соединения не старше 30 минут
            pool_pre_ping=True  # Проверять соединение перед использованием
        )
        
        self.session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(self.session_factory)

    # Добавляем декоратор для автоматической обработки транзакций и повторных попыток
    @staticmethod
    def with_retry(max_attempts=5, delay=1.0, backoff_factor=2.0, error_types=(Exception,)):
        """
        Декоратор для повторных попыток выполнения операций с БД
        (Метод экземпляра класса)
        с экспоненциальной задержкой.
        
        Args:
            max_attempts (int): Максимальное количество попыток
            delay (float): Начальная задержка между попытками в секундах
            backoff_factor (float): Множитель для увеличения задержки с каждой попыткой
            error_types (tuple): Типы ошибок, при которых выполнять повторные попытки
        """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(obj_self, *args, **kwargs): # Переименовано self в obj_self для ясности
                attempt = 0
                current_delay = delay
                last_error = None
                
                while attempt < max_attempts:
                    try:
                        return func(obj_self, *args, **kwargs) # Передача obj_self обратно в обернутую функцию
                    except error_types as e:
                        # Определяем, связана ли ошибка с блокировкой БД
                        is_db_lock_error = (
                            isinstance(e, sqlalchemy.exc.OperationalError) and 
                            ("database is locked" in str(e).lower() or 
                            "database disk image is malformed" in str(e).lower() or
                            "busy" in str(e).lower())
                        )
                        
                        attempt += 1
                        last_error = e
                        
                        if attempt < max_attempts:
                            # Для ошибок блокировки используем экспоненциальную задержку
                            if is_db_lock_error:
                                retry_delay = current_delay
                                current_delay *= backoff_factor  # Увеличиваем задержку для следующей попытки
                                
                                logger.warning(f"База данных заблокирована, повторная попытка {attempt}/{max_attempts} через {retry_delay:.2f}с")
                                time.sleep(retry_delay)
                            else:
                                # Если это не ошибка блокировки, используем постоянную задержку
                                logger.warning(f"Ошибка при выполнении операции с БД: {str(e)}, повторная попытка {attempt}/{max_attempts} через {delay}с")
                                time.sleep(delay)
                        else:
                            # Если это последняя попытка - прерываем
                            break
                
                # Если все попытки не удались, логируем и выбрасываем исключение
                logger.error(f"Не удалось выполнить операцию с БД после {max_attempts} попыток: {str(last_error)}")
                raise last_error
                
            return wrapper
        return decorator
    
    @with_retry(max_attempts=3, delay=0.5)
    def save_message(self, channel, message_id, text, date):
        """
        Сохранение сообщения из Telegram-канала
        
        Args:
            channel (str): Название канала
            message_id (int): ID сообщения
            text (str): Текст сообщения
            date (datetime): Дата публикации
            
        Returns:
            Message: Созданная запись сообщения
        """
        session = self.Session()
        try:
            # Проверяем, существует ли уже сообщение с таким ID в этом канале
            existing_message = session.query(Message).filter_by(
                channel=channel, message_id=message_id
            ).first()
            
            if existing_message:
                logger.debug(f"Сообщение {message_id} из канала {channel} уже существует")
                return existing_message, False # Return existing message and False for "not new"
            
            # Создаем новое сообщение
            message = Message(
                channel=channel,
                message_id=message_id,
                text=text,
                date=date
            )
            session.add(message)
            session.commit()
            logger.debug(f"Сохранено сообщение {message_id} из канала {channel}") # This line was duplicated, fixed
            return message, True # Return new message and True for "new" # This return was already here, keeping it.
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при сохранении сообщения: {str(e)}")
            raise
        finally:
            session.close()
    
    def get_unanalyzed_messages(self, limit=100):
        """
        Получение сообщений без категории
        
        Args:
            limit (int): Максимальное количество сообщений
            
        Returns:
            list: Список объектов Message
        """
        session = self.Session()
        try:
            messages = session.query(Message).filter_by(category=None).limit(limit).all()
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении непроанализированных сообщений: {str(e)}")
            return []
        finally:
            session.close()
    
    @with_retry(max_attempts=3, delay=0.5)
    def update_message_category(self, message_id, category, confidence=3):
        """
        Обновление категории сообщения с уровнем уверенности
        
        Args:
            message_id (int): ID сообщения в нашей БД
            category (str): Категория сообщения
            confidence (int): Уровень уверенности (1-5, где 1 - самая низкая, 5 - самая высокая)
            
        Returns:
            bool: True если обновление успешно, иначе False
        """
        session = self.Session()
        try:
            message = session.query(Message).filter_by(id=message_id).first()
            if not message:
                logger.warning(f"Сообщение с ID {message_id} не найдено")
                return False
            
            message.category = category
            message.confidence = confidence
            session.commit()
            logger.debug(f"Обновлена категория сообщения {message_id}: {category} (уверенность: {confidence})")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при обновлении категории сообщения: {str(e)}")
            return False
        finally:
            session.close()
    def get_messages_with_low_confidence(self, confidence_threshold=3, limit=50, start_date=None, end_date=None):
        """
        Получение сообщений с низкой уверенностью с фильтрацией по дате
        """
        session = self.Session()
        try:
            query = session.query(Message)\
                .filter(Message.category != None)\
                .filter(Message.confidence <= confidence_threshold)
            
            # Добавляем фильтр по дате, если даты указаны
            if start_date:
                query = query.filter(Message.date >= start_date)
            if end_date:
                query = query.filter(Message.date <= end_date)
            
            # Сортировка и лимит
            messages = query.order_by(Message.confidence, Message.id.desc())\
                .limit(limit)\
                .all()
                
            logger.info(f"Получено {len(messages)} сообщений с низкой уверенностью" + 
                    (f" за период {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}" 
                        if start_date and end_date else ""))
            
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений с низкой уверенностью: {str(e)}")
            return []
        finally:
            session.close()

    def get_messages_by_date_range(self, start_date, end_date, category=None,
                            channels=None, keywords=None):
        """
        Получение сообщений за указанный период с фильтрацией по категории
        
        Args:
            start_date (datetime): Начальная дата (включительно)
            end_date (datetime): Конечная дата (включительно)
            category (str, optional): Категория для фильтрации
            channels (list, optional): Список каналов для фильтрации
            keywords (list, optional): Ключевые слова для фильтрации
        """
        session = self.Session()
        try:
            # Приводим даты к нормализованному виду, если они не datetime
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            if isinstance(end_date, str):
                # Если передана строковая дата, берем конец дня
                end_date = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                
            # Логируем диапазон дат для отладки
            logger.info(f"Поиск сообщений с {start_date.strftime('%Y-%m-%d %H:%M:%S')} по "
                    f"{end_date.strftime('%Y-%m-%d %H:%M:%S')}")

            # Строим базовый запрос с фильтром по дате
            query = session.query(Message).filter(
                Message.date >= start_date,
                Message.date <= end_date  # Включаем конечную дату
            )
            
            # Применяем дополнительные фильтры, если они заданы
            if category:
                query = query.filter(Message.category == category)
            
            if channels:
                query = query.filter(Message.channel.in_(channels))
            
            if keywords:
                # Импортируем or_ для корректной работы условий
                from sqlalchemy import or_
                
                # Фильтрация по ключевым словам в тексте
                keyword_conditions = []
                for keyword in keywords:
                    keyword_conditions.append(Message.text.ilike(f'%{keyword}%'))
                if keyword_conditions:
                    query = query.filter(or_(*keyword_conditions))
            
            # Сортируем по дате (сначала новые)
            messages = query.order_by(Message.date.desc()).all()
            
            logger.info(f"Найдено {len(messages)} сообщений за указанный период")
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений по дате: {str(e)}", exc_info=True)
            return []
        finally:
            session.close()

    def save_digest(self, date, text, sections, digest_type="brief"):
        """
        Сохранение дайджеста с секциями
        
        Args:
            date (datetime): Дата дайджеста
            text (str): Полный текст дайджеста
            sections (dict): Словарь секций {категория: текст}
            digest_type (str): Тип дайджеста: "brief" (краткий) или "detailed" (подробный)
            
        Returns:
            dict: Информация о созданном дайджесте
        """
        session = self.Session()
        try:
            # Создаем запись дайджеста
            digest = Digest(date=date, text=text, digest_type=digest_type)
            session.add(digest)
            session.flush()  # Чтобы получить ID дайджеста
            
            # Добавляем секции
            sections_data = []
            for category, section_text in sections.items():
                section = DigestSection(
                    digest_id=digest.id,
                    category=category,
                    text=section_text
                )
                session.add(section)
                sections_data.append({
                    "category": category,
                    "text": section_text
                })
            
            # Фиксируем изменения
            session.commit()
            
            # Создаем результат для возврата
            result = {
                "id": digest.id,
                "date": date,
                "digest_type": digest_type,
                "sections": sections_data
            }
            
            logger.info(f"Сохранен дайджест типа '{digest_type}' за {date.strftime('%Y-%m-%d')}")
            return result
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при сохранении дайджеста: {str(e)}")
            raise
        finally:
            session.close()
          
    def get_recently_categorized_messages(self, limit=20):
        """
        Получение последних сообщений с категориями
        
        Args:
            limit (int): Максимальное количество сообщений
            
        Returns:
            list: Список объектов Message
        """
        session = self.Session()
        try:
            messages = session.query(Message)\
                .filter(Message.category != None)\
                .order_by(Message.id.desc())\
                .limit(limit)\
                .all()
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении недавно категоризированных сообщений: {str(e)}")
            return []
        finally:
            session.close()
    def get_message_by_id(self, message_id):
        """
        Получение сообщения по ID
        
        Args:
            message_id (int): ID сообщения
            
        Returns:
            Message: Объект сообщения или None
        """
        session = self.Session()
        try:
            message = session.query(Message).filter_by(id=message_id).first()
            return message
        except Exception as e:
            logger.error(f"Ошибка при получении сообщения по ID: {str(e)}")
            return None
        finally:
            session.close()
    def get_latest_digest(self, digest_type=None):
        """
        Получение последнего дайджеста
        
        Args:
            digest_type (str, optional): Тип дайджеста ("brief", "detailed")
            
        Returns:
            Digest: Объект последнего дайджеста
        """
        session = self.Session()
        try:
            query = session.query(Digest).order_by(Digest.date.desc())
            
            if digest_type:
                query = query.filter(Digest.digest_type == digest_type)
                
            digest = query.first()
            return digest
        except Exception as e:
            logger.error(f"Ошибка при получении последнего дайджеста: {str(e)}")
            return None
        finally:
            session.close()
    """
    Дополнительные методы для оптимизированной работы с БД
    """

    def get_recently_categorized_messages_by_category(self, category, limit=10):
        """
        Получение последних сообщений с конкретной категорией
        
        Args:
            category (str): Категория для фильтрации
            limit (int): Максимальное количество сообщений
            
        Returns:
            list: Список объектов Message
        """
        session = self.Session()
        try:
            messages = session.query(Message)\
                .filter(Message.category == category)\
                .order_by(Message.id.desc())\
                .limit(limit)\
                .all()
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений по категории: {str(e)}")
            return []
        finally:
            session.close()

    def get_recently_categorized_messages_excluding_ids(self, message_ids, limit=10):
        """
        Получение последних сообщений с категориями, исключая указанные ID
        
        Args:
            message_ids (list): Список ID для исключения
            limit (int): Максимальное количество сообщений
            
        Returns:
            list: Список объектов Message
        """
        session = self.Session()
        try:
            messages = session.query(Message)\
                .filter(Message.category != None)\
                .filter(~Message.id.in_(message_ids))\
                .order_by(Message.id.desc())\
                .limit(limit)\
                .all()
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений с исключением: {str(e)}")
            return []
        finally:
            session.close()

    @with_retry(max_attempts=3, delay=0.5)        
    def batch_save_messages(self, messages_data):
        """Пакетное сохранение сообщений"""
        session = self.Session()
        try:
            saved_count = 0
            for i, data in enumerate(messages_data): # Added i for clearer logging
                # Проверяем существование сообщения более эффективно
                existing = session.query(Message).filter_by(
                    channel=data['channel'], 
                    message_id=data['message_id']
                ).first()
                
                if not existing:
                    message = Message(
                        channel=data['channel'],
                        message_id=data['message_id'],
                        text=data['text'],
                        date=data['date']
                    )
                    session.add(message) # Add to session
                    saved_count += 1
                    logger.debug(f"Подготовлено к пакетному сохранению сообщение {data['message_id']} из канала {data['channel']}")
            
            session.commit() # Commit once after all messages are added
            logger.info(f"Пакетно сохранено {saved_count} новых сообщений")
            return saved_count
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при пакетном сохранении сообщений: {str(e)}")
            return 0
        finally:
            session.close()
            
    def batch_update_message_categories(self, updates):
        """
        Пакетное обновление категорий сообщений
        
        Args:
            updates (list): Список кортежей (message_id, category)
            
        Returns:
            int: Количество успешно обновленных сообщений
        """
        session = self.Session()
        try:
            success_count = 0
            for message_id, category in updates:
                message = session.query(Message).filter_by(id=message_id).first()
                if message:
                    message.category = category
                    success_count += 1
                
            session.commit()
            return success_count
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при пакетном обновлении категорий: {str(e)}")
            return 0
        finally:
            session.close()        

    def get_message_by_channel_and_id(self, channel, message_id):
            """
            Получение сообщения по названию канала и ID сообщения в этом канале
            
            Args:
                channel (str): Название канала
                message_id (int): ID сообщения в канале
                
            Returns:
                Message: Объект сообщения или None
            """
            session = self.Session()
            try:
                message = session.query(Message).filter_by(
                    channel=channel, 
                    message_id=message_id
                ).first()
                return message
            except Exception as e:
                logger.error(f"Ошибка при получении сообщения по каналу и ID: {str(e)}")
                return None
            finally:
                session.close()                 



    def get_digest_by_date(self, date):
            """
            Получение дайджеста по дате
            
            Args:
                date (datetime): Дата дайджеста
                
            Returns:
                Digest: Объект дайджеста
            """
            session = self.Session()
            try:
                # Ищем дайджест по дате (округляя до дня)
                start_date = datetime(date.year, date.month, date.day)
                end_date = start_date + timedelta(days=1)
                
                digest = session.query(Digest).filter(
                    Digest.date >= start_date,
                    Digest.date < end_date
                ).first()
                
                return digest
            except Exception as e:
                logger.error(f"Ошибка при получении дайджеста по дате: {str(e)}")
                return None
            finally:
                session.close() 
    def get_latest_digest_with_sections(self, digest_type=None):
        """
        Получение последнего дайджеста со всеми секциями
        
        Args:
            digest_type (str, optional): Тип дайджеста ("brief", "detailed")
            
        Returns:
            dict: Данные о дайджесте и его секциях
        """
        from sqlalchemy.orm import joinedload

        session = self.Session()
        try:
            query = session.query(Digest).options(
                joinedload(Digest.sections)
            ).order_by(Digest.date.desc())
            
            if digest_type:
                query = query.filter(Digest.digest_type == digest_type)
                
            digest = query.first()
            
            if not digest:
                return None
                
            # Создаем словарь с данными
            result = {
                "id": digest.id,
                "date": digest.date,
                "text": digest.text,
                "digest_type": digest.digest_type,
                "sections": []
            }
            
            # Добавляем данные о секциях
            for section in digest.sections:
                result["sections"].append({
                    "id": section.id,
                    "category": section.category,
                    "text": section.text
                })
                
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении последнего дайджеста: {str(e)}")
            return None
        finally:
            session.close()

    # Изменения в database/db_manager.py

    def get_digest_by_date_with_sections(self, date, generate_if_missing=True):
        """
        Получение дайджеста по дате со всеми секциями
        
        Args:
            date (datetime): Дата дайджеста
            generate_if_missing (bool): Генерировать новый дайджест, если не найден
            
        Returns:
            dict: Данные о дайджесте и его секциях
        """
        from sqlalchemy.orm import joinedload

        session = self.Session()
        try:
            # Ищем дайджест по дате (округляя до дня)
            start_date = datetime(date.year, date.month, date.day)
            end_date = start_date + timedelta(days=1)
            
            digest = session.query(Digest).options(
                joinedload(Digest.sections)
            ).filter(
                Digest.date >= start_date,
                Digest.date < end_date
            ).first()
            
            if not digest and generate_if_missing:
                # Проверяем, есть ли сообщения за указанную дату
                messages = self.get_messages_by_date_range(start_date, end_date)
                
                if not messages:
                    logger.warning(f"Нет сообщений за дату {start_date.strftime('%Y-%m-%d')}, невозможно сгенерировать дайджест")
                    return None
                
                logger.info(f"Найдено {len(messages)} сообщений за {start_date.strftime('%Y-%m-%d')}, генерируем дайджест")
                
                # Импортируем здесь, чтобы избежать циклического импорта
                from agents.digester import DigesterAgent
                
                # Создаем дайджестер и генерируем дайджест
                digester = DigesterAgent(self)
                result = digester.create_digest(date=start_date, days_back=1)
                
                if result and "brief_digest_id" in result:
                    # Получаем новый дайджест
                    digest = session.query(Digest).options(
                        joinedload(Digest.sections)
                    ).filter_by(id=result["brief_digest_id"]).first()
                    
                    logger.info(f"Сгенерирован новый дайджест (ID: {digest.id if digest else 'unknown'})")
                else:
                    logger.error("Не удалось сгенерировать дайджест")
                    return None
            
            if not digest:
                return None
                
            # Создаем словарь с данными
            result = {
                "id": digest.id,
                "date": digest.date,
                "text": digest.text,
                "digest_type": digest.digest_type,
                "sections": []
            }
            
            # Добавляем данные о секциях
            for section in digest.sections:
                result["sections"].append({
                    "id": section.id,
                    "category": section.category,
                    "text": section.text
                })
                
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении дайджеста по дате: {str(e)}")
            return None
        finally:
            session.close()

    @with_retry(max_attempts=5, delay=1.0)
    def save_digest_with_parameters(self, date, text, sections, digest_type="brief", 
                            date_range_start=None, date_range_end=None, 
                            focus_category=None, channels_filter=None, 
                            keywords_filter=None, digest_id=None,
                            is_today=False, last_updated=None):
        """
        Сохранение дайджеста с расширенными параметрами и улучшенной обработкой ошибок
        """
        session = self.Session()
        try:
            # Подготавливаем данные JSON полей
            channels_json = None
            keywords_json = None
            
            if channels_filter is not None: # Changed logic to handle dict_keys type
                # Ensure channels_filter is a list if it's a dict_keys object
                if isinstance(channels_filter, type({}.keys())): # Check for dict_keys type
                    channels_filter = list(channels_filter) # Convert to list

                # Existing logic for JSON serialization
                try:
                    # Проверяем, не является ли значение уже строкой JSON
                    if isinstance(channels_filter, str):
                        # Пробуем распарсить и снова сериализовать для проверки
                        json.loads(channels_filter)
                        channels_json = channels_filter
                    else:
                        channels_json = json.dumps(channels_filter)
                except (TypeError, json.JSONDecodeError):
                    # Если не получается распарсить как JSON, сохраняем как есть
                    channels_json = json.dumps(None)
            
            if keywords_filter is not None:
                try:
                    if isinstance(keywords_filter, str):
                        json.loads(keywords_filter)
                        keywords_json = keywords_filter
                    else:
                        keywords_json = json.dumps(keywords_filter)
                except (TypeError, json.JSONDecodeError):
                    keywords_json = json.dumps(None)
            
            # Устанавливаем время последнего обновления, если не указано
            if last_updated is None:
                last_updated = datetime.now()
                
            if digest_id:
                # Поиск существующего дайджеста с обработкой блокировок
                digest = None
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries:
                    try:
                        digest = session.query(Digest).filter_by(id=digest_id).with_for_update().first()
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count >= max_retries:
                            raise
                        logger.warning(f"Не удалось получить блокировку на дайджест ID={digest_id}, попытка {retry_count}/{max_retries}")
                        time.sleep(1)
                
                if digest:
                    # Обновляем поля дайджеста
                    digest.text = text
                    digest.date = date
                    digest.last_updated = last_updated
                    
                    # Обновляем дополнительные параметры, если они предоставлены
                    if date_range_start is not None:
                        digest.date_range_start = date_range_start
                    if date_range_end is not None:
                        digest.date_range_end = date_range_end
                    if focus_category is not None:
                        digest.focus_category = focus_category
                    if channels_json is not None:
                        digest.channels_filter = channels_json
                    if keywords_json is not None:
                        digest.keywords_filter = keywords_json
                    
                    # Добавляем признак дайджеста за текущий день
                    if hasattr(digest, 'is_today'):
                        digest.is_today = is_today
                    
                    # Удаляем существующие секции
                    session.query(DigestSection).filter_by(digest_id=digest_id).delete()
                else:
                    # Если дайджест не найден, создаем новый
                    digest = Digest(
                        date=date, 
                        text=text, 
                        digest_type=digest_type,
                        date_range_start=date_range_start,
                        date_range_end=date_range_end,
                        focus_category=focus_category,
                        channels_filter=channels_json,
                        keywords_filter=keywords_json,
                        last_updated=last_updated,
                        is_today=is_today
                    )
                    session.add(digest)
            else:
                # Создаем новый дайджест
                digest = Digest(
                    date=date, 
                    text=text, 
                    digest_type=digest_type,
                    date_range_start=date_range_start,
                    date_range_end=date_range_end,
                    focus_category=focus_category,
                    channels_filter=channels_json,
                    keywords_filter=keywords_json,
                    last_updated=last_updated,
                    is_today=is_today
                )
                session.add(digest)
            
            # Применяем изменения и получаем ID
            session.flush()
            
            # Добавляем секции (с обработкой блокировок)
            sections_data = []
            for category, section_text in sections.items():
                try:
                    section = DigestSection(
                        digest_id=digest.id,
                        category=category,
                        text=section_text
                    )
                    session.add(section)
                    sections_data.append({
                        "category": category,
                        "text": section_text
                    })
                except Exception as e:
                    logger.error(f"Ошибка при добавлении секции '{category}': {str(e)}")
            
            # Фиксируем изменения с повторными попытками при ошибках
            retry_commit = 0
            while retry_commit < 3:
                try:
                    session.commit()
                    break
                except Exception as e:
                    retry_commit += 1
                    if retry_commit >= 3:
                        raise
                    logger.warning(f"Ошибка при фиксации изменений: {str(e)}, повторная попытка {retry_commit}/3")
                    time.sleep(retry_commit)
            
            # Создаем результат для возврата
            result = {
                "id": digest.id,
                "date": date,
                "digest_type": digest_type,
                "sections": sections_data,
                "is_today": is_today,
                "last_updated": last_updated
            }
            
            logger.info(f"Сохранен дайджест типа '{digest_type}' за {date.strftime('%Y-%m-%d')}, обновлен: {digest_id is not None}")
            return result
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при сохранении дайджеста: {str(e)}")
            raise
        finally:
            session.close()
              
    def find_digests_by_parameters(self, digest_type=None, date=None, 
                           date_range_start=None, date_range_end=None,
                           focus_category=None, is_today=None, limit=5):
        """
        Поиск дайджестов по параметрам с улучшенной обработкой типов даты
        """
        from datetime import datetime, time
        
        session = self.Session()
        try:
            query = session.query(Digest)
            
            # ДИАГНОСТИКА: логируем параметры поиска
            logger.info(f"find_digests_by_parameters вызван с: digest_type={digest_type}, limit={limit}, is_today={is_today}")
        
            if digest_type:
                query = query.filter(Digest.digest_type == digest_type)
            
            # Обновите проверку даты (примерно строка 1520-1540)
            if date:
                # Корректная обработка объектов date и datetime
                try:
                    # Если это date, а не datetime, преобразуем в datetime
                    if hasattr(date, 'date') and callable(getattr(date, 'date')):
                        # Это datetime объект
                        start_date = datetime.combine(date.date(), time(0, 0, 0))
                        end_date = datetime.combine(date.date(), time(23, 59, 59))
                    else:
                        # Это date объект
                        start_date = datetime.combine(date, time(0, 0, 0))
                        end_date = datetime.combine(date, time(23, 59, 59))
                    
                    # Ищем все дайжесты, которые попадают в этот день
                    query = query.filter(
                        or_(
                            and_(Digest.date >= start_date, Digest.date <= end_date),
                            and_(Digest.date_range_start <= end_date, Digest.date_range_end >= start_date)
                        )
                    )
                    
                    logger.info(f"Поиск дайджестов за дату {date}")
                except Exception as e:
                    logger.error(f"Ошибка при обработке параметра date: {str(e)}")
            
            if date_range_start and date_range_end:
                # Поиск дайджестов, которые охватывают указанный период
                query = query.filter(
                    or_(
                        # Дайджест начинается внутри указанного периода
                        and_(
                            Digest.date_range_start >= date_range_start,
                            Digest.date_range_start <= date_range_end
                        ),
                        # Дайджест заканчивается внутри указанного периода
                        and_(
                            Digest.date_range_end >= date_range_start,
                            Digest.date_range_end <= date_range_end
                        ),
                        # Дайджест охватывает весь указанный период
                        and_(
                            Digest.date_range_start <= date_range_start,
                            Digest.date_range_end >= date_range_end
                        )
                    )
                )
            
            if focus_category:
                query = query.filter(Digest.focus_category == focus_category)
                
            # Добавляем фильтр по признаку "сегодняшнего" дайджеста
            if is_today is not None:
                if hasattr(Digest, 'is_today'):  # Проверка наличия атрибута
                    query = query.filter(Digest.is_today == is_today)
                else:
                    # Если поле не существует, используем фильтр по текущей дате
                    today = datetime.now().date()
                    start_of_today = datetime.combine(today, time(0, 0, 0))
                    end_of_today = datetime.combine(today, time(23, 59, 59))
                    query = query.filter(Digest.date >= start_of_today, Digest.date <= end_of_today)
            
            # Сортируем сначала по типу, потом по дате создания (сначала самые ранние)
            # Это поможет избежать дублей за один день
            
            #digests = query.order_by(Digest.digest_type, Digest.created_at).limit(limit).all()
            digests = query.order_by(Digest.id.desc()).all()
            
            # ДИАГНОСТИКА: логируем найденные дайджесты
            #logger.info(f"Найдено дайджестов в БД: {len(digests)}")
            #for digest in digests:
             #   logger.info(f"  БД: ID={digest.id}, тип={digest.digest_type}, дата={digest.date}, is_today={digest.is_today}")
            

            results = []
            for digest in digests:
                # Преобразуем в dict с нужными полями
                digest_data = {
                    "id": digest.id,
                    "date": digest.date,
                    "digest_type": digest.digest_type,
                    "focus_category": digest.focus_category,
                    "date_range_start": digest.date_range_start,
                    "date_range_end": digest.date_range_end,
                    "created_at": digest.created_at,
                    "last_updated": digest.last_updated
                }
                
                # Добавляем is_today, если поле существует
                if hasattr(digest, 'is_today'):
                    digest_data["is_today"] = digest.is_today
                
                results.append(digest_data)
                
                # ДИАГНОСТИКА: логируем результат (ИСПРАВЛЕНО)
                #logger.info(f"Возвращаем дайджестов: {len(results)}")
                #for r in results:
                 #   logger.info(f"  Результат: ID={r['id']}, тип={r['digest_type']}, дата={r['date']}")
                                
            
            return results
        except Exception as e:
            logger.error(f"Ошибка при поиске дайджестов: {str(e)}")
            return []
        finally:
            session.close()

    def get_digests_containing_date(self, date):
        """
        Находит все дайджесты, которые включают указанную дату
        """
        session = self.Session()
        try:
            results = []
            
            # Поиск дайджестов, у которых указан диапазон дат
            range_digests = session.query(Digest).filter(
                Digest.date_range_start != None,
                Digest.date_range_end != None,
                Digest.date_range_start <= date,
                Digest.date_range_end >= date
            ).all()
            
            # Поиск дайджестов за конкретную дату
            single_day_digests = session.query(Digest).filter(
                Digest.date == date,
                or_(
                    Digest.date_range_start == None,
                    Digest.date_range_end == None
                )
            ).all()
            
            # Объединяем результаты
            all_digests = range_digests + single_day_digests
            
            for digest in all_digests:
                results.append({
                    "id": digest.id,
                    "date": digest.date,
                    "digest_type": digest.digest_type,
                    "date_range_start": digest.date_range_start,
                    "date_range_end": digest.date_range_end,
                    "focus_category": digest.focus_category,
                    "channels_filter": json.loads(digest.channels_filter) if digest.channels_filter else None,
                    "keywords_filter": json.loads(digest.keywords_filter) if digest.keywords_filter else None
                })
            
            return results
        except Exception as e:
            logger.error(f"Ошибка при поиске дайджестов, содержащих дату: {str(e)}")
            return []
        finally:
            session.close()

    def get_digest_by_id_with_sections(self, digest_id):
        """
        Получение дайджеста по ID со всеми секциями
        """
        from sqlalchemy.orm import joinedload
        
        session = self.Session()
        try:
            digest = session.query(Digest).options(
                joinedload(Digest.sections)
            ).filter_by(id=digest_id).first()
            
            if not digest:
                return None
            
            # Создаем словарь с данными
            result = {
                "id": digest.id,
                "date": digest.date,
                "text": digest.text,
                "digest_type": digest.digest_type,
                "date_range_start": digest.date_range_start,
                "date_range_end": digest.date_range_end,
                "focus_category": digest.focus_category,
                "channels_filter": json.loads(digest.channels_filter) if digest.channels_filter else None,
                "keywords_filter": json.loads(digest.keywords_filter) if digest.keywords_filter else None,
                "created_at": digest.created_at,
                "last_updated": digest.last_updated,
                "sections": []
            }
            
            # Добавляем данные о секциях
            for section in digest.sections:
                result["sections"].append({
                    "id": section.id,
                    "category": section.category,
                    "text": section.text
                })
            
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении дайджеста по ID: {str(e)}")
            return None
        finally:
            session.close()
    def get_filtered_messages(self, start_date, end_date, category=None, 
                         channels=None, keywords=None, page=1, page_size=100):
        """
        Получение сообщений с расширенной фильтрацией и пагинацией
        """
        from sqlalchemy import or_
        
        session = self.Session()
        try:
            logger.info(f"Запрос сообщений с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")
            logger.info(f"Фильтры: категория='{category}', каналы={channels}, ключевые слова={keywords}")
            
            # Преобразуем даты в datetime.datetime если они строки или date
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            elif hasattr(start_date, 'date') and callable(getattr(start_date, 'date')):
                # Если это date, преобразуем в datetime с временем 00:00:00
                start_date = datetime.combine(start_date, datetime.min.time())
            
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            elif hasattr(end_date, 'date') and callable(getattr(end_date, 'date')):
                # Если это date, преобразуем в datetime с временем 23:59:59
                end_date = datetime.combine(end_date, datetime.max.time())
            
            query = session.query(Message).filter(
                Message.date >= start_date,
                Message.date <= end_date
            )
            
            # Логируем количество сообщений после фильтрации по дате
            date_count = query.count()
            logger.info(f"Найдено {date_count} сообщений в указанном диапазоне дат")
            
            if category:
                query = query.filter(Message.category == category)
                logger.info(f"После фильтрации по категории '{category}': {query.count()} сообщений")
            
            if channels:
                query = query.filter(Message.channel.in_(channels))
                logger.info(f"После фильтрации по каналам: {query.count()} сообщений")
            
            if keywords:
                # Создаем условия поиска для каждого ключевого слова
                keyword_conditions = []
                for keyword in keywords:
                    keyword_conditions.append(Message.text.ilike(f'%{keyword}%'))
                
                # Объединяем условия через OR
                query = query.filter(or_(*keyword_conditions))
                logger.info(f"После фильтрации по ключевым словам: {query.count()} сообщений")
        
            # Получаем общее количество записей для пагинации
            total = query.count()
            logger.info(f"Всего сообщений после всех фильтров: {total}")
            
            # Применяем пагинацию
            offset = (page - 1) * page_size
            messages = query.order_by(Message.date.desc()).offset(offset).limit(page_size).all()
            
            logger.info(f"Получено {len(messages)} сообщений после пагинации")
            
            # Формируем результат
            return {
                "messages": messages,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": (total + page_size - 1) // page_size
            }
        except Exception as e:
            logger.error(f"Ошибка при получении отфильтрованных сообщений: {str(e)}", exc_info=True)
            return {"messages": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0}
        finally:
            session.close()
    def get_latest_messages(self, limit=100):
        """
        Получение последних сообщений без сложной фильтрации
        """
        session = self.Session()
        try:
            messages = session.query(Message).order_by(Message.date.desc()).limit(limit).all()
            logger.info(f"Получено {len(messages)} последних сообщений")
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении последних сообщений: {str(e)}")
            return []
        finally:
            session.close()                                       
    # В database/db_manager.py:

    def save_digest_generation(self, source, user_id=None, channels=None, messages_count=0, digest_ids=None, start_date=None, end_date=None, focus_category=None):
        """Сохраняет информацию о генерации дайджеста"""
        session = self.Session()
        try:
            # Проверяем, что digest_ids - это словарь, а не dict_keys
            if digest_ids is not None and not isinstance(digest_ids, dict):
                digest_ids = dict(digest_ids)  # Преобразуем в dict, если это не словарь
                
            generation = DigestGeneration(
                timestamp=datetime.now(),
                source=source,
                user_id=user_id,
                channels=json.dumps(channels) if channels else None,
                messages_count=messages_count,
                digest_ids=json.dumps(digest_ids) if digest_ids else None,
                start_date=start_date,
                end_date=end_date,
                focus_category=focus_category
            )
            session.add(generation)
            session.commit()
            logger.info(f"Создана запись о генерации дайджеста ID {generation.id}, источник: {source}")
            return generation.id
        except Exception as e:
            session.rollback()
            logger.exception(f"Ошибка при сохранении информации о генерации дайджеста: {str(e)}")
            return None
        finally:
            session.close()

    def get_last_digest_generation(self, source=None, user_id=None):
        """Получает информацию о последней генерации дайджеста"""
        session = self.Session()
        try:
            query = session.query(DigestGeneration).order_by(DigestGeneration.timestamp.desc())
            
            if source:
                query = query.filter(DigestGeneration.source == source)
            
            if user_id:
                query = query.filter(DigestGeneration.user_id == user_id)
                
            generation = query.first()
            return {
                "id": generation.id,
                "timestamp": generation.timestamp,
                "source": generation.source,
                "user_id": generation.user_id,
                "channels": json.loads(generation.channels) if generation.channels else None,
                "messages_count": generation.messages_count,
                "digest_ids": json.loads(generation.digest_ids) if generation.digest_ids else None
            } if generation else None
        except Exception as e:
            logger.error(f"Ошибка при получении информации о последней генерации дайджеста: {str(e)}")
            return None
        finally:
            session.close()      
    # В начале файла database/db_manager.py добавьте импорт:


    def update_today_flags(self):
        """Обновляет флаги is_today в соответствии с текущей датой"""
        max_attempts = 5
        delay = 1.0
        
        for attempt in range(max_attempts):
            session = self.Session()
            try:
                today = datetime.now().date()
                
                # Находим все дайджесты с is_today=True
                outdated_digests = session.query(Digest).filter(
                    Digest.is_today == True
                ).all()
                
                updated_count = 0
                wrong_flags_count = 0
                
                for digest in outdated_digests:
                    digest_date = digest.date.date()
                    should_be_today = (digest_date == today)
                    
                    if digest.is_today != should_be_today:
                        digest.is_today = should_be_today
                        wrong_flags_count += 1
                        
                        # Логируем изменения
                        action = "установлен" if should_be_today else "снят"
                        logger.info(f"Для дайджеста ID={digest.id} ({digest_date}) {action} флаг is_today")
                        updated_count += 1
                
                # Также проверяем дайджесты за сегодня, у которых флаг может быть не установлен
                todays_digests = []
                all_digests = session.query(Digest).filter(Digest.is_today == False).all()
                
                for digest in all_digests:
                    digest_date = digest.date.date()
                    if digest_date == today:
                        todays_digests.append(digest)
                
                for digest in todays_digests:
                    digest.is_today = True
                    logger.info(f"Для дайджеста ID={digest.id} ({digest.date.date()}) установлен флаг is_today")
                    updated_count += 1
                
                # Фиксируем изменения
                session.commit()
                
                logger.info(f"Проверены флаги is_today для всех дайджестов. "
                        f"Сегодня: {today}, обновлено: {updated_count}, исправлено неправильных: {wrong_flags_count}")
                return {"updated": updated_count, "wrong_flags": wrong_flags_count}
                
            except Exception as e:
                session.rollback()
                logger.error(f"Ошибка при обновлении флагов is_today (попытка {attempt + 1}/{max_attempts}): {str(e)}")
                
                if attempt < max_attempts - 1:
                    # Если не последняя попытка, ждем и пробуем снова
                    import time
                    time.sleep(delay * (attempt + 1))  # Увеличиваем задержку с каждой попыткой
                    continue
                else:
                    # Если последняя попытка - возвращаем ошибку
                    return {"error": str(e)}
            finally:
                session.close()

# В файле database/db_manager.py добавим новый метод для поиска дайджестов за сегодня

def find_todays_digests(self, digest_type=None):
    """
    Находит все дайджесты за сегодня
    
    Args:
        digest_type (str, optional): Тип дайджеста для фильтрации
        
    Returns:
        list: Список дайджестов
    """
    session = self.Session()
    try:
        today = datetime.now().date()
        start_of_today = datetime.combine(today, time(0, 0, 0))
        end_of_today = datetime.combine(today, time(23, 59, 59))
        
        # Создаем базовый запрос
        query = session.query(Digest).filter(
            Digest.date >= start_of_today,
            Digest.date <= end_of_today
        )
        
        # Проверяем, есть ли поле is_today
        if hasattr(Digest, 'is_today'):
            # Если поле есть, сначала пробуем найти по флагу
            flagged_query = session.query(Digest).filter(Digest.is_today == True)
            if digest_type:
                flagged_query = flagged_query.filter(Digest.digest_type == digest_type)
            
            flagged_digests = flagged_query.all()
            if flagged_digests:
                # Если нашли дайджесты с флагом is_today, возвращаем их
                result = []
                for digest in flagged_digests:
                    result.append({
                        "id": digest.id,
                        "date": digest.date,
                        "digest_type": digest.digest_type,
                        "date_range_start": digest.date_range_start,
                        "date_range_end": digest.date_range_end,
                        "focus_category": digest.focus_category,
                        "is_today": True
                    })
                return result
        
        # Если не нашли по флагу или поля нет, фильтруем по дате
        if digest_type:
            query = query.filter(Digest.digest_type == digest_type)
        
        # Получаем результаты
        digests = query.order_by(Digest.id).all()
        
        # Преобразуем в список словарей
        result = []
        for digest in digests:
            result.append({
                "id": digest.id,
                "date": digest.date,
                "digest_type": digest.digest_type,
                "date_range_start": digest.date_range_start,
                "date_range_end": digest.date_range_end,
                "focus_category": digest.focus_category,
                "is_today": hasattr(digest, 'is_today') and digest.is_today
            })
        
        return result
    except Exception as e:
        logger.error(f"Ошибка при поиске сегодняшних дайджестов: {str(e)}")
        return []
    finally:
        session.close()
