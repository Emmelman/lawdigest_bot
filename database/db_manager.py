"""
Управление базой данных
"""
import logging
import functools
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime, timedelta
import json
from sqlalchemy import or_, and_
from .models import Base, Message, Digest, DigestSection, init_db

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Менеджер для работы с базой данных"""
    
    def __init__(self, db_url):
        """
        Инициализация менеджера БД
        
        Args:
            db_url (str): URL для подключения к БД (например, sqlite:///lawdigest.db)
        """
        self.engine = init_db(db_url)
        self.session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(self.session_factory)
    
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
                return existing_message
            
            # Создаем новое сообщение
            message = Message(
                channel=channel,
                message_id=message_id,
                text=text,
                date=date
            )
            session.add(message)
            session.commit()
            logger.debug(f"Сохранено сообщение {message_id} из канала {channel}")
            return message
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
    def get_messages_with_low_confidence(self, confidence_threshold=3, limit=50):
        """
        Получение сообщений с уровнем уверенности ниже указанного порога
        
        Args:
            confidence_threshold (int): Пороговое значение уверенности (сообщения с уверенностью <= этого значения)
            limit (int): Максимальное количество сообщений
            
        Returns:
            list: Список объектов Message
        """
        session = self.Session()
        try:
            messages = session.query(Message)\
                .filter(Message.category != None)\
                .filter(Message.confidence <= confidence_threshold)\
                .order_by(Message.confidence, Message.id.desc())\
                .limit(limit)\
                .all()
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений с низкой уверенностью: {str(e)}")
            return []
        finally:
            session.close()

    def get_messages_by_date_range(self, start_date, end_date, category=None):
        """
        Получение сообщений за указанный период с фильтрацией по категории
        
        Args:
            start_date (datetime): Начальная дата
            end_date (datetime): Конечная дата
            category (str, optional): Категория для фильтрации
            
        Returns:
            list: Список объектов Message
        """
        session = self.Session()
        try:
            query = session.query(Message).filter(
                Message.date >= start_date,
                Message.date <= end_date
            )
            
            if category:
                query = query.filter(Message.category == category)
            
            return query.all()
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений по дате: {str(e)}")
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

    def get_digest_by_date_with_sections(self, date):
        """
        Получение дайджеста по дате со всеми секциями
        
        Args:
            date (datetime): Дата дайджеста
            
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
    def save_digest_with_parameters(self, date, text, sections, digest_type="brief", 
                              date_range_start=None, date_range_end=None, 
                              focus_category=None, channels_filter=None, 
                              keywords_filter=None, digest_id=None):
        """
        Сохранение дайджеста с расширенными параметрами
        """
        session = self.Session()
        try:
            if digest_id:
                # Обновляем существующий дайджест
                digest = session.query(Digest).filter_by(id=digest_id).first()
                if digest:
                    digest.text = text
                    digest.date = date
                    digest.last_updated = datetime.now()
                    # Обновляем параметры, если они предоставлены
                    if date_range_start is not None:
                        digest.date_range_start = date_range_start
                    if date_range_end is not None:
                        digest.date_range_end = date_range_end
                    if focus_category is not None:
                        digest.focus_category = focus_category
                    if channels_filter is not None:
                        digest.channels_filter = json.dumps(channels_filter) if channels_filter else None
                    if keywords_filter is not None:
                        digest.keywords_filter = json.dumps(keywords_filter) if keywords_filter else None
                    
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
                        channels_filter=json.dumps(channels_filter) if channels_filter else None,
                        keywords_filter=json.dumps(keywords_filter) if keywords_filter else None,
                        last_updated=datetime.now()
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
                    channels_filter=json.dumps(channels_filter) if channels_filter else None,
                    keywords_filter=json.dumps(keywords_filter) if keywords_filter else None,
                    last_updated=datetime.now()
                )
                session.add(digest)
            
            session.flush()  # Получаем ID дайджеста
            
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

    def find_digests_by_parameters(self, digest_type=None, date=None, 
                                date_range_start=None, date_range_end=None,
                                focus_category=None, limit=5):
            """
            Поиск дайджестов по параметрам
            """
            session = self.Session()
            try:
                query = session.query(Digest)
                
                if digest_type:
                    query = query.filter(Digest.digest_type == digest_type)
                
                if date:
                    # Поиск дайджестов на конкретную дату
                    start_date = datetime(date.year, date.month, date.day)
                    end_date = start_date + timedelta(days=1)
                    query = query.filter(Digest.date >= start_date, Digest.date < end_date)
                
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
                
                # Сортируем по дате создания (сначала новые)
                digests = query.order_by(Digest.created_at.desc()).limit(limit).all()
                
                results = []
                for digest in digests:
                    results.append({
                        "id": digest.id,
                        "date": digest.date,
                        "digest_type": digest.digest_type,
                        "focus_category": digest.focus_category,
                        "date_range_start": digest.date_range_start,
                        "date_range_end": digest.date_range_end,
                        "created_at": digest.created_at
                    })
                
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
        
        Args:
            start_date (datetime): Начальная дата
            end_date (datetime): Конечная дата
            category (str, optional): Категория для фильтрации
            channels (list, optional): Список каналов для фильтрации
            keywords (list, optional): Ключевые слова для фильтрации
            page (int): Номер страницы
            page_size (int): Размер страницы
            
        Returns:
            dict: Сообщения и информация о пагинации
        """
        from sqlalchemy import or_
        
        session = self.Session()
        try:
            query = session.query(Message).filter(
                Message.date >= start_date,
                Message.date <= end_date
            )
            
            if category:
                query = query.filter(Message.category == category)
            
            if channels:
                query = query.filter(Message.channel.in_(channels))
            
            if keywords:
            # Создаем условия поиска для каждого ключевого слова
                keyword_conditions = []
                for keyword in keywords:
                    keyword_conditions.append(Message.text.ilike(f'%{keyword}%'))
                
                # Объединяем условия через OR
                query = query.filter(or_(*keyword_conditions))
        
            # Получаем общее количество записей для пагинации
            total = query.count()
            
            # Применяем пагинацию
            offset = (page - 1) * page_size
            messages = query.order_by(Message.date.desc()).offset(offset).limit(page_size).all()
            
            # Формируем результат
            return {
                "messages": messages,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": (total + page_size - 1) // page_size
            }
        except Exception as e:
            logger.error(f"Ошибка при получении отфильтрованных сообщений: {str(e)}")
            return {"messages": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 0}
        finally:
            session.close()                               
                       