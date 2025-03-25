"""
Управление базой данных
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime, timedelta

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

    def update_message_category(self, message_id, category):
        """
        Обновление категории сообщения
        
        Args:
            message_id (int): ID сообщения в нашей БД
            category (str): Категория сообщения
            
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
            session.commit()
            logger.debug(f"Обновлена категория сообщения {message_id}: {category}")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при обновлении категории сообщения: {str(e)}")
            return False
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

    def save_digest(self, date, text, sections):
        """
        Сохранение дайджеста с секциями
        
        Args:
            date (datetime): Дата дайджеста
            text (str): Полный текст дайджеста
            sections (dict): Словарь секций {категория: текст}
            
        Returns:
            Digest: Созданный дайджест
        """
        session = self.Session()
        try:
            # Создаем запись дайджеста
            digest = Digest(date=date, text=text)
            session.add(digest)
            session.flush()  # Чтобы получить ID дайджеста
            
            # Добавляем секции
            for category, section_text in sections.items():
                section = DigestSection(
                    digest_id=digest.id,
                    category=category,
                    text=section_text
                )
                session.add(section)
            
            session.commit()
            logger.info(f"Сохранен дайджест за {date.strftime('%Y-%m-%d')}")
            return digest
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка при сохранении дайджеста: {str(e)}")
            raise
        finally:
            session.close()

    def get_latest_digest(self):
        """
        Получение последнего дайджеста
        
        Returns:
            Digest: Объект последнего дайджеста
        """
        session = self.Session()
        try:
            digest = session.query(Digest).order_by(Digest.date.desc()).first()
            return digest
        except Exception as e:
            logger.error(f"Ошибка при получении последнего дайджеста: {str(e)}")
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