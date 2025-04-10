"""
Модели данных для SQLite
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine, ForeignKey, Boolean, Index, UniqueConstraint

Base = declarative_base()

# В database/models.py
class Message(Base):
    """Модель для хранения сообщений из Telegram-каналов"""
    __tablename__ = "messages"
    
    id = Column(Integer, primary_key=True)
    channel = Column(String(100), nullable=False)
    message_id = Column(Integer, nullable=False)
    text = Column(Text, nullable=False)
    date = Column(DateTime, nullable=False)
    category = Column(String(100))
    confidence = Column(Integer, default=0)  # Уровень уверенности 1-5
    
    # Уникальный индекс чтобы избежать дублирования сообщений
    __table_args__ = (
        UniqueConstraint('channel', 'message_id', name='uix_message_channel_id'),
    )
    
    def __repr__(self):
        return f"<Message(id={self.id}, channel='{self.channel}', message_id={self.message_id})>"
    
class Digest(Base):
    """Модель для хранения сформированных дайджестов"""
    __tablename__ = "digests"
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    text = Column(Text, nullable=False)
    digest_type = Column(String(20), nullable=False, default="brief")  # Тип дайджеста (brief/detailed)
    
    # Дополнительные поля для расширенных функций
    date_range_start = Column(DateTime, nullable=True)  # Начало периода дайджеста
    date_range_end = Column(DateTime, nullable=True)    # Конец периода дайджеста
    focus_category = Column(String(100), nullable=True) # Фокус на категорию
    channels_filter = Column(Text, nullable=True)       # JSON строка с каналами
    keywords_filter = Column(Text, nullable=True)       # JSON строка с ключевыми словами
    is_auto_generated = Column(Boolean, default=False)  # Признак автоматической генерации
    last_updated = Column(DateTime, nullable=True)      # Время последнего обновления
    created_at = Column(DateTime, default=datetime.now) # Время создания
    is_today = Column(Boolean, default=False)           # Признак дайджеста за текущий день
    
    # Индексы для быстрого поиска
    __table_args__ = (
        Index('idx_digest_date', date),
        Index('idx_digest_type', digest_type),
        Index('idx_digest_date_range', date_range_start, date_range_end),
        Index('idx_digest_focus', focus_category),
        Index('idx_digest_creation', created_at),
        Index('idx_digest_is_today', is_today),
    )
    def __repr__(self):
        return f"<Digest(id={self.id}, date='{self.date}', type='{self.digest_type}')>"


class DigestSection(Base):
    """Модель для хранения секций дайджеста по категориям"""
    __tablename__ = "digest_sections"
    
    id = Column(Integer, primary_key=True)
    digest_id = Column(Integer, ForeignKey("digests.id"), nullable=False)
    category = Column(String(100), nullable=False)
    text = Column(Text, nullable=False)
    
    # Отношение к таблице дайджестов
    digest = relationship("Digest", backref="sections")
    
    def __repr__(self):
        return f"<DigestSection(id={self.id}, category='{self.category}')>"
# В database/models.py добавим:

# В database/models.py добавим:

class DigestGeneration(Base):
    """Модель для хранения информации о генерации дайджестов"""
    __tablename__ = "digest_generations"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)  # Время генерации
    start_date = Column(DateTime, nullable=True)  # Начало периода
    end_date = Column(DateTime, nullable=True)  # Конец периода
    source = Column(String(50), nullable=False)  # Источник запуска: 'bot', 'scheduler', 'manual'
    user_id = Column(Integer, nullable=True)  # ID пользователя (если запущено из бота)
    channels = Column(Text, nullable=True)  # JSON строка с каналами
    focus_category = Column(String(100), nullable=True)  # Фокус на категорию
    messages_count = Column(Integer, default=0)  # Количество собранных сообщений
    digest_ids = Column(Text, nullable=True)  # JSON строка с ID сгенерированных дайджестов

def init_db(engine_url):
    """Инициализация базы данных"""
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)
    return engine