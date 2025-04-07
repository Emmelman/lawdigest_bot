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

class Digest(Base):
    """Модель для хранения сформированных дайджестов"""
    __tablename__ = "digests"
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    text = Column(Text, nullable=False)
    digest_type = Column(String(20), nullable=False, default="brief")  # Тип дайджеста (brief/detailed)
    
    # Новые поля для расширенных функций
    date_range_start = Column(DateTime, nullable=True)  # Начало периода дайджеста
    date_range_end = Column(DateTime, nullable=True)    # Конец периода дайджеста
    focus_category = Column(String(100), nullable=True) # Фокус на категорию
    channels_filter = Column(Text, nullable=True)       # JSON строка с каналами
    keywords_filter = Column(Text, nullable=True)       # JSON строка с ключевыми словами
    is_auto_generated = Column(Boolean, default=False)  # Признак автоматической генерации
    last_updated = Column(DateTime, nullable=True)      # Время последнего обновления
    created_at = Column(DateTime, default=datetime.now) # Время создания
    
    # Индексы для быстрого поиска
    __table_args__ = (
        Index('idx_digest_date', date),
        Index('idx_digest_type', digest_type),
        Index('idx_digest_date_range', date_range_start, date_range_end),
        Index('idx_digest_focus', focus_category),
        Index('idx_digest_creation', created_at),
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
    created_at = Column(DateTime, default=datetime.now)
    
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


def init_db(engine_url):
    """Инициализация базы данных"""
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)
    return engine