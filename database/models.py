"""
Модели данных для SQLite
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Message(Base):
    """Модель для хранения сообщений из Telegram-каналов"""
    __tablename__ = "messages"
    
    id = Column(Integer, primary_key=True)
    channel = Column(String(50), nullable=False)
    message_id = Column(Integer, nullable=False)
    text = Column(Text, nullable=True)
    date = Column(DateTime, nullable=False)
    category = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<Message(id={self.id}, channel='{self.channel}', message_id={self.message_id})>"


class Digest(Base):
    """Модель для хранения сформированных дайджестов"""
    __tablename__ = "digests"
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<Digest(id={self.id}, date='{self.date}')>"


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