# delete_digest.py
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database.models import Digest, DigestSection
from config.settings import DATABASE_URL

# Загрузка переменных окружения
load_dotenv()

def delete_digest_for_date(date):
    """Удаление дайджеста за указанную дату"""
    print(f"Удаление дайджеста за {date.strftime('%d.%m.%Y')}...")
    
    # Вычисляем границы дня
    start_date = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    end_date = datetime(date.year, date.month, date.day + 1, tzinfo=timezone.utc)
    
    # Создаем соединение с БД
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Находим дайджест за указанную дату
        digest = session.query(Digest).filter(
            Digest.date >= start_date,
            Digest.date < end_date
        ).first()
        
        if digest:
            # Сначала удаляем все связанные секции
            sections = session.query(DigestSection).filter(
                DigestSection.digest_id == digest.id
            ).all()
            
            print(f"Найдено {len(sections)} секций для удаления")
            
            for section in sections:
                session.delete(section)
                
            # Затем удаляем сам дайджест
            session.delete(digest)
            session.commit()
            print(f"Дайджест за {date.strftime('%d.%m.%Y')} успешно удален (ID: {digest.id})")
        else:
            print(f"Дайджест за {date.strftime('%d.%m.%Y')} не найден")
    except Exception as e:
        session.rollback()
        print(f"Ошибка при удалении дайджеста: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    # Удаление дайджеста за 25.03.2025
    target_date = datetime(2025, 3, 25)
    delete_digest_for_date(target_date)