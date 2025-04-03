# test_init.py
import os
from dotenv import load_dotenv
from database.db_manager import DatabaseManager

# Загрузка переменных окружения
load_dotenv()

def test_database_init():
    """Тестирование инициализации базы данных"""
    from sqlalchemy import create_engine
    from database.models import Base
    
    # Настройки
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///lawdigest.db")
    
    # Создаем соединение
    engine = create_engine(DATABASE_URL)
    
    # Создаем таблицы
    Base.metadata.create_all(engine)
    
    print(f"База данных инициализирована по адресу: {DATABASE_URL}")
    
    return True

if __name__ == "__main__":
    success = test_database_init()
    if success:
        print("Инициализация успешно завершена!")
    else:
        print("Ошибка при инициализации.")