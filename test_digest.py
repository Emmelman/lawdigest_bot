# test_digest.py
import os
from dotenv import load_dotenv
from database.db_manager import DatabaseManager
from agents.digester import DigesterAgent
from config.settings import DATABASE_URL

# Загрузка переменных окружения
load_dotenv()

def test_digest():
    """Тестирование создания дайджеста"""
    # Инициализация менеджера БД
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Создаем агента
    digester = DigesterAgent(db_manager)
    
    # Запускаем создание дайджеста
    print("Запуск создания дайджеста...")
    result = digester.create_digest()
    
    print("Результаты создания дайджеста:")
    print(result["status"])
    
    if "digest_text" in result:
        print("\nТекст дайджеста:")
        print(result["digest_text"])

if __name__ == "__main__":
    test_digest()