# test_collector.py
import os
from dotenv import load_dotenv
from database.db_manager import DatabaseManager
from agents.data_collector import DataCollectorAgent
from config.settings import DATABASE_URL

# Загрузка переменных окружения
load_dotenv()

def test_collector():
    """Тестирование сбора данных из каналов"""
    # Инициализация менеджера БД
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Создаем агента
    collector = DataCollectorAgent(db_manager)
    
    # Запускаем сбор данных
    print("Запуск сбора данных из каналов...")
    result = collector.collect_data()
    
    print("Результаты сбора данных:")
    print(result)

if __name__ == "__main__":
    test_collector()