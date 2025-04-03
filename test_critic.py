# test_critic.py
import os
from dotenv import load_dotenv

from database.db_manager import DatabaseManager
from agents.critic import CriticAgent

# Загрузка переменных окружения
load_dotenv()

def test_critic():
    """Тестирование агента-критика"""
    # Настройки
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///lawdigest.db")
    
    # Инициализация менеджера БД
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Создаем агента-критика
    critic = CriticAgent(db_manager)
    
    # Запускаем проверку категоризации
    print("Запуск проверки категоризации последних сообщений...")
    results = critic.review_recent_categorizations(limit=5)  # Проверяем только 5 последних сообщений
    
    # Выводим результаты
    print(f"\nПроверка завершена. Всего: {results['total']}, обновлено: {results['updated']}, без изменений: {results['unchanged']}")
    
    # Выводим детали для каждого сообщения
    print("\nДетали проверки:")
    for i, result in enumerate(results['details']):
        print(f"\n--- Сообщение {i+1} ---")
        if result['status'] == 'updated':
            print(f"Статус: категория изменена")
            print(f"Предыдущая категория: {result['old_category']}")
            print(f"Новая категория: {result['new_category']}")
            print(f"Обоснование: {result['justification']}")
        elif result['status'] == 'unchanged':
            print(f"Статус: категория не изменена")
            print(f"Категория: {result['category']}")
            print(f"Обоснование: {result['justification']}")
        else:
            print(f"Статус: ошибка")
            print(f"Сообщение: {result.get('message', 'Неизвестная ошибка')}")

if __name__ == "__main__":
    test_critic()