# test_llm_connection.py
import requests
import json
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Явно задаем URL
LLM_STUDIO_URL = "http://127.0.0.1:1234"

def test_connection():
    """Тест базового подключения к LLM Studio"""
    try:
        print(f"Пытаемся подключиться к: {LLM_STUDIO_URL}")
        
        # Проверим доступность списка моделей
        models_url = f"{LLM_STUDIO_URL}/v1/models"
        print(f"URL запроса: {models_url}")
        
        response = requests.get(models_url)
        response.raise_for_status()
        
        models = response.json()
        print("Доступные модели:")
        print(json.dumps(models, indent=2))
        
        return True
    except Exception as e:
        print(f"Ошибка подключения к LLM Studio: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_connection()
    if success:
        print("\nПодключение к LLM Studio успешно!")
    else:
        print("\nНе удалось подключиться к LLM Studio.")