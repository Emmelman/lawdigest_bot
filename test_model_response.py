# test_model_response.py
import requests
import json

# URL API
LLM_STUDIO_URL = "http://127.0.0.1:1234"
CHAT_COMPLETIONS_URL = f"{LLM_STUDIO_URL}/v1/chat/completions"

def test_model_response():
    """Тест отправки запроса к модели"""
    model_name = "gemma-3-12b-it"  # Используем одну из доступных моделей
    
    payload = {
        "model": model_name,
        "messages": [
            {"role": "user", "content": "Привет! Как дела?"}
        ],
        "max_tokens": 100,
        "temperature": 0.7
    }
    
    try:
        print(f"Отправляем запрос к модели {model_name}")
        response = requests.post(CHAT_COMPLETIONS_URL, json=payload)
        
        # Проверяем статус-код
        print(f"Статус-код ответа: {response.status_code}")
        
        # Выводим ответ сервера
        if response.status_code == 200:
            result = response.json()
            print("\nОтвет модели:")
            generated_text = result['choices'][0]['message']['content']
            print(generated_text)
            return True
        else:
            print(f"Ошибка: {response.text}")
            return False
        
    except Exception as e:
        print(f"Ошибка запроса: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_model_response()
    if success:
        print("\nЗапрос к модели успешно обработан!")
    else:
        print("\nНе удалось получить ответ от модели.")