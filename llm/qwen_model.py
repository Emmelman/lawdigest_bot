"""
Интерфейс для работы с моделью Qwen2.5 через LLM Studio
"""
import logging
import requests
import hashlib
import os
import time

logger = logging.getLogger(__name__)

class QwenLLM:
    """Класс для работы с моделью Qwen2.5"""
    
    def __init__(self, model_name="qwen2.5-14b-instruct-1m", api_url="http://127.0.0.1:1234"):
        """
        Инициализация модели
        
        Args:
            model_name (str): Название модели
            api_url (str): Базовый URL для API запросов
        """
        self.model_name = model_name
        self.api_url = f"{api_url}/v1/chat/completions"
        
    def classify(self, text, categories):
        """
        Классификация текста по категориям
        
        Args:
            text (str): Текст для классификации
            categories (list): Список возможных категорий
            
        Returns:
            str: Определенная категория
        """
        categories_str = ", ".join(categories)
        prompt = f"""
        Твоя задача - классифицировать следующий текст по одной из категорий: {categories_str}.
        Верни ТОЛЬКО название категории без каких-либо дополнительных пояснений или текста.
        
        Текст для классификации:
        {text}
        
        Категория:
        """
        
        try:
            # Создаем ключ кэша для этого запроса
            cache_key = hashlib.md5((prompt + f"_classify_tokens{50}").encode()).hexdigest()
            cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'llm_cache')
            os.makedirs(cache_dir, exist_ok=True)
            cache_file = os.path.join(cache_dir, f"{cache_key}.txt")
            
            # Проверяем кэш
            if os.path.exists(cache_file):
                file_age = time.time() - os.path.getmtime(cache_file)
                if file_age < 86400:  # 24 часа
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        result = f.read().strip()
                        # Далее обработка результата как обычно...
                        for category in categories:
                            if category.lower() in result.lower():
                                return category
                        return categories[-1]
            
            # Если не нашли в кэше, делаем запрос
            response = self._generate_response(prompt, max_tokens=50)
            result = response.strip()
            
            # Сохраняем в кэш
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(result)
            
            # Обрабатываем и возвращаем результат
            for category in categories:
                if category.lower() in result.lower():
                    return category
            return categories[-1]
        
        except Exception as e:
            logger.error(f"Ошибка при классификации текста: {str(e)}")
            return categories[-1]
    
    def _generate_response(self, prompt, max_tokens=1000, temperature=0.7):
        """
        Отправка запроса к API и получение ответа
        
        Args:
            prompt (str): Запрос к модели
            max_tokens (int): Максимальное количество токенов в ответе
            temperature (float): Температура генерации (0.0-1.0)
            
        Returns:
            str: Ответ модели
        """
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        try:
            response = requests.post(self.api_url, json=payload)
            response.raise_for_status()
            
            data = response.json()
            return data['choices'][0]['message']['content']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка API запроса: {str(e)}")
            raise