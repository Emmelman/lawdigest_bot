"""
Интерфейс для работы с моделью Qwen2.5 через LLM Studio
"""
import logging
import requests

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
            response = self._generate_response(prompt, max_tokens=50)
            
            # Определяем наиболее подходящую категорию из списка
            result = response.strip()
            for category in categories:
                if category.lower() in result.lower():
                    return category
            
            # Если не нашли точного совпадения
            return categories[-1]  # Возвращаем "другое" или последнюю категорию
            
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