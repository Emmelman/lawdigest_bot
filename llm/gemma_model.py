"""
Интерфейс для работы с моделью Gemma 3 через LLM Studio
"""
import logging
import requests
import hashlib
import os
import time
from .base_llm import BaseLLM # Added import for BaseLLM

logger = logging.getLogger(__name__)

class GemmaLLM(BaseLLM):
    """Класс для работы с моделью Gemma 3"""
    
    def __init__(self, model_name="gemma-3-12b-it", api_url="http://127.0.0.1:1234"):
        """
        Инициализация модели
        
        Args:
            model_name (str): Название модели
            api_url (str): Базовый URL для API запросов
        """ 
        super().__init__(model_name, api_url) # Call the base class constructor

    def generate(self, prompt, max_tokens=1500, temperature=0.7):
        """
        Генерация текста на основе запроса
        
        Args:
            prompt (str): Запрос к модели
            max_tokens (int): Максимальное количество токенов в ответе
            temperature (float): Температура генерации (0.0-1.0)
            
        Returns:
            str: Сгенерированный текст
        """
        # Use the caching logic from the base class
        cached_response, is_cached = self._get_cached_response(prompt, max_tokens, temperature)
        if is_cached:
            return cached_response

        # If no cache, generate response
        response = self._generate_response(prompt, max_tokens, temperature)
        
        # Save to cache
        with open(os.path.join(self.cache_dir, f"{hashlib.md5((prompt + f"_tokens{max_tokens}_temp{temperature}").encode()).hexdigest()}.txt"), 'w', encoding='utf-8') as f:
            f.write(response)
        
        return response.strip()
    
    def summarize(self, text, max_tokens=500):
        """
        Резюмирование текста
        
        Args:
            text (str): Текст для резюмирования
            max_tokens (int): Максимальное количество токенов в ответе
            
        Returns:
            str: Резюме текста
        """
        prompt = f"""
        Создай краткое резюме следующего текста:
        
        {text}
        
        Резюме должно быть информативным, структурированным и не превышать 3-4 абзаца.
        """
        
        try:
            response = self._generate_response(prompt, max_tokens, temperature=0.5) # Call _generate_response from base
            return response.strip()
        except Exception as e:
            logger.error(f"Ошибка при резюмировании текста: {str(e)}")
            return "Не удалось создать резюме из-за технической ошибки."
