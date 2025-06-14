"""
Интерфейс для работы с моделью Qwen2.5 через LLM Studio
"""
import logging
from .base_llm import BaseLLM # Import the new base class
import os # Added import for os
import hashlib # Added import for hashlib

logger = logging.getLogger(__name__) 

class QwenLLM(BaseLLM):
    """Класс для работы с моделью Qwen2.5"""
    
    def __init__(self, model_name="qwen2.5-14b-instruct-1m", api_url="http://127.0.0.1:1234"):
        """
        Инициализация модели
        
        Args:
            model_name (str): Название модели
            api_url (str): Базовый URL для API запросов
        """ 
        super().__init__(model_name, api_url) # Call the base class constructor
        
    def classify(self, text, categories):
        """
        Классификация текста по категориям
        
        Args:
            text (str): Текст для классификации
            categories (list): Список возможных категорий
            
        Returns:
            str: Наиболее подходящая категория
        """
        prompt = f"""
        Классифицируй следующий текст по одной из категорий: {', '.join(categories)}.
        Текст: {text}
        Категория:
        """
        
        try: # Now using base class caching and retry logic
            # Use the caching logic from the base class, ensure prompt includes categories for caching key
            cached_response, is_cached = self._get_cached_response(prompt, max_tokens=50, temperature=0.0) # Temperature is 0 for classification
            if is_cached:
                # Process cached response as before
                return self._process_classification_response(cached_response, categories)
            
            # If not found in cache, make the request
            response = self._generate_response(prompt, max_tokens=50)
            result = response.strip()
            
            # Save to cache
            with open(os.path.join(self.cache_dir, f"{hashlib.md5((prompt + f"_tokens{50}_temp{0.0}").encode()).hexdigest()}.txt"), 'w', encoding='utf-8') as f:
                f.write(result)
            
            # Process the response and save to cache.
            return self._process_classification_response(result, categories)
        
        except Exception as e: # Catch exceptions from _generate_response or file ops
            logger.error(f"Ошибка при классификации текста: {str(e)}")
            return categories[-1] # Fallback on error

    def _process_classification_response(self, response_text, categories):
        """Helper to process classification response text and return the best category."""
        response_text = response_text.strip()
        for category in categories:
            if category.lower() in response_text.lower():
                return category
        return categories[-1] # Fallback to the last category (often "другое")
