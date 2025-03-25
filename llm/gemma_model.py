"""
Интерфейс для работы с моделью Gemma 3 через LLM Studio
"""
import logging
import requests

logger = logging.getLogger(__name__)

class GemmaLLM:
    """Класс для работы с моделью Gemma 3"""
    
    def __init__(self, model_name="gemma-3-12b-it", api_url="http://127.0.0.1:1234"):
        """
        Инициализация модели
        
        Args:
            model_name (str): Название модели
            api_url (str): Базовый URL для API запросов
        """
        self.model_name = model_name
        self.api_url = f"{api_url}/v1/chat/completions"
    
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
        try:
            response = self._generate_response(prompt, max_tokens, temperature)
            return response.strip()
        except Exception as e:
            logger.error(f"Ошибка при генерации текста: {str(e)}")
            return "Не удалось сгенерировать текст из-за технической ошибки."
    
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
            response = self._generate_response(prompt, max_tokens, temperature=0.5)
            return response.strip()
        except Exception as e:
            logger.error(f"Ошибка при резюмировании текста: {str(e)}")
            return "Не удалось создать резюме из-за технической ошибки."
    
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