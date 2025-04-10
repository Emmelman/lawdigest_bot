"""
Интерфейс для работы с моделью Gemma 3 через LLM Studio
"""
import logging
import requests
import hashlib
import os
import time

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
            # Проверяем, есть ли результат в кэше
            cache_key = hashlib.md5((prompt + f"_tokens{max_tokens}_temp{temperature}").encode()).hexdigest()
            cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'llm_cache')
            os.makedirs(cache_dir, exist_ok=True)
            cache_file = os.path.join(cache_dir, f"{cache_key}.txt")
            
            # Если кэш существует и не старше 24 часов, используем его
            if os.path.exists(cache_file):
                file_age = time.time() - os.path.getmtime(cache_file)
                if file_age < 86400:  # 24 часа
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cached_response = f.read()
                        logger.debug(f"Использован кэш для промпта (хэш: {cache_key[:8]}...)")
                        return cached_response
            
            # Если нет кэша, генерируем ответ
            response = self._generate_response(prompt, max_tokens, temperature)
            
            # Сохраняем в кэш
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(response)
            
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
    
    def _generate_response(self, prompt, max_tokens=1000, temperature=0.7, retry_count=0):
        """
        Отправка запроса к API и получение ответа с обработкой таймаутов
        """
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        # Логируем размер запроса (только для отладки)
        prompt_length = len(prompt)
        logger.debug(f"Отправка запроса к LLM ({prompt_length} символов, {max_tokens} токенов)")
        
        start_time = time.time()
        try:
            # Добавляем таймаут
            response = requests.post(self.api_url, json=payload, timeout=30)
            response.raise_for_status()
            
            elapsed = time.time() - start_time
            logger.debug(f"Получен ответ от LLM за {elapsed:.2f} секунд")
            
            data = response.json()
            return data['choices'][0]['message']['content']
            
        except requests.exceptions.Timeout:
            # Обработка таймаута
            logger.warning(f"Таймаут запроса к LLM после {time.time() - start_time:.2f} секунд")
            
            # Стратегия повторного запроса
            if retry_count < 2:
                # При первой попытке уменьшаем токены
                if retry_count == 0 and max_tokens > 500:
                    logger.info(f"Повторная попытка с уменьшенным числом токенов ({max_tokens} -> 500)")
                    return self._generate_response(prompt, max_tokens=500, temperature=temperature, retry_count=retry_count+1)
                # При второй попытке уменьшаем промпт
                elif retry_count == 1 and prompt_length > 1500:
                    shortened_prompt = prompt[:1500] + "...[сокращено]"
                    logger.info(f"Повторная попытка с сокращенным промптом ({prompt_length} -> 1500 символов)")
                    return self._generate_response(shortened_prompt, max_tokens=500, temperature=temperature, retry_count=retry_count+1)
            
            # Если все попытки не удались, возвращаем результат по умолчанию
            category_fallback = None
            for cat in ["законодательные инициативы", "новая судебная практика", "новые законы", "поправки к законам", "другое"]:
                if cat.lower() in prompt.lower():
                    category_fallback = cat
                    break
            
            if "Категория:" in prompt:
                # Запрос на классификацию
                return f"Категория: {category_fallback or 'другое'}\nУверенность: 1"
            else:
                # Обычный запрос
                return "Не удалось получить ответ от LLM из-за превышения времени ожидания. Попробуйте упростить запрос."
        
        except requests.exceptions.RequestException as e:
            elapsed = time.time() - start_time
            logger.error(f"Ошибка API запроса после {elapsed:.2f} секунд: {str(e)}")
            
            if retry_count < 1:
                logger.info("Повторная попытка запроса после ошибки...")
                time.sleep(1)  # Небольшая пауза перед повторной попыткой
                return self._generate_response(prompt, max_tokens, temperature, retry_count+1)
            
            raise
    # В GemmaLLM и QwenLLM:
    def _get_cached_response(self, prompt, max_tokens, temperature):
        """Получение ответа из кэша с учетом типа запроса"""
        cache_key = hashlib.md5((prompt + f"_tokens{max_tokens}_temp{temperature}").encode()).hexdigest()
        cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'llm_cache')
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"{cache_key}.txt")
        
        if os.path.exists(cache_file):
            file_age = time.time() - os.path.getmtime(cache_file)
            
            # Определяем TTL в зависимости от типа запроса
            cache_ttl = 86400  # 24 часа по умолчанию
            
            # Для классификации более длительное хранение
            if "классифицировать" in prompt.lower() or "категори" in prompt.lower():
                cache_ttl = 604800  # 7 дней
            
            # Для дайджестов более короткое хранение
            if "дайджест" in prompt.lower() or "новост" in prompt.lower():
                cache_ttl = 43200  # 12 часов
                
            # Для длинных запросов еще более короткое хранение
            if len(prompt) > 5000:
                cache_ttl = 21600  # 6 часов
                
            if file_age < cache_ttl:
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cached_response = f.read()
                        logger.debug(f"Использован кэш для промпта (хэш: {cache_key[:8]}...)")
                        return cached_response, True
                except Exception as e:
                    logger.error(f"Ошибка при чтении кэша: {str(e)}")
        
        return None, False    