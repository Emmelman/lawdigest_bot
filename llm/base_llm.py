"""
Базовый класс для всех LLM моделей, обеспечивающий общую логику кэширования и обработки запросов.
"""
import logging
import requests
import hashlib
import os
import time

logger = logging.getLogger(__name__)

class BaseLLM:
    """
    Базовый класс для LLM моделей, предоставляющий функциональность кэширования
    и обработки запросов к API.
    """
    def __init__(self, model_name: str, api_url: str, cache_dir: str = 'llm_cache'):
        self.model_name = model_name
        self.api_url = f"{api_url}/v1/chat/completions"
        self.cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
 
    def _get_cached_response(self, prompt: str, max_tokens: int, temperature: float) -> tuple[str | None, bool]:
        """
        Получение ответа из кэша с учетом типа запроса и TTL.
        """
        # Используем полный промпт для хэша, чтобы обеспечить уникальность
        cache_key = hashlib.md5((prompt + f"_tokens{max_tokens}_temp{temperature}").encode()).hexdigest()
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.txt")

        if os.path.exists(cache_file):
            file_age = time.time() - os.path.getmtime(cache_file)

            # Определяем TTL в зависимости от типа запроса
            cache_ttl = 86400  # 24 часа по умолчанию

            if "классифицировать" in prompt.lower() or "категори" in prompt.lower():
                cache_ttl = 604800  # 7 дней
            elif "дайджест" in prompt.lower() or "новост" in prompt.lower():
                cache_ttl = 43200  # 12 часов
            
            # Для очень длинных запросов (больше 5000 символов) - более короткое кэширование
            if len(prompt) > 5000:
                cache_ttl = 21600  # 6 часов
            
            if file_age < cache_ttl:
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cached_response = f.read()
                        logger.debug(f"Использован кэш для промпта (хэш: {cache_key[:8]}..., TTL: {cache_ttl/3600:.1f}h)")
                        return cached_response, True
                except Exception as e:
                    logger.error(f"Ошибка при чтении кэша из файла {cache_file}: {str(e)}")

        return None, False

    def _generate_response(self, prompt: str, max_tokens: int = 1000, temperature: float = 0.7, retry_count: int = 0) -> str:
        """
        Отправка запроса к API и получение ответа с обработкой таймаутов и повторными попытками.
        """
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        prompt_length = len(prompt)
        logger.debug(f"Отправка запроса к LLM ({prompt_length} символов, {max_tokens} токенов) к {self.api_url}")
        
        start_time = time.time()
        try:
            response = requests.post(self.api_url, json=payload, timeout=60) # Увеличиваем таймаут
            response.raise_for_status()
            
            elapsed = time.time() - start_time
            logger.debug(f"Получен ответ от LLM за {elapsed:.2f} секунд")
            
            data = response.json()
            return data['choices'][0]['message']['content']
            
        except requests.exceptions.Timeout:
            logger.warning(f"Таймаут запроса к LLM после {time.time() - start_time:.2f} секунд (попытка {retry_count+1})")
            if retry_count < 2: # Max 3 attempts
                # Strategy: first retry with fewer tokens, then shorten prompt
                if retry_count == 0 and max_tokens > 500:
                    new_max_tokens = int(max_tokens * 0.5) # Try halving tokens
                    logger.info(f"Повторная попытка с уменьшенным числом токенов ({max_tokens} -> {new_max_tokens})")
                    return self._generate_response(prompt, max_tokens=new_max_tokens, temperature=temperature, retry_count=retry_count+1)
                elif retry_count == 1 and prompt_length > 1500:
                    shortened_prompt = prompt[:1500] + "...[текст сокращен для ретрая]"
                    logger.info(f"Повторная попытка с сокращенным промптом ({prompt_length} -> {len(shortened_prompt)} символов)")
                    return self._generate_response(shortened_prompt, max_tokens=500, temperature=temperature, retry_count=retry_count+1)
            
            # If all retries fail due to timeout, return a default error message
            return "Не удалось получить ответ от LLM из-за превышения времени ожидания. Пожалуйста, попробуйте упростить запрос или измените настройки таймаута."
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка API запроса после {time.time() - start_time:.2f} секунд (попытка {retry_count+1}): {str(e)}")
            if retry_count < 1: # Max 2 attempts for other request errors
                time.sleep(1) # Small pause before retrying
                return self._generate_response(prompt, max_tokens, temperature, retry_count+1)
            raise # Re-raise if retries exhausted or non-retryable error
