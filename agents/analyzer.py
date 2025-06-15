"""
Агент для анализа и классификации сообщений
"""
import logging
import json
import os
from langchain.tools import Tool
from crewai import Agent, Task
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import CATEGORIES
import datetime
from datetime import datetime as dt
import time
from utils.learning_manager import LearningExamplesManager
logger = logging.getLogger(__name__)


class AnalyzerAgent:
    def __init__(self, db_manager, llm_model=None):
        """Инициализация агента"""
        self.db_manager = db_manager
        
        # Импорт здесь, чтобы избежать циклических импортов
        from llm.qwen_model import QwenLLM
        self.llm_model = llm_model or QwenLLM()
        
        # Флаг для быстрой проверки критиком сообщений с низкой уверенностью
        self.fast_check = False
        
        # Инициализируем менеджер обучающих примеров
        self.learning_manager = LearningExamplesManager()
        
        # Создаем инструмент для анализа сообщений
        analyze_tool = Tool(
            name="analyze_messages",
            func=self.analyze_messages,
            description="Анализирует и классифицирует сообщения из Telegram-каналов"
        )
        
        # Создаем агента CrewAI
        self.agent = Agent(
            name="Analyzer",
            role="Аналитик",
            goal="Анализировать и классифицировать сообщения по категориям",
            backstory="Я анализирую содержание сообщений из официальных каналов и определяю их тематику для формирования дайджеста.",
            verbose=True,
            tools=[analyze_tool]
        )
    
    
    # В agents/analyzer.py - улучшенный метод _classify_message
# Заменить существующий метод на этот

    def _classify_message(self, message_text):
        """
        Классификация текста сообщения с многошаговым reasoning и оценкой уверенности
        
        Args:
            message_text (str): Текст сообщения
            
        Returns:
            tuple: (категория сообщения, уровень уверенности 1-5)
        """
        # Получаем примеры через LearningExamplesManager
        examples = self.learning_manager.get_examples(limit=3)
        examples_text = ""
        if examples:
            examples_text = "ПРИМЕРЫ ПРАВИЛЬНОЙ КЛАССИФИКАЦИИ:\n\n"
            for i, ex in enumerate(examples, 1):
                short_text = ex['text'][:120] + "..." if len(ex['text']) > 120 else ex['text']
                examples_text += f"Пример {i}:\n"
                examples_text += f"Текст: {short_text}\n"
                examples_text += f"Категория: {ex['category']}\n"
                examples_text += f"Обоснование: {ex['justification']}\n\n"

        # УЛУЧШЕННЫЙ ПРОМПТ с более четким форматом
        enhanced_prompt = f"""
    Ты - эксперт по правовому анализу. Проанализируй сообщение и определи категорию.

    {examples_text if examples else ""}

    СООБЩЕНИЕ:
    {message_text}

    ДОСТУПНЫЕ КАТЕГОРИИ:
    1. законодательные инициативы - проекты, предложения, рассмотрение в Госдуме
    2. новые законы - принятые и подписанные законы, вступающие в силу
    3. поправки к законам - изменения в существующие законы
    4. новая судебная практика - решения, постановления судов
    5. другое - не относится к правовым вопросам

    АНАЛИЗ:
    Найди ключевые слова и определи стадию процесса.

    Если видишь "принят", "подписан", "вступает в силу" + номер закона = "новые законы"
    Если видишь "проект", "рассмотрение", "предложение", "инициатива" = "законодательные инициативы"  
    Если видишь "изменения", "поправки", "внесены в" + название закона = "поправки к законам"
    Если видишь "суд", "решение", "постановление", "определение" = "новая судебная практика"
    Иначе = "другое"

    СТРОГО отвечай в формате:
    Категория: [одна из 5 категорий точно как написано выше]
    Уверенность: [число 1-5]"""

        try:
            # Используем существующий метод classify с улучшенным промптом
            response = self.llm_model.classify(enhanced_prompt, CATEGORIES + ["другое"])
            
            # УЛУЧШЕННЫЙ ПАРСИНГ ответа
            category = None
            confidence = 3
            
            # Разбиваем ответ на строки и ищем паттерны
            lines = response.strip().split('\n')
            response_text = response.lower()
            
            # Ищем строку с категорией
            for line in lines:
                line_clean = line.strip()
                if line_clean.lower().startswith("категория"):
                    # Извлекаем категорию после двоеточия
                    if ":" in line_clean:
                        category_part = line_clean.split(":", 1)[1].strip().lower()
                        
                        # Точное сопоставление с категориями
                        for cat in CATEGORIES + ["другое"]:
                            if cat.lower() == category_part or cat.lower() in category_part:
                                category = cat
                                break
                    break
            
            # Ищем уверенность
            for line in lines:
                line_clean = line.strip()
                if line_clean.lower().startswith("уверенность"):
                    if ":" in line_clean:
                        conf_part = line_clean.split(":", 1)[1].strip()
                        # Извлекаем число
                        import re
                        numbers = re.findall(r'\d+', conf_part)
                        if numbers:
                            confidence = int(numbers[0])
                            confidence = max(1, min(5, confidence))
                    break
            
            # Альтернативный поиск категории в тексте ответа
            if not category:
                for cat in CATEGORIES + ["другое"]:
                    if cat.lower() in response_text:
                        category = cat
                        break
            
            logger.debug(f"Парсинг: найдена категория='{category}', уверенность={confidence}")
            
            # Логируем enhanced результат для отладки
            if category:
                self._log_classification_reasoning(message_text, category, confidence, response)
                logger.info(f"Enhanced классификация успешна: {category} (уверенность: {confidence})")
                return category, confidence
            
            # Fallback: если не удалось распарсить enhanced ответ, используем простой анализ
            logger.warning("Не удалось распарсить enhanced ответ, используем fallback")
            
            # Простой поиск категорий в ответе (оригинальная логика)
            response_lower = response.lower()
            for cat in CATEGORIES + ["другое"]:
                if cat.lower() in response_lower:
                    # Определяем базовую уверенность
                    confidence = 3 if cat != "другое" else 2
                    
                    # Повышаем уверенность, если есть явные маркеры
                    if any(marker in response_lower for marker in [
                        "закон принят", "подписан", "вступает в силу", 
                        "решение суда", "постановление", "инициатива"
                    ]):
                        confidence = min(5, confidence + 1)
                    
                    return cat, confidence
            
            # Если ничего не найдено
            return "другое", 1
            
        except Exception as e:
            logger.error(f"Ошибка при enhanced классификации: {str(e)}")
            # Fallback на самую простую логику
            if any(word in message_text.lower() for word in ["закон", "постановление", "решение"]):
                return "другое", 2
            return "другое", 1


    # Дополнительный helper метод для форматирования примеров
    def _format_examples_for_reasoning(self, examples):
        """Форматирование примеров для reasoning промпта"""
        if not examples:
            return ""
        
        formatted = "ПРИМЕРЫ УСПЕШНОЙ КЛАССИФИКАЦИИ:\n\n"
        for i, ex in enumerate(examples, 1):
            # Сокращаем текст для экономии токенов
            short_text = ex['text'][:100] + "..." if len(ex['text']) > 100 else ex['text']
            formatted += f"Пример {i}:\n"
            formatted += f"Текст: {short_text}\n"
            formatted += f"Категория: {ex['category']}\n"
            formatted += f"Почему: {ex['justification'][:80]}{'...' if len(ex['justification']) > 80 else ''}\n\n"
        
        return formatted

    def _log_classification_reasoning(self, message_text, category, confidence, response):
        """Логирование reasoning процесса анализатора"""
        # Извлекаем reasoning из ответа LLM
        reasoning_parts = {
            "ключевые_признаки": "",
            "обоснование": "",
            "стадия_процесса": "",
            "raw_response": response[:200] + "..." if len(response) > 200 else response
        }
        
        lines = response.strip().split('\n')
        for line in lines:
            line_clean = line.strip()
            if line_clean.lower().startswith("ключевые признаки:"):
                reasoning_parts["ключевые_признаки"] = line_clean.split(":", 1)[1].strip()
            elif line_clean.lower().startswith("обоснование:"):
                reasoning_parts["обоснование"] = line_clean.split(":", 1)[1].strip()
            elif "стадия" in line_clean.lower() and "процесс" in line_clean.lower():
                reasoning_parts["стадия_процесса"] = line_clean.split(":", 1)[1].strip()
        
        # Выводим красивый reasoning в терминал
        logger.info("🧠 REASONING АНАЛИЗАТОРА:")
        logger.info(f"   📝 Текст: {message_text[:80]}{'...' if len(message_text) > 80 else ''}")
        logger.info(f"   🎯 Результат: {category} (уверенность: {confidence})")
        
        if reasoning_parts["ключевые_признаки"]:
            logger.info(f"   🔍 Ключевые признаки: {reasoning_parts['ключевые_признаки']}")
        if reasoning_parts["обоснование"]:
            logger.info(f"   💭 Обоснование: {reasoning_parts['обоснование']}")
        if reasoning_parts["стадия_процесса"]:
            logger.info(f"   ⚖️ Стадия процесса: {reasoning_parts['стадия_процесса']}")
        
        # Показываем сырой ответ LLM для отладки (только первые 100 символов)
        logger.debug(f"   🤖 Ответ LLM: {reasoning_parts['raw_response']}")
        
        logger.info("   " + "─" * 60)
        
    def analyze_messages(self, limit=50, batch_size=10):
        """
        Инструмент для анализа и классификации сообщений с оценкой уверенности
        """
        logger.info(f"Запуск анализа сообщений, лимит: {limit}, размер пакета: {batch_size}")
        
        # Получаем непроанализированные сообщения
        messages = self.db_manager.get_unanalyzed_messages(limit=limit)
        
        if not messages:
            logger.info("Нет сообщений для анализа")
            return {
                "status": "success",
                "analyzed_count": 0,
                "categories": {}
            }
        
        categories_count = {category: 0 for category in CATEGORIES + ["другое"]}
        confidence_stats = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}  # Статистика по уровням уверенности
        analyzed_count = 0
        
        # Разбиваем сообщения на пакеты для более эффективной обработки
        batches = [messages[i:i+batch_size] for i in range(0, len(messages), batch_size)]
        logger.info(f"Разделено на {len(batches)} пакетов по ~{batch_size} сообщений")
        
        # Создаем пул для обработки каждого пакета
        with ThreadPoolExecutor(max_workers=min(4, len(batches))) as executor:
            future_to_batch = {}
            
            # Функция для обработки одного пакета
            def process_batch(batch_idx, batch):
                batch_start_time = time.time()
                logger.info(f"Начало обработки пакета {batch_idx+1}/{len(batches)}")
                
                batch_results = []
                for msg_idx, msg in enumerate(batch):
                    if not msg.text:
                        continue
                    
                    try:
                        msg_start_time = time.time()
                        # Сокращаем текст сообщения, если он слишком длинный
                        msg_text = msg.text
                        if len(msg_text) > 2000:
                            msg_text = msg_text[:2000] + "... [сокращено]"
                            logger.debug(f"Сообщение {msg.id} сокращено с {len(msg.text)} до 2000 символов")
                        
                        # Классифицируем сообщение
                        category, confidence = self._classify_message(msg_text)
                        
                        msg_elapsed = time.time() - msg_start_time
                        logger.debug(f"Сообщение {msg_idx+1}/{len(batch)} в пакете {batch_idx+1} обработано за {msg_elapsed:.2f}с: {category} ({confidence})")
                        
                        # Создаем результат для этого сообщения
                        result = {
                            "message_id": msg.id,
                            "category": category,
                            "confidence": confidence,
                            "success": True,
                            "processing_time": msg_elapsed
                        }
                        
                        # Если включена быстрая проверка и нужен дополнительный анализ
                        if self.fast_check and (category == "другое" or confidence <= 2):
                            try:
                                critic_start = time.time()
                                from agents.critic import CriticAgent
                                critic = CriticAgent(self.db_manager)
                                critic_result = critic.review_categorization(msg.id, category)
                                
                                critic_elapsed = time.time() - critic_start
                                logger.debug(f"Критик проверил сообщение за {critic_elapsed:.2f}с")
                                
                                # Если критик изменил категорию, используем его результат
                                if critic_result["status"] == "updated":
                                    result["category"] = critic_result["new_category"]
                                    result["confidence"] = critic_result["confidence"]
                                    result["reviewed_by_critic"] = True
                                    result["critic_time"] = critic_elapsed
                            except Exception as e:
                                logger.error(f"Ошибка при быстрой проверке сообщения {msg.id}: {str(e)}")
                    except Exception as e:
                        logger.error(f"Ошибка при классификации сообщения {msg.id}: {str(e)}")
                        result = {
                            "message_id": msg.id,
                            "error": str(e),
                            "success": False
                        }
                    
                    batch_results.append(result)
                
                batch_elapsed = time.time() - batch_start_time
                logger.info(f"Завершена обработка пакета {batch_idx+1}/{len(batches)} за {batch_elapsed:.2f}с")
                return batch_results
            
            # Запускаем задачи на обработку пакетов
            for i, batch in enumerate(batches):
                future = executor.submit(process_batch, i, batch)
                future_to_batch[future] = i
            
            # Обрабатываем результаты
            all_results = []
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                    
                    # Вычисляем статистику по этому пакету
                    batch_success = sum(1 for r in batch_results if r["success"])
                    batch_times = [r.get("processing_time", 0) for r in batch_results if "processing_time" in r]
                    avg_time = sum(batch_times) / len(batch_times) if batch_times else 0
                    
                    logger.info(f"Обработан пакет {batch_idx+1}/{len(batches)}: {batch_success} успешно, среднее время: {avg_time:.2f}с")
                except Exception as e:
                    logger.error(f"Ошибка при обработке пакета {batch_idx+1}: {str(e)}")
        
        # Обновляем категории всех сообщений в БД
        successful_updates = []
        for result in all_results:
            if result["success"]:
                # update_message_category returns True/False
                success = self.db_manager.update_message_category( 
                    result["message_id"],
                    result["category"],
                    result["confidence"])
                
                if success:
                    categories_count[result["category"]] += 1
                    confidence_stats[result["confidence"]] += 1
                    analyzed_count += 1
                    successful_updates.append(result)
        
        logger.info(f"Анализ завершен. Проанализировано и успешно обновлено {analyzed_count} сообщений")
        logger.info(f"Распределение по категориям: {categories_count}")
        logger.info(f"Распределение по уверенности: {confidence_stats}")
        
        return {
            "status": "success",
            "analyzed_count": analyzed_count,
            "categories": categories_count,
            "confidence_stats": confidence_stats,
            "all_results": all_results
        }
    def create_task(self):
        """
        Создание задачи для агента
        
        Returns:
            Task: Задача CrewAI
        """
        return Task(
            description="Проанализировать и классифицировать непроанализированные сообщения",
            agent=self.agent,
            expected_output="Результаты анализа с информацией о количестве проанализированных сообщений и их категориях"
        )
