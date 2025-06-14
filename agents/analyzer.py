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
    
    def _classify_message(self, message_text):
        """
        Классификация текста сообщения с оценкой уверенности
        
        Args:
            message_text (str): Текст сообщения
            
        Returns:
            tuple: (категория сообщения, уровень уверенности 1-5)
        """
        # Всегда получаем примеры через LearningExamplesManager, он сам управляет кэшем
        examples = self.learning_manager.get_examples(limit=5)
        # Формируем текст с примерами
        examples_text = ""
        if examples:
            examples_text = "Примеры правильной классификации из прошлого опыта:\n\n"
            for i, ex in enumerate(examples):
                # Сокращаем текст примера для экономии токенов
                short_text = ex['text'][:150] + "..." if len(ex['text']) > 150 else ex['text']
                examples_text += f"Пример {i+1}:\nТекст: {short_text}\nКатегория: {ex['category']}\nОбоснование: {ex['justification']}\n\n"

        prompt = f"""
        Внимательно проанализируй следующий текст из правительственного Telegram-канала и определи, к какой из следующих категорий он относится:
        {examples_text if examples else ""}
        1. Законодательные инициативы - предложения о создании новых законов или нормативных актов, находящиеся на стадии обсуждения, внесения или рассмотрения в Госдуме. Обычно содержат фразы: "законопроект", "проект закона", "внесен на рассмотрение", "планируется принять".

        2. Новая судебная практика - решения, определения, постановления судов, создающие прецеденты или разъясняющие применение норм права. Признаки: упоминание судов (ВС, Верховный Суд, КС, арбитражный суд), номеров дел, дат решений, слов "решение", "определение", "постановление", "практика".

        3. Новые законы - недавно принятые и вступившие или вступающие в силу законодательные акты. Признаки: "закон принят", "закон подписан", "вступает в силу", "вступил в силу", указание номера федерального закона.

        4. Поправки к законам - изменения в существующих законах, внесенные или вступившие в силу. Признаки: "внесены изменения", "поправки", "новая редакция", "дополнен статьей", указания на изменение конкретных статей существующих законов.

        Если текст не относится ни к одной из категорий, то верни "другое".

        После выбора категории, укажи уровень своей уверенности по шкале от 1 до 5, где:
        1 - очень низкая уверенность, признаки категории почти отсутствуют
        2 - низкая уверенность, есть некоторые признаки категории
        3 - средняя уверенность, признаки категории присутствуют, но не очевидны
        4 - высокая уверенность, явные признаки категории
        5 - очень высокая уверенность, абсолютно точно эта категория
        
        Текст сообщения:
        {message_text}
        
        Ответ в формате:
        Категория: [название категории]
        Уверенность: [число от 1 до 5]
        """

        try:
            # Используем имеющийся метод classify, но с более сложным промптом
            response = self.llm_model.classify(prompt, CATEGORIES + ["другое"])
            
            # Парсим ответ для извлечения категории и уверенности
            if "\n" in response:
                lines = response.strip().split('\n')
                category = None
                confidence = 3  # По умолчанию средняя уверенность
                
                for line in lines:
                    if line.lower().startswith("категория:"):
                        category_text = line.replace("Категория:", "", 1).strip().lower()
                        # Находим наиболее подходящую категорию
                        for cat in CATEGORIES + ["другое"]:
                            if cat.lower() in category_text:
                                category = cat
                                break
                    
                    if line.lower().startswith("уверенность:"):
                        try:
                            confidence_text = line.replace("Уверенность:", "", 1).strip()
                            confidence = int(confidence_text)
                            # Проверяем, что уверенность в диапазоне 1-5
                            confidence = max(1, min(5, confidence))
                        except (ValueError, TypeError):
                            # Если не удалось преобразовать в число, используем значение по умолчанию
                            confidence = 3
                
                if category:
                    return category, confidence
            
            # Если не удалось распарсить ответ, используем обычную классификацию
            for category in CATEGORIES + ["другое"]:
                if category.lower() in response.lower():
                    # Уровень уверенности для простой категоризации
                    confidence = 3 if category != "другое" else 2
                    return category, confidence
            
            # Если категория не определена, используем "другое" с низкой уверенностью
            return "другое", 1
            
        except Exception as e:
            logger.error(f"Ошибка при классификации текста: {str(e)}")
            return "другое", 1
        
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
