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

logger = logging.getLogger(__name__)

class AnalyzerAgent:
    """Агент для анализа и классификации сообщений"""
    
    def __init__(self, db_manager, llm_model=None):
        """
        Инициализация агента
        
        Args:
            db_manager (DatabaseManager): Менеджер БД
            llm_model (QwenLLM, optional): Модель для обработки текста
        """
        self.db_manager = db_manager
        
        # Импорт здесь, чтобы избежать циклических импортов
        from llm.qwen_model import QwenLLM
        self.llm_model = llm_model or QwenLLM()
        
        # Флаг для быстрой проверки критиком сообщений с низкой уверенностью
        self.fast_check = False
        
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
        examples = self._load_learning_examples(limit=5)  # Ограничиваем 5 примерами
    
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
        
    def analyze_messages(self, limit=50, batch_size=5):
        """
        Инструмент для анализа и классификации сообщений с оценкой уверенности
        
        Args:
            limit (int): Максимальное количество сообщений для анализа
            batch_size (int): Размер пакета для обработки
            
        Returns:
            dict: Результаты анализа
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
        
        for batch_idx, batch in enumerate(batches):
            logger.info(f"Обработка пакета {batch_idx+1}/{len(batches)}")
            
            for msg in batch:
                if not msg.text:
                    continue
                    
                try:
                    # Классифицируем сообщение с оценкой уверенности
                    category, confidence = self._classify_message(msg.text)
                    
                    # Обновляем категорию и уровень уверенности в БД
                    if self.db_manager.update_message_category(msg.id, category, confidence):
                        categories_count[category] += 1
                        confidence_stats[confidence] += 1
                        analyzed_count += 1
                        
                        # Логируем результат для отладки
                        logger.debug(f"Сообщение {msg.id}: категория='{category}', уверенность={confidence}")
                        
                        # Если категория "другое" или низкая уверенность, и включена быстрая проверка
                        if hasattr(self, 'fast_check') and self.fast_check and (category == "другое" or confidence <= 2):
                            try:
                                from agents.critic import CriticAgent
                                critic = CriticAgent(self.db_manager)
                                result = critic.review_categorization(msg.id, category)
                                if result["status"] == "updated":
                                    # Обновляем статистику, если критик изменил категорию
                                    categories_count[category] -= 1
                                    categories_count[result["new_category"]] += 1
                                    logger.info(f"Критик изменил категорию сообщения {msg.id}: {category} -> {result['new_category']}")
                            except Exception as e:
                                logger.error(f"Ошибка при быстрой проверке сообщения {msg.id}: {str(e)}")
                except Exception as e:
                    logger.error(f"Ошибка при анализе сообщения {msg.id}: {str(e)}")
        
        logger.info(f"Анализ завершен. Проанализировано {analyzed_count} сообщений")
        logger.info(f"Распределение по категориям: {categories_count}")
        logger.info(f"Распределение по уверенности: {confidence_stats}")
        
        return {
            "status": "success",
            "analyzed_count": analyzed_count,
            "categories": categories_count,
            "confidence_stats": confidence_stats
        }
    # В AnalyzerAgent:
    def analyze_messages_batched(self, limit=500, batch_size=20, confidence_threshold=3):
        """
        Анализ сообщений большими партиями с приоритизацией
        
        Args:
            limit (int): Максимальное количество сообщений для анализа
            batch_size (int): Размер пакета для обработки
            confidence_threshold (int): Пороговое значение уверенности для повторного анализа
            
        Returns:
            dict: Результаты анализа
        """
        logger.info(f"Запуск анализа сообщений с оптимизацией, лимит: {limit}")
        
        # Получаем непроанализированные сообщения
        unanalyzed = self.db_manager.get_unanalyzed_messages(limit=limit)
        
        if not unanalyzed:
            logger.info("Нет новых сообщений для анализа")
            
            # Проверяем наличие сообщений с низкой уверенностью для повторного анализа
            low_confidence = self.db_manager.get_messages_with_low_confidence(
                confidence_threshold=confidence_threshold,
                limit=min(limit, 50)  # Ограничиваем количество для повторного анализа
            )
            
            if not low_confidence:
                logger.info("Нет сообщений с низкой уверенностью для повторного анализа")
                return {
                    "status": "success",
                    "analyzed_count": 0,
                    "categories": {},
                    "reanalyzed_count": 0
                }
            
            logger.info(f"Найдено {len(low_confidence)} сообщений с низкой уверенностью для повторного анализа")
            messages_to_analyze = low_confidence
            is_reanalysis = True
        else:
            logger.info(f"Найдено {len(unanalyzed)} новых сообщений для анализа")
            messages_to_analyze = unanalyzed
            is_reanalysis = False
        
        # Разбиваем сообщения на пакеты
        batches = [messages_to_analyze[i:i+batch_size] for i in range(0, len(messages_to_analyze), batch_size)]
        
        # Счетчики для статистики
        categories_count = {category: 0 for category in CATEGORIES + ["другое"]}
        confidence_stats = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        analyzed_count = 0
        updated_count = 0
        
        # Обрабатываем пакеты параллельно
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Функция для обработки одного пакета
            def process_batch(batch):
                batch_results = []
                for msg in batch:
                    try:
                        # Классифицируем сообщение
                        category, confidence = self._classify_message(msg.text)
                        
                        # Если это повторный анализ и уверенность не улучшилась, пропускаем
                        if is_reanalysis and confidence <= msg.confidence:
                            batch_results.append({
                                "message_id": msg.id,
                                "status": "unchanged",
                                "category": msg.category,
                                "confidence": msg.confidence
                            })
                            continue
                        
                        # Обновляем категорию в БД
                        success = self.db_manager.update_message_category(msg.id, category, confidence)
                        
                        if success:
                            batch_results.append({
                                "message_id": msg.id,
                                "status": "updated" if is_reanalysis else "analyzed",
                                "category": category,
                                "confidence": confidence
                            })
                        else:
                            batch_results.append({
                                "message_id": msg.id,
                                "status": "error",
                                "error": "Не удалось обновить категорию"
                            })
                    except Exception as e:
                        batch_results.append({
                            "message_id": msg.id,
                            "status": "error",
                            "error": str(e)
                        })
                
                return batch_results
            
            # Запускаем задачи
            future_to_batch = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}
            
            # Собираем результаты
            all_results = []
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                    logger.info(f"Обработан пакет {batch_idx+1}/{len(batches)}: {len(batch_results)} сообщений")
                except Exception as e:
                    logger.error(f"Ошибка при обработке пакета {batch_idx+1}: {str(e)}")
        
        # Обрабатываем итоговые результаты
        for result in all_results:
            if result["status"] in ["analyzed", "updated"]:
                category = result["category"]
                confidence = result["confidence"]
                
                categories_count[category] += 1
                confidence_stats[confidence] += 1
                analyzed_count += 1
                
                if result["status"] == "updated":
                    updated_count += 1
        
        logger.info(f"Анализ завершен. Проанализировано {analyzed_count} сообщений, обновлено {updated_count}")
        logger.info(f"Распределение по категориям: {categories_count}")
        logger.info(f"Распределение по уверенности: {confidence_stats}")
        
        return {
            "status": "success",
            "analyzed_count": analyzed_count,
            "categories": categories_count,
            "confidence_stats": confidence_stats,
            "reanalyzed_count": updated_count if is_reanalysis else 0
        }
    def _load_learning_examples(self, limit=10):
        """Загружает примеры для улучшения классификации"""
        examples_path = "learning_examples/examples.jsonl"
        if not os.path.exists(examples_path):
            return []
        
        examples = []
        try:
            with open(examples_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                # Берем последние примеры, ограниченные лимитом
                for line in lines[-limit:]:
                    examples.append(json.loads(line))
            return examples
        except Exception as e:
            logger.error(f"Ошибка при загрузке обучающих примеров: {str(e)}")
            return []
        
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
    
    