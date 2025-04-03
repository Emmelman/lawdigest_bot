"""
Агент для анализа и классификации сообщений
"""
import logging
from langchain.tools import Tool
from crewai import Agent, Task

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
        prompt = f"""
        Внимательно проанализируй следующий текст из правительственного Telegram-канала и определи, к какой из следующих категорий он относится:

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