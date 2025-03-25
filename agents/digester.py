"""
Агент для формирования дайджеста
"""
import logging
from datetime import datetime, timedelta
from crewai import Agent, Task

from config.settings import CATEGORIES
from database.db_manager import DatabaseManager
from llm.gemma_model import GemmaLLM
from langchain.tools import Tool

logger = logging.getLogger(__name__)

class DigesterAgent:
    """Агент для формирования дайджеста"""
    
    def __init__(self, db_manager, llm_model=None):
        """
        Инициализация агента
        
        Args:
            db_manager (DatabaseManager): Менеджер БД
            llm_model (GemmaLLM, optional): Модель для генерации текста
        """
        self.db_manager = db_manager
        self.llm_model = llm_model or GemmaLLM()
        
        create_digest_tool = Tool(
        name="create_digest",
        func=self.create_digest,
        description="Формирует дайджест правовых новостей"
        )

        # Создаем агента CrewAI
        self.agent = Agent(
            name="Digester",
            role="Дайджест-мейкер",  # Добавьте поле role
            goal="Формировать информативный дайджест по правовым изменениям",
            backstory="Я создаю краткие и содержательные обзоры правовых изменений на основе данных из официальных источников.",
            verbose=True,
            # Инструменты должны быть созданы с помощью BaseTool или иметь специальный формат
            tools=[]  # Временно оставим пустой список, затем добавим правильно оформленные инструменты
        )
    
    def _generate_section_summary(self, category, messages):
        """
        Генерация краткого обзора по категории
        
        Args:
            category (str): Категория сообщений
            messages (list): Список сообщений этой категории
            
        Returns:
            str: Текст обзора по категории
        """
        if not messages:
            return f"За данный период новостей по категории '{category}' не обнаружено."
        
        messages_text = "\n---\n".join([msg.text for msg in messages])
        prompt = f"""
        Сформируй краткий дайджест по категории '{category}' на основе следующих сообщений из правительственных Telegram-каналов.
        
        Сообщения:
        {messages_text}
        
        Требования к дайджесту:
        1. Объедини похожие сообщения, выдели ключевые события.
        2. Используй четкий, лаконичный язык.
        3. Расположи информацию по степени важности.
        4. Добавь краткие пояснения, где это необходимо.
        5. Объем: 2-3 абзаца.
        """
        
        try:
            response = self.llm_model.generate(prompt)
            return response
        except Exception as e:
            logger.error(f"Ошибка при генерации обзора по категории '{category}': {str(e)}")
            return f"За данный период информация по категории '{category}' недоступна из-за технической ошибки."
    
    def _generate_digest_intro(self, date, total_messages, categories_count):
        """
        Генерация вводной части дайджеста
        
        Args:
            date (datetime): Дата дайджеста
            total_messages (int): Общее количество сообщений
            categories_count (dict): Количество сообщений по категориям
            
        Returns:
            str: Текст вводной части
        """
        formatted_date = date.strftime("%d.%m.%Y")
        categories_info = "\n".join([f"- {cat}: {count} сообщений" for cat, count in categories_count.items() if count > 0])
        
        prompt = f"""
        Напиши краткое вступление к дайджесту правовых новостей за {formatted_date}.
        
        Информация для вступления:
        - Дата: {formatted_date}
        - Всего сообщений: {total_messages}
        - Распределение по категориям:
        {categories_info}
        
        Вступление должно быть лаконичным (1-2 абзаца) и содержать общую характеристику новостей за этот день.
        """
        
        try:
            response = self.llm_model.generate(prompt, max_tokens=300)
            return response
        except Exception as e:
            logger.error(f"Ошибка при генерации вступления к дайджесту: {str(e)}")
            return f"Дайджест правовых новостей за {formatted_date}"
    
    def create_digest(self, date=None, days_back=1):
        """
        Инструмент для создания дайджеста
        
        Args:
            date (datetime, optional): Дата дайджеста (по умолчанию сегодня)
            days_back (int): Количество дней для сбора сообщений
            
        Returns:
            dict: Результаты создания дайджеста
        """
        # Определяем даты
        end_date = date or datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Создание дайджеста за период с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")
        
        # Счетчики для статистики
        total_messages = 0
        categories_count = {category: 0 for category in CATEGORIES}
        categories_count["другое"] = 0
        
        # Собираем сообщения по категориям
        sections = {}
        section_texts = {}
        
        for category in CATEGORIES + ["другое"]:
            # Получаем сообщения для этой категории
            messages = self.db_manager.get_messages_by_date_range(
                start_date=start_date,
                end_date=end_date,
                category=category
            )
            
            categories_count[category] = len(messages)
            total_messages += len(messages)
            
            # Если есть сообщения, создаем обзор
            if messages:
                section_text = self._generate_section_summary(category, messages)
                sections[category] = section_text
                section_texts[category] = section_text
        
        # Генерируем вводную часть
        intro_text = self._generate_digest_intro(end_date, total_messages, categories_count)
        
        # Формируем полный текст дайджеста
        full_text = f"{intro_text}\n\n"
        
        for category in CATEGORIES:
            if category in sections:
                full_text += f"## {category.upper()}\n\n{sections[category]}\n\n"
        
        # Добавляем категорию "другое" в конец, если есть сообщения
        if "другое" in sections:
            full_text += f"## ДРУГИЕ НОВОСТИ\n\n{sections['другое']}\n\n"
        
        # Сохраняем дайджест в БД
        try:
            digest = self.db_manager.save_digest(end_date, full_text, section_texts)
            logger.info(f"Дайджест успешно создан и сохранен (ID: {digest.id})")
            
            return {
                "status": "success",
                "digest_id": digest.id,
                "date": end_date.strftime("%Y-%m-%d"),
                "total_messages": total_messages,
                "categories": categories_count,
                "digest_text": full_text
            }
        except Exception as e:
            logger.error(f"Ошибка при сохранении дайджеста: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "date": end_date.strftime("%Y-%m-%d"),
                "digest_text": full_text
            }
    
    def create_task(self):
        """
        Создание задачи для агента
        
        Returns:
            Task: Задача CrewAI
        """
        return Task(
            description="Сформировать дайджест правовых новостей за последний день",
            agent=self.agent,
            expected_output="Результаты создания дайджеста с полным текстом"
        )