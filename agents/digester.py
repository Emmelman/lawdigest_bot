"""
Агент для формирования дайджеста
"""
import logging
import re
from datetime import datetime, timedelta
from crewai import Agent, Task

from config.settings import CATEGORIES, BOT_USERNAME
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
            role="Дайджест-мейкер",
            goal="Формировать информативный дайджест по правовым изменениям",
            backstory="Я создаю краткие и содержательные обзоры правовых изменений на основе данных из официальных источников.",
            verbose=True,
            tools=[create_digest_tool]
        )
    
    def _extract_links_and_headlines(self, text):
        """
        Извлечение ссылок и заголовков из текста сообщения
        
        Args:
            text (str): Текст сообщения
            
        Returns:
            list: Список словарей с заголовками и ссылками
        """
        results = []
        
        # Шаблон для поиска ссылок в markdown формате [текст](ссылка)
        markdown_pattern = r'\[(.*?)\]\((https?://[^\s\)]+)\)'
        markdown_links = re.findall(markdown_pattern, text)
        
        for title, url in markdown_links:
            results.append({
                "title": title.strip(),
                "url": url.strip()
            })
        
        # Шаблон для поиска обычных URL
        url_pattern = r'https?://[^\s]+'
        
        # Находим URL, которые не были найдены в markdown формате
        all_urls = re.findall(url_pattern, text)
        markdown_urls = [link[1] for link in markdown_links]
        
        for url in all_urls:
            if url not in markdown_urls and url.strip():
                # Пытаемся найти текст, связанный с этой ссылкой
                # (например, текст в строке перед URL или после URL)
                context = text.split(url)
                
                if len(context) > 1:
                    # Берем 100 символов до и после URL
                    before = context[0][-100:] if len(context[0]) > 100 else context[0]
                    after = context[1][:100] if len(context[1]) > 100 else context[1]
                    
                    # Ищем возможный заголовок в тексте до или после URL
                    possible_title = before.strip() if before.strip() else after.strip()
                    
                    # Ограничиваем длину заголовка до 100 символов
                    title = possible_title[:100] + "..." if len(possible_title) > 100 else possible_title
                else:
                    title = url[:50] + "..." if len(url) > 50 else url
                
                results.append({
                    "title": title.strip(),
                    "url": url.strip()
                })
        
        return results
    
    def _generate_brief_section(self, category, messages):
        """
        Генерация краткого обзора по категории с заголовками и ссылками
        
        Args:
            category (str): Категория сообщений
            messages (list): Список сообщений этой категории
            
        Returns:
            str: Текст краткого обзора по категории
        """
        if not messages:
            return f"За данный период новостей по категории '{category}' не обнаружено."
        
        # Извлекаем ссылки и заголовки из всех сообщений
        all_links = []
        
        for msg in messages:
            links = self._extract_links_and_headlines(msg.text)
            
            # Добавляем информацию о канале и дате к каждой ссылке
            for link in links:
                link["channel"] = msg.channel
                link["date"] = msg.date
                all_links.append(link)
        
        # Если нет ссылок в сообщениях, генерируем их из текста
        if not all_links:
            for idx, msg in enumerate(messages[:10]):  # Ограничиваем до 10 сообщений
                # Создаем ссылку на сообщение в боте
                bot_link = f"https://t.me/{BOT_USERNAME}?start=msg_{msg.id}"
                
                # Берем первые 100 символов текста как заголовок
                title = msg.text[:100].strip() + "..." if len(msg.text) > 100 else msg.text.strip()
                
                all_links.append({
                    "title": title,
                    "url": bot_link,
                    "channel": msg.channel,
                    "date": msg.date
                })
        
        # Сортируем ссылки по дате (сначала самые новые)
        all_links.sort(key=lambda x: x["date"], reverse=True)
        
        # Формируем текст секции
        section_text = f"## {category.upper()}\n\n"
        
        # Добавляем краткое описание категории
        category_descriptions = {
            'законодательные инициативы': "Предложения о создании новых законов, находящиеся на стадии обсуждения",
            'новая судебная практика': "Решения и разъяснения судов, создающие прецеденты",
            'новые законы': "Недавно принятые и вступившие в силу законодательные акты",
            'поправки к законам': "Изменения в существующих законах",
            'другое': "Другие правовые новости и информация"
        }
        
        description = category_descriptions.get(category, "")
        if description:
            section_text += f"{description}:\n\n"
        
        # Добавляем ссылки с заголовками
        for idx, link in enumerate(all_links[:15]):  # Ограничиваем до 15 ссылок
            formatted_date = link["date"].strftime("%d.%m.%Y")
            channel_name = link["channel"].replace("@", "")
            
            section_text += f"{idx+1}. [{link['title']}]({link['url']}) - {channel_name}, {formatted_date}\n\n"
        
        # Добавляем ссылку на полный обзор
        section_text += f"\n[Открыть полный обзор по категории '{category}'](/category/{category})\n"
        
        return section_text

    def _generate_detailed_section(self, category, messages):
        """
        Генерация подробного обзора по категории
        
        Args:
            category (str): Категория сообщений
            messages (list): Список сообщений этой категории
            
        Returns:
            str: Текст подробного обзора по категории
        """
        if not messages:
            return f"За данный период новостей по категории '{category}' не обнаружено."
        
        # Формируем контекст из сообщений для LLM
        messages_text = "\n\n---\n\n".join([
            f"Канал: {msg.channel}\nДата: {msg.date.strftime('%d.%m.%Y')}\n\n{msg.text}" 
            for msg in messages[:15]  # Ограничиваем до 15 сообщений для контекста
        ])
        
        prompt = f"""
        Сформируй подробный дайджест по категории '{category}' на основе следующих сообщений из правительственных Telegram-каналов.
        
        СООБЩЕНИЯ:
        {messages_text}
        
        ТРЕБОВАНИЯ К ДАЙДЖЕСТУ:
        1. Структурируй текст по важным темам и событиям, объединяя связанные сообщения.
        2. Начни с самых важных и новых изменений в законодательстве.
        3. Для каждого пункта дайджеста укажи источник (канал).
        4. Сохрани важные детали: номера законов, даты вступления в силу, особенности применения и т.д.
        5. Используй профессиональный юридический язык, но понятный широкой аудитории.
        6. Выделяй полужирным шрифтом (через **) ключевые термины, названия законов и важные даты.
        7. При упоминании законопроектов указывай их текущий статус рассмотрения.
        8. Сохрани и интегрируй все важные URL-ссылки из оригинальных сообщений.
        9. Объем: 3-5 содержательных абзацев.
        """
        
        try:
            response = self.llm_model.generate(prompt, max_tokens=1000)
            return response
        except Exception as e:
            logger.error(f"Ошибка при генерации подробного обзора по категории '{category}': {str(e)}")
            return f"За данный период подробная информация по категории '{category}' недоступна из-за технической ошибки."

    def _generate_digest_intro(self, date, total_messages, categories_count, is_brief=True):
        """
        Генерация вводной части дайджеста
        
        Args:
            date (datetime): Дата дайджеста
            total_messages (int): Общее количество сообщений
            categories_count (dict): Количество сообщений по категориям
            is_brief (bool): Признак краткого дайджеста
            
        Returns:
            str: Текст вводной части
        """
        formatted_date = date.strftime("%d.%m.%Y")
        categories_info = "\n".join([f"- {cat}: {count} сообщений" for cat, count in categories_count.items() if count > 0])
        
        prompt = f"""
        Напиши краткое вступление к {"краткому" if is_brief else "подробному"} дайджесту правовых новостей за {formatted_date}.
        
        Информация для вступления:
        - Дата: {formatted_date}
        - Всего сообщений: {total_messages}
        - Распределение по категориям:
        {categories_info}
        
        Вступление должно быть лаконичным (1-2 абзаца) и содержать общую характеристику новостей за этот день.
        {"Упомяни, что это краткая версия, и полный текст доступен по ссылкам." if is_brief else "Упомяни, что это подробная версия дайджеста."}
        """
        
        try:
            response = self.llm_model.generate(prompt, max_tokens=300)
            return response
        except Exception as e:
            logger.error(f"Ошибка при генерации вступления к дайджесту: {str(e)}")
            intro_text = f"# Дайджест правовых новостей за {formatted_date}"
            if is_brief:
                intro_text += "\n\n*Краткая версия. Для подробной информации переходите по ссылкам.*"
            else:
                intro_text += "\n\n*Подробная версия дайджеста.*"
            return intro_text

    def create_digest(self, date=None, days_back=1, digest_type="both"):
        """
        Инструмент для создания дайджеста
        
        Args:
            date (datetime, optional): Дата дайджеста (по умолчанию сегодня)
            days_back (int): Количество дней для сбора сообщений
            digest_type (str): Тип дайджеста: "brief" (краткий), "detailed" (подробный), "both" (оба)
            
        Returns:
            dict: Результаты создания дайджеста
        """
        # Определяем даты
        end_date = date or datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Создание дайджеста за период с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}, тип: {digest_type}")
        
        # Счетчики для статистики
        total_messages = 0
        categories_count = {category: 0 for category in CATEGORIES}
        categories_count["другое"] = 0
        
        # Словари для хранения секций разных типов дайджеста
        brief_sections = {}
        detailed_sections = {}
        
        # Словарь для хранения сообщений по категориям (чтобы не запрашивать их дважды)
        messages_by_category = {}
        
        # Собираем сообщения по категориям
        for category in CATEGORIES + ["другое"]:
            # Получаем сообщения для этой категории
            messages = self.db_manager.get_messages_by_date_range(
                start_date=start_date,
                end_date=end_date,
                category=category
            )
            
            messages_by_category[category] = messages
            categories_count[category] = len(messages)
            total_messages += len(messages)
            
            # Если есть сообщения, создаем обзоры нужных типов
            if messages:
                if digest_type in ["brief", "both"]:
                    brief_sections[category] = self._generate_brief_section(category, messages)
                
                if digest_type in ["detailed", "both"]:
                    detailed_sections[category] = self._generate_detailed_section(category, messages)
        
        results = {
            "status": "success",
            "date": end_date.strftime("%Y-%m-%d"),
            "total_messages": total_messages,
            "categories": categories_count,
        }
        
        # Формируем краткий дайджест, если запрошено
        if digest_type in ["brief", "both"]:
            # Генерируем вводную часть
            intro_text = self._generate_digest_intro(end_date, total_messages, categories_count, is_brief=True)
            
            # Формируем полный текст краткого дайджеста
            brief_text = f"{intro_text}\n\n"
            
            for category in CATEGORIES:
                if category in brief_sections:
                    brief_text += f"{brief_sections[category]}\n\n"
            
            # Добавляем категорию "другое" в конец, если есть сообщения
            if "другое" in brief_sections:
                brief_text += f"{brief_sections['другое']}\n\n"
            
            # Добавляем ссылку на подробный дайджест, если генерируются оба
            if digest_type == "both":
                brief_text += "\n\n[Просмотреть подробный дайджест](/digest/detailed)\n"
            
            results["brief_digest_text"] = brief_text
            
            # Сохраняем краткий дайджест в БД
            try:
                brief_digest = self.db_manager.save_digest(
                    end_date, 
                    brief_text, 
                    brief_sections,
                    digest_type="brief"
                )
                results["brief_digest_id"] = brief_digest.id
                logger.info(f"Краткий дайджест успешно создан и сохранен (ID: {brief_digest.id})")
            except Exception as e:
                logger.error(f"Ошибка при сохранении краткого дайджеста: {str(e)}")
                results["brief_error"] = str(e)
        
        # Формируем подробный дайджест, если запрошено
        if digest_type in ["detailed", "both"]:
            # Генерируем вводную часть
            intro_text = self._generate_digest_intro(end_date, total_messages, categories_count, is_brief=False)
            
            # Формируем полный текст подробного дайджеста
            detailed_text = f"{intro_text}\n\n"
            
            for category in CATEGORIES:
                if category in detailed_sections:
                    detailed_text += f"## {category.upper()}\n\n{detailed_sections[category]}\n\n"
            
            # Добавляем категорию "другое" в конец, если есть сообщения
            if "другое" in detailed_sections:
                detailed_text += f"## ДРУГИЕ НОВОСТИ\n\n{detailed_sections['другое']}\n\n"
            
            # Добавляем ссылку на краткий дайджест, если генерируются оба
            if digest_type == "both":
                detailed_text += "\n\n[Просмотреть краткий дайджест](/digest/brief)\n"
            
            results["detailed_digest_text"] = detailed_text
            
            # Сохраняем подробный дайджест в БД
            try:
                detailed_digest = self.db_manager.save_digest(
                    end_date, 
                    detailed_text, 
                    detailed_sections,
                    digest_type="detailed"
                )
                results["detailed_digest_id"] = detailed_digest.id
                logger.info(f"Подробный дайджест успешно создан и сохранен (ID: {detailed_digest.id})")
            except Exception as e:
                logger.error(f"Ошибка при сохранении подробного дайджеста: {str(e)}")
                results["detailed_error"] = str(e)
        
        return results
    
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