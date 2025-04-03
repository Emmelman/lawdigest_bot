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
    def _extract_title_for_url(self, text, url):
        """
        Улучшенный метод извлечения заголовка для URL
        
        Args:
            text (str): Полный текст сообщения
            url (str): URL для которого нужно найти заголовок
            
        Returns:
            str: Извлеченный заголовок
        """
        # Разделим текст на части до и после URL
        parts = text.split(url)
        
        if len(parts) < 2:
            return url[:50] + "..." if len(url) > 50 else url
        
        before_url = parts[0]
        after_url = parts[1]
        
        # Ищем заголовок перед URL
        # Разбиваем текст на абзацы и берем последний перед URL
        before_paragraphs = before_url.split('\n\n')
        last_paragraph = before_paragraphs[-1] if before_paragraphs else ""
        
        # Для улучшения точности можно разбить на предложения
        sentences = last_paragraph.split('.')
        
        # Берем последнее предложение, которое обычно содержит заголовок
        candidate_title = sentences[-1].strip() if sentences else last_paragraph.strip()
        
        # Если заголовок слишком короткий или его нет, ищем в тексте после URL
        if len(candidate_title) < 15:
            after_paragraphs = after_url.split('\n\n')
            first_paragraph = after_paragraphs[0] if after_paragraphs else ""
            sentences = first_paragraph.split('.')
            candidate_title = sentences[0].strip() if sentences else first_paragraph.strip()
        
        # Окончательная проверка заголовка
        if len(candidate_title) < 10:
            # Если все еще нет подходящего заголовка, используем название домена из URL
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
            return f"Ссылка на {domain}"
        
        # Очищаем заголовок от лишних символов
        candidate_title = candidate_title.replace("\n", " ").strip()
        
        # Восстановление обрезанных слов в начале 
        # (как в примере "[ная Дума (VK)" -> "Государственная Дума (VK)")
        if candidate_title.startswith("[") and "]" not in candidate_title[:15]:
            # Ищем полное слово в тексте
            words = candidate_title.split()
            if words and words[0].startswith("["):
                first_word = words[0][1:]  # Удаляем открывающую скобку
                
                # Ищем полное слово в тексте
                possible_words = [word for word in text.split() 
                                if word.endswith(first_word) or first_word in word]
                
                if possible_words:
                    # Берем самое длинное подходящее слово
                    full_word = max(possible_words, key=len)
                    candidate_title = candidate_title.replace(words[0], "[" + full_word)
        
        return candidate_title
    def _add_category_icon(self, category):
        """
        Добавляет иконку в зависимости от категории
        
        Args:
            category (str): Название категории
            
        Returns:
            str: Иконка для категории
        """
        icons = {
            'законодательные инициативы': '📝',
            'новая судебная практика': '⚖️',
            'новые законы': '📜',
            'поправки к законам': '✏️',
            'другое': '📌'
        }
        return icons.get(category, '•')

    def _clean_text_with_links(self, text):
        """
        Очищает текст от дублирующихся ссылок и нормализует форматирование
        
        Args:
            text (str): Исходный текст с ссылками
            
        Returns:
            str: Очищенный текст
        """
        # Находим все URL в тексте
        url_pattern = r'https?://[^\s\)\]\>]+'
        urls = re.findall(url_pattern, text)
        
        # Удаляем дубликаты URL, сохраняя первое вхождение каждого URL
        for url in set(urls):
            if urls.count(url) > 1:
                # Находим все позиции этого URL
                positions = [text.find(url, pos) for pos in range(len(text)) 
                            if text.find(url, pos) != -1]
                
                # Сохраняем только первое вхождение
                for pos in positions[1:]:
                    end_pos = pos + len(url)
                    # Проверяем, не является ли это частью markdown ссылки
                    if (pos > 0 and text[pos-1] == '(' and end_pos < len(text) and text[end_pos] == ')'):
                        # Находим открывающую скобку перед URL
                        bracket_pos = text.rfind('[', 0, pos)
                        if bracket_pos != -1:
                            # Это часть markdown ссылки, не удаляем
                            continue
                    
                    # Удаляем URL
                    text = text[:pos] + text[end_pos:]
                    # Корректируем позиции остальных вхождений
                    positions = [p - len(url) if p > pos else p for p in positions]
        
        # Заменяем обычные URL на markdown ссылки, если они не являются частью markdown
        for url in set(urls):
            # Проверяем, не является ли URL частью markdown ссылки
            markdown_pattern = r'\[(.*?)\]\(' + re.escape(url) + r'\)'
            if not re.search(markdown_pattern, text):
                # Заменяем URL на markdown ссылку с тем же URL в качестве текста
                short_url = url.replace('https://', '').replace('http://', '')
                if len(short_url) > 30:
                    short_url = short_url[:30] + "..."
                new_link = f"[{short_url}]({url})"
                text = text.replace(url, new_link)
        
        return text
    def _extract_links_and_headlines(self, text):
        """
        Улучшенное извлечение ссылок и заголовков из текста сообщения
        
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
            # Удостоверимся, что заголовок не пустой и содержательный
            if title and len(title.strip()) > 3:
                results.append({
                    "title": title.strip(),
                    "url": url.strip(),
                    "is_markdown": True
                })
        
        # Шаблон для поиска обычных URL
        url_pattern = r'https?://[^\s\)\]\>]+'
        
        # Находим URL, которые не были найдены в markdown формате
        all_urls = re.findall(url_pattern, text)
        markdown_urls = [link[1] for link in markdown_links]
        
        for url in all_urls:
            if url not in markdown_urls and url.strip():
                # Извлекаем контекст для этого URL
                title = self._extract_title_for_url(text, url)
                
                results.append({
                    "title": title,
                    "url": url.strip(),
                    "is_markdown": False
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
                link["message_id"] = msg.id
                all_links.append(link)
        
        # Если нет ссылок в сообщениях, генерируем их из текста сообщений
        if not all_links:
            for msg in messages[:10]:  # Ограничиваем до 10 сообщений
                # Создаем ссылку на сообщение в боте
                bot_link = f"https://t.me/{BOT_USERNAME}?start=msg_{msg.id}"
                
                # Получаем первые 100 символов как заголовок, удаляем переносы строк
                title = msg.text[:100].replace('\n', ' ').strip()
                title = title + "..." if len(msg.text) > 100 else title
                
                all_links.append({
                    "title": title,
                    "url": bot_link,
                    "channel": msg.channel,
                    "date": msg.date,
                    "message_id": msg.id,
                    "is_markdown": False
                })
        
        # Сортируем ссылки по дате (сначала самые новые)
        all_links.sort(key=lambda x: x["date"], reverse=True)
        
        # Формируем текст секции
        category_icon = self._add_category_icon(category)
        section_text = f"## {category_icon} {category.upper()}\n\n"
        
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
            
            # Проверяем и исправляем заголовок
            title = link["title"]
            if title.startswith("[") and not title.endswith("]") and "]" not in title[:15]:
                # Ищем полное слово
                title = self._extract_title_for_url(messages[0].text, link["url"])
            
            section_text += f"{idx+1}. [{title}]({link['url']}) - {channel_name}, {formatted_date}\n\n"
        
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
        
        # Добавляем иконку к названию категории
        category_icon = self._add_category_icon(category)
        category_display = f"{category_icon} {category}"
        
        # Очищаем и нормализуем тексты сообщений
        cleaned_messages = []
        for msg in messages[:15]:  # Ограничиваем до 15 сообщений для контекста
            cleaned_text = self._clean_text_with_links(msg.text)
            cleaned_messages.append(
                f"Канал: {msg.channel}\nДата: {msg.date.strftime('%d.%m.%Y')}\n\n{cleaned_text}"
            )
        
        # Формируем контекст из очищенных сообщений для LLM
        messages_text = "\n\n---\n\n".join(cleaned_messages)
        
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
        8. ВАЖНО: сохрани все markdown-ссылки в формате [текст](ссылка). НЕ ДУБЛИРУЙ ссылки в простом текстовом виде.
        9. Для отдельных разделов или важных пунктов используй символы/эмодзи в начале строки (📝, ⚖️, 📜, ✏️ и т.д.)
        10. Объем: 3-5 содержательных абзацев.
        """
        
        try:
            response = self.llm_model.generate(prompt, max_tokens=1000)
            # Дополнительная очистка результата от возможных дублирующихся ссылок
            cleaned_response = self._clean_text_with_links(response)
            return cleaned_response
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
            try:
                # Генерируем вводную часть
                intro_text = self._generate_digest_intro(end_date, total_messages, categories_count, is_brief=True)
                
                # Формируем полный текст краткого дайджеста
                brief_text = f"{intro_text}\n\n"
                
                # Сначала добавляем категории с сообщениями в порядке значимости
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
                    brief_result = self.db_manager.save_digest(
                        end_date, 
                        brief_text, 
                        brief_sections,
                        digest_type="brief"
                    )
                    results["brief_digest_id"] = brief_result["id"]
                    logger.info(f"Краткий дайджест успешно создан и сохранен (ID: {brief_result['id']})")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении краткого дайджеста: {str(e)}")
                    results["brief_error"] = str(e)
            except Exception as e:
                logger.error(f"Ошибка при создании краткого дайджеста: {str(e)}")
                results["brief_error"] = str(e)
        
        # Формируем подробный дайджест, если запрошено
        if digest_type in ["detailed", "both"]:
            try:
                # Генерируем вводную часть
                intro_text = self._generate_digest_intro(end_date, total_messages, categories_count, is_brief=False)
                
                # Формируем полный текст подробного дайджеста
                detailed_text = f"{intro_text}\n\n"
                
                # Сначала добавляем категории с сообщениями в порядке значимости
                for category in CATEGORIES:
                    if category in detailed_sections:
                        category_icon = self._add_category_icon(category)
                        detailed_text += f"## {category_icon} {category.upper()}\n\n{detailed_sections[category]}\n\n"
                
                # Добавляем категорию "другое" в конец, если есть сообщения
                if "другое" in detailed_sections:
                    category_icon = self._add_category_icon("другое")
                    detailed_text += f"## {category_icon} ДРУГИЕ НОВОСТИ\n\n{detailed_sections['другое']}\n\n"
                
                # Добавляем ссылку на краткий дайджест, если генерируются оба
                if digest_type == "both":
                    detailed_text += "\n\n[Просмотреть краткий дайджест](/digest/brief)\n"
                
                results["detailed_digest_text"] = detailed_text
                
                # Сохраняем подробный дайджест в БД
                try:
                    detailed_result = self.db_manager.save_digest(
                        end_date, 
                        detailed_text, 
                        detailed_sections,
                        digest_type="detailed"
                    )
                    results["detailed_digest_id"] = detailed_result["id"]
                    logger.info(f"Подробный дайджест успешно создан и сохранен (ID: {detailed_result['id']})")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении подробного дайджеста: {str(e)}")
                    results["detailed_error"] = str(e)
            except Exception as e:
                logger.error(f"Ошибка при создании подробного дайджеста: {str(e)}")
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