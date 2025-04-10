"""
Агент для формирования дайджеста
"""
import logging
import re
from datetime import datetime, timedelta
from crewai import Agent, Task
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import CATEGORIES, BOT_USERNAME
from database.db_manager import DatabaseManager
from llm.gemma_model import GemmaLLM
from langchain.tools import Tool
from datetime import datetime, time, timedelta
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
        Улучшенное извлечение заголовка для URL из @dumainfo
        """
        # Проверка на сообщение из канала думы
        if "@dumainfo" in url or "dumainfo" in text:
            # Разделим по строкам и найдем подходящий заголовок
            lines = text.split('\n')
            for i, line in enumerate(lines):
                # Пропускаем пустые строки и стандартные заголовки
                if len(line.strip()) < 10 or "Государственная Дума" in line:
                    continue
                
                # Берем первую содержательную строку как заголовок
                if len(line.strip()) > 15 and "http" not in line and "@" not in line:
                    return line.strip()
        # Разделим текст на части до и после URL
        parts = text.split(url)
        
        if len(parts) < 2:
            return url[:50] + "..." if len(url) > 50 else url
        
        before_url = parts[0]
        after_url = parts[1]
        
        # Ищем заголовок перед URL
        before_paragraphs = before_url.split('\n\n')
        last_paragraph = before_paragraphs[-1] if before_paragraphs else ""
        
        # Проверяем на стандартные шаблонные заголовки
        if "Государственная Дума" in last_paragraph or "VK" in last_paragraph or len(last_paragraph.strip()) < 20:
            # Ищем более содержательный текст в первых нескольких строках
            lines = text.split('\n')
            for line in lines[1:5]:  # Проверяем первые 5 строк
                line = line.strip()
                if len(line) > 30 and "http" not in line and "Telegram" not in line:
                    return line[:100] + "..." if len(line) > 100 else line
        
        # Далее стандартная логика
        sentences = last_paragraph.split('.')
        candidate_title = sentences[-1].strip() if sentences else last_paragraph.strip()
        
        # Если заголовок слишком короткий, ищем в тексте после URL
        if len(candidate_title) < 15:
            after_paragraphs = after_url.split('\n\n')
            first_paragraph = after_paragraphs[0] if after_paragraphs else ""
            sentences = first_paragraph.split('.')
            candidate_title = sentences[0].strip() if sentences else first_paragraph.strip()
        
        # Очищаем и форматируем заголовок
        candidate_title = candidate_title.replace("\n", " ").strip()
        
        # Ограничиваем длину заголовка
        if len(candidate_title) > 80:
            candidate_title = candidate_title[:77] + "..."
        
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
        """
        # Находим все URL в тексте
        url_pattern = r'https?://[^\s\)\]\>]+'
        urls = re.findall(url_pattern, text)
        
        # Удаляем дубликаты URL, сохраняя первое вхождение каждого URL
        for url in set(urls):
            if urls.count(url) > 1:
                # Находим все позиции этого URL
                positions = [m.start() for m in re.finditer(re.escape(url), text)]
                
                # Сохраняем только первое вхождение
                for pos in positions[1:]:
                    end_pos = pos + len(url)
                    # Проверяем, не является ли это частью markdown ссылки
                    if pos > 0 and text[pos-1:pos] == '(' and end_pos < len(text) and text[end_pos:end_pos+1] == ')':
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
            # Проверяем, уже является ли URL частью markdown ссылки
            if not re.search(r'\[.*?\]\(' + re.escape(url) + r'\)', text):
                # Здесь мы НЕ будем добавлять дублирующий текст
                # Оставляем просто URL как есть или делаем простую ссылку
                # НЕ делаем: new_link = f"[{url}]({url})"
                continue  # Просто пропускаем, не меняем обычные URL
        
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
        """
        logger.info(f"Начало генерации краткого обзора для категории '{category}'. Получено {len(messages)} сообщений.")

        if not messages:
            return f"За данный период новостей по категории '{category}' не обнаружено."
        
        # Формируем список всех сообщений с информацией о ссылках
        all_items = []
        
        for msg in messages:
            try:
                # Проверяем, что msg - это объект сообщения, а не строка
                if hasattr(msg, 'text'):
                    # Ищем ссылки в сообщении
                    links = self._extract_links_and_headlines(msg.text)
                    
                    if links:
                        # Если нашли ссылки, добавляем каждую из них
                        for link in links:
                            all_items.append({
                                "title": link["title"],
                                "url": link["url"],
                                "channel": msg.channel,
                                "date": msg.date,
                                "message_id": msg.id,
                                "has_url": True
                            })
                    else:
                        # Если ссылок нет, добавляем само сообщение
                        # Используем первую строку или первые 100 символов как заголовок
                        first_line = msg.text.split('\n')[0]
                        title = first_line[:100] + "..." if len(first_line) > 100 else first_line
                        
                        all_items.append({
                            "title": title,
                            "url": f"https://t.me/{BOT_USERNAME}?start=msg_{msg.id}",
                            "channel": msg.channel,
                            "date": msg.date,
                            "message_id": msg.id,
                            "has_url": False  # Отмечаем, что это не настоящая ссылка
                        })
                else:
                    # Если msg не является объектом сообщения, логируем и пропускаем
                    logger.warning(f"Элемент в списке сообщений категории '{category}' не является объектом Message: {type(msg)}")
            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения для категории '{category}': {str(e)}")
        
        # Проверяем, есть ли сообщения после фильтрации
        if not all_items:
            logger.warning(f"После фильтрации не осталось сообщений для категории '{category}'")
            return f"За данный период новостей по категории '{category}' не удалось обработать."
        
        # Сортируем сообщения по дате (сначала самые новые)
        all_items.sort(key=lambda x: x["date"], reverse=True)
        
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
        
        # Добавляем все сообщения, включая те, где нет ссылок
        for idx, item in enumerate(all_items):
            formatted_date = item["date"].strftime("%d.%m.%Y")
            channel_name = item["channel"]
            
            # Создаем краткую аннотацию сообщения
            message = self.db_manager.get_message_by_id(item["message_id"])
            annotation = self._generate_short_annotation(message.text)

            if item["has_url"]:
                # Если есть настоящая ссылка, используем HTML-формат
                section_text += f"<b>{idx+1}.</b> <a href='{item['url']}'>{item['title']}</a> - {channel_name}, {formatted_date}\n<i>{annotation}</i>\n\n"
            else:
                # Если нет ссылки, просто выводим текст с HTML-форматированием
                section_text += f"<b>{idx+1}.</b> <b>{item['title']}</b> - {channel_name}, {formatted_date}\n<i>{annotation}</i>\n\n"
        
        # Добавляем ссылку на полный обзор
        section_text += f"\n[Открыть полный обзор по категории '{category}'](/category/{category})\n"
    
        # Удаляем лишние экранирования точек после цифр
        section_text = re.sub(r'(\d+)\\\.\s*', r'\1. ', section_text)
        return section_text

    def _generate_short_annotation(self, text, max_length=150):
        """
        Генерация краткой аннотации сообщения, избегая дублирования заголовка
        """
        # Удаляем URL и лишние пробелы
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Разбиваем на абзацы
        paragraphs = text.split('\n\n')
        
        # Ищем содержательный абзац, отличный от заголовка
        first_paragraph = paragraphs[0] if paragraphs else ""
        content_paragraph = None
        
        # Ищем первый неявляющийся заголовком абзац 
        for paragraph in paragraphs[1:]:
            # Пропускаем короткие или служебные абзацы
            clean_paragraph = paragraph.strip()
            if len(clean_paragraph) < 30 or clean_paragraph.startswith("http") or "@" in clean_paragraph:
                continue
                
            # Используем этот абзац для аннотации
            content_paragraph = clean_paragraph
            break
        
        # Если не нашли подходящий абзац, используем первый
        if not content_paragraph:
            if len(paragraphs) > 1 and len(paragraphs[1].strip()) > 20:
                content_paragraph = paragraphs[1].strip()
            else:
                content_paragraph = first_paragraph
        
        # Берем только первое предложение для аннотации
        sentences = re.split(r'(?<=[.!?])\s+', content_paragraph)
        annotation = sentences[0] if sentences else content_paragraph
        
        # Если предложение слишком длинное, обрезаем
        if len(annotation) > max_length:
            # Ищем последнюю точку перед лимитом
            last_period = annotation[:max_length].rfind('.')
            if last_period > max_length // 2:  # Если точка найдена во второй половине
                annotation = annotation[:last_period+1]
            else:
                # Если точка не найдена или слишком в начале, обрезаем по словам
                words = annotation[:max_length].split()
                annotation = ' '.join(words[:-1]) + '...'
        
        return annotation
    
    def _generate_detailed_section(self, category, messages):
        """
        Генерация подробного обзора по категории
        
        Args:
            category (str): Категория сообщений
            messages (list): Список сообщений этой категории
            
        Returns:
            str: Текст подробного обзора по категории
        """
        logger.info(f"Начало генерации подробного обзора для категории '{category}'. Получено {len(messages)} сообщений.")
        
        if not messages:
            logger.warning(f"Список сообщений для категории '{category}' пуст")
            return f"За данный период новостей по категории '{category}' не обнаружено."
        
        # Логгируем типы первых элементов для отладки
        logger.info(f"Типы первых 3 элементов в списке сообщений для категории '{category}':")
        for i, msg in enumerate(messages[:3]):
            logger.info(f"  Элемент {i}: тип={type(msg)}, атрибуты={dir(msg) if hasattr(msg, '__dict__') else 'Нет атрибутов'}")
        
        # Добавляем иконку к названию категории
        category_icon = self._add_category_icon(category)
        category_display = f"{category_icon} {category}"
        
        # Ограничиваем количество и размер сообщений для запроса
        MAX_MESSAGES = 5  # Ограничиваем кол-во сообщений
        MAX_MESSAGE_LENGTH = 1500  # Ограничиваем длину каждого сообщения
        
        # Очищаем и нормализуем тексты сообщений
        cleaned_messages = []
        for i, msg in enumerate(messages[:MAX_MESSAGES]):
            try:
                # Проверяем, что msg - это объект сообщения, а не строка
                if hasattr(msg, 'text'):
                    # Сокращаем длинные сообщения
                    message_text = msg.text
                    logger.debug(f"Сообщение {i} для категории '{category}': длина текста = {len(message_text)}")
                    
                    if len(message_text) > MAX_MESSAGE_LENGTH:
                        message_text = message_text[:MAX_MESSAGE_LENGTH] + "... (текст сокращен)"
                        
                    cleaned_text = self._clean_text_with_links(message_text)
                    cleaned_messages.append(
                        f"Канал: {msg.channel}\nДата: {msg.date.strftime('%d.%m.%Y')}\n\n{cleaned_text}"
                    )
                else:
                    # Если msg - не объект сообщения, логируем подробную информацию
                    logger.error(f"Сообщение {i} для категории '{category}' не имеет атрибута 'text'")
                    logger.error(f"Тип сообщения: {type(msg)}")
                    if isinstance(msg, dict):
                        logger.error(f"Содержимое словаря: {msg}")
                    elif isinstance(msg, str):
                        logger.error(f"Содержимое строки: {msg[:100]}")
                    else:
                        logger.error(f"Доступные атрибуты: {dir(msg)}")
                    
                    # Пытаемся получить строковое представление объекта
                    logger.error(f"Строковое представление: {str(msg)}")
            except Exception as e:
                logger.exception(f"Ошибка при обработке сообщения {i} для категории '{category}': {str(e)}")
        
        # Проверяем, остались ли сообщения после фильтрации
        if not cleaned_messages:
            logger.warning(f"После фильтрации не осталось сообщений для категории '{category}'")
            return f"За данный период новостей по категории '{category}' не удалось обработать."
        
        logger.info(f"Успешно подготовлено {len(cleaned_messages)} сообщений для категории '{category}'")
        
        # Формируем контекст из очищенных сообщений для LLM
        messages_text = "\n\n---\n\n".join(cleaned_messages)
        
        try:
            # Более короткий и точный промпт
            prompt = f"""
            Составь краткий обзор новостей категории '{category}' на основе следующих сообщений:
            
            {messages_text}
            
            Обзор должен:
            1. Объединить связанные сообщения
            2. Упомянуть источники (каналы)
            3. Сохранить важные детали
            4. Использовать **полужирное выделение** для ключевых терминов
            5. Быть 2-3 абзаца длиной
            """
            
            logger.info(f"Отправка запроса к LLM для категории '{category}'")
            response = self.llm_model.generate(prompt, max_tokens=1500, temperature=0.7)
            
            if not response or len(response.strip()) < 50:
                logger.warning(f"Получен пустой или слишком короткий ответ для категории '{category}'")
                raise ValueError("Получен пустой или слишком короткий ответ")
                
            logger.info(f"Успешно получен ответ для категории '{category}', длина: {len(response)} символов")
            return response
            
        except Exception as e:
            logger.error(f"Ошибка при генерации подробного обзора по категории '{category}': {str(e)}", exc_info=True)
            
            # Создаем базовый обзор на основе имеющихся сообщений
            fallback_text = f"Обзор новостей категории '{category}':\n\n"
            for i, msg in enumerate(messages[:5]):
                try:
                    if hasattr(msg, 'channel') and hasattr(msg, 'date') and hasattr(msg, 'text'):
                        channel_name = msg.channel
                        date_str = msg.date.strftime("%d.%m.%Y")
                        
                        # Извлекаем заголовок сообщения или первую строку
                        lines = msg.text.split('\n')
                        title = lines[0][:100]
                        if len(title) == 100:
                            title += "..."
                            
                        fallback_text += f"**{i+1}.** {title} (Источник: {channel_name}, {date_str})\n\n"
                    else:
                        logger.warning(f"Пропуск сообщения {i} при создании резервного текста - нет необходимых атрибутов")
                except Exception as inner_e:
                    logger.error(f"Ошибка при формировании резервного текста для сообщения {i}: {str(inner_e)}")
            
            logger.info(f"Создан резервный текст для категории '{category}', длина: {len(fallback_text)} символов")
            return fallback_text

    def _generate_digest_intro(self, date, total_messages, categories_count, is_brief=True, days_back=1):
        """
        Генерация вводной части дайджеста
    
        Args:
            date (datetime): Дата дайджеста
            total_messages (int): Общее количество сообщений
            categories_count (dict): Количество сообщений по категориям
            is_brief (bool): Признак краткого дайджеста
            days_back (int): Количество дней, за которые формируется дайджест
            
        Returns:
            str: Текст вводной части
        """
        formatted_date = date.strftime("%d.%m.%Y")
        
        # Формируем строку с периодом
        period_text = formatted_date
        if days_back > 1:
            start_date = (date - timedelta(days=days_back-1)).strftime("%d.%m.%Y")
            period_text = f"период с {start_date} по {formatted_date}"
        
        categories_info = "\n".join([f"- {cat}: {count} сообщений" for cat, count in categories_count.items() if count > 0])
        
        prompt = f"""
        Напиши краткое вступление к {"краткому" if is_brief else "подробному"} дайджесту правовых новостей за {period_text}.
        
        Информация для вступления:
        - Период: {period_text}
        - Всего сообщений: {total_messages}
        - Распределение по категориям:
        {categories_info}
        
        Вступление должно быть лаконичным (1-2 абзаца) и содержать общую характеристику новостей за этот период.
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
    
    def _process_categories_parallel(self, categories_to_process, messages_by_category, digest_type):
        """
        Параллельная обработка секций дайджеста
        """
        results = {}
        
        logger.info(f"Начало параллельной обработки {len(categories_to_process)} категорий для типа дайджеста '{digest_type}'")
        for category in categories_to_process:
            logger.info(f"Категория '{category}': {len(messages_by_category[category])} сообщений")
        
        with ThreadPoolExecutor(max_workers=min(4, len(categories_to_process))) as executor:
            future_to_category = {}
            
            for category in categories_to_process:
                logger.info(f"Отправка задачи на обработку категории '{category}'")
                if digest_type == "brief":
                    future = executor.submit(
                        self._generate_brief_section, category, messages_by_category[category]
                    )
                else:
                    future = executor.submit(
                        self._generate_detailed_section, category, messages_by_category[category]
                    )
                future_to_category[future] = category
            
            for future in as_completed(future_to_category):
                category = future_to_category[future]
                try:
                    logger.info(f"Получение результата для категории '{category}'")
                    section_text = future.result()
                    results[category] = section_text
                    logger.info(f"Успешно обработана категория '{category}', длина текста: {len(section_text)} символов")
                except Exception as e:
                    logger.error(f"Ошибка при обработке категории '{category}': {str(e)}", exc_info=True)
        
        logger.info(f"Завершена параллельная обработка категорий. Обработано {len(results)} из {len(categories_to_process)}")
        return results

    def create_digest(self, date=None, days_back=1, digest_type="both", 
                update_existing=True, focus_category=None,
                channels=None, keywords=None, digest_id=None):
        """
        Инструмент для создания дайджеста с расширенными параметрами
        
        Args:
            date (datetime, optional): Дата дайджеста (по умолчанию сегодня)
            days_back (int): Количество дней для сбора сообщений
            digest_type (str): Тип дайджеста: "brief", "detailed", "both"
            update_existing (bool): Обновлять существующий дайджест или создать новый
            focus_category (str, optional): Фокус на определенную категорию
            channels (list, optional): Список каналов для фильтрации
            keywords (list, optional): Ключевые слова для фильтрации
            digest_id (int, optional): ID существующего дайджеста для обновления
            
        Returns:
            dict: Результаты создания дайджеста
        """
        logger.info(f"Запрос на создание дайджеста: date={date}, days_back={days_back}, тип={digest_type}")
    
        # Определяем конечную дату
        if date:
            # Если задана конкретная дата, используем конец этого дня
            end_date = datetime.combine(date.date() if isinstance(date, datetime) else date, 
                                    time(23, 59, 59))
            
            # Начальная дата - это начало указанной даты минус (days_back-1) дней
            if days_back == 1:
                # Если запрошен 1 день, используем только указанную дату
                start_date = datetime.combine(end_date.date(), time(0, 0, 0))
                logger.info(f"Используем конкретную дату: с {start_date.strftime('%Y-%m-%d %H:%M')} "
                        f"по {end_date.strftime('%Y-%m-%d %H:%M')}")
            else:
                # Если запрошено больше дней, отсчитываем назад
                start_date = (end_date - timedelta(days=days_back-1)).replace(hour=0, minute=0, second=0)
                logger.info(f"Используем период из {days_back} дней: с {start_date.strftime('%Y-%m-%d %H:%M')} "
                        f"по {end_date.strftime('%Y-%m-%d %H:%M')}")
        else:
            # Если дата не задана, используем текущую дату и время
            end_date = datetime.now()
            
            if days_back == 1:
                # Для одного дня - только текущие сутки
                start_date = datetime.combine(end_date.date(), time(0, 0, 0))
                logger.info(f"Используем текущий день: с {start_date.strftime('%Y-%m-%d %H:%M')} "
                        f"по {end_date.strftime('%Y-%m-%d %H:%M')}")
            else:
                # Для нескольких дней - соответствующий период
                start_date = (end_date - timedelta(days=days_back-1)).replace(hour=0, minute=0, second=0)
                logger.info(f"Используем период из {days_back} дней до текущего момента: "
                        f"с {start_date.strftime('%Y-%m-%d %H:%M')} по {end_date.strftime('%Y-%m-%d %H:%M')}")
        
        logger.info(f"Создание дайджеста за период с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}, тип: {digest_type}")
        
        
        # БЛОК 3: ПОЛУЧЕНИЕ СООБЩЕНИЙ - УСТРАНЕНА ДУБЛИКАЦИЯ ВЫЗОВА
        filter_result = self.db_manager.get_filtered_messages(
            start_date=start_date,
            end_date=end_date,
            category=focus_category,
            channels=channels,
            keywords=keywords
        )
        
        # Извлекаем список сообщений из результата
        if isinstance(filter_result, dict) and "messages" in filter_result:
            messages = filter_result["messages"]
            logger.info(f"Получено {len(messages)} сообщений из {filter_result.get('total', 0)} доступных")
        else:
            messages = filter_result  # На случай, если формат возврата изменится
            logger.info(f"Получено {len(messages)} сообщений (прямой результат)")
        
        # Если сообщений нет, проверим все сообщения за указанный период без фильтров
        if not messages:
            logger.warning("Не найдено сообщений с указанными фильтрами, пробуем получить все сообщения за период")
            
            # Пробуем получить любые сообщения за этот период
            all_messages = self.db_manager.get_messages_by_date_range(start_date, end_date)
            
            if all_messages:
                logger.info(f"Найдено {len(all_messages)} сообщений без применения фильтров")
                messages = all_messages
            else:
                logger.info("Сообщения за указанный период не найдены, запускаем сбор из Telegram...")
                from agents.data_collector import DataCollectorAgent
                import asyncio
                
                collector = DataCollectorAgent(self.db_manager)
                
                # Создаем цикл событий для синхронного вызова асинхронной функции
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    # Запускаем сбор с явным указанием start_date и end_date
                    collect_result = loop.run_until_complete(collector.collect_data(
                        days_back=days_back,
                        force_update=True,
                        start_date=start_date,
                        end_date=end_date
                    ))
                    
                    logger.info(f"Результат сбора данных: {collect_result}")
                    
                    # Проверяем снова после сбора
                    messages = self.db_manager.get_messages_by_date_range(start_date, end_date)
                finally:
                    loop.close()
                    
                if not messages:
                    logger.error("Сообщения за указанный период не найдены даже после сбора из Telegram")
                    return {
                        "status": "no_messages",
                        "message": "Нет сообщений, соответствующих критериям фильтрации"
                    }
        
        # Оставшаяся часть метода без изменений...
        # Группировка по категориям
        messages_by_category = {}
        categories_count = {category: 0 for category in CATEGORIES}
        categories_count["другое"] = 0
        total_messages = 0
        
        for msg in messages:
            if not hasattr(msg, 'category') or not hasattr(msg, 'text'):
                logger.warning(f"Пропуск объекта, не являющегося сообщением: {type(msg)}")
                continue
                
            category = msg.category if msg.category else "другое"
            if category not in messages_by_category:
                messages_by_category[category] = []
            messages_by_category[category].append(msg)
            
            if category in categories_count:
                categories_count[category] += 1
            else:
                categories_count["другое"] += 1
            
            total_messages += 1
        
        
        # Если после фильтрации не осталось сообщений
        if total_messages == 0:
            logger.error("После группировки по категориям не осталось подходящих сообщений")
            return {
                "status": "no_messages",
                "message": "Нет подходящих сообщений для формирования дайджеста"
            }

        # В методе create_digest добавьте следующий код после группировки сообщений по категориям:

        logger.info(f"Группировка сообщений по категориям завершена. Всего категорий: {len(messages_by_category)}")
        for category, msgs in messages_by_category.items():
            logger.info(f"Категория '{category}': {len(msgs)} сообщений")
            # Проверяем типы первых трех элементов для отладки
            for i, msg in enumerate(msgs[:3]):
                logger.info(f"  Сообщение {i} для '{category}': тип={type(msg)}, имеет атрибут 'text'={hasattr(msg, 'text')}")

        # Формируем секции дайджеста в зависимости от типа
        brief_sections = {}
        detailed_sections = {}
        
        if digest_type in ["brief", "both"]:
            # Параллельная обработка категорий для краткого дайджеста
            categories_to_process = [cat for cat in messages_by_category.keys()]
            brief_sections = self._process_categories_parallel(
                categories_to_process, messages_by_category, "brief"
            )
        
        if digest_type in ["detailed", "both"]:
            # Параллельная обработка категорий для подробного дайджеста
            categories_to_process = [cat for cat in messages_by_category.keys()]
            detailed_sections = self._process_categories_parallel(
                categories_to_process, messages_by_category, "detailed"
            )

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
                intro_text = self._generate_digest_intro(
                    end_date, total_messages, categories_count, 
                    is_brief=True, days_back=days_back
                )
                
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
                
                # Сохраняем краткий дайджест в БД с параметрами
                try:
                    brief_result = self.db_manager.save_digest_with_parameters(
                        end_date, 
                        brief_text, 
                        brief_sections,
                        digest_type="brief",
                        date_range_start=start_date,
                        date_range_end=end_date,
                        focus_category=focus_category,
                        channels_filter=channels,
                        keywords_filter=keywords,
                        digest_id=digest_id if digest_type == "brief" else None
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
        # Формируем подробный дайджест, если запрошено
        if digest_type in ["detailed", "both"]:
            try:
                # Генерируем вводную часть
                intro_text = self._generate_digest_intro(
                    end_date, total_messages, categories_count, 
                    is_brief=False, days_back=days_back
                )
                
                # Формируем полный текст подробного дайджеста
                detailed_text = f"{intro_text}\n\n"
                
                # Добавляем секции по категориям в порядке значимости
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
                
                # Сохраняем подробный дайджест в БД с параметрами
                try:
                    detailed_result = self.db_manager.save_digest_with_parameters(
                        end_date, 
                        detailed_text, 
                        detailed_sections,
                        digest_type="detailed",
                        date_range_start=start_date,
                        date_range_end=end_date,
                        focus_category=focus_category,
                        channels_filter=channels,
                        keywords_filter=keywords,
                        digest_id=digest_id if digest_type == "detailed" else None
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
    def update_digests_for_date(self, date):
        """
        Обновляет все дайджесты, содержащие указанную дату
        
        Args:
            date (datetime): Дата для обновления дайджестов
            
        Returns:
            dict: Результаты обновления
        """
        logger.info(f"Обновление дайджестов, содержащих дату {date.strftime('%Y-%m-%d')}")
        
        # Получаем все дайджесты, включающие эту дату
        digests = self.db_manager.get_digests_containing_date(date)
        
        if not digests:
            logger.info(f"Дайджесты, содержащие дату {date.strftime('%Y-%m-%d')}, не найдены")
            return {"status": "no_digests", "date": date.strftime('%Y-%m-%d')}
        
        results = {"updated_digests": []}
        
        for digest in digests:
            # Извлекаем параметры для создания нового дайджеста
            digest_date = digest["date"]
            digest_type = digest["digest_type"]
            focus_category = digest["focus_category"]
            channels = digest["channels_filter"]
            keywords = digest["keywords_filter"]
            
            # Определяем период для обновления
            if digest["date_range_start"] and digest["date_range_end"]:
                start_date = digest["date_range_start"]
                end_date = digest["date_range_end"]
                days_back = (end_date - start_date).days + 1
            else:
                # Если диапазон не указан, считаем, что это дайджест за один день
                start_date = end_date = digest_date
                days_back = 1
            
            try:
                # Обновляем дайджест с теми же параметрами
                result = self.create_digest(
                    date=end_date,
                    days_back=days_back,
                    digest_type=digest_type,
                    update_existing=True,
                    focus_category=focus_category,
                    channels=channels,
                    keywords=keywords,
                    digest_id=digest["id"]
                )
                
                results["updated_digests"].append({
                    "digest_id": digest["id"],
                    "digest_type": digest_type,
                    "date": end_date.strftime('%Y-%m-%d'),
                    "status": "success"
                })
                
                logger.info(f"Дайджест ID {digest['id']} успешно обновлен")
            except Exception as e:
                logger.error(f"Ошибка при обновлении дайджеста ID {digest['id']}: {str(e)}")
                results["updated_digests"].append({
                    "digest_id": digest["id"],
                    "digest_type": digest_type,
                    "date": end_date.strftime('%Y-%m-%d'),
                    "status": "error",
                    "error": str(e)
                })
        
        logger.info(f"Обновлено {len(results['updated_digests'])} дайджестов для даты {date.strftime('%Y-%m-%d')}")
        return results
    def save_digest_with_parameters(self, date, text, sections, digest_type="brief", 
                              date_range_start=None, date_range_end=None, 
                              focus_category=None, channels_filter=None, 
                              keywords_filter=None, digest_id=None):
        """
        Сохранение дайджеста с расширенными параметрами
        
        Args:
            date (datetime): Дата дайджеста
            text (str): Текст дайджеста
            sections (dict): Словарь секций
            digest_type (str): Тип дайджеста
            date_range_start (datetime): Начальная дата диапазона
            date_range_end (datetime): Конечная дата диапазона
            focus_category (str): Фокусная категория
            channels_filter (list): Список каналов для фильтрации
            keywords_filter (list): Список ключевых слов для фильтрации
            digest_id (int): ID существующего дайджеста для обновления
            
        Returns:
            dict: Информация о созданном дайджесте
        """
        # Определяем признак дайджеста за текущий день
        today = datetime.now().date()
        is_today_digest = date.date() == today
        
        # Сохраняем дайджест в БД
        result = self.db_manager.save_digest_with_parameters(
            date=date,
            text=text,
            sections=sections,
            digest_type=digest_type,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
            focus_category=focus_category,
            channels_filter=channels_filter,
            keywords_filter=keywords_filter,
            digest_id=digest_id,
            is_today=is_today_digest,
            last_updated=datetime.now()  # Всегда обновляем время последнего обновления
        )
        
        logger.info(f"Сохранен дайджест типа '{digest_type}' за {date.strftime('%Y-%m-%d')}, ID: {result['id']}")
        return result