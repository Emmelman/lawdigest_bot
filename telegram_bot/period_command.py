"""
Улучшенный обработчик команды /period для генерации дайджеста за произвольный период,
включая поддержку ключевых слов "сегодня" и "вчера"
"""
import logging
import re
from datetime import time, datetime, timedelta
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from agents.data_collector import DataCollectorAgent
from agents.analyzer import AnalyzerAgent
from agents.critic import CriticAgent
from agents.digester import DigesterAgent
from llm.qwen_model import QwenLLM
from llm.gemma_model import GemmaLLM
from utils.text_utils import TextUtils

logger = logging.getLogger(__name__)

# Утилиты для работы с текстом
utils = TextUtils()

async def period_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /period - генерация дайджеста за произвольный период"""
    # Проверяем, есть ли аргументы
    if not context.args:
        # Показываем инструкцию по использованию команды
        await update.message.reply_text(
            "Команда позволяет получить дайджест за указанный период.\n\n"
            "Форматы:\n"
            "• /period сегодня - дайджест за сегодня\n"
            "• /period вчера - дайджест за вчерашний день\n"
            "• /period YYYY-MM-DD - дайджест за указанную дату\n"
            "• /period YYYY-MM-DD YYYY-MM-DD - дайджест за период\n\n"
            "Указание типа (опционально):\n"
            "• /period сегодня brief - краткий дайджест (по умолчанию)\n"
            "• /period вчера detailed - подробный дайджест\n"
            "• /period 2025-04-01 both - оба типа дайджеста\n"
            "• /period 2025-04-01 2025-04-10 both - оба типа дайджеста"
        )
        return
    
    # Разбираем аргументы
    digest_type = "brief"  # Тип дайджеста по умолчанию
    force_update = False   # Флаг для принудительного обновления
    today = datetime.now().date()
    is_today_request = False  # Флаг запроса дайджеста за сегодня
    
    # Проверяем первый аргумент на ключевые слова
    if context.args[0].lower() in ["сегодня", "today"]:
        start_date = datetime.combine(today, time.min)
        end_date = datetime.now()  # Текущее время для сегодняшнего дня
        start_date_str = today.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d %H:%M")
        period_description = f"за сегодня (до {end_date.strftime('%H:%M')})"
        is_today_request = True
        force_update = True  # Всегда обновляем для сегодняшнего дня
        
        # Проверяем, есть ли указание типа дайджеста
        if len(context.args) > 1:
            digest_type_arg = context.args[1].lower()
            if digest_type_arg in ["detailed", "full", "подробный", "полный"]:
                digest_type = "detailed"
            elif digest_type_arg in ["both", "оба"]:
                digest_type = "both"
    
    elif context.args[0].lower() in ["вчера", "yesterday"]:
        yesterday = today - timedelta(days=1)
        start_date = datetime.combine(yesterday, time.min)
        end_date = datetime.combine(yesterday, time.max)
        start_date_str = end_date_str = yesterday.strftime("%Y-%m-%d")
        period_description = "за вчера"
        
        # Проверяем, есть ли указание типа дайджеста
        if len(context.args) > 1:
            digest_type_arg = context.args[1].lower()
            if digest_type_arg in ["detailed", "full", "подробный", "полный"]:
                digest_type = "detailed"
            elif digest_type_arg in ["both", "оба"]:
                digest_type = "both"
    
    else:
        # Обрабатываем разные форматы ввода с датами
        if len(context.args) == 1:
            # Один аргумент - только дата
            try:
                # Проверяем, может быть это период в одном аргументе через дефис
                if "-" in context.args[0] and len(context.args[0].split("-")) > 3:
                    # Формат: 2025-04-01-2025-04-10
                    date_parts = context.args[0].split("-")
                    if len(date_parts) >= 6:
                        start_date_str = f"{date_parts[0]}-{date_parts[1]}-{date_parts[2]}"
                        end_date_str = f"{date_parts[3]}-{date_parts[4]}-{date_parts[5]}"
                        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                        period_description = f"за период с {start_date_str} по {end_date_str}"
                    else:
                        raise ValueError("Некорректный формат периода")
                else:
                    # Только одна дата
                    start_date_str = end_date_str = context.args[0]
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                    period_description = f"за {start_date_str}"
                    
                    # Проверяем, не "сегодня" ли это
                    if start_date.date() == today:
                        is_today_request = True
                        end_date = datetime.now()  # Текущее время для сегодняшнего дня
                        period_description = f"за сегодня (до {end_date.strftime('%H:%M')})"
                        force_update = True
            except Exception as e:
                await update.message.reply_text(
                    f"Ошибка при разборе даты: {str(e)}\n"
                    f"Используйте формат YYYY-MM-DD или ключевые слова 'сегодня'/'вчера'"
                )
                return
        elif len(context.args) == 2:
            # Проверяем, может быть второй аргумент это тип дайджеста
            if context.args[1].lower() in ["brief", "detailed", "both", "краткий", "подробный", "оба"]:
                start_date_str = end_date_str = context.args[0]
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                period_description = f"за {start_date_str}"
                
                # Проверяем, не "сегодня" ли это
                if start_date.date() == today:
                    is_today_request = True
                    end_date = datetime.now()  # Текущее время для сегодняшнего дня
                    period_description = f"за сегодня (до {end_date.strftime('%H:%M')})"
                    force_update = True
                
                digest_type_arg = context.args[1].lower()
                if digest_type_arg in ["detailed", "full", "подробный", "полный"]:
                    digest_type = "detailed"
                elif digest_type_arg in ["both", "оба"]:
                    digest_type = "both"
            else:
                # Два аргумента - начальная и конечная даты
                start_date_str = context.args[0]
                end_date_str = context.args[1]
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                period_description = f"за период с {start_date_str} по {end_date_str}"
                
                # Проверяем, содержит ли период только сегодняшний день
                if start_date.date() == today and end_date.date() == today:
                    is_today_request = True
                    end_date = datetime.now()  # Текущее время для сегодняшнего дня
                    period_description = f"за сегодня (до {end_date.strftime('%H:%M')})"
                    force_update = True
        elif len(context.args) >= 3:
            # Три и более аргумента - даты и тип дайджеста
            start_date_str = context.args[0]
            end_date_str = context.args[1]
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            period_description = f"за период с {start_date_str} по {end_date_str}"
            
            # Проверяем, содержит ли период только сегодняшний день
            if start_date.date() == today and end_date.date() == today:
                is_today_request = True
                end_date = datetime.now()  # Текущее время для сегодняшнего дня
                period_description = f"за сегодня (до {end_date.strftime('%H:%M')})"
                force_update = True
            
            # Получаем тип дайджеста
            digest_type_arg = context.args[2].lower()
            if digest_type_arg in ["detailed", "full", "подробный", "полный"]:
                digest_type = "detailed"
            elif digest_type_arg in ["both", "оба"]:
                digest_type = "both"
        
        # Проверяем формат дат
        try:
            # Проверка уже выполнена выше, но на всякий случай оставляем дополнительную проверку
            if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
                raise ValueError("Даты не были правильно преобразованы")
        except ValueError:
            await update.message.reply_text(
                "Ошибка формата даты. Используйте формат YYYY-MM-DD (например, 2025-04-01) "
                "или ключевые слова 'сегодня'/'вчера'."
            )
            return
    
    # Проверяем, что начальная дата не позже конечной
    if start_date > end_date:
        await update.message.reply_text(
            "Ошибка: начальная дата позже конечной. Пожалуйста, укажите корректный период."
        )
        return
    
    # Рассчитываем количество дней в периоде
    days_in_period = (end_date.date() - start_date.date()).days + 1
    
    if days_in_period > 60:
        await update.message.reply_text(
            f"Указан слишком длинный период ({days_in_period} дней). "
            f"Максимальный период - 60 дней. Пожалуйста, укажите более короткий период."
        )
        return
    
    # Отправляем сообщение о начале сбора данных
    status_message = await update.message.reply_text(
        f"Начинаю создание {get_digest_type_name(digest_type)} дайджеста {period_description}.\n\n"
        f"Сбор данных... ⏳"
    )
    
    # Шаг 1: Проверяем наличие существующего дайджеста за указанный период
    try:
        existing_digests = None
        # Для запроса "за сегодня" используем особую логику
        # Для запроса "за сегодня" используем особую логику
        if is_today_request:
            # Ищем дайджест за сегодня с приоритетом дайджестов с is_today=True
            today_digests = db_manager.find_digests_by_parameters(
                is_today=True,
                limit=10
            )
            
            if not today_digests:
                # Если не нашли по is_today, ищем по диапазону дат
                today_start = datetime.combine(today, time.min)
                today_end = datetime.combine(today, time.max)
                
                today_digests = db_manager.find_digests_by_parameters(
                    date_range_start=today_start,
                    date_range_end=today_end,
                    digest_type=digest_type if digest_type != "both" else None,
                    limit=10
                )
            
            if today_digests:
                # Группируем по типу и находим самые ранние
                unique_digests = {}
                for d in today_digests:
                    d_type = d["digest_type"]
                    if d_type not in unique_digests or d["id"] < unique_digests[d_type]["id"]:
                        unique_digests[d_type] = d
                
                # Ищем соответствующий дайджест
                target_digest = None
                target_id = None
                
                if digest_type == "both":
                    # Для типа "both" проверяем оба типа, начиная с "brief"
                    if "brief" in unique_digests:
                        target_digest = unique_digests["brief"]
                        target_id = target_digest["id"]
                    elif "detailed" in unique_digests:
                        target_digest = unique_digests["detailed"]
                        target_id = target_digest["id"]
                elif digest_type in unique_digests:
                    target_digest = unique_digests[digest_type]
                    target_id = target_digest["id"]
                
                if target_digest and target_id:
                    digest = db_manager.get_digest_by_id_with_sections(target_id)
                    
                    if digest:
                        # Проверяем время последнего обновления
                        last_updated = digest.get("last_updated", today_start)
                        current_time = datetime.now()
                        
                        # Если прошло менее 5 минут с последнего обновления, используем существующий дайджест
                        if (current_time - last_updated).total_seconds() < 300:  # 5 минут
                            await status_message.edit_text(
                                f"{status_message.text}\n"
                                f"✅ Найден актуальный дайджест {period_description}. Отправляю..."
                            )
                            
                            # Отправляем найденный дайджест
                            safe_text = utils.clean_markdown_text(digest["text"])
                            chunks = utils.split_text(safe_text)
                            
                            for i, chunk in enumerate(chunks):
                                if i == 0:
                                    text_html = utils.convert_to_html(chunk)
                                    await update.message.reply_text(
                                        f"{get_digest_type_name(digest['digest_type']).capitalize()} дайджест {period_description}:\n\n{text_html}",
                                        parse_mode='HTML'
                                    )
                                else:
                                    await update.message.reply_text(utils.convert_to_html(chunk), parse_mode='HTML')
                            
                            return
                        else:
                            # Обновляем дайджест с данными с момента последнего обновления
                            await status_message.edit_text(
                                f"{status_message.text}\n"
                                f"🔄 Обновляю существующий дайджест за сегодня (ID: {target_id}, последнее обновление: {last_updated.strftime('%H:%M')})..."
                            )
                            
                            # Меняем начальную дату для сбора только новых данных
                            start_date = last_updated
                            force_update = True  # Обязательно обновляем
                            
                            # Важно: сохраняем ID дайджеста для последующего обновления
                            digest_id = target_id
                else:
                    # Если дайджест не найден, будем создавать новый
                    await status_message.edit_text(
                        f"{status_message.text}\n"
                        f"🆕 Создаю новый дайджест {period_description}..."
                    )
            else:
                # Если дайджест не найден, будем создавать новый
                await status_message.edit_text(
                    f"{status_message.text}\n"
                    f"🆕 Создаю новый дайджест {period_description}..."
                )
        else:
            # Для обычных запросов используем стандартную логику
            existing_digests = db_manager.find_digests_by_parameters(
                date_range_start=start_date,
                date_range_end=end_date,
                digest_type=digest_type if digest_type != "both" else None,
                limit=1
            )
            
            if existing_digests:
                digest_id = existing_digests[0]['id']
                digest = db_manager.get_digest_by_id_with_sections(digest_id)
                
                if digest and not force_update:
                    await status_message.edit_text(
                        f"{status_message.text}\n"
                        f"✅ Найден существующий дайджест {period_description}. Отправляю..."
                    )
                    
                    # Отправляем найденный дайджест
                    safe_text = utils.clean_markdown_text(digest["text"])
                    chunks = utils.split_text(safe_text)
                    
                    for i, chunk in enumerate(chunks):
                        if i == 0:
                            text_html = utils.convert_to_html(chunk)
                            await update.message.reply_text(
                                f"{get_digest_type_name(digest['digest_type']).capitalize()} дайджест {period_description}:\n\n{text_html}",
                                parse_mode='HTML'
                            )
                        else:
                            await update.message.reply_text(utils.convert_to_html(chunk), parse_mode='HTML')
                    
                    return
    except Exception as e:
        logger.error(f"Ошибка при проверке существующих дайджестов: {str(e)}")
    
    # Шаг 2: Сбор данных за указанный период
    try:
        collector = DataCollectorAgent(db_manager)
        
        # Обновляем статус
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"Собираю данные {period_description}... 📥"
        )
        days_back_value = (end_date.date() - start_date.date()).days + 1
         # Запускаем сбор данных с принудительным обновлением
        collect_result = await collector.collect_data(
            days_back=days_back_value,
            force_update=True,  # Принудительно обновляем данные
            start_date=start_date,
            end_date=end_date
        )
        
        total_messages = collect_result.get("total_new_messages", 0)
        
        # Обновляем статус
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"✅ Собрано {total_messages} сообщений из каналов"
        )
        
        # Если нет сообщений, проверяем еще раз с более глубоким поиском
        if total_messages == 0:
            existing_messages = db_manager.get_messages_by_date_range(
                start_date=start_date,
                end_date=end_date
            )
            
            if not existing_messages:
                # Если запрос за сегодня и нет сообщений, возможно их просто не было с прошлого обновления
                if is_today_request:
                    # Расширяем период до начала дня
                    day_start = datetime.combine(today, time.min)
                    await status_message.edit_text(
                        f"{status_message.text}\n"
                        f"📅 Расширяю поиск на весь сегодняшний день..."
                    )
                    
                    # Получаем все сообщения за сегодня
                    all_today_messages = db_manager.get_messages_by_date_range(
                        start_date=day_start,
                        end_date=end_date
                    )
                    
                    if all_today_messages:
                        await status_message.edit_text(
                            f"{status_message.text}\n"
                            f"✅ Найдено {len(all_today_messages)} сообщений за сегодня"
                        )
                        start_date = day_start
                        existing_messages = all_today_messages
                    else:
                        await status_message.edit_text(
                            f"{status_message.text}\n"
                            f"⚠️ Не найдено сообщений за сегодня. Выполняю глубокий поиск... 🔍"
                        )
                        
                        # Запускаем глубокий поиск для сегодняшнего дня
                        for channel in collect_result.get("channels_stats", {}).keys():
                            deep_result = await collector.collect_deep_history(
                                channel,
                                day_start,
                                end_date
                            )
                            
                            if deep_result.get("status") == "success":
                                saved_count = deep_result.get("saved_count", 0)
                                total_messages += saved_count
                                await status_message.edit_text(
                                    f"{status_message.text}\n"
                                    f"📥 Канал {channel}: собрано {saved_count} сообщений глубоким поиском"
                                )
                        
                        # Проверяем снова
                        existing_messages = db_manager.get_messages_by_date_range(
                            start_date=day_start,
                            end_date=end_date
                        )
                else:
                    await status_message.edit_text(
                        f"{status_message.text}\n"
                        f"⚠️ Не найдено сообщений {period_description}. Выполняю глубокий поиск... 🔍"
                    )
                    
                    # Для каждого канала пробуем глубокий сбор
                    for channel in collect_result.get("channels_stats", {}).keys():
                        # Запускаем глубокий сбор истории
                        deep_result = await collector.collect_deep_history(
                            channel,
                            start_date,
                            end_date
                        )
                        
                        # Обновляем статус по каждому каналу
                        if deep_result.get("status") == "success":
                            saved_count = deep_result.get("saved_count", 0)
                            total_messages += saved_count
                            await status_message.edit_text(
                                f"{status_message.text}\n"
                                f"📥 Канал {channel}: собрано {saved_count} сообщений глубоким поиском"
                            )
                
                # Проверяем еще раз наличие сообщений
                if not existing_messages:
                    existing_messages = db_manager.get_messages_by_date_range(
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if not existing_messages:
                        await status_message.edit_text(
                            f"{status_message.text}\n"
                            f"❌ Не удалось найти сообщения {period_description} даже при глубоком поиске."
                        )
                        return
            else:
                total_messages = len(existing_messages)
                await status_message.edit_text(
                    f"{status_message.text}\n"
                    f"✅ Найдено {total_messages} существующих сообщений {period_description}"
                )
        
        # Шаг 3: Анализ и классификация сообщений
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"Анализирую и классифицирую сообщения... 🧠"
        )
        
        # Получаем список некатегоризированных сообщений
        unanalyzed_messages = db_manager.get_unanalyzed_messages(limit=total_messages)
        
        if unanalyzed_messages:
            # Создаем анализатор и выполняем классификацию
            analyzer = AnalyzerAgent(db_manager, QwenLLM())
            analyzer.fast_check = True  # Включаем режим быстрой проверки
            
            analyze_result = analyzer.analyze_messages_batched(
                limit=len(unanalyzed_messages),
                batch_size=10
            )
            
            analyzed_count = analyze_result.get("analyzed_count", 0)
            
            await status_message.edit_text(
                f"{status_message.text}\n"
                f"✅ Проанализировано {analyzed_count} сообщений"
            )
            
            # Проверка категоризации для сообщений с низкой уверенностью
            critic = CriticAgent(db_manager)
            review_result = critic.review_recent_categorizations(
                confidence_threshold=2,
                limit=min(30, analyzed_count),
                start_date=start_date,
                end_date=end_date
            )
            
            if review_result.get("updated", 0) > 0:
                await status_message.edit_text(
                    f"{status_message.text}\n"
                    f"✅ Улучшена категоризация {review_result.get('updated', 0)} сообщений"
                )
        
        # Шаг 4: Создание или обновление дайджеста
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"Формирую дайджест... 📝"
        )
        
        # Создаем генератор дайджеста
        digester = DigesterAgent(db_manager, GemmaLLM())
        
        # Определяем существующий digest_id для обновления
        digest_id = None
        if existing_digests:
            digest_id = existing_digests[0]['id']
            
        # Создаем дайджест с указанием digest_id для обновления существующего
        digest_result = digester.create_digest(
            date=end_date,
            days_back=days_in_period,
            digest_type=digest_type,
            update_existing=True,
            digest_id=digest_id
        )
        
        # Получаем ID созданного дайджеста в зависимости от типа
        if digest_type == "brief" and "brief_digest_id" in digest_result:
            digest_id = digest_result["brief_digest_id"]
            digest_type_name = "краткий"
        elif digest_type == "detailed" and "detailed_digest_id" in digest_result:
            digest_id = digest_result["detailed_digest_id"]
            digest_type_name = "подробный"
        elif digest_type == "both":
            # Если запрошены оба типа, отправляем их последовательно
            brief_id = digest_result.get("brief_digest_id")
            detailed_id = digest_result.get("detailed_digest_id")
            
            if brief_id and detailed_id:
                await status_message.edit_text(
                    f"{status_message.text}\n"
                    f"✅ Оба типа дайджеста успешно созданы!"
                )
                
                # Отправляем сначала краткий дайджест
                brief_digest = db_manager.get_digest_by_id_with_sections(brief_id)
                if brief_digest:
                    # Отправляем краткий дайджест
                    safe_text = utils.clean_markdown_text(brief_digest["text"])
                    chunks = utils.split_text(safe_text)
                    
                    await update.message.reply_text(
                        f"Краткий дайджест {period_description}:"
                    )
                    
                    for chunk in chunks:
                        text_html = utils.convert_to_html(chunk)
                        await update.message.reply_text(text_html, parse_mode='HTML')
                
                # Затем отправляем подробный дайджест
                detailed_digest = db_manager.get_digest_by_id_with_sections(detailed_id)
                if detailed_digest:
                    # Отправляем подробный дайджест
                    safe_text = utils.clean_markdown_text(detailed_digest["text"])
                    chunks = utils.split_text(safe_text)
                    
                    await update.message.reply_text(
                        f"Подробный дайджест {period_description}:"
                    )
                    
                    for chunk in chunks:
                        text_html = utils.convert_to_html(chunk)
                        await update.message.reply_text(text_html, parse_mode='HTML')
                
                return
            elif brief_id:
                digest_id = brief_id
                digest_type_name = "краткий"
            elif detailed_id:
                digest_id = detailed_id
                digest_type_name = "подробный"
            else:
                await status_message.edit_text(
                    f"{status_message.text}\n"
                    f"❌ Не удалось создать дайджест {period_description}."
                )
                return
        else:
            await status_message.edit_text(
                f"{status_message.text}\n"
                f"❌ Не удалось создать дайджест типа {digest_type} {period_description}."
            )
            return
        
        # Получаем созданный дайджест
        digest = db_manager.get_digest_by_id_with_sections(digest_id)
        
        if not digest:
            await status_message.edit_text(
                f"{status_message.text}\n"
                f"❌ Не удалось получить созданный дайджест (ID: {digest_id})."
            )
            return
        
        # Обновляем статус
        status_text = f"{status_message.text}\n✅ Дайджест успешно"
        if is_today_request and existing_digests:
            status_text += " обновлен!"
        else:
            status_text += " создан!"
        await status_message.edit_text(f"{status_text} Отправляю...")
        
        # Отправляем дайджест
        safe_text = utils.clean_markdown_text(digest["text"])
        chunks = utils.split_text(safe_text)
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                text_html = utils.convert_to_html(chunk)
                await update.message.reply_text(
                    f"{digest_type_name.capitalize()} дайджест {period_description}:\n\n{text_html}",
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(utils.convert_to_html(chunk), parse_mode='HTML')
                
    except Exception as e:
        logger.error(f"Ошибка при создании дайджеста {period_description}: {str(e)}", exc_info=True)
        
        # Обновляем статус с ошибкой
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"❌ Произошла ошибка: {str(e)}"
        )
def get_digest_type_name(digest_type):
    """Возвращает название типа дайджеста на русском языке"""
    if digest_type == "brief":
        return "краткий"
    elif digest_type == "detailed":
        return "подробный"
    elif digest_type == "both":
        return "полный"
    else:
        return digest_type    