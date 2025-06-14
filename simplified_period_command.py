"""
Улучшенный обработчик команды /period для генерации дайджеста за произвольный период,
включая поддержку ключевых слов "сегодня" и "вчера"
"""
import logging
import re
from datetime import datetime, timedelta
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
            "• /period 2025-04-01 2025-04-10 both - оба типа дайджеста"
        )
        return
    
    # Разбираем аргументы
    digest_type = "brief"  # Тип дайджеста по умолчанию
    
    # Проверяем первый аргумент на ключевые слова
    if context.args[0].lower() in ["сегодня", "today"]:
        start_date = end_date = datetime.now()
        start_date_str = end_date_str = start_date.strftime("%Y-%m-%d")
        period_description = "за сегодня"
        
        # Проверяем, есть ли указание типа дайджеста
        if len(context.args) > 1:
            digest_type_arg = context.args[1].lower()
            if digest_type_arg in ["detailed", "full", "подробный", "полный"]:
                digest_type = "detailed"
            elif digest_type_arg in ["both", "оба"]:
                digest_type = "both"
    
    elif context.args[0].lower() in ["вчера", "yesterday"]:
        start_date = end_date = datetime.now() - timedelta(days=1)
        start_date_str = end_date_str = start_date.strftime("%Y-%m-%d")
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
                        period_description = f"за период с {start_date_str} по {end_date_str}"
                    else:
                        raise ValueError("Некорректный формат периода")
                else:
                    # Только одна дата
                    start_date_str = end_date_str = context.args[0]
                    period_description = f"за {start_date_str}"
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
                period_description = f"за {start_date_str}"
                
                digest_type_arg = context.args[1].lower()
                if digest_type_arg in ["detailed", "full", "подробный", "полный"]:
                    digest_type = "detailed"
                elif digest_type_arg in ["both", "оба"]:
                    digest_type = "both"
            else:
                # Два аргумента - начальная и конечная даты
                start_date_str = context.args[0]
                end_date_str = context.args[1]
                period_description = f"за период с {start_date_str} по {end_date_str}"
        elif len(context.args) >= 3:
            # Три и более аргумента - даты и тип дайджеста
            start_date_str = context.args[0]
            end_date_str = context.args[1]
            period_description = f"за период с {start_date_str} по {end_date_str}"
            
            # Получаем тип дайджеста
            digest_type_arg = context.args[2].lower()
            if digest_type_arg in ["detailed", "full", "подробный", "полный"]:
                digest_type = "detailed"
            elif digest_type_arg in ["both", "оба"]:
                digest_type = "both"
        
        # Проверяем формат дат
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
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
        existing_digests = db_manager.find_digests_by_parameters(
            date_range_start=start_date,
            date_range_end=end_date,
            digest_type=digest_type if digest_type != "both" else None,
            limit=1
        )
        
        if existing_digests:
            digest_id = existing_digests[0]['id']
            digest = db_manager.get_digest_by_id_with_sections(digest_id)
            
            if digest:
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
        
        # Запускаем сбор данных
        collect_result = await collector.collect_data(
            start_date=start_date,
            end_date=end_date,
            force_update=False
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
        
        # Шаг 4: Создание дайджеста
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"Формирую дайджест... 📝"
        )
        
        # Создаем генератор дайджеста
        digester = DigesterAgent(db_manager, GemmaLLM())
        
        # Определяем количество дней для дайджеста
        days_back = (end_date.date() - start_date.date()).days + 1
        
        # Создаем дайджест
        digest_result = digester.create_digest(
            date=end_date,
            days_back=days_back,
            digest_type=digest_type,
            update_existing=False
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
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"✅ Дайджест успешно создан! Отправляю..."
        )
        
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