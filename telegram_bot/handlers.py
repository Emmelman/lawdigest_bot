"""
Обработчики команд для Telegram-бота
"""
import logging
import re
import asyncio
import telegram
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import time, datetime, timedelta
from config.settings import CATEGORIES, BOT_USERNAME, TELEGRAM_CHANNELS
from llm.gemma_model import GemmaLLM
from agents.digester import DigesterAgent
from agents.data_collector import DataCollectorAgent
from agents.analyzer import AnalyzerAgent
from agents.critic import CriticAgent
from utils.text_utils import TextUtils
from telegram_bot.improved_message_handler import improved_message_handler
from telegram_bot.view_digest_helpers import (
    show_full_digest, start_digest_generation, get_category_icon
)
from telegram_bot.period_command import period_command
from telegram_bot.improved_view_digest import (
       view_digest_callback, 
       view_digest_section_callback,
       page_navigation_callback,
       show_full_digest,
       get_category_icon,
       get_short_category_id
   )
from telegram_bot.improved_view_digest import get_short_category_id

logger = logging.getLogger(__name__)

# Утилиты для работы с текстом
utils = TextUtils()

# Базовые обработчики команд
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /start"""
    user = update.effective_user
    
    # Проверяем, есть ли параметры в команде start
    if context.args and context.args[0].startswith('msg_'):
        try:
            message_id = int(context.args[0].replace('msg_', ''))
            message = db_manager.get_message_by_id(message_id)
            
            if message:
                await update.message.reply_text(
                    f"Сообщение из канала {message.channel} от {message.date.strftime('%d.%m.%Y')}:\n\n{message.text}"
                )
                return
        except (ValueError, Exception) as e:
            logger.error(f"Ошибка при обработке параметра start: {str(e)}")
    
    # Обычная команда /start без параметров
    await update.message.reply_text(
        f"Здравствуйте, {user.first_name}! Я бот для дайджеста правовых новостей.\n\n"
        "Доступные команды:\n"
        #"/digest - получить краткий дайджест\n"
        #"/digest_detailed - получить подробный дайджест\n"
        "/period - получить дайджест за произвольный период (сегодня/вчера/YYYY-MM-DD)\n"
        "/category - выбрать категорию новостей\n"
        "/help - получить справку"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /help"""
    await update.message.reply_text(
        "Я могу предоставить вам дайджест правовых новостей.\n\n"
        "Доступные команды:\n"
        #"/digest - получить краткий дайджест\n"
        #"/digest_detailed - получить подробный дайджест\n"
        "/period - получить дайджест за произвольный период (сегодня/вчера/YYYY-MM-DD)\n"
        "/category - выбрать категорию новостей\n"
        "/help - получить справку\n\n"
        "Вы также можете задать мне вопрос по правовым новостям."
    )

async def category_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /category"""
    keyboard = []
    
    # Для каждой категории создаем две кнопки с сокращённым текстом
    for cat in CATEGORIES:
        # Сокращаем название категории, если оно длинное
        short_name = cat[:15] + "..." if len(cat) > 15 else cat
        keyboard.append([
            InlineKeyboardButton(f"{short_name} (кратко)", callback_data=f"cat_brief_{cat}"),
            InlineKeyboardButton(f"{short_name} (подр.)", callback_data=f"cat_detailed_{cat}")
        ])
    
    # Добавляем кнопку для категории "другое"
    keyboard.append([
        InlineKeyboardButton("другое (кратко)", callback_data="cat_brief_другое"),
        InlineKeyboardButton("другое (подр.)", callback_data="cat_detailed_другое")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Выберите категорию и тип обзора:", 
        reply_markup=reply_markup
    )

"""
Улучшенный обработчик команды /list для интерактивного просмотра дайджестов
"""
async def list_digests_command(message_object: telegram.Message, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /list - интерактивный список доступных дайджестов"""
    # Получаем последние дайджесты (увеличиваем лимит до 15)
    digests = db_manager.find_digests_by_parameters(limit=15)
    logger.info(f"Найдено {len(digests)} дайджестов: {[d['id'] for d in digests]}")
    if not digests:
        await message_object.reply_text("На данный момент нет доступных дайджестов.")
        return
    
    # Группируем дайджесты по дате для более компактного отображения
    digests_by_date = {}
    for digest in digests:
        date_str = digest['date'].strftime('%Y-%m-%d')
        
        # Учитываем диапазон дат, если он указан
        if digest.get("date_range_start") and digest.get("date_range_end"):
            days_diff = (digest["date_range_end"] - digest["date_range_start"]).days
            if days_diff > 0:
                date_str = f"{digest['date_range_start'].strftime('%Y-%m-%d')} - {digest['date_range_end'].strftime('%Y-%m-%d')}"
        
        if date_str not in digests_by_date:
            digests_by_date[date_str] = []
        
        digests_by_date[date_str].append(digest)
    
    # Создаем клавиатуру с кнопками для каждой даты
    keyboard = []
    
    # Сортируем даты в обратном порядке (сначала новые)
    sorted_dates = sorted(digests_by_date.keys(), reverse=True)
    
    for date_str in sorted_dates:
        date_digests = digests_by_date[date_str]
        
        # Определяем, есть ли дайджесты разных типов за эту дату
        has_brief = any(d["digest_type"] == "brief" for d in date_digests)
        has_detailed = any(d["digest_type"] == "detailed" for d in date_digests)
        
        # Если есть оба типа, создаем отдельные кнопки
        if has_brief and has_detailed:
            brief_digest = next((d for d in date_digests if d["digest_type"] == "brief"), None)
            detailed_digest = next((d for d in date_digests if d["digest_type"] == "detailed"), None)
            
            # Добавляем метки для кнопок
            brief_label = f"📋 {date_str} (краткий)"
            detailed_label = f"📚 {date_str} (подробный)"
            
            # Если дайджест за сегодня, добавляем метку
            today = datetime.now().date()
            if brief_digest and brief_digest.get("date").date() == today:
                brief_label = f"📌 {brief_label}"
            if detailed_digest and detailed_digest.get("date").date() == today:
                detailed_label = f"📌 {detailed_label}"
            
            keyboard.append([
                InlineKeyboardButton(brief_label, callback_data=f"view_digest_{brief_digest['id']}") if brief_digest else None,
                InlineKeyboardButton(detailed_label, callback_data=f"view_digest_{detailed_digest['id']}") if detailed_digest else None
            ])
        else:
            # Если есть только один тип, создаем одну кнопку
            for digest in date_digests:
                digest_type_label = "краткий" if digest["digest_type"] == "brief" else "подробный"
                
                # Формируем метку с учетом фокуса, если есть
                button_label = f"📋 {date_str} ({digest_type_label})"
                if digest.get("focus_category"):
                    button_label += f" - {digest['focus_category']}"
                
                # Если дайджест за сегодня, добавляем метку
                today = datetime.now().date()
                if digest.get("date").date() == today:
                    button_label = f"📌 {button_label}"
                
                keyboard.append([
                    InlineKeyboardButton(button_label, callback_data=f"view_digest_{digest['id']}")
                ])
    
    # Добавляем кнопку для создания нового дайджеста
    keyboard.append([
        InlineKeyboardButton("🆕 Создать новый дайджест", callback_data="cd")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await message_object.reply_text(
        "📊 Доступные дайджесты:\n\n"
        "Выберите дайджест для просмотра:",
        reply_markup=reply_markup
    )

async def category_selection_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Улучшенный обработчик команды /cat - выбор категории из дайджеста"""
    
    # Шаг 1: Получаем список доступных дайджестов
    digests = db_manager.find_digests_by_parameters(limit=10)
    
    if not digests:
        await update.message.reply_text("Дайджесты еще не сформированы.")
        return
    
    # Группируем по датам и типам (краткий/подробный)
    digests_by_date = {}
    for digest in digests:
        date_str = digest['date'].strftime('%Y-%m-%d')
        if date_str not in digests_by_date:
            digests_by_date[date_str] = []
        
        # Учитываем диапазон дат
        if digest.get("date_range_start") and digest.get("date_range_end"):
            days_diff = (digest["date_range_end"] - digest["date_range_start"]).days
            if days_diff > 0:
                date_str = f"{digest['date_range_start'].strftime('%Y-%m-%d')} - {digest['date_range_end'].strftime('%Y-%m-%d')}"
        
        digests_by_date[date_str].append(digest)
    
    # Создаем кнопки для выбора дайджеста
    keyboard = []
    for date_str, date_digests in sorted(digests_by_date.items(), reverse=True):
        # Если несколько типов дайджестов за одну дату, создаем отдельные кнопки
        if len(date_digests) > 1:
            for digest in date_digests:
                is_today = digest.get('is_today', False)
                today_mark = "📌 " if is_today else ""
                type_mark = "📝" if digest['digest_type'] == "brief" else "📚"
                button_text = f"{today_mark}{type_mark} {date_str} ({digest['digest_type']})"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=f"select_digest_{digest['id']}")])
        else:
            # Если только один дайджест за дату, упрощаем отображение
            digest = date_digests[0]
            is_today = digest.get('is_today', False)
            today_mark = "📌 " if is_today else ""
            type_mark = "📝" if digest['digest_type'] == "brief" else "📚"
            button_text = f"{today_mark}{type_mark} {date_str}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"select_digest_{digest['id']}")])
    
    # Добавляем кнопку "Сегодня" для быстрого доступа к сегодняшнему дайджесту
    today_digests = [d for d in digests if d.get('is_today', False)]
    if today_digests:
        keyboard.append([InlineKeyboardButton("📆 Сегодняшний дайджест", callback_data="select_today_digest")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Выберите дайджест для просмотра категорий:", 
        reply_markup=reply_markup
    )

# Дополнительный обработчик для кнопок выбора дайджеста
async def handle_digest_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager, callback_data):
    """Обработчик выбора дайджеста из списка"""
    query = update.callback_query
    await query.answer()
    
    if callback_data.startswith("select_digest_"):
        digest_id = int(callback_data.replace("select_digest_", ""))
        await show_digest_categories(query.message, digest_id, db_manager)
    elif callback_data == "select_today_digest":
        # Найти самый свежий дайджест за сегодня
        today_digests = db_manager.find_digests_by_parameters(is_today=True, limit=5)
        if today_digests:
            # Группируем по типу и берем самый ранний для каждого типа
            unique_digests = {}
            for d in today_digests:
                d_type = d["digest_type"]
                if d_type not in unique_digests or d["id"] < unique_digests[d_type]["id"]:
                    unique_digests[d_type] = d
            
            # Предпочитаем краткий дайджест
            if "brief" in unique_digests:
                digest_id = unique_digests["brief"]["id"]
            else:
                digest_id = today_digests[0]["id"]
            
            await show_digest_categories(query.message, digest_id, db_manager)
        else:
            await query.message.reply_text("Дайджест за сегодня не найден.")

# В файле telegram_bot/handlers.py 

async def show_digest_categories(message, digest_id, db_manager):
    """Показывает категории из выбранного дайджеста"""
    digest = db_manager.get_digest_by_id_with_sections(digest_id)
    
    if not digest:
        await message.reply_text("Дайджест не найден.")
        return
    
    # Получаем список категорий из дайджеста
    categories = []
    for section in digest["sections"]:
        categories.append(section["category"])
    
    # Создаем кнопки для выбора категории
    keyboard = []
    for category in categories:
        icon = get_category_icon(category)
        # Используем формат cat_digest_id_category для передачи ID дайджеста
        keyboard.append([InlineKeyboardButton(f"{icon} {category}", callback_data=f"ds_{digest_id}_{get_short_category_id(category)}")])
    
    # Добавляем кнопку "Весь дайджест"
    keyboard.append([InlineKeyboardButton("📄 Весь дайджест", callback_data=f"df_{digest_id}")])
    
    # Добавляем кнопку "Назад к списку дайджестов"
    keyboard.append([InlineKeyboardButton("⬅️ Назад к списку", callback_data="sl")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    digest_date = digest['date'].strftime('%d.%m.%Y')
    digest_type = "краткий" if digest['digest_type'] == "brief" else "подробный"
    
    await message.reply_text(
        f"Дайджест за {digest_date} ({digest_type}).\n"
        f"Выберите категорию для просмотра:",
        reply_markup=reply_markup
    )

# Обработчики ввода данных пользователем
async def handle_date_range_input(update, context, db_manager, user_input):
    """Обработка ввода диапазона дат"""
    context.user_data.pop("awaiting_date_range", None)
    
    try:
        # Парсим диапазон дат
        if "-" in user_input:
            # Формат ДД.ММ.ГГГГ-ДД.ММ.ГГГГ
            start_str, end_str = user_input.split("-")
            
            # Парсим начальную дату
            start_parts = start_str.strip().split(".")
            if len(start_parts) != 3:
                raise ValueError("Неверный формат начальной даты")
            
            day, month, year = map(int, start_parts)
            start_date = datetime(year, month, day)
            
            # Парсим конечную дату
            end_parts = end_str.strip().split(".")
            if len(end_parts) != 3:
                raise ValueError("Неверный формат конечной даты")
            
            day, month, year = map(int, end_parts)
            end_date = datetime(year, month, day)
            
            description = f"за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}"
            
        else:
            # Простой формат ДД.ММ.ГГГГ
            date_parts = user_input.strip().split(".")
            if len(date_parts) != 3:
                raise ValueError("Неверный формат даты")
            
            day, month, year = map(int, date_parts)
            start_date = end_date = datetime(year, month, day)
            description = f"за {start_date.strftime('%d.%m.%Y')}"
        
        # Запускаем генерацию дайджеста
        await handle_digest_generation(
            update, context, db_manager, 
            start_date, end_date, description
        )
        
    except ValueError as e:
        await update.message.reply_text(
            f"Ошибка в формате даты: {str(e)}. Пожалуйста, используйте формат ДД.ММ.ГГГГ или ДД.ММ.ГГГГ-ДД.ММ.ГГГГ"
        )
    except Exception as e:
        logger.error(f"Ошибка при обработке диапазона дат: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"Произошла ошибка: {str(e)}. Пожалуйста, проверьте формат ввода."
        )

async def handle_category_period_input(update, context, db_manager, user_input):
    """Обработка ввода периода для категории"""
    focus_category = context.user_data.get("focus_category")
    context.user_data.pop("awaiting_category_period", None)
    context.user_data.pop("focus_category", None)
    
    try:
        # Обрабатываем специальные значения
        if user_input.lower() == "сегодня":
            start_date = end_date = datetime.now()
            description = f"за сегодня с фокусом на категорию '{focus_category}'"
        elif user_input.lower() == "вчера":
            start_date = end_date = datetime.now() - timedelta(days=1)
            description = f"за вчера с фокусом на категорию '{focus_category}'"
        elif "-" in user_input:
            # Формат ДД.ММ.ГГГГ-ДД.ММ.ГГГГ
            start_str, end_str = user_input.split("-")
            
            # Парсим даты
            start_date = datetime.strptime(start_str.strip(), "%d.%m.%Y")
            end_date = datetime.strptime(end_str.strip(), "%d.%m.%Y")
            
            description = f"за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')} с фокусом на категорию '{focus_category}'"
        else:
            # Простой формат ДД.ММ.ГГГГ
            date = datetime.strptime(user_input.strip(), "%d.%m.%Y")
            start_date = end_date = date
            description = f"за {date.strftime('%d.%m.%Y')} с фокусом на категорию '{focus_category}'"
        
        # Запускаем генерацию дайджеста
        await handle_digest_generation(
            update, context, db_manager, 
            start_date, end_date, description, 
            focus_category=focus_category
        )
        
    except ValueError:
        await update.message.reply_text(
            "Не удалось распознать указанный период. Пожалуйста, используйте формат ДД.ММ.ГГГГ, ДД.ММ.ГГГГ-ДД.ММ.ГГГГ или слова 'сегодня'/'вчера'."
        )
    except Exception as e:
        logger.error(f"Ошибка при обработке периода для категории: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"Произошла ошибка: {str(e)}. Пожалуйста, проверьте формат ввода."
        )

async def handle_channel_period_input(update, context, db_manager, user_input):
    """Обработка ввода периода для канала"""
    channels = context.user_data.get("channels", [])
    context.user_data.pop("awaiting_channel_period", None)
    context.user_data.pop("focus_channel", None)
    context.user_data.pop("channels", None)
    
    if not channels:
        await update.message.reply_text("Произошла ошибка: не указаны каналы для фильтрации.")
        return
    
    try:
        # Обрабатываем специальные значения
        if user_input.lower() == "сегодня":
            start_date = end_date = datetime.now()
            description = f"за сегодня с фильтрацией по каналам"
        elif user_input.lower() == "вчера":
            start_date = end_date = datetime.now() - timedelta(days=1)
            description = f"за вчера с фильтрацией по каналам"
        elif "-" in user_input:
            # Формат ДД.ММ.ГГГГ-ДД.ММ.ГГГГ
            start_str, end_str = user_input.split("-")
            
            # Парсим даты
            start_date = datetime.strptime(start_str.strip(), "%d.%m.%Y")
            end_date = datetime.strptime(end_str.strip(), "%d.%m.%Y")
            
            description = f"за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')} с фильтрацией по каналам"
        else:
            # Простой формат ДД.ММ.ГГГГ
            date = datetime.strptime(user_input.strip(), "%d.%m.%Y")
            start_date = end_date = date
            description = f"за {date.strftime('%d.%m.%Y')} с фильтрацией по каналам"
        
        # Запускаем генерацию дайджеста
        await handle_digest_generation(
            update, context, db_manager, 
            start_date, end_date, description, 
            channels=channels
        )
        
    except ValueError:
        await update.message.reply_text(
            "Не удалось распознать указанный период. Пожалуйста, используйте формат ДД.ММ.ГГГГ, ДД.ММ.ГГГГ-ДД.ММ.ГГГГ или слова 'сегодня'/'вчера'."
        )
    except Exception as e:
        logger.error(f"Ошибка при обработке периода для канала: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"Произошла ошибка: {str(e)}. Пожалуйста, проверьте формат ввода."
        )

# Обработчик кнопок и генерация дайджеста (см. ранее определенную функцию handle_digest_generation)
# В файле telegram_bot/handlers.py нужно обновить функцию button_callback

"""
Обновленный обработчик колбэков для работы с интерактивными кнопками
"""
"""
Обновленный обработчик колбэков для интерактивных кнопок
"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timedelta, time

logger = logging.getLogger(__name__)

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик нажатий на кнопки с сокращенными callback_data"""
    query = update.callback_query
    
    # Предварительно обрабатываем колбэк, чтобы пользователь не ждал
    await query.answer()
    
    try:
        # Обработка различных типов колбэков с сокращенными данными
        
        # Просмотр дайджеста (view_digest_X)
        if query.data.startswith("view_digest_"):
            await view_digest_callback(update, context, db_manager)
        
        # Просмотр секции дайджеста (ds_X_Y - digest section)
        elif query.data.startswith("ds_"):
            await view_digest_section_callback(update, context, db_manager)
        
        # Пагинация (pg_X_Y_Z - page navigation)
        elif query.data.startswith("pg_"):
            await page_navigation_callback(update, context, db_manager)
        
        # Просмотр полного дайджеста (df_X - digest full)
        elif query.data.startswith("df_"):
            await show_full_digest(update, context, db_manager)
        
        # Список дайджестов (sl - show list)
        elif query.data == "sl": # Эта кнопка "Назад к списку дайджестов"
            try:
                # Передаем query.message, которое является объектом сообщения, к которому привязана кнопка
                await list_digests_command(query.message, context, db_manager)
            except Exception as e:
                logger.error(f"Ошибка при отображении списка дайджестов: {str(e)}")
                await query.message.reply_text(f"Произошла ошибка при загрузке списка дайджестов: {str(e)}")
        
        # Создание нового дайджеста (cd - create digest)
        elif query.data == "cd":
            # Предлагаем пользователю выбрать период для дайджеста
            keyboard = [
                [InlineKeyboardButton("📅 За сегодня", callback_data="nd_today")],
                [InlineKeyboardButton("📆 За вчера", callback_data="nd_yesterday")],
                [InlineKeyboardButton("📊 Указать период", callback_data="nd_custom")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.message.edit_text(
                "Выберите период для создания нового дайджеста:",
                reply_markup=reply_markup
            )
        
        # Обработка выбора периода для нового дайджеста (nd_X - new digest)
        elif query.data.startswith("nd_"):
            period_type = query.data.replace("nd_", "")
            
            if period_type == "today":
                # Запускаем генерацию дайджеста за сегодня
                today = datetime.now().date()
                start_date = datetime.combine(today, time.min)
                end_date = datetime.now()
                
                await start_digest_generation(
                    query.message, 
                    start_date, 
                    end_date, 
                    "За сегодня", 
                    db_manager, 
                    context
                )
                
            elif period_type == "yesterday":
                # Запускаем генерацию дайджеста за вчера
                yesterday = (datetime.now() - timedelta(days=1)).date()
                start_date = datetime.combine(yesterday, time.min)
                end_date = datetime.combine(yesterday, time.max)
                
                await start_digest_generation(
                    query.message, 
                    start_date, 
                    end_date, 
                    "За вчера", 
                    db_manager, 
                    context
                )
                
            elif period_type == "custom":
                # Запрашиваем у пользователя период
                await query.message.edit_text(
                    "Введите период для дайджеста в формате:\n"
                    "1. ГГГГ-ММ-ДД (одна дата)\n"
                    "2. ГГГГ-ММ-ДД ГГГГ-ММ-ДД (период)\n\n"
                    "Например: 2025-04-15 или 2025-04-10 2025-04-15"
                )
                
                # Устанавливаем флаг ожидания ввода периода
                context.user_data["awaiting_date_range"] = True
                
        # Показать категорию из определенного дайджеста (cat_X_Y)

        
        # Обработка для возврата к списку дайджестов
        elif query.data == "sl":
            try:
                # Используем доработанную команду list_digests_command
                await list_digests_command(update, context, db_manager)
            except Exception as e:
                logger.error(f"Ошибка при отображении списка дайджестов: {str(e)}")
                await query.message.reply_text(f"Произошла ошибка при загрузке списка дайджестов: {str(e)}")
        
        # Обработка выбора дайджеста из списка
        elif query.data.startswith("select_digest_"):
            try:
                digest_id = int(query.data.replace("select_digest_", ""))
                # Теперь вызываем show_digest_categories, которая покажет категории этого дайджеста
                await show_digest_categories(query.message, digest_id, db_manager)
            except Exception as e:
                logger.error(f"Ошибка при выборе дайджеста: {str(e)}")
                await query.message.reply_text(f"Произошла ошибка при выборе дайджеста: {str(e)}")
        
        # Если колбэк не распознан
        else:
            logger.warning(f"Неизвестный callback_data: {query.data}")
            await query.message.reply_text(f"Неизвестная команда. Пожалуйста, используйте /list для просмотра дайджестов.")
            
    except Exception as e:
        logger.error(f"Общая ошибка в обработчике колбэков: {str(e)}", exc_info=True)
        
        # Если возникла ошибка, отправляем новое сообщение
        try:
            await query.message.reply_text(
                f"Произошла ошибка при обработке команды: {str(e)}\n"
                "Пожалуйста, используйте /list для просмотра дайджестов."
            )
        except Exception:
            # Если не можем отправить сообщение, логируем ошибку
            logger.error("Не удалось отправить сообщение об ошибке пользователю")
async def show_digest_by_id(message, digest_id, db_manager):
    """Показывает дайджест по его ID"""
    # Получаем дайджест с секциями
    digest = db_manager.get_digest_by_id_with_sections(digest_id)
    
    if not digest:
        await message.reply_text("Дайджест не найден.")
        return
    
    # Очищаем текст от проблемных символов
    safe_text = utils.clean_markdown_text(digest["text"])
    
    # Отправляем дайджест по частям
    chunks = utils.split_text(safe_text)
    
    # Формируем заголовок в зависимости от параметров дайджеста
    header = f"Дайджест за {digest['date'].strftime('%d.%m.%Y')}"
    
    if digest.get("date_range_start") and digest.get("date_range_end"):
        start_date = digest["date_range_start"].strftime("%d.%m.%Y")
        end_date = digest["date_range_end"].strftime("%d.%m.%Y")
        if start_date != end_date:
            header = f"Дайджест за период с {start_date} по {end_date}"
    
    if digest.get("focus_category"):
        header += f" (фокус: {digest['focus_category']})"
        
    if digest.get("digest_type"):
        header += f" - {digest['digest_type']}"
    
    for i, chunk in enumerate(chunks):
        if i == 0:
            text_html = utils.convert_to_html(chunk)
            await message.reply_text(
                f"{header}\n\n{text_html}",
                parse_mode='HTML'
            )
        else:
            await message.reply_text(chunk, parse_mode='HTML')

async def handle_digest_generation(update, context, db_manager, start_date, end_date, 
                          description, focus_category=None, channels=None, keywords=None, force_update=False):
    """Асинхронный запуск генерации дайджеста с использованием оптимизаций workflow"""
    # Определяем количество дней для обработки на основе дат
    if start_date and end_date:
        days_back = (end_date.date() - start_date.date()).days + 1
        logger.info(f"Рассчитан period days_back={days_back} на основе указанного диапазона")
    else:
        days_back = 1  # Значение по умолчанию
        logger.info(f"Используется значение days_back={days_back} по умолчанию")
   
    # Определяем, откуда пришел запрос (от сообщения или колбэка)
   # message = update.message if hasattr(update, 'message') else update.message
    #user_id = update.effective_user.id
    # Определяем, откуда пришел запрос (от сообщения или колбэка)
    message = update.message
    #if hasattr(update, 'callback_query'):
        # Это объект Update с callback_query
     #   message = update.callback_query.message
      #  user_id = update.callback_query.from_user.id
    # Определяем, откуда пришел запрос (от сообщения или колбэка)
    if hasattr(update, 'message') and update.message:
        # Это объект Update с message
        message = update.message
        user_id = update.effective_user.id
    elif hasattr(update, 'callback_query') and update.callback_query:
        # Это объект Update с callback_query
        message = update.callback_query.message
        user_id = update.callback_query.from_user.id
    else:
        # Fallback для любых других случаев
        message = update.effective_message if hasattr(update, 'effective_message') else None
        user_id = update.effective_user.id if hasattr(update, 'effective_user') else None
        
    if not message:
        logger.error("Не удалось определить источник сообщения")
        return

    # Обработка дат и проверка предыдущей генерации (оставляем как есть)
    if not start_date:
        last_generation = db_manager.get_last_digest_generation(source="bot", user_id=user_id)
        
        if last_generation:
            start_date = last_generation["timestamp"]
            today = datetime.now().date()
            if start_date.date() == today and not focus_category and not channels:
                await message.reply_text(
                    f"Вы уже генерировали дайджест сегодня в {start_date.strftime('%H:%M')}. "
                    f"Хотите создать новый дайджест с {start_date.strftime('%H:%M')} по текущее время?"
                )
                # Добавляем кнопки
                keyboard = [
                    [InlineKeyboardButton("Да, обновить дайджест", callback_data="gen_digest_since_last")],
                    [InlineKeyboardButton("Нет, полный дайджест за сегодня", callback_data="gen_digest_today")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await message.reply_text("Выберите вариант:", reply_markup=reply_markup)
                return
        else:
            start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    if not end_date:
        end_date = datetime.now()
    
    if not description:
        if start_date.date() == end_date.date():
            description = f"за {start_date.strftime('%d.%m.%Y')}"
        else:
            description = f"за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}"

    # Отправляем начальное сообщение о статусе (упрощенное)
    status_message = await message.reply_text(
        f"Запущена генерация дайджеста {description}.\n"
        f"{'Фокус на категории: ' + focus_category if focus_category else ''}\n"
        f"{'Каналы: ' + ', '.join(channels) if channels else ''}\n\n"
        "Обработка... ⏳"
    )
    
    # Определяем количество дней для обработки
    days_back = (end_date - start_date).days + 1
    
    try:
        # Инициализация компонентов - создаем их только раз
        from llm.qwen_model import QwenLLM
        from llm.gemma_model import GemmaLLM
        from agents.data_collector import DataCollectorAgent
        from agents.analyzer import AnalyzerAgent
        from agents.critic import CriticAgent
        from agents.digester import DigesterAgent
        
        qwen_model = QwenLLM()
        gemma_model = GemmaLLM()
        
        # Этап 1: Параллельный сбор данных - используем оптимизированный метод как в workflow
        collector = DataCollectorAgent(db_manager)
        
        # Используем асинхронный метод collect_data вместо _collect_all_channels_parallel
        collect_result = await collector.collect_data(
        days_back=days_back,
        force_update=force_update,
        start_date=start_date,
        end_date=end_date
        )
        
        total_messages = collect_result.get("total_new_messages", 0)
        
        # Обновляем статус (только один раз после сбора данных)
        await status_message.edit_text(
            f"{status_message.text}\n✅ Собрано {total_messages} новых сообщений\n"
            f"Анализ и категоризация... 🧠"
        )
        
        # Этап 2: Оптимизированный анализ сообщений с быстрой проверкой
        analyzer = AnalyzerAgent(db_manager, qwen_model)
        analyzer.fast_check = True  # Важно! Включаем быстрые проверки как в workflow
        
        # Используем batched-версию метода для ускорения
        analyze_result = analyzer.analyze_messages_batched(
            limit=max(total_messages, 50),
            batch_size=10,
            confidence_threshold=2
        )
        
        analyzed_count = analyze_result.get("analyzed_count", 0)
        
        # Этап 3: Оптимизированная проверка категоризации - только для сообщений с низкой уверенностью
        critic = CriticAgent(db_manager)
        review_result = critic.review_recent_categorizations(
            confidence_threshold=2,  # Только сообщения с уверенностью ≤ 2
            limit=min(30, analyzed_count),  # Ограничиваем количество проверяемых сообщений
            batch_size=5,
            max_workers=3  # Используем несколько потоков
        )
        
        # Обновляем статус перед созданием дайджеста
        await status_message.edit_text(
            f"{status_message.text}\n✅ Проанализировано {analyzed_count} сообщений\n"
            f"Формирование дайджеста... 📝"
        )
        
        # Этап 4: Создание дайджеста
        digester = DigesterAgent(db_manager, gemma_model)
        result = digester.create_digest(
            date=end_date,
            days_back=days_back,
            digest_type="both",  # Создаем оба типа дайджеста
            focus_category=focus_category,
            channels=channels,
            keywords=keywords
        )
        
        if result.get("status") == "no_messages":
            await status_message.edit_text(
                f"{status_message.text}\n❌ Не найдено сообщений, соответствующих критериям фильтрации."
            )
            return
        
        # Сохраняем информацию о генерации
        digest_ids = {}
        if "brief_digest_id" in result:
            digest_ids["brief"] = result["brief_digest_id"]
        if "detailed_digest_id" in result:
            digest_ids["detailed"] = result["detailed_digest_id"]
        
        db_manager.save_digest_generation(
        source="bot",
        user_id=user_id,
        channels=channels,
        messages_count=total_messages,
        digest_ids=digest_ids,
        start_date=start_date,
        end_date=end_date,
        focus_category=focus_category
        )
        
        # Финальное сообщение
        await status_message.edit_text(
            f"✅ Дайджест {description} успешно сформирован!\n\n"
            f"Обработано {total_messages} сообщений, проанализировано {analyzed_count}\n\n"
            f"Используйте команду /list для просмотра доступных дайджестов."
        )
        
    except Exception as e:
        logger.error(f"Ошибка при генерации дайджеста: {str(e)}", exc_info=True)
        await status_message.edit_text(
            f"{status_message.text}\n\n❌ Произошла ошибка: {str(e)}"
        )
"""
Обработчик для интерактивного просмотра дайджестов
"""
async def view_digest_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик колбэка для просмотра дайджеста"""
    query = update.callback_query
    await query.answer()
   
    # Извлекаем ID дайджеста из callback_data
    digest_id = int(query.data.replace("view_digest_", ""))
   
    # Получаем дайджест с секциями по ID
    digest = db_manager.get_digest_by_id_with_sections(digest_id)
   
    if not digest:
        await query.message.reply_text("❌ Дайджест не найден или был удален.")
        return
   
    # Формируем сводное содержание дайджеста
    digest_type = "краткий" if digest["digest_type"] == "brief" else "подробный"
    date_str = digest["date"].strftime("%d.%m.%Y")
   
    # Если есть диапазон дат, используем его
    if digest.get("date_range_start") and digest.get("date_range_end"):
        if digest["date_range_start"].date() != digest["date_range_end"].date():
            date_str = f"{digest['date_range_start'].strftime('%d.%m.%Y')} - {digest['date_range_end'].strftime('%d.%m.%Y')}"
   
    # Создаем статистику категорий
    categories_stats = {}
    for section in digest["sections"]:
        categories_stats[section["category"]] = len(section["text"].split("\n\n"))
   
    # Формируем текст оглавления
    table_of_contents = f"📊 {digest_type.capitalize()} дайджест за {date_str}\n\n"
   
    # Добавляем информацию о фокусе, если есть
    if digest.get("focus_category"):
        table_of_contents += f"🔍 Фокус: {digest['focus_category']}\n\n"
   
    # Добавляем статистику по категориям
    table_of_contents += "📋 Содержание:\n"
    for category, count in categories_stats.items():
        icon = get_category_icon(category)
        table_of_contents += f"{icon} {category.capitalize()}: примерно {count} сообщений\n"
   
    # Создаем клавиатуру для выбора категорий
    keyboard = []
   
    # Инициализируем кэш категорий, если его нет
    if not context.user_data.get("category_mapping"):
        context.user_data["category_mapping"] = {}
   
    # ИЗМЕНЕНИЕ: Для каждой категории создаем кнопку с коротким ID
    for section in digest["sections"]:
        category = section["category"]
        icon = get_category_icon(category)
        
        # Создаем короткий ID для категории
        short_id = get_short_category_id(category)
        
        # Сохраняем маппинг ID -> категория
        mapping_key = f"{digest_id}_{short_id}"
        context.user_data["category_mapping"][mapping_key] = category
        
        # Создаем кнопку с коротким callback_data
        keyboard.append([
            InlineKeyboardButton(
                f"{icon} {category.capitalize()}", 
                callback_data=f"ds_{digest_id}_{short_id}"
            )
        ])
   
    # ИЗМЕНЕНИЕ: Обновляем кнопку полного просмотра
    keyboard.append([
        InlineKeyboardButton("📄 Полный текст дайджеста", callback_data=f"df_{digest_id}")
    ])
   
    # Кнопка возврата к списку дайджестов (изменена для краткости)
    keyboard.append([
        InlineKeyboardButton("⬅️ Назад к списку дайджестов", callback_data="sl")
    ])
   
    reply_markup = InlineKeyboardMarkup(keyboard)
   
    # Отправляем оглавление дайджеста
    await query.message.edit_text(
        table_of_contents,
        reply_markup=reply_markup
    )

       
"""
Вспомогательные функции для интерактивного просмотра дайджестов
"""
async def start_digest_generation(message, start_date, end_date, period_description, db_manager, context):
    """
    Запускает процесс генерации дайджеста по указанным параметрам
    
    Args:
        message (Message): Объект сообщения Telegram
        start_date (datetime): Начальная дата
        end_date (datetime): Конечная дата
        period_description (str): Описание периода для отображения
        db_manager (DatabaseManager): Менеджер БД
        context (CallbackContext): Контекст Telegram
    """
    # Отправляем сообщение о начале генерации
    status_message = await message.edit_text(
        f"Начинаю создание дайджеста {period_description}.\n\n"
        f"Сбор данных... ⏳"
    )
    
    try:
        # Рассчитываем количество дней в периоде
        days_back = (end_date.date() - start_date.date()).days + 1
        
        # Инициализация компонентов
        from llm.qwen_model import QwenLLM
        from llm.gemma_model import GemmaLLM
        from agents.data_collector import DataCollectorAgent
        from agents.analyzer import AnalyzerAgent
        from agents.digester import DigesterAgent
        
        qwen_model = QwenLLM()
        gemma_model = GemmaLLM()
        
        # Этап 1: Сбор данных
        collector = DataCollectorAgent(db_manager)
        
        # Обновляем статус
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"Собираю данные {period_description}... 📥"
        )
        
        # Асинхронно собираем данные
        collect_result = await collector.collect_data(
            days_back=days_back,
            force_update=False,
            start_date=start_date,
            end_date=end_date
        )
        
        total_messages = collect_result.get("total_new_messages", 0)
        
        # Обновляем статус
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"✅ Собрано {total_messages} сообщений из каналов\n"
            f"Анализирую сообщения... 🧠"
        )
        
        # Этап 2: Анализ сообщений
        analyzer = AnalyzerAgent(db_manager, qwen_model)
        analyzer.fast_check = True
        
        analyze_result = analyzer.analyze_messages_batched(
            limit=max(total_messages, 50),
            batch_size=10
        )
        
        analyzed_count = analyze_result.get("analyzed_count", 0)
        
        # Обновляем статус
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"✅ Проанализировано {analyzed_count} сообщений\n"
            f"Формирую дайджест... 📝"
        )
        
        # Этап 3: Создание дайджеста
        digester = DigesterAgent(db_manager, gemma_model)
        
        result = digester.create_digest(
            date=end_date,
            days_back=days_back,
            digest_type="both",  # Создаем оба типа дайджеста
            update_existing=True
        )
        
        # Проверяем результат
        if not (result.get("brief_digest_id") or result.get("detailed_digest_id")):
            await status_message.edit_text(
                f"{status_message.text}\n"
                f"❌ Не удалось создать дайджест. Возможно, не найдено достаточно данных."
            )
            return
        
        # Сохраняем информацию о генерации
        digest_ids = {}
        if "brief_digest_id" in result:
            digest_ids["brief"] = result["brief_digest_id"]
        if "detailed_digest_id" in result:
            digest_ids["detailed"] = result["detailed_digest_id"]
        
        db_manager.save_digest_generation(
        source="bot",
        user_id=context.user_data.get("user_id"),
        messages_count=total_messages,
        digest_ids=dict(digest_ids),  # Преобразуйте в dict, если это не словарь
        start_date=start_date,
        end_date=end_date
        )   
        
        # Финальное сообщение
        await status_message.edit_text(
            f"✅ Дайджест {period_description} успешно сформирован!\n\n"
            f"Обработано {total_messages} сообщений, проанализировано {analyzed_count}\n\n"
            f"Выберите дайджест для просмотра:"
        )
        
        # Создаем кнопки для просмотра созданных дайджестов
        keyboard = []
        
        if "brief_digest_id" in result:
            keyboard.append([
                InlineKeyboardButton("📋 Открыть краткий дайджест", callback_data=f"view_digest_{result['brief_digest_id']}")
            ])
        
        if "detailed_digest_id" in result:
            keyboard.append([
                InlineKeyboardButton("📚 Открыть подробный дайджест", callback_data=f"view_digest_{result['detailed_digest_id']}")
            ])
        
        # Добавляем кнопку возврата к списку дайджестов
        keyboard.append([
            InlineKeyboardButton("📋 К списку всех дайджестов", callback_data="sl")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message.reply_text(
            f"Дайджесты за {period_description} готовы к просмотру:",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка при генерации дайджеста: {str(e)}", exc_info=True)
        await status_message.edit_text(
            f"{status_message.text}\n\n❌ Произошла ошибка: {str(e)}"
        )
