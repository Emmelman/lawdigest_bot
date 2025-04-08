"""
Обработчики команд для Telegram-бота
"""
import logging
import re
import asyncio
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config.settings import CATEGORIES, BOT_USERNAME, TELEGRAM_CHANNELS
from llm.gemma_model import GemmaLLM
from agents.digester import DigesterAgent
from agents.data_collector import DataCollectorAgent
from agents.analyzer import AnalyzerAgent
from agents.critic import CriticAgent
from utils.text_utils import TextUtils

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
        "/digest - получить краткий дайджест\n"
        "/digest_detailed - получить подробный дайджест\n"
        "/category - выбрать категорию новостей\n"
        "/help - получить справку"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /help"""
    await update.message.reply_text(
        "Я могу предоставить вам дайджест правовых новостей.\n\n"
        "Доступные команды:\n"
        "/digest - получить краткий дайджест\n"
        "/digest_detailed - получить подробный дайджест\n"
        "/category - выбрать категорию новостей\n"
        "/help - получить справку\n\n"
        "Вы также можете задать мне вопрос по правовым новостям."
    )

# Обработчики дайджестов
async def digest_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /digest - краткий дайджест"""
    # Получаем последний краткий дайджест
    digest = db_manager.get_latest_digest_with_sections(digest_type="brief")
    
    if not digest:
        # Если краткого нет, пробуем получить любой
        digest = db_manager.get_latest_digest_with_sections()
    
    if not digest:
        await update.message.reply_text("К сожалению, дайджест еще не сформирован.")
        return
    
    # Очищаем текст и отправляем дайджест по частям
    safe_text = utils.clean_markdown_text(digest["text"])
    chunks = utils.split_text(safe_text)
    
    for i, chunk in enumerate(chunks):
        if i == 0:
            text_html = utils.convert_to_html(chunk)
            await update.message.reply_text(
                f"Дайджест за {digest['date'].strftime('%d.%m.%Y')} (краткая версия):\n\n{text_html}",
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(chunk, parse_mode='HTML')

async def digest_detailed_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /digest_detailed - подробный дайджест"""
    # Получаем последний подробный дайджест
    digest = db_manager.get_latest_digest_with_sections(digest_type="detailed")
    
    if not digest:
        # Если подробного нет, пробуем получить любой
        digest = db_manager.get_latest_digest_with_sections()
    
    if not digest:
        await update.message.reply_text("К сожалению, подробный дайджест еще не сформирован.")
        return
    
    # Очищаем текст и отправляем дайджест по частям
    safe_text = utils.clean_markdown_text(digest["text"])
    chunks = utils.split_text(safe_text)
    
    for i, chunk in enumerate(chunks):
        if i == 0:
            text_html = utils.convert_to_html(chunk)
            await update.message.reply_text(
                f"Дайджест за {digest['date'].strftime('%d.%m.%Y')} (подробная версия):\n\n{text_html}",
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(chunk, parse_mode='HTML')

async def date_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /date - получение дайджеста за определенную дату"""
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "Пожалуйста, укажите дату в формате ДД.ММ.ГГГГ, например: /date 01.04.2025"
        )
        return
    
    date_str = context.args[0]
    try:
        # Парсим дату из строки
        date_parts = date_str.split(".")
        if len(date_parts) != 3:
            raise ValueError("Неверный формат даты")
        
        day, month, year = map(int, date_parts)
        target_date = datetime(year, month, day)
        
        # Проверяем, есть ли сообщения за указанную дату
        messages = db_manager.get_messages_by_date_range(
            start_date=target_date, 
            end_date=target_date + timedelta(days=1)
        )
        
        if not messages:
            # Отправляем сообщение о начале сбора данных
            status_message = await update.message.reply_text(
                f"За {date_str} не найдено сообщений. Начинаю сбор данных... ⏳"
            )
            
            # Запускаем сбор данных
            collector = DataCollectorAgent(db_manager)
            collect_days = (datetime.now() - target_date).days + 1
            
            await status_message.edit_text(
                f"{status_message.text}\nСобираю данные за последние {collect_days} дней..."
            )
            
            # Асинхронно собираем данные
            collect_result = await collector._collect_all_channels_parallel(days_back=collect_days)
            total_messages = sum(collect_result.values())
            
            await status_message.edit_text(
                f"{status_message.text}\n✅ Собрано {total_messages} сообщений."
            )
            
            if total_messages == 0:
                await status_message.edit_text(
                    f"{status_message.text}\n❌ К сожалению, не удалось найти сообщения за {date_str}."
                )
                return
            
            # Проверяем, появились ли сообщения за указанную дату
            messages = db_manager.get_messages_by_date_range(
                start_date=target_date, 
                end_date=target_date + timedelta(days=1)
            )
            
            if not messages:
                await status_message.edit_text(
                    f"{status_message.text}\n❌ К сожалению, не удалось найти сообщения за {date_str}."
                )
                return
                
            await status_message.edit_text(
                f"{status_message.text}\n✅ Найдено {len(messages)} сообщений за {date_str}."
                f"\nНачинаю анализ и формирование дайджеста..."
            )
        
        # Получаем дайджест по дате (с автоматическим созданием при необходимости)
        digest = db_manager.get_digest_by_date_with_sections(target_date, generate_if_missing=True)
        
        if not digest:
            await update.message.reply_text(
                f"К сожалению, не удалось сформировать дайджест за {date_str}."
            )
            return
        
        # Отправляем дайджест по частям
        safe_text = utils.clean_markdown_text(digest["text"])
        chunks = utils.split_text(safe_text)
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                text_html = utils.convert_to_html(chunk)
                await update.message.reply_text(
                    f"Дайджест за {digest['date'].strftime('%d.%m.%Y')}:\n\n{text_html}",
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(chunk, parse_mode='HTML')
                
    except ValueError as e:
        await update.message.reply_text(
            f"Ошибка в формате даты. Пожалуйста, используйте формат ДД.ММ.ГГГГ, например: 01.04.2025"
        )
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /date: {str(e)}", exc_info=True)
        await update.message.reply_text(
            "Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте позже."
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

async def generate_digest_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /generate - запуск генерации дайджеста"""
    keyboard = [
        [InlineKeyboardButton("За сегодня", callback_data="gen_digest_today")],
        [InlineKeyboardButton("За вчера", callback_data="gen_digest_yesterday")],
        [InlineKeyboardButton("За период", callback_data="gen_digest_range")],
        [InlineKeyboardButton("С фокусом на категорию", callback_data="gen_digest_category")],
        [InlineKeyboardButton("С фильтрацией по каналам", callback_data="gen_digest_channels")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Выберите тип дайджеста для генерации:", 
        reply_markup=reply_markup
    )

async def list_digests_command(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик команды /list - список доступных дайджестов"""
    # Получаем последние 10 дайджестов
    digests = db_manager.find_digests_by_parameters(limit=10)
    
    if not digests:
        await update.message.reply_text("Дайджесты еще не сформированы.")
        return
    
    keyboard = []
    for digest in digests:
        # Формируем описание дайджеста
        if digest.get("date_range_start") and digest.get("date_range_end"):
            days_diff = (digest["date_range_end"] - digest["date_range_start"]).days
            if days_diff > 0:
                start_date = digest["date_range_start"].strftime("%d.%m.%Y")
                end_date = digest["date_range_end"].strftime("%d.%m.%Y")
                date_text = f"{start_date} - {end_date} ({days_diff+1} дн.)"
            else:
                date_text = digest["date"].strftime("%d.%m.%Y")
        else:
            date_text = digest["date"].strftime("%d.%m.%Y")
        
        # Добавляем информацию о фокусе, если есть
        focus_text = ""
        if digest.get("focus_category"):
            focus_text = f" - {digest['focus_category']}"
        
        # Добавляем время создания
        created_at = ""
        if digest.get("created_at"):
            created_at = f" ({digest['created_at'].strftime('%H:%M')})"
        
        button_text = f"{date_text}{focus_text} ({digest['digest_type']}){created_at}"
        keyboard.append([
            InlineKeyboardButton(button_text, callback_data=f"show_digest_{digest['id']}")
        ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Выберите дайджест для просмотра:", 
        reply_markup=reply_markup
    )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager, llm_model):
    """Обработчик текстовых сообщений"""
    user_message = update.message.text
    
    # Проверяем, ждем ли мы от пользователя конкретный ввод 
    # (например, диапазон дат или название категории)
    if context.user_data.get("awaiting_date_range"):
        # Обрабатываем ввод диапазона дат
        await handle_date_range_input(update, context, db_manager, user_message)
        return
    
    if context.user_data.get("awaiting_category_period"):
        # Обрабатываем ввод периода для категории
        await handle_category_period_input(update, context, db_manager, user_message)
        return
    
    if context.user_data.get("awaiting_channel_period"):
        # Обрабатываем ввод периода для канала
        await handle_channel_period_input(update, context, db_manager, user_message)
        return
    
    # Если нет особых ожиданий, рассматриваем как вопрос к боту
    # Получаем контекст для ответа
    brief_digest = db_manager.get_latest_digest_with_sections(digest_type="brief")
    detailed_digest = db_manager.get_latest_digest_with_sections(digest_type="detailed")
    
    # Используем подробный дайджест для контекста, если он есть
    digest = detailed_digest or brief_digest
    
    if not digest:
        await update.message.reply_text(
            "К сожалению, у меня пока нет информации для ответа на ваш вопрос. "
            "Дайджест еще не сформирован."
        )
        return
    
    # Формируем запрос к модели
    prompt = f"""
    Вопрос: {user_message}
    
    Контекст (дайджест правовых новостей):
    {digest["text"]}
    
    Дай краткий и точный ответ на вопрос на основе представленного контекста.
    Если информации недостаточно, так и скажи.
    Если вопрос касается определенной категории новостей, укажи, что пользователь может 
    получить более подробную информацию по этой категории с помощью команды /category.
    """
    
    # Получаем ответ от модели
    try:
        response = llm_model.generate(prompt, max_tokens=500)
        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Ошибка при генерации ответа: {str(e)}")
        await update.message.reply_text(
            "Извините, произошла ошибка при обработке вашего запроса. "
            "Пожалуйста, попробуйте позже или воспользуйтесь командами /digest или /category."
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
async def handle_gen_digest_callback(query, context, db_manager):
    """Обработка колбэков для генерации дайджеста"""
    action = query.data.replace("gen_digest_", "")
    
    if action == "today":
        # Генерация за сегодня
        today = datetime.now()
        await handle_digest_generation(
            query, context, db_manager, today, today, "За сегодня"
        )
    elif action == "yesterday":
        # Генерация за вчера
        yesterday = datetime.now() - timedelta(days=1)
        await handle_digest_generation(
            query, context, db_manager, yesterday, yesterday, "За вчера"
        )
    elif action == "range":
        # Запрашиваем диапазон дат
        context.user_data["awaiting_date_range"] = True
        await query.message.reply_text(
            "Укажите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, например: 01.04.2025-07.04.2025"
        )
    elif action == "category":
        # Выбор категории для фокуса
        keyboard = []
        for category in CATEGORIES + ["другое"]:
            keyboard.append([
                InlineKeyboardButton(
                    category, callback_data=f"gen_digest_cat_{category}"
                )
            ])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text(
            "Выберите категорию для формирования фокусированного дайджеста:", 
            reply_markup=reply_markup
        )
    elif action == "channels":
        # Выбор каналов для фильтрации
        keyboard = []
        for channel in TELEGRAM_CHANNELS:
            display_name = channel.replace("@", "") if channel.startswith("@") else channel
            keyboard.append([
                InlineKeyboardButton(
                    display_name, callback_data=f"gen_digest_chan_{channel}"
                )
            ])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text(
            "Выберите канал для формирования дайджеста:", 
            reply_markup=reply_markup
        )
    elif action.startswith("cat_"):
        # Выбрана категория для фокуса
        category = action.replace("cat_", "")
        context.user_data["focus_category"] = category
        context.user_data["awaiting_category_period"] = True
        await query.message.reply_text(
            f"Выбрана категория: {category}\n"
            "Теперь укажите период в формате ДД.ММ.ГГГГ или ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, "
            "или напишите 'сегодня' или 'вчера'"
        )
    elif action.startswith("chan_"):
        # Выбран канал для фильтрации
        channel = action.replace("chan_", "")
        context.user_data["focus_channel"] = channel
        context.user_data["channels"] = [channel]
        await query.message.reply_text(
            f"Выбран канал: {channel}\n"
            "Теперь укажите период в формате ДД.ММ.ГГГГ или ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, "
            "или напишите 'сегодня' или 'вчера'"
        )
        context.user_data["awaiting_channel_period"] = True

# Общий обработчик колбэков
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    # Обработка запросов на генерацию дайджеста
    if query.data.startswith("gen_digest_"):
        await handle_gen_digest_callback(query, context, db_manager)
    # Обработка запросов на просмотр дайджеста
    elif query.data.startswith("show_digest_"):
        digest_id = int(query.data.replace("show_digest_", ""))
        await show_digest_by_id(query.message, digest_id, db_manager)
    # Обработка выбора категории
    elif query.data.startswith("cat_"):
        # Формат: cat_[тип]_[категория]
        parts = query.data.split("_", 2)
        if len(parts) == 3:
            digest_type = parts[1]  # brief или detailed
            category = parts[2]     # название категории
            
            # Получаем последний дайджест нужного типа
            digest = db_manager.get_latest_digest_with_sections(digest_type=digest_type)
            
            if not digest:
                # Если дайджеста такого типа нет, берем любой
                digest = db_manager.get_latest_digest_with_sections()
            
            if not digest:
                await query.message.reply_text(f"К сожалению, дайджест еще не сформирован.")
                return
            
            # Ищем соответствующую секцию в дайджесте
            section = next(
                (s for s in digest["sections"] if s["category"] == category), 
                None
            )
            
            if not section:
                await query.message.reply_text(
                    f"Информация по категории '{category}' отсутствует в последнем дайджесте.",
                    parse_mode='HTML'
                )
                return
            
            # Подготавливаем текст для ответа
            digest_type_name = "Краткий обзор" if digest_type == "brief" else "Подробный обзор"
            header = f"Дайджест за {digest['date'].strftime('%d.%m.%Y')}\n{digest_type_name} категории: {category}\n\n"
            
            # Отправляем секцию (возможно, разбитую на части)
            full_text = header + section["text"]
            safe_text = utils.clean_markdown_text(full_text)
            chunks = utils.split_text(safe_text)
            
            for chunk in chunks:
                text_html = utils.convert_to_html(chunk)
                await query.message.reply_text(text_html, parse_mode='HTML')

# Вспомогательные функции
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
                          description, focus_category=None, channels=None, keywords=None):
    """Асинхронный запуск генерации дайджеста с отслеживанием статуса"""
    
    # Определяем, откуда пришел запрос (от сообщения или колбэка)
    message = update.message if hasattr(update, 'message') else update.message
    user_id = update.effective_user.id
    
    # Если даты не указаны, используем время последней генерации
    if not start_date:
        last_generation = db_manager.get_last_digest_generation(source="bot", user_id=user_id)
        
        if last_generation:
            # Используем время последней генерации как начальную точку
            start_date = last_generation["timestamp"]
            
            # Если генерация была сегодня и не указан специальный режим, сообщаем пользователю
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
            # Если нет предыдущей генерации, используем начало дня
            start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Если конечная дата не указана, используем текущее время
    if not end_date:
        end_date = datetime.now()
    
    # Если описание не указано, создаем его
    if not description:
        if start_date.date() == end_date.date():
            description = f"за {start_date.strftime('%d.%m.%Y')} с {start_date.strftime('%H:%M')} по {end_date.strftime('%H:%M')}"
        else:
            description = f"за период с {start_date.strftime('%d.%m.%Y %H:%M')} по {end_date.strftime('%d.%m.%Y %H:%M')}"

    # Отправляем начальное сообщение о статусе
    status_message = await message.reply_text(
        f"Запущена генерация дайджеста {description}.\n"
        f"Период: с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}\n"
        f"{'Фокус на категории: ' + focus_category if focus_category else ''}\n"
        f"{'Каналы: ' + ', '.join(channels) if channels else ''}\n\n"
        "Сбор данных начнется через несколько секунд... ⏳"
    )
    
    # Определяем количество дней для сбора данных
    days_back = (end_date - start_date).days + 1
    
    try:
        # Этап 1: Сбор данных
        await status_message.edit_text(
            f"{status_message.text}\n\nСбор данных за {days_back} дней... 🔍"
        )
        
        collector = DataCollectorAgent(db_manager)
        collect_results = await collector._collect_all_channels_parallel(days_back=days_back)
        
        total_messages = sum(collect_results.values())
        await status_message.edit_text(
            f"{status_message.text}\n✅ Собрано {total_messages} новых сообщений."
        )
        
        # Этап 2: Анализ сообщений
        await status_message.edit_text(
            f"{status_message.text}\n\nАнализ и категоризация сообщений... 🧠"
        )
        
        # Создаем экземпляр аналитика и запускаем анализ
        analyzer = AnalyzerAgent(db_manager)
        analyzer.fast_check = True  # Включаем быстрые проверки
        analyze_result = analyzer.analyze_messages(limit=total_messages or 100)
        
        analyzed_count = analyze_result.get("analyzed_count", 0)
        await status_message.edit_text(
            f"{status_message.text}\n✅ Проанализировано {analyzed_count} сообщений."
        )
        
        # Этап 3: Проверка категоризации
        await status_message.edit_text(
            f"{status_message.text}\n\nПроверка категоризации сообщений с низкой уверенностью... 🔍"
        )
        
        critic = CriticAgent(db_manager)
        review_result = critic.review_recent_categorizations(
            confidence_threshold=2,  # Проверяем сообщения с уверенностью <= 2
            limit=30
        )
        
        updated_count = review_result.get("updated", 0)
        await status_message.edit_text(
            f"{status_message.text}\n✅ Проверено {review_result.get('total', 0)} сообщений, "
            f"уточнено {updated_count} категорий."
        )
        
        # Этап 4: Формирование дайджеста
        await status_message.edit_text(
            f"{status_message.text}\n\nФормирование дайджеста... 📝"
        )
        
        # Создаем экземпляр дайджестера и формируем дайджест
        digester = DigesterAgent(db_manager)
        result = digester.create_digest(
            date=end_date,
            days_back=days_back,
            digest_type="both",
            focus_category=focus_category,
            channels=channels,
            keywords=keywords
        )
        
        # Проверяем результат
        if result.get("status") == "no_messages":
            await status_message.edit_text(
                f"{status_message.text}\n❌ Не найдено сообщений, соответствующих критериям фильтрации."
            )
            return
        
        # Сообщение об успешном завершении
        await status_message.edit_text(
            f"✅ Дайджест {description} успешно сформирован!\n\n"
            f"Период: с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}\n"
            f"{'Фокус на категории: ' + focus_category if focus_category else ''}\n"
            f"{'Каналы: ' + ', '.join(channels) if channels else ''}\n\n"
            f"Используйте команду /list для просмотра доступных дайджестов."
        )
        
    except Exception as e:
        logger.error(f"Ошибка при генерации дайджеста: {str(e)}", exc_info=True)
        await status_message.edit_text(
            f"{status_message.text}\n\n❌ Произошла ошибка: {str(e)}"
        )
        
        # В конце при успешной генерации сохраняем информацию
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
            digest_ids=digest_ids
        )