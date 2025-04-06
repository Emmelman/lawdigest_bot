# telegram_bot/bot.py
"""
Telegram-бот для взаимодействия с пользователями
"""
import logging
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, 
    CommandHandler, 
    MessageHandler, 
    CallbackQueryHandler,
    ContextTypes, 
    filters
)

from config.settings import TELEGRAM_BOT_TOKEN, CATEGORIES, BOT_USERNAME
from database.db_manager import DatabaseManager
from llm.gemma_model import GemmaLLM

logger = logging.getLogger(__name__)

class TelegramBot:
    """Класс для управления Telegram-ботом"""
    
    def __init__(self, db_manager, llm_model=None):
        """
        Инициализация бота
        
        Args:
            db_manager (DatabaseManager): Менеджер БД
            llm_model (GemmaLLM, optional): Модель для ответов на вопросы
        """
        self.db_manager = db_manager
        self.llm_model = llm_model or GemmaLLM()
        self.application = None
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user = update.effective_user
        
        # Проверяем, есть ли параметры в команде start
        if context.args and context.args[0].startswith('msg_'):
            # Извлекаем ID сообщения
            try:
                message_id = int(context.args[0].replace('msg_', ''))
                message = self.db_manager.get_message_by_id(message_id)
                
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
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    
    async def digest_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /digest - краткий дайджест"""
        # Получаем последний краткий дайджест
        digest = self.db_manager.get_latest_digest_with_sections(digest_type="brief")
        
        if not digest:
            # Если краткого нет, пробуем получить любой
            digest = self.db_manager.get_latest_digest_with_sections()
        
        if not digest:
            await update.message.reply_text("К сожалению, дайджест еще не сформирован.")
            return
        
        # Очищаем текст от проблемных Markdown-сущностей
        safe_text = self._clean_markdown_text(digest["text"])
        
        # Отправляем дайджест по частям, так как Telegram ограничивает длину сообщения
        chunks = self._split_text(safe_text)
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                await update.message.reply_text(
                    f"Дайджест за {digest['date'].strftime('%d.%m.%Y')} (краткая версия):\n\n{chunk}",
                    parse_mode=None  # Отключаем Markdown parsing
                )
            else:
                await update.message.reply_text(chunk, parse_mode=None)

    async def digest_detailed_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /digest_detailed - подробный дайджест"""
        # Получаем последний подробный дайджест
        digest = self.db_manager.get_latest_digest_with_sections(digest_type="detailed")
        
        if not digest:
            # Если подробного нет, пробуем получить любой
            digest = self.db_manager.get_latest_digest_with_sections()
        
        if not digest:
            await update.message.reply_text("К сожалению, подробный дайджест еще не сформирован.")
            return
        
        # Очищаем текст от проблемных Markdown-сущностей
        safe_text = self._clean_markdown_text(digest["text"])
        
        # Отправляем дайджест по частям, так как Telegram ограничивает длину сообщения
        chunks = self._split_text(safe_text)
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                await update.message.reply_text(
                    f"Дайджест за {digest['date'].strftime('%d.%m.%Y')} (подробная версия):\n\n{chunk}",
                    parse_mode=None  # Отключаем Markdown parsing
                )
            else:
                await update.message.reply_text(chunk, parse_mode=None)

    def _clean_markdown_text(self, text):
        """
        Очищает текст от проблемных Markdown-сущностей
        
        Args:
            text (str): Исходный текст с Markdown
            
        Returns:
            str: Безопасный текст
        """
        import re
        
        # Экранируем специальные символы Markdown
        text = re.sub(r'([_*\[\]()~`>#\+\-=|{}.!])', r'\\\1', text)
        
        # Удаляем незакрытые ссылки
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)(?!\))', r'\1', text)
        
        return text
    
    async def category_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /category"""
        keyboard = []
        
        # Для каждой категории создаем две кнопки - краткий и подробный обзор
        for cat in CATEGORIES:
            keyboard.append([
                InlineKeyboardButton(f"{cat} (кратко)", callback_data=f"cat_brief_{cat}"),
                InlineKeyboardButton(f"{cat} (подробно)", callback_data=f"cat_detailed_{cat}")
            ])
        
        # Добавляем кнопку для категории "другое"
        keyboard.append([
            InlineKeyboardButton("другое (кратко)", callback_data="cat_brief_другое"),
            InlineKeyboardButton("другое (подробно)", callback_data="cat_detailed_другое")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Выберите категорию и тип новостей:", 
            reply_markup=reply_markup
        )
    
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатий на кнопки"""
        query = update.callback_query
        await query.answer()
        
        # Обработка категорий
        if query.data.startswith("cat_"):
            # Формат: cat_[тип]_[категория]
            parts = query.data.split("_", 2)
            if len(parts) == 3:
                digest_type = parts[1]  # brief или detailed
                category = parts[2]     # название категории
            
                # Получаем последний дайджест нужного типа
                digest = self.db_manager.get_latest_digest_with_sections(digest_type=digest_type)
                
                if not digest:
                    # Если дайджеста такого типа нет, берем любой
                    digest = self.db_manager.get_latest_digest_with_sections()
                
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
                        parse_mode='Markdown'
                    )
                    return
                
                # Подготавливаем текст для ответа
                digest_type_name = "Краткий обзор" if digest_type == "brief" else "Подробный обзор"
                header = f"Дайджест за {digest['date'].strftime('%d.%m.%Y')}\n{digest_type_name} категории: {category}\n\n"
                
                # Отправляем секцию (возможно, разбитую на части)
                full_text = header + section["text"]
                chunks = self._split_text(full_text)
                
                for chunk in chunks:
                    await query.message.reply_text(chunk)
    
    async def message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений"""
        user_message = update.message.text
        
        # Получаем контекст для ответа
        brief_digest = self.db_manager.get_latest_digest_with_sections(digest_type="brief")
        detailed_digest = self.db_manager.get_latest_digest_with_sections(digest_type="detailed")
        
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
            response = self.llm_model.generate(prompt, max_tokens=500)
            await update.message.reply_text(response)
        except Exception as e:
            logger.error(f"Ошибка при генерации ответа: {str(e)}")
            await update.message.reply_text(
                "Извините, произошла ошибка при обработке вашего запроса. "
                "Пожалуйста, попробуйте позже или воспользуйтесь командами /digest или /category.",
                parse_mode='Markdown'
            )
    
    async def date_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            
            # Получаем дайджест по дате
            digest = self.db_manager.get_digest_by_date_with_sections(target_date)
            
            if not digest:
                await update.message.reply_text(
                    f"Дайджест за {date_str} не найден. Возможно, он еще не был сформирован."
                )
                return
            
            # Отправляем дайджест по частям
            chunks = self._split_text(digest["text"])
            
            for i, chunk in enumerate(chunks):
                if i == 0:
                    await update.message.reply_text(
                        f"Дайджест за {digest['date'].strftime('%d.%m.%Y')}:\n\n{chunk}",
                        parse_mode='Markdown'
                    )
                else:
                    await update.message.reply_text(chunk)
                    
        except ValueError as e:
            await update.message.reply_text(
                f"Ошибка в формате даты. Пожалуйста, используйте формат ДД.ММ.ГГГГ, например: 01.04.2025"
            )
        except Exception as e:
            logger.error(f"Ошибка при обработке команды /date: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте позже."
            )
    
    def _split_text(self, text, max_length=4000):
        """
        Разбивает длинный текст на части, учитывая ограничения Telegram
        
        Args:
            text (str): Исходный текст
            max_length (int): Максимальная длина части
            
        Returns:
            list: Список частей текста
        """
        if len(text) <= max_length:
            return [text]
        
        parts = []
        current_part = ""
        
        for paragraph in text.split("\n\n"):
            # Если абзац сам по себе слишком длинный
            if len(paragraph) > max_length:
                # Добавляем предыдущий фрагмент, если есть
                if current_part:
                    parts.append(current_part)
                    current_part = ""
                
                # Разбиваем длинный абзац на предложения
                sentences = paragraph.split(". ")
                sentence_part = ""
                
                for sentence in sentences:
                    if len(sentence_part) + len(sentence) + 2 <= max_length:
                        if sentence_part:
                            sentence_part += ". " + sentence
                        else:
                            sentence_part = sentence
                    else:
                        parts.append(sentence_part.strip())
                        sentence_part = sentence
                
                if sentence_part:
                    parts.append(sentence_part.strip())
            
            # Обычный случай - проверяем, можно ли добавить абзац к текущей части
            elif len(current_part) + len(paragraph) + 2 <= max_length:
                if current_part:
                    current_part += "\n\n" + paragraph
                else:
                    current_part = paragraph
            else:
                parts.append(current_part)
                current_part = paragraph
        
        if current_part:
            parts.append(current_part)
        
        return parts
    def _clean_markdown_text(self, text):
        """
        Корректная обработка Markdown текста
        """
        import re
        
        # Специальная функция для экранирования только специфических символов
        def escape_markdown(text):
            escape_chars = r'_*[]()~`>#+-=|{}.!'
            return ''.join(['\\' + char if char in escape_chars else char for char in text])
        
        # Обработка ссылок, чтобы они корректно отображались
        def process_links(match):
            text, url = match.groups()
            # Экранируем текст внутри квадратных скобок
            safe_text = escape_markdown(text)
            return f'[{safe_text}]({url})'
        
        # Обработка жирного текста
        def process_bold(match):
            text = match.group(1)
            # Экранируем текст внутри жирного форматирования
            safe_text = escape_markdown(text)
            return f'*{safe_text}*'
        
        # Сначала обрабатываем ссылки
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', process_links, text)
        
        # Затем обрабатываем жирный текст
        text = re.sub(r'\*\*([^*]+)\*\*', process_bold, text)
        
        return text

    def run(self):
        """Запуск бота"""
        logger.info("Запуск Telegram-бота")
        
        # Создаем приложение
        self.application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
        # Регистрируем обработчики
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("digest", self.digest_command))
        self.application.add_handler(CommandHandler("digest_detailed", self.digest_detailed_command))
        self.application.add_handler(CommandHandler("category", self.category_command))
        self.application.add_handler(CommandHandler("date", self.date_command))
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.message_handler))
        
        # Запускаем бота
        self.application.run_polling()
        
        return self.application