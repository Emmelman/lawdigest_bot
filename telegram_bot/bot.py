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

from config.settings import TELEGRAM_BOT_TOKEN, CATEGORIES
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
        await update.message.reply_text(
            f"Здравствуйте, {user.first_name}! Я бот для дайджеста правовых новостей.\n\n"
            "Доступные команды:\n"
            "/digest - получить последний дайджест\n"
            "/category - выбрать категорию новостей\n"
            "/help - получить справку"
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        await update.message.reply_text(
            "Я могу предоставить вам дайджест правовых новостей.\n\n"
            "Доступные команды:\n"
            "/digest - получить последний дайджест\n"
            "/category - выбрать категорию новостей\n"
            "/help - получить справку\n\n"
            "Вы также можете задать мне вопрос по правовым новостям."
        )
    
    async def digest_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /digest"""
        digest = self.db_manager.get_latest_digest()
        
        if not digest:
            await update.message.reply_text("К сожалению, дайджест еще не сформирован.")
            return
        
        # Отправляем дайджест по частям, так как Telegram ограничивает длину сообщения
        chunks = self._split_text(digest.text)
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                await update.message.reply_text(f"Дайджест за {digest.date.strftime('%d.%m.%Y')}:\n\n{chunk}")
            else:
                await update.message.reply_text(chunk)
    
    async def category_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /category"""
        keyboard = [[InlineKeyboardButton(cat, callback_data=f"cat_{cat}")] for cat in CATEGORIES]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Выберите категорию новостей:", 
            reply_markup=reply_markup
        )
    
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатий на кнопки"""
        query = update.callback_query
        await query.answer()
        
        if query.data.startswith("cat_"):
            category = query.data[4:]  # Убираем префикс "cat_"
            
            # Получаем последний дайджест
            digest = self.db_manager.get_latest_digest()
            
            if not digest:
                await query.message.reply_text("К сожалению, дайджест еще не сформирован.")
                return
            
            # Ищем соответствующую секцию в дайджесте
            section = next(
                (s for s in digest.sections if s.category == category), 
                None
            )
            
            if not section:
                await query.message.reply_text(f"Информация по категории '{category}' отсутствует в последнем дайджесте.")
                return
            
            # Отправляем секцию
            await query.message.reply_text(
                f"Дайджест за {digest.date.strftime('%d.%m.%Y')}\n"
                f"Категория: {category}\n\n"
                f"{section.text}"
            )
    
    async def message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений"""
        user_message = update.message.text
        
        # Получаем контекст для ответа
        digest = self.db_manager.get_latest_digest()
        context_text = "информация отсутствует"
        
        if digest:
            context_text = digest.text
        
        # Формируем запрос к модели
        prompt = f"""
        Вопрос: {user_message}
        
        Контекст (дайджест правовых новостей):
        {context_text}
        
        Дай краткий и точный ответ на вопрос на основе представленного контекста.
        Если информации недостаточно, так и скажи.
        """
        
        # Получаем ответ от модели
        try:
            response = self.llm_model.generate(prompt, max_tokens=500)
            await update.message.reply_text(response)
        except Exception as e:
            logger.error(f"Ошибка при генерации ответа: {str(e)}")
            await update.message.reply_text(
                "Извините, произошла ошибка при обработке вашего запроса. "
                "Пожалуйста, попробуйте позже или воспользуйтесь командами /digest или /category."
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
    
    def run(self):
        """Запуск бота"""
        logger.info("Запуск Telegram-бота")
        
        # Создаем приложение
        self.application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
        # Регистрируем обработчики
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("digest", self.digest_command))
        self.application.add_handler(CommandHandler("category", self.category_command))
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.message_handler))
        
        # Запускаем бота
        self.application.run_polling()
        
        return self.application