# telegram_bot/bot.py
"""
Telegram-бот для взаимодействия с пользователями
"""
import logging
from telegram import BotCommand
import asyncio
from telegram.ext import (
    Application, 
    CommandHandler, 
    MessageHandler, 
    CallbackQueryHandler,
    filters
)
from config.settings import TELEGRAM_BOT_TOKEN
from telegram_bot.handlers import (
    start_command, help_command, digest_command, digest_detailed_command,
    period_command, category_command, list_digests_command,
    message_handler, button_callback
)
from llm.gemma_model import GemmaLLM

logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self, db_manager, llm_model=None): 
        """Инициализация бота"""
        self.db_manager = db_manager
        self.llm_model = llm_model or GemmaLLM()
        self.application = None
        self.menu_commands = [
            #("digest", "Краткий дайджест новостей"),
            #("detail", "Подробный дайджест"),
            ("period", "Дайджест за произвольный период (сегодня/вчера/YYYY-MM-DD)"),
            ("cat", "Выбрать категорию новостей"),
            ("list", "Список доступных дайджестов"),
            ("help", "Справка")
        ]
    
    async def setup_commands(self):
        """Регистрация обработчиков команд"""
        # Обработчики основных команд
        self.application.add_handler(
            CommandHandler("start", lambda update, context: 
                        start_command(update, context, self.db_manager))
        )
        
        self.application.add_handler(
            CommandHandler("help", lambda update, context: 
                        help_command(update, context, self.db_manager))
        )
        
        # Команды дайджестов
        self.application.add_handler(
            CommandHandler("digest", lambda update, context: 
                        digest_command(update, context, self.db_manager))
        )
        
        self.application.add_handler(
            CommandHandler("brief", lambda update, context: 
                        digest_command(update, context, self.db_manager))
        )
        
        #self.application.add_handler(
        #    CommandHandler("digest_detailed", lambda update, context: 
        #                digest_detailed_command(update, context, self.db_manager))
        #)
        
        #self.application.add_handler(
        #   CommandHandler("detail", lambda update, context: 
        #              digest_detailed_command(update, context, self.db_manager))
        #)
        
        # Команда для периода
        self.application.add_handler(
            CommandHandler("period", lambda update, context: 
                        period_command(update, context, self.db_manager))
        )
        
        # Команды выбора категории
        self.application.add_handler(
            CommandHandler("category", lambda update, context: 
                        category_command(update, context, self.db_manager))
        )
        
        self.application.add_handler(
            CommandHandler("cat", lambda update, context: 
                        category_command(update, context, self.db_manager))
        )
        
        # Список дайджестов
        self.application.add_handler(
            CommandHandler("list", lambda update, context: 
                        list_digests_command(update, context, self.db_manager))
        )
        
        # Обработчик колбэков от кнопок
        self.application.add_handler(
            CallbackQueryHandler(lambda update, context: 
                                button_callback(update, context, self.db_manager))
        )
        
        # Обработчик текстовых сообщений
        self.application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, lambda update, context: 
                        message_handler(update, context, self.db_manager, self.llm_model))
        )
    
    def run(self):
        """Запуск бота"""
        logger.info("Запуск Telegram-бота")
        
        # Создаем приложение
        self.application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
        # Настраиваем команды для меню бота
        commands = [
            BotCommand(command, description) for command, description in self.menu_commands
        ]
        
        # Настраиваем обработчики команд
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.setup_commands())
        
        # Устанавливаем команды в интерфейсе Telegram
        async def setup_commands_job(context):
            await context.bot.set_my_commands(commands)
        
        self.application.job_queue.run_once(setup_commands_job, 1)
        
        # Запускаем бота
        self.application.run_polling()
        
        return self.application