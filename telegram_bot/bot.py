"""
Обновления файла telegram_bot/bot.py для интеграции новых обработчиков команд
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
    start_command, help_command,
    period_command, category_command, list_digests_command, category_selection_command, button_callback,
)
from telegram_bot.view_digest_helpers import (
    show_full_digest, start_digest_generation, get_category_icon
)
from telegram_bot.improved_message_handler import improved_message_handler
from llm.gemma_model import GemmaLLM


logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self, db_manager, llm_model=None): 
        """Инициализация бота"""
        self.db_manager = db_manager
        self.llm_model = llm_model or GemmaLLM()
        self.application = None
        self.menu_commands = [
            # Убираем команды digest и detail, так как они больше не нужны
            # ("digest", "Краткий дайджест новостей"),
            # ("detail", "Подробный дайджест"),
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
        
              
               
        # Команда для периода
        self.application.add_handler(
            CommandHandler("period", lambda update, context: 
                        period_command(update, context, self.db_manager))
        )
        
        # Команды выбора категории
        self.application.add_handler(
            CommandHandler("category", lambda update, context: 
                        category_selection_command(update, context, self.db_manager))
        )

        self.application.add_handler(
            CommandHandler("cat", lambda update, context: 
                        category_selection_command(update, context, self.db_manager))
        )
        
        # Улучшенная команда списка дайджестов
        self.application.add_handler(
            CommandHandler("list", lambda update, context: 
                        list_digests_command(update.message, context, self.db_manager))
        )
        
        # Обработчик колбэков от кнопок (обновленная версия)
        self.application.add_handler(
            CallbackQueryHandler(lambda update, context: 
                                button_callback(update, context, self.db_manager))
        )
        
        # Обработчик текстовых сообщений для ввода произвольного периода
        #self.application.add_handler(
         #   MessageHandler(filters.TEXT & ~filters.COMMAND, lambda update, context: 
          #              message_handler(update, context, self.db_manager, self.llm_model))
        #)
    
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