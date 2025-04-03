# run_telegram_bot.py
import os
import asyncio
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters

from config.settings import TELEGRAM_BOT_TOKEN
from database.db_manager import DatabaseManager
from telegram_bot.bot import TelegramBot

# Загрузка переменных окружения
load_dotenv()

# URL базы данных
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///lawdigest.db")

async def main():
    """Главная функция для запуска Telegram-бота"""
    print("Запуск Telegram-бота для показа дайджеста...")

    # Инициализация менеджера БД
    db_manager = DatabaseManager(DATABASE_URL)
    
    # Проверяем, есть ли дайджест в БД
    digest = db_manager.get_latest_digest()
    if not digest:
        print("В базе данных нет дайджестов. Сначала сгенерируйте дайджест.")
        return
    
    print(f"Найден дайджест от {digest.date.strftime('%d.%m.%Y')} (ID: {digest.id})")
    print(f"Текст дайджеста: {digest.text[:100]}...")
    
    # Инициализация бота
    try:
        bot = TelegramBot(db_manager)
        
        # Создание приложения
        application = (
            ApplicationBuilder()
            .token(TELEGRAM_BOT_TOKEN)
            .connect_timeout(30.0)
            .pool_timeout(30.0)
            .read_timeout(30.0)
            .build()
        )
        
        # Регистрация обработчиков
        application.add_handler(CommandHandler("start", bot.start_command))
        application.add_handler(CommandHandler("help", bot.help_command))
        application.add_handler(CommandHandler("digest", bot.digest_command))
        application.add_handler(CommandHandler("category", bot.category_command))
        application.add_handler(CallbackQueryHandler(bot.button_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.message_handler))
        
        print("Запуск бота...")
        # Удаляем старые обновления перед запуском
        await application.initialize()
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)
        
        print(f"Бот успешно запущен")
        print("Отправьте команду /digest в Telegram-боте, чтобы получить последний дайджест")
        print("Нажмите Ctrl+C для остановки бота")
        
        # Держим бота запущенным, пока не нажмут Ctrl+C
        while True:
            await asyncio.sleep(1)
            
    except Exception as e:
        print(f"Ошибка при запуске Telegram-бота: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем")