"""
Настройки проекта
"""
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Telegram настройки
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")

# Список каналов для мониторинга
TELEGRAM_CHANNELS = [
    "@dumainfo",     # Государственная Дума
    "@sovfedinfo",   # Совет Федерации
    "@vsrf_ru"       # Верховный суд РФ
]

# Настройки БД
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///lawdigest.db")

# Настройки LLM
LLM_STUDIO_URL = os.getenv("LLM_STUDIO_URL", "http://127.0.0.1:1234")
QWEN_API_KEY = os.getenv("QWEN_API_KEY")
QWEN_MODEL = "qwen2.5-14b"

GEMMA_API_KEY = os.getenv("GEMMA_API_KEY")
GEMMA_MODEL = "gemma-3-12b"

# Категории для классификации
CATEGORIES = [
    'законодательные инициативы',
    'новая судебная практика',
    'новые законы',
    'поправки к законам'
]

# Расписание задач
COLLECT_INTERVAL_MINUTES = 30
ANALYZE_INTERVAL_MINUTES = 30
DIGEST_TIME_HOUR = 18
DIGEST_TIME_MINUTE = 0