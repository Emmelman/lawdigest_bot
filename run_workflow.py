# run_workflow.py
import os
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telethon import TelegramClient

from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS
from database.db_manager import DatabaseManager
from llm.qwen_model import QwenLLM
from llm.gemma_model import GemmaLLM

# Загрузка переменных окружения
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///lawdigest.db")
db_manager = DatabaseManager(DATABASE_URL)

async def collect_messages(client, db_manager, channel, limit=10):
    """Сбор сообщений из канала и сохранение в БД"""
    print(f"Сбор сообщений из канала {channel}...")
    
    try:
        entity = await client.get_entity(channel)
        messages = await client.get_messages(entity, limit=limit)
        
        saved_count = 0
        for msg in messages:
            if msg.message:
                try:
                    print(f"  Сохранение сообщения: ID={msg.id}")
                    db_manager.save_message(
                        channel=channel,
                        message_id=msg.id,
                        text=msg.message,
                        date=msg.date
                    )
                    saved_count += 1
                except Exception as e:
                    print(f"  Ошибка при сохранении сообщения: {str(e)}")
        
        print(f"  Сохранено {saved_count} сообщений из канала {channel}")
        return saved_count
    except Exception as e:
        print(f"  Ошибка при сборе сообщений из канала {channel}: {str(e)}")
        return 0

async def analyze_messages(db_manager, llm_model, limit=10):
    """Анализ и классификация сообщений"""
    print(f"Анализ сообщений (лимит: {limit})...")
    
    # Получаем непроанализированные сообщения
    messages = db_manager.get_unanalyzed_messages(limit=limit)
    print(f"  Найдено {len(messages)} сообщений для анализа")
    
    if not messages:
        print("  Нет сообщений для анализа")
        return 0
    
    analyzed_count = 0
    
    for i, message in enumerate(messages):
        if not message.text:
            continue
        
        print(f"  Анализ сообщения {i+1}/{len(messages)} (ID={message.id})")
        
        # Классифицируем сообщение
        categories = [
            'законодательные инициативы',
            'новая судебная практика',
            'новые законы',
            'поправки к законам',
            'другое'
        ]
        
        try:
            # Формируем короткое описание сообщения для лога
            text_preview = message.text[:50] + "..." if len(message.text) > 50 else message.text
            print(f"    Сообщение: {text_preview}")
            
            prompt = f"""
            Проанализируй следующий текст из правительственного Telegram-канала и определи, 
            к какой из следующих категорий он относится:
            - законодательные инициативы
            - новая судебная практика
            - новые законы
            - поправки к законам
            
            Если текст не относится ни к одной из категорий, то верни "другое".
            
            Текст сообщения:
            {message.text}
            
            Категория:
            """
            
            print("    Отправка запроса к модели классификации...")
            category = llm_model.classify(prompt, categories)
            print(f"    Определена категория: {category}")
            
            # Обновляем категорию в БД
            success = db_manager.update_message_category(message.id, category)
            if success:
                analyzed_count += 1
                print(f"    Категория успешно обновлена в БД")
            else:
                print(f"    Ошибка при обновлении категории в БД")
                
        except Exception as e:
            print(f"    Ошибка при анализе сообщения: {str(e)}")
    
    print(f"  Проанализировано {analyzed_count} сообщений")
    return analyzed_count
async def review_categorization(db_manager, limit=10):
    """Проверка и исправление категоризации"""
    print("\n--- Шаг 3: Проверка категоризации ---")
    
    from agents.critic import CriticAgent
    
    # Создаем агента-критика
    critic = CriticAgent(db_manager)
    
    print(f"Проверка категоризации последних {limit} сообщений...")
    results = critic.review_recent_categorizations(limit=limit)
    
    print(f"Проверка завершена. Всего: {results['total']}, обновлено: {results['updated']}, без изменений: {results['unchanged']}")
    
    if results['updated'] > 0:
        print("Категории некоторых сообщений были исправлены:")
        for result in results['details']:
            if result['status'] == 'updated':
                print(f"  - Сообщение ID={result['message_id']}: '{result['old_category']}' -> '{result['new_category']}'")
                print(f"    Обоснование: {result['justification']}")
    
    return results
async def create_digest(db_manager, llm_model):
    """Создание дайджеста"""
    print("Создание дайджеста...")
    
    # Получаем сегодняшнюю дату
    today = datetime.now()
    
    # Получаем все сообщения за последние 7 дней
    start_date = today - timedelta(days=7)
    
    print(f"  Получение сообщений за период {start_date.strftime('%Y-%m-%d')} - {today.strftime('%Y-%m-%d')}")
    messages = db_manager.get_messages_by_date_range(
        start_date=start_date,
        end_date=today
    )
    
    if not messages:
        print("  Нет сообщений для формирования дайджеста")
        return None
    
    print(f"  Найдено {len(messages)} сообщений")
    
    # Создаем словарь сообщений по категориям
    messages_by_category = {}
    for msg in messages:
        category = msg.category or "другое"
        if category not in messages_by_category:
            messages_by_category[category] = []
        messages_by_category[category].append(msg)
    
    print(f"  Распределение по категориям: {', '.join([f'{cat}: {len(msgs)}' for cat, msgs in messages_by_category.items()])}")
    
    # Формируем секции дайджеста
    sections = {}
    for category, cat_messages in messages_by_category.items():
        if not cat_messages:
            continue
        
        print(f"  Формирование секции для категории '{category}'...")
        
        # Формируем текст для категории
        category_messages = "\n\n".join([
            f"Канал: {msg.channel}\nДата: {msg.date}\n{msg.text}"
            for msg in cat_messages[:5]  # Берем только первые 5 сообщений для каждой категории
        ])
        
        prompt = f"""
        Сформируй краткий обзор для категории '{category}' на основе следующих сообщений 
        из правительственных Telegram-каналов:
        
        {category_messages}
        
        Обзор должен содержать краткое описание ключевых событий и быть информативным.
        Объем: 1-2 абзаца.
        """
        
        try:
            print("    Отправка запроса к модели генерации...")
            section_text = llm_model.generate(prompt, max_tokens=500)
            print(f"    Получен текст секции ({len(section_text)} символов)")
            
            sections[category] = section_text
        except Exception as e:
            print(f"    Ошибка при формировании секции для категории '{category}': {str(e)}")
            sections[category] = f"Не удалось сформировать обзор для категории '{category}'"
    
    # Формируем полный текст дайджеста
    print("  Формирование полного текста дайджеста...")
    
    digest_text = f"# Дайджест правовых новостей за {today.strftime('%d.%m.%Y')}\n\n"
    
    # Добавляем введение
    intro_prompt = f"""
    Напиши краткое введение для дайджеста правовых новостей за {today.strftime('%d.%m.%Y')}.
    Упомяни, что дайджест составлен на основе информации из официальных Telegram-каналов Госдумы, Совета Федерации и Верховного Суда РФ.
    Объем: 1 абзац.
    """
    
    try:
        intro_text = llm_model.generate(intro_prompt, max_tokens=200)
        digest_text += f"{intro_text}\n\n"
    except Exception as e:
        print(f"  Ошибка при формировании введения: {str(e)}")
        digest_text += "Дайджест составлен на основе информации из официальных Telegram-каналов Госдумы, Совета Федерации и Верховного Суда РФ.\n\n"
    
    # Добавляем секции по категориям
    for category, section_text in sections.items():
        digest_text += f"## {category.upper()}\n\n{section_text}\n\n"
    
    # Сохраняем дайджест
    try:
        print("  Сохранение дайджеста в БД...")
        digest = db_manager.save_digest(today, digest_text, sections)
        print(f"  Дайджест успешно сохранен (ID: {digest.id})")
    except Exception as e:
        print(f"  Ошибка при сохранении дайджеста: {str(e)}")
    
    return digest_text

async def run_full_workflow():
    """Запуск полного рабочего процесса"""
    print("Запуск полного рабочего процесса...")
    
    # Шаг 0: Инициализация компонентов
    print("\n--- Шаг 0: Инициализация компонентов ---")
    
    db_manager = DatabaseManager(DATABASE_URL)
    qwen_model = QwenLLM()
    gemma_model = GemmaLLM()
    
    client = TelegramClient('workflow_session', TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start()
    
    # Шаг 1: Сбор данных
    print("\n--- Шаг 1: Сбор данных ---")
    
    total_messages = 0
    for channel in TELEGRAM_CHANNELS:
        count = await collect_messages(client, db_manager, channel, limit=5)
        total_messages += count
    
    print(f"Всего собрано {total_messages} сообщений")
    
    # Шаг 2: Анализ сообщений
    print("\n--- Шаг 2: Анализ сообщений ---")
    
    analyzed_count = await analyze_messages(db_manager, qwen_model, limit=total_messages)
    
    # Шаг 3: Проверка категоризации (новый шаг!)
    results = await review_categorization(db_manager, limit=total_messages)
    
    # Шаг 4: Создание дайджеста (раньше был Шаг 3)
    print("\n--- Шаг 4: Создание дайджеста ---")
    
    digest_text = await create_digest(db_manager, gemma_model)
    
    # Вывод результата
    if digest_text:
        print("\n=== СФОРМИРОВАННЫЙ ДАЙДЖЕСТ ===\n")
        print(digest_text)
    else:
        print("\nНе удалось сформировать дайджест")
    
    # Закрываем соединение с Telegram
    await client.disconnect()
    
    print("\nРабочий процесс успешно завершен!")

if __name__ == "__main__":
    asyncio.run(run_full_workflow())