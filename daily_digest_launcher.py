# daily_digest_launcher.py
import os
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from telethon import TelegramClient
from telegram import Bot
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters

from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS, TELEGRAM_BOT_TOKEN
from database.db_manager import DatabaseManager
from llm.qwen_model import QwenLLM
from llm.gemma_model import GemmaLLM
from agents.critic import CriticAgent

# Загрузка переменных окружения
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///lawdigest.db")

async def collect_data_for_date(client, db_manager, date):
    """Сбор данных за указанную дату"""
    print(f"Сбор данных за {date.strftime('%d.%m.%Y')}...")
    
    # Вычисляем границы дня с часовым поясом
    start_date = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=1)
    
    print(f"  Временной диапазон: {start_date} - {end_date}")
    
    total_messages = 0
    
    for channel in TELEGRAM_CHANNELS:
        print(f"Обработка канала {channel}...")
        try:
            entity = await client.get_entity(channel)
            messages = await client.get_messages(
                entity,
                limit=50,  # Увеличиваем лимит, чтобы охватить больше сообщений
                offset_date=end_date
            )
            
            saved_count = 0
            for msg in messages:
                # Выводим дату сообщения для отладки
                print(f"  Сообщение #{msg.id}, дата: {msg.date}")
                
                # Проверяем, что сообщение в нужном диапазоне дат
                if start_date <= msg.date < end_date and msg.message:
                    try:
                        db_manager.save_message(
                            channel=channel,
                            message_id=msg.id,
                            text=msg.message,
                            date=msg.date
                        )
                        saved_count += 1
                        print(f"  Сохранено сообщение #{msg.id}")
                    except Exception as e:
                        print(f"  Ошибка при сохранении сообщения: {str(e)}")
            
            print(f"  Сохранено {saved_count} сообщений из канала {channel}")
            total_messages += saved_count
            
        except Exception as e:
            print(f"  Ошибка при обработке канала {channel}: {str(e)}")
    
    print(f"Всего собрано {total_messages} сообщений за {date.strftime('%d.%m.%Y')}")
    return total_messages

async def analyze_all_messages(db_manager, llm_model, date):
    """Анализ и категоризация всех сообщений за указанную дату"""
    print(f"Анализ сообщений за {date.strftime('%d.%m.%Y')}...")
    
    # Вычисляем границы дня
    start_date = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=1)
    
    # Импортируем модели
    from database.models import Message
    
    # Получаем все сообщения за указанную дату без категории
    session = db_manager.Session()
    try:
        messages = session.query(Message).filter(
            Message.date >= start_date,
            Message.date < end_date,
            Message.category == None
        ).all()
        print(f"  Найдено {len(messages)} сообщений для анализа")
    finally:
        session.close()
    
    if not messages:
        print("  Нет сообщений для анализа")
        return 0
    
    analyzed_count = 0
    
    for i, message in enumerate(messages):
        if not message.text:
            continue
        
        print(f"  Анализ сообщения {i+1}/{len(messages)} (ID={message.id})")
        
        # Классифицируем сообщение с улучшенным промптом
        prompt = f"""
        Внимательно проанализируй следующий текст из правительственного Telegram-канала и определи, к какой из следующих категорий он относится:

        1. Законодательные инициативы - предложения о создании новых законов или нормативных актов, находящиеся на стадии обсуждения, внесения или рассмотрения в Госдуме. Обычно содержат фразы: "законопроект", "проект закона", "внесен на рассмотрение", "планируется принять", "предлагается установить".

        2. Новая судебная практика - решения, определения, постановления судов, создающие прецеденты или разъясняющие применение норм права. Признаки: упоминание судов (ВС, Верховный Суд, КС, арбитражный суд), номеров дел, дат решений, слов "решение", "определение", "постановление", "практика", "разъяснение".

        3. Новые законы - недавно принятые и вступившие или вступающие в силу законодательные акты. Признаки: "закон принят", "закон подписан", "вступает в силу", "вступил в силу", указание номера федерального закона.

        4. Поправки к законам - изменения в существующих законах, внесенные или вступившие в силу. Признаки: "внесены изменения", "поправки", "новая редакция", "дополнен статьей", указания на изменение конкретных статей существующих законов.

        Если текст не относится ни к одной из категорий, то верни "другое".

        Особые указания для точной категоризации:
        - Если описывается решение суда, определение суда, обзор практики - это "новая судебная практика"
        - Если указаны названия судов (ВС, КС) и описываются их решения - это "новая судебная практика"
        - Если упоминаются номера дел или определений - это "новая судебная практика"
        - Если говорится о внесении законопроекта или его рассмотрении, но не о принятии - это "законодательные инициативы"
        - Если упоминается о принятии закона в третьем чтении или о подписании Президентом - это "новые законы"
        
        Текст сообщения:
        {message.text}
        
        Категория (выбери только одну):
        """
        
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
            
            category = llm_model.classify(prompt, categories)
            print(f"    Определена категория: {category}")
            
            # Обновляем категорию в БД
            success = db_manager.update_message_category(message.id, category)
            if success:
                analyzed_count += 1
        except Exception as e:
            print(f"    Ошибка при анализе сообщения: {str(e)}")
    
    print(f"  Проанализировано {analyzed_count} сообщений")
    return analyzed_count

async def review_categorization_for_date(db_manager, date):
    """Проверка и исправление категоризации сообщений за указанную дату"""
    print(f"Проверка категоризации за {date.strftime('%d.%m.%Y')}...")
    
    # Вычисляем границы дня
    start_date = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=1)
    
    # Импортируем модель Message
    from database.models import Message
    
    # Получаем все сообщения за указанную дату с категорией
    session = db_manager.Session()
    try:
        messages = session.query(Message).filter(
            Message.date >= start_date,
            Message.date < end_date,
            Message.category != None
        ).all()
        print(f"  Найдено {len(messages)} категоризированных сообщений для проверки")
    finally:
        session.close()
    
    if not messages:
        print("  Нет категоризированных сообщений для проверки")
        return None
    
    # Создаем агента-критика
    critic = CriticAgent(db_manager)
    
    results = {
        "status": "success",
        "total": len(messages),
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
        "details": []
    }
    
    print(f"  Проверка {len(messages)} сообщений...")
    
    for message in messages:
        result = critic.review_categorization(message.id, message.category)
        results["details"].append(result)
        
        if result["status"] == "updated":
            results["updated"] += 1
            print(f"  Исправлена категория сообщения ID={message.id}: '{result['old_category']}' -> '{result['new_category']}'")
        elif result["status"] == "unchanged":
            results["unchanged"] += 1
        else:
            results["errors"] += 1
    
    print(f"  Проверка завершена. Всего: {results['total']}, исправлено: {results['updated']}, без изменений: {results['unchanged']}")
    return results

async def create_digest_for_date(db_manager, llm_model, date):
    """Создание дайджеста за указанную дату"""
    print(f"Создание дайджеста за {date.strftime('%d.%m.%Y')}...")
    
    # Вычисляем границы дня
    start_date = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=1)
    
    # Получаем все сообщения за день
    messages = db_manager.get_messages_by_date_range(start_date, end_date)
    if not messages:
        print("  Нет сообщений для формирования дайджеста")
        return None
    
    print(f"  Найдено {len(messages)} сообщений")
    
    # Формируем сообщения по категориям
    messages_by_category = {}
    for msg in messages:
        category = msg.category or "другое"
        if category not in messages_by_category:
            messages_by_category[category] = []
        messages_by_category[category].append(msg)
    
    # Формируем секции дайджеста
    sections = {}
    for category, cat_messages in messages_by_category.items():
        if not cat_messages:
            continue
        
        print(f"  Формирование секции для категории '{category}'...")
        
        # Формируем текст для категории
        category_messages = "\n\n".join([
            f"Канал: {msg.channel}\nДата: {msg.date}\n{msg.text}"
            for msg in cat_messages[:10]  # Берем до 10 сообщений для каждой категории
        ])
        
        prompt = f"""
        Сформируй подробный и информативный обзор для категории '{category}' на основе следующих сообщений 
        из правительственных Telegram-каналов. Ваша задача - создать структурированный, содержательный текст,
        объединяющий ключевые моменты всех сообщений в единый связный обзор.
        
        Сообщения:
        {category_messages}
        
        Требования к обзору:
        1. Начните с информативного заголовка "## Обзор {category} (март 2025)"
        2. Объедините похожие темы и выделите ключевые события
        3. Не просто перечисляйте события, а создайте содержательный анализ
        4. Подчеркните значимость изменений и их влияние на правовую систему
        5. Используйте профессиональный, но доступный язык
        6. Объем: 2-3 содержательных абзаца
        
        Пример стиля и структуры:
        ## Обзор новых законов (март 2025)
        
        Государственная Дума приняла ряд важных законов, направленных на регулирование [тема]. Ключевые изменения включают [основные положения], что позволит [описание эффекта]. Эксперты отмечают особую значимость [конкретное положение], поскольку ранее эта область оставалась недостаточно урегулированной.
        
        Второе важное направление законотворчества касается [другая тема]. Принятые поправки предусматривают [конкретные меры], что должно привести к [ожидаемый результат]. Данные изменения вступят в силу [сроки] и затронут интересы [целевая аудитория].
        """
        
        try:
            section_text = llm_model.generate(prompt, max_tokens=1000)
            print(f"    Получен текст секции ({len(section_text)} символов)")
            
            sections[category] = section_text
        except Exception as e:
            print(f"    Ошибка при формировании секции для категории '{category}': {str(e)}")
            sections[category] = f"В категории '{category}' за указанный период обнаружены важные сообщения, но их обработка временно недоступна из-за технической ошибки."
    
    # Формируем полный текст дайджеста
    print("  Формирование полного текста дайджеста...")
    
    digest_text = f"# Дайджест правовых новостей за {date.strftime('%d.%m.%Y')}\n\n"
    
    # Добавляем введение
    intro_prompt = f"""
    Напиши профессиональное введение для дайджеста правовых новостей за {date.strftime('%d.%m.%Y')}.
    
    Дайджест содержит следующие категории:
    {', '.join(sections.keys())}
    
    Введение должно быть информативным, подчеркивать значимость событий дня и упоминать, что дайджест составлен на основе 
    официальной информации из Telegram-каналов Госдумы, Совета Федерации и Верховного Суда РФ.
    
    Объем: 1-2 абзаца.
    """
    
    try:
        intro_text = llm_model.generate(intro_prompt, max_tokens=500)
        digest_text += f"{intro_text}\n\n"
    except Exception as e:
        print(f"  Ошибка при формировании введения: {str(e)}")
        digest_text += "В новом выпуске нашего дайджеста представлены ключевые правовые новости за сегодняшний день. Материалы подготовлены на основе официальной информации из Telegram-каналов Государственной Думы, Совета Федерации и Верховного Суда РФ.\n\n"
    
    # Добавляем секции по категориям
    # Сначала основные категории
    main_categories = ['новые законы', 'законодательные инициативы', 'поправки к законам', 'новая судебная практика']
    for category in main_categories:
        if category in sections:
            digest_text += f"{sections[category]}\n\n"
    
    # Затем "другое" в конце, если есть
    if "другое" in sections:
        digest_text += f"{sections['другое']}\n\n"
    
    # Сохраняем дайджест
    try:
        print("  Сохранение дайджеста в БД...")
        
        # Использование метода save_digest должно происходить в рамках одной сессии
        digest = db_manager.save_digest(date, digest_text, sections)
        print(f"  Дайджест успешно сохранен (ID: {digest.id})")
        
        return digest_text
    except Exception as e:
        print(f"  Ошибка при сохранении дайджеста: {str(e)}")
        return digest_text  # Возвращаем текст дайджеста, даже если не удалось сохранить в БД

async def send_digest_to_telegram(digest_text):
    """Отправка дайджеста в Telegram-бот"""
    print("Отправка дайджеста в Telegram...")
    
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        # Разбиваем дайджест на части (из-за ограничений Telegram на длину сообщения)
        max_length = 4000
        
        if len(digest_text) <= max_length:
            chunks = [digest_text]
        else:
            chunks = []
            for i in range(0, len(digest_text), max_length):
                chunks.append(digest_text[i:i+max_length])
        
        # Получаем список администраторов (можно настроить в конфиге)
        admin_ids = [int(id) for id in os.getenv("ADMIN_TELEGRAM_IDS", "").split(",") if id]
        
        # Если админы не настроены, выводим только в консоль
        if not admin_ids:
            print("⚠️ Не найдены ID администраторов. Дайджест не будет отправлен.")
            print("Добавьте ADMIN_TELEGRAM_IDS в файл .env в формате: ADMIN_TELEGRAM_IDS=12345,67890")
        
        for admin_id in admin_ids:
            for i, chunk in enumerate(chunks):
                if i == 0:
                    await bot.send_message(
                        chat_id=admin_id, 
                        text=f"📋 Дайджест правовых новостей за {datetime.now().strftime('%d.%m.%Y')}\n\n{chunk}"
                    )
                else:
                    await bot.send_message(chat_id=admin_id, text=chunk)
            
            print(f"  Дайджест отправлен пользователю {admin_id}")
        
        print("Отправка завершена")
        return True
    except Exception as e:
        print(f"Ошибка при отправке дайджеста: {str(e)}")
        return False
async def run_telegram_bot(db_manager, token):
    """Запускает Telegram-бота"""
    try:
        from telegram.ext import ApplicationBuilder
        from telegram_bot.bot import TelegramBot
        
        print("Инициализация Telegram-бота...")
        bot = TelegramBot(db_manager)
        
        # Создание и запуск приложения с дополнительными параметрами
        application = ApplicationBuilder().token(token).connect_timeout(30.0).pool_timeout(30.0).read_timeout(30.0).build()
        
        # Регистрация обработчиков
        application.add_handler(CommandHandler("start", bot.start_command))
        application.add_handler(CommandHandler("help", bot.help_command))
        application.add_handler(CommandHandler("digest", bot.digest_command))
        application.add_handler(CommandHandler("category", bot.category_command))
        application.add_handler(CallbackQueryHandler(bot.button_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.message_handler))
        
        print("Запуск Telegram-бота...")
        
        # Запуск в блокирующем режиме для более простой обработки
        await application.initialize()
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)  # Игнорируем старые обновления
        
        print(f"Бот успешно запущен и доступен по @{application.bot.username}")
        return application
    except Exception as e:
        print(f"Ошибка при запуске Telegram-бота: {str(e)}")
        import traceback
        traceback.print_exc()  # Выводим полный стек-трейс для более детальной отладки
        return None
    
async def daily_digest_workflow(date=None):
    """Полный рабочий процесс создания и отправки дайджеста"""
    # Если дата не указана, используем текущую
    if date is None:
        date = datetime.now()
    
    print(f"Запуск формирования дайджеста за {date.strftime('%d.%m.%Y')}...")
    
    # Инициализация компонентов
    db_manager = DatabaseManager(DATABASE_URL)
    qwen_model = QwenLLM()
    gemma_model = GemmaLLM()
    
    # Инициализация Telegram-клиента
    client = TelegramClient('daily_session', TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start()
    
    try:
        # Шаг 1: Сбор данных
        total_messages = await collect_data_for_date(client, db_manager, date)
        
        if total_messages == 0:
            print("Не найдено сообщений за указанную дату. Дайджест не будет сформирован.")
            return
        
        # Шаг 2: Анализ сообщений
        analyzed_count = await analyze_all_messages(db_manager, qwen_model, date)
        
        # Шаг 3: Проверка категоризации
        review_results = await review_categorization_for_date(db_manager, date)
        
        # Шаг 4: Создание дайджеста
        digest_text = await create_digest_for_date(db_manager, gemma_model, date)
        
        if not digest_text:
            print("Не удалось сформировать дайджест.")
            return
        
        print("\n=== ДАЙДЖЕСТ УСПЕШНО СФОРМИРОВАН ===")
        print(digest_text[:500] + "...")  # Показываем начало дайджеста
        
        # Шаг 5: Запуск Telegram-бота для доступа к дайджесту
        print("\nЗапуск Telegram-бота...")
        bot_app = await run_telegram_bot(db_manager, TELEGRAM_BOT_TOKEN)
        
        if bot_app:
            # Ждем пока не нажмут Ctrl+C
            print("Бот запущен и готов к использованию. Нажмите Ctrl+C для завершения.")
            while True:
                await asyncio.sleep(1)
        else:
            print("Не удалось запустить Telegram-бота")
        
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем.")
    finally:
        # Закрываем соединение с Telegram
        await client.disconnect()
        print("Работа завершена.")

# Запуск процесса с конкретной датой
if __name__ == "__main__":
    # Задаем дату 25.03.2025
    target_date = datetime(2025, 3, 25)
    asyncio.run(daily_digest_workflow(target_date))