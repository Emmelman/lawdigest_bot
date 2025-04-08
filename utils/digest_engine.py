import logging
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

async def generate_digest(db_manager, start_date, end_date, focus_category=None, 
                        channels=None, keywords=None, update_status_callback=None):
    """
    Универсальная функция для генерации дайджеста
    
    Args:
        db_manager: Менеджер БД
        start_date (datetime): Начальная дата периода
        end_date (datetime): Конечная дата периода
        focus_category (str, optional): Фокусная категория
        channels (list, optional): Список каналов для фильтрации
        keywords (list, optional): Ключевые слова для фильтрации
        update_status_callback (callable, optional): Функция для обновления статуса 
                                                    в интерфейсе пользователя
    
    Returns:
        dict: Результаты генерации дайджеста
    """
    try:
        # Импорты всех необходимых компонентов
        from llm.qwen_model import QwenLLM
        from llm.gemma_model import GemmaLLM
        from agents.data_collector import DataCollectorAgent
        from agents.analyzer import AnalyzerAgent
        from agents.critic import CriticAgent
        from agents.digester import DigesterAgent
        
        qwen_model = QwenLLM()
        gemma_model = GemmaLLM()
        
        # Рассчитываем количество дней для сбора
        days_back = (end_date - start_date).days + 1
        
        today = datetime.now().date()
        if start_date.date() < today:
            days_from_today = (today - start_date.date()).days
            collection_days_back = max(days_from_today, days_back)
        else:
            collection_days_back = days_back
            
        if update_status_callback:
            await update_status_callback(f"Сбор данных за {collection_days_back} дней...")
         # Проверяем, это историческая дата?
        today = datetime.now().date()
        historical_data = start_date.date() < today
        
        # Меняем алгоритм для исторических дат
        if historical_data:
            if update_status_callback:
                await update_status_callback(f"Сбор исторических данных за {start_date.strftime('%d.%m.%Y')}...")
            
            # Для исторических дат используем конкретные даты вместо days_back
            from telethon import TelegramClient
            from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS
            
            client = TelegramClient('session_name', TELEGRAM_API_ID, TELEGRAM_API_HASH)
            await client.start()
            
            try:
                total_messages = 0
                for channel in TELEGRAM_CHANNELS:
                    collector = DataCollectorAgent(db_manager)
                    channel_messages = await collector._process_channel(
                        channel, 
                        start_date=start_date,
                        end_date=end_date,
                        days_back=None  # Отключаем days_back для исторических дат
                    )
                    total_messages += channel_messages
                    
                if update_status_callback:
                    await update_status_callback(f"Собрано {total_messages} сообщений за {start_date.strftime('%d.%m.%Y')}")
            finally:
                await client.disconnect()
        else:
            # Обычный сбор за последние дни  
            collector = DataCollectorAgent(db_manager)
            collect_result = await collector.collect_data(days_back=collection_days_back)

        # Шаг 1: Сбор данных
        if update_status_callback:
            await update_status_callback("Сбор данных...")
            
        collector = DataCollectorAgent(db_manager)
        collect_result = await collector.collect_data(days_back=days_back)
        total_messages = collect_result.get("total_new_messages", 0)
        
        if update_status_callback:
            await update_status_callback(f"Собрано {total_messages} новых сообщений. Анализ...")
        
        # Шаг 2: Анализ сообщений
        analyzer = AnalyzerAgent(db_manager, qwen_model)
        analyzer.fast_check = True
        
        # Используем оптимизированный метод для анализа
        analyze_result = analyzer.analyze_messages_batched(
            limit=max(total_messages, 50),
            batch_size=10,
            confidence_threshold=2
        )
        
        analyzed_count = analyze_result.get("analyzed_count", 0)
        
        # Шаг 3: Проверка категоризации сообщений с низкой уверенностью
        if update_status_callback:
            await update_status_callback(f"Проанализировано {analyzed_count} сообщений. Проверка категоризации...")
            
        critic = CriticAgent(db_manager)
        review_result = critic.review_recent_categorizations(
            confidence_threshold=2,
            limit=min(30, analyzed_count),
            batch_size=5,
            max_workers=3
        )
        
        # Шаг 4: Создание дайджеста
        if update_status_callback:
            await update_status_callback(f"Формирование дайджеста...")
            
        # Сначала проверяем, есть ли дайджест за указанный период
        existing_digests = db_manager.find_digests_by_parameters(
            date_range_start=start_date,
            date_range_end=end_date,
            focus_category=focus_category,
            limit=1
        )
        
        digester = DigesterAgent(db_manager, gemma_model)
        
        if existing_digests:
            # Обновляем существующий дайджест
            digest_id = existing_digests[0]['id']
            digest_type = existing_digests[0]['digest_type']
            
            result = digester.create_digest(
                date=end_date,
                days_back=days_back,
                digest_type=digest_type,
                focus_category=focus_category,
                channels=channels,
                keywords=keywords,
                digest_id=digest_id,
                update_existing=True
            )
            
            logger.info(f"Дайджест обновлен (ID: {digest_id})")
            
            # Дополнительно обновляем другие дайджесты за этот период
            update_result = digester.update_digests_for_date(end_date)
            logger.info(f"Результат обновления других дайджестов: {update_result}")
        else:
            # Создаем новый дайджест
            result = digester.create_digest(
                date=end_date,
                days_back=days_back,
                digest_type="both",
                focus_category=focus_category,
                channels=channels,
                keywords=keywords
            )
            
            logger.info(f"Созданы новые дайджесты")
        
        if result.get("status") == "no_messages":
            if update_status_callback:
                await update_status_callback(f"❌ Не найдено сообщений для формирования дайджеста")
            return {
                "status": "no_messages",
                "message": "Не найдено сообщений, соответствующих критериям фильтрации"
            }
        
        # Сохраняем информацию о генерации
        digest_ids = {}
        if "brief_digest_id" in result:
            digest_ids["brief"] = result["brief_digest_id"]
        if "detailed_digest_id" in result:
            digest_ids["detailed"] = result["detailed_digest_id"]
        
        db_manager.save_digest_generation(
            source="bot",
            channels=channels,
            messages_count=total_messages,
            digest_ids=digest_ids,
            start_date=start_date,
            end_date=end_date,
            focus_category=focus_category
        )
        
        if update_status_callback:
            await update_status_callback(f"✅ Дайджест успешно создан!")
            
        return {
            "status": "success",
            "total_messages": total_messages,
            "analyzed_count": analyzed_count,
            "brief_digest_id": result.get("brief_digest_id"),
            "detailed_digest_id": result.get("detailed_digest_id")
        }
        
    except Exception as e:
        logger.error(f"Ошибка при генерации дайджеста: {str(e)}", exc_info=True)
        if update_status_callback:
            await update_status_callback(f"❌ Ошибка: {str(e)}")
            
        return {
            "status": "error",
            "error": str(e)
        }
# В utils/digest_engine.py

async def check_and_update_digests(db_manager, date=None):
    """
    Проверяет наличие дайджестов за указанную дату и обновляет их при необходимости
    
    Args:
        db_manager: Менеджер БД
        date (datetime, optional): Дата для проверки, по умолчанию сегодняшняя
        
    Returns:
        dict: Результат обновления
    """
    from agents.digester import DigesterAgent
    
    if date is None:
        date = datetime.now()
    
    # Находим все дайджесты, включающие указанную дату
    digests = db_manager.get_digests_containing_date(date)
    
    if not digests:
        return {"status": "no_digests", "date": date.strftime('%Y-%m-%d')}
    
    # Обновляем каждый дайджест
    digester = DigesterAgent(db_manager)
    update_result = digester.update_digests_for_date(date)
    
    return update_result    