import logging
from utils.text_utils import TextUtils
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)
utils = TextUtils()


async def show_full_digest(message, digest_id, db_manager):
    """
    Отображает полный текст дайджеста с разбивкой на части
    
    Args:
        message (Message): Объект сообщения Telegram
        digest_id (int): ID дайджеста
        db_manager (DatabaseManager): Менеджер БД
    """
    # Получаем дайджест по ID
    digest = db_manager.get_digest_by_id_with_sections(digest_id)
    
    if not digest:
        await message.reply_text("❌ Дайджест не найден или был удален.")
        return
    
    # Очищаем текст от проблемных символов
    safe_text = utils.clean_markdown_text(digest["text"])
    
    # Разбиваем на части, чтобы не превышать лимит Telegram
    chunks = utils.split_text(safe_text, max_length=3500)
    
    # Формируем заголовок
    digest_type = "краткий" if digest["digest_type"] == "brief" else "подробный"
    date_str = digest["date"].strftime("%d.%m.%Y")
    
    # Если есть диапазон дат, используем его
    if digest.get("date_range_start") and digest.get("date_range_end"):
        if digest["date_range_start"].date() != digest["date_range_end"].date():
            date_str = f"{digest['date_range_start'].strftime('%d.%m.%Y')} - {digest['date_range_end'].strftime('%d.%m.%Y')}"
    
    header = f"📊 {digest_type.capitalize()} дайджест за {date_str}"
    
    # Добавляем информацию о фокусе, если есть
    if digest.get("focus_category"):
        header += f"\n🔍 Фокус: {digest['focus_category']}"
    
    # Отправляем части дайджеста
    for i, chunk in enumerate(chunks):
        # Конвертируем текст для корректного отображения в Telegram
        text_html = utils.convert_to_html(chunk)
        
        # Для первой части добавляем заголовок
        if i == 0:
            # Создаем клавиатуру с кнопкой возврата только для последней части
            if i == len(chunks) - 1:
                keyboard = [[
                    InlineKeyboardButton("🔙 К оглавлению дайджеста", callback_data=f"view_digest_{digest_id}")
                ]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await message.edit_text(
                    f"{header}\n\n{text_html}",
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await message.edit_text(
                    f"{header}\n\n{text_html}",
                    parse_mode='HTML'
                )
        else:
            # Для последней части добавляем кнопку возврата
            if i == len(chunks) - 1:
                keyboard = [[
                    InlineKeyboardButton("🔙 К оглавлению дайджеста", callback_data=f"view_digest_{digest_id}")
                ]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await message.reply_text(
                    text_html,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await message.reply_text(
                    text_html,
                    parse_mode='HTML'
                )

async def start_digest_generation(message, start_date, end_date, period_description, db_manager, context):
    """
    Запускает процесс генерации дайджеста по указанным параметрам
    
    Args:
        message (Message): Объект сообщения Telegram
        start_date (datetime): Начальная дата
        end_date (datetime): Конечная дата
        period_description (str): Описание периода для отображения
        db_manager (DatabaseManager): Менеджер БД
        context (CallbackContext): Контекст Telegram
    """
    # Отправляем сообщение о начале генерации
    status_message = await message.edit_text(
        f"Начинаю создание дайджеста {period_description}.\n\n"
        f"Сбор данных... ⏳"
    )
    
    try:
        # Рассчитываем количество дней в периоде
        days_back = (end_date.date() - start_date.date()).days + 1
        
        # Инициализация компонентов
        from llm.qwen_model import QwenLLM
        from llm.gemma_model import GemmaLLM
        from agents.data_collector import DataCollectorAgent
        from agents.analyzer import AnalyzerAgent
        from agents.digester import DigesterAgent
        
        qwen_model = QwenLLM()
        gemma_model = GemmaLLM()
        
        # Этап 1: Сбор данных
        collector = DataCollectorAgent(db_manager)
        
        # Обновляем статус
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"Собираю данные {period_description}... 📥"
        )
        
        # Асинхронно собираем данные
        collect_result = await collector.collect_data(
            days_back=days_back,
            force_update=False,
            start_date=start_date,
            end_date=end_date
        )
        
        total_messages = collect_result.get("total_new_messages", 0)
        
        # Обновляем статус
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"✅ Собрано {total_messages} сообщений из каналов\n"
            f"Анализирую сообщения... 🧠"
        )
        
        # Этап 2: Анализ сообщений
        analyzer = AnalyzerAgent(db_manager, qwen_model)
        analyzer.fast_check = True
        
        analyze_result = analyzer.analyze_messages_batched(
            limit=max(total_messages, 50),
            batch_size=10
        )
        
        analyzed_count = analyze_result.get("analyzed_count", 0)
        
        # Обновляем статус
        await status_message.edit_text(
            f"{status_message.text}\n"
            f"✅ Проанализировано {analyzed_count} сообщений\n"
            f"Формирую дайджест... 📝"
        )
        
        # Этап 3: Создание дайджеста
        digester = DigesterAgent(db_manager, gemma_model)
        
        result = digester.create_digest(
            date=end_date,
            days_back=days_back,
            digest_type="both",  # Создаем оба типа дайджеста
            update_existing=True
        )
        
        # Проверяем результат
        if not (result.get("brief_digest_id") or result.get("detailed_digest_id")):
            await status_message.edit_text(
                f"{status_message.text}\n"
                f"❌ Не удалось создать дайджест. Возможно, не найдено достаточно данных."
            )
            return
        
        # Сохраняем информацию о генерации
        digest_ids = {}
        if "brief_digest_id" in result:
            digest_ids["brief"] = result["brief_digest_id"]
        if "detailed_digest_id" in result:
            digest_ids["detailed"] = result["detailed_digest_id"]
        
        db_manager.save_digest_generation(
            source="bot",
            user_id=context.user_data.get("user_id"),
            messages_count=total_messages,
            digest_ids=digest_ids,
            start_date=start_date,
            end_date=end_date
        )
        
        # Финальное сообщение
        await status_message.edit_text(
            f"✅ Дайджест {period_description} успешно сформирован!\n\n"
            f"Обработано {total_messages} сообщений, проанализировано {analyzed_count}\n\n"
            f"Выберите дайджест для просмотра:"
        )
        
        # Создаем кнопки для просмотра созданных дайджестов
        keyboard = []
        
        if "brief_digest_id" in result:
            keyboard.append([
                InlineKeyboardButton("📋 Открыть краткий дайджест", callback_data=f"view_digest_{result['brief_digest_id']}")
            ])
        
        if "detailed_digest_id" in result:
            keyboard.append([
                InlineKeyboardButton("📚 Открыть подробный дайджест", callback_data=f"view_digest_{result['detailed_digest_id']}")
            ])
        
        # Добавляем кнопку возврата к списку дайджестов
        keyboard.append([
            InlineKeyboardButton("📋 К списку всех дайджестов", callback_data="show_digests_list")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message.reply_text(
            f"Дайджесты за {period_description} готовы к просмотру:",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка при генерации дайджеста: {str(e)}", exc_info=True)
        await status_message.edit_text(
            f"{status_message.text}\n\n❌ Произошла ошибка: {str(e)}"
        )

def get_category_icon(category):
    """Возвращает иконку для категории"""
    icons = {
        'законодательные инициативы': '📝',
        'новая судебная практика': '⚖️',
        'новые законы': '📜',
        'поправки к законам': '✏️',
        'другое': '📌'
    }
    return icons.get(category.lower(), '•')    