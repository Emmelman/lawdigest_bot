"""
Улучшенная версия функций для просмотра дайджеста с сокращенными данными кнопок
"""
import hashlib
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

def get_short_category_id(category):
    """
    Создает короткий уникальный ID для категории
    
    Args:
        category (str): Полное название категории
        
    Returns:
        str: Короткий уникальный ID (до 8 символов)
    """
    # Используем хеш для создания уникального и короткого ID
    hash_object = hashlib.md5(category.encode())
    # Берем первые 6 символов хеша, чтобы он был коротким
    return hash_object.hexdigest()[:6]

async def view_digest_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик колбэка для просмотра дайджеста с улучшенной обработкой ошибок"""
    query = update.callback_query
    await query.answer()
    
    try:
        # Извлекаем ID дайджеста из callback_data
        digest_id = int(query.data.replace("view_digest_", ""))
        
        # Получаем дайджест с секциями по ID
        digest = db_manager.get_digest_by_id_with_sections(digest_id)
        
        if not digest:
            await query.message.reply_text("❌ Дайджест не найден или был удален.")
            return
        
        # Формируем сводное содержание дайджеста
        digest_type = "краткий" if digest["digest_type"] == "brief" else "подробный"
        date_str = digest["date"].strftime("%d.%m.%Y")
        
        # Если есть диапазон дат, используем его
        if digest.get("date_range_start") and digest.get("date_range_end"):
            if digest["date_range_start"].date() != digest["date_range_end"].date():
                date_str = f"{digest['date_range_start'].strftime('%d.%m.%Y')} - {digest['date_range_end'].strftime('%d.%m.%Y')}"
        
        # Создаем статистику категорий
        categories_stats = {}
        for section in digest["sections"]:
            categories_stats[section["category"]] = len(section["text"].split("\n\n"))
        
        # Формируем текст оглавления
        table_of_contents = f"📊 {digest_type.capitalize()} дайджест за {date_str}\n\n"
        
        # Добавляем информацию о фокусе, если есть
        if digest.get("focus_category"):
            table_of_contents += f"🔍 Фокус: {digest['focus_category']}\n\n"
        
        # Добавляем статистику по категориям
        table_of_contents += "📋 Содержание:\n"
        for category, count in categories_stats.items():
            icon = get_category_icon(category)
            table_of_contents += f"{icon} {category.capitalize()}: примерно {count} сообщений\n"
        
        # Создаем клавиатуру для выбора категорий с короткими ID
        keyboard = []
        
        # Сохраняем маппинг ID категорий для этого дайджеста
        if not context.user_data.get("category_mapping"):
            context.user_data["category_mapping"] = {}
        
        # Для каждой категории создаем кнопку с коротким ID
        for section in digest["sections"]:
            category = section["category"]
            icon = get_category_icon(category)
            
            # Генерируем короткий ID для категории
            short_id = get_short_category_id(category)
            
            # Сохраняем маппинг ID -> категория
            mapping_key = f"{digest_id}_{short_id}"
            context.user_data["category_mapping"][mapping_key] = category
            
            # Создаем кнопку с коротким callback_data
            keyboard.append([
                InlineKeyboardButton(
                    f"{icon} {category.capitalize()}", 
                    callback_data=f"ds_{digest_id}_{short_id}"
                )
            ])
        
        # Добавляем кнопку для просмотра полного дайджеста
        keyboard.append([
            InlineKeyboardButton("📄 Полный текст дайджеста", callback_data=f"df_{digest_id}")
        ])
        
        # Добавляем кнопку возврата к списку дайджестов
        keyboard.append([
            InlineKeyboardButton("⬅️ Назад к списку", callback_data="sl")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем оглавление дайджеста
        await query.message.edit_text(
            table_of_contents,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Ошибка при просмотре дайджеста: {str(e)}", exc_info=True)
        # В случае ошибки отправляем новое сообщение вместо редактирования
        await query.message.reply_text(
            f"Произошла ошибка при загрузке дайджеста: {str(e)}\n"
            "Пожалуйста, попробуйте использовать команду /list для просмотра списка дайджестов."
        )

async def view_digest_section_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик колбэка для просмотра секции дайджеста с короткими ID"""
    query = update.callback_query
    await query.answer()
        
    try:
        # Извлекаем информацию из callback_data
        # Формат: ds_DIGEST_ID_SHORT_CATEGORY_ID
        parts = query.data.split("_", 2)
        if len(parts) < 3:
            await query.message.reply_text("❌ Неверный формат callback_data")
            return
        
        digest_id = int(parts[1])
        short_category_id = parts[2]
        logger.info(f"digest_id: {digest_id}, short_category_id: {short_category_id}")
        
        # Получаем категорию из маппинга
        mapping_key = f"{digest_id}_{short_category_id}"
        
        logger.info(f"Ищем маппинг для ключа: '{mapping_key}'")
        if context.user_data.get("category_mapping"):
            logger.info(f"Доступные ключи в category_mapping: {list(context.user_data['category_mapping'].keys())}")
        else:
            logger.error("category_mapping вообще отсутствует!")

        if not context.user_data.get("category_mapping") or mapping_key not in context.user_data["category_mapping"]:
            await query.message.reply_text(
                "❌ Информация о категории не найдена. Пожалуйста, начните просмотр заново."
            )
            return
        
        category = context.user_data["category_mapping"][mapping_key]
        
        if mapping_key not in context.user_data["category_mapping"]:
            logger.error(f"Ключ '{mapping_key}' НЕ НАЙДЕН в category_mapping!")
            logger.error(f"Возможно, show_digest_categories была вызвана без context")
        else:
            logger.info(f"Ключ '{mapping_key}' найден, категория: '{context.user_data['category_mapping'][mapping_key]}'")
        # Получаем дайджест с секциями
        digest = db_manager.get_digest_by_id_with_sections(digest_id)
        
        if not digest:
            await query.message.reply_text("❌ Дайджест не найден или был удален.")
            return
        
        # Ищем секцию по категории
        section = None
        for s in digest["sections"]:
            if s["category"] == category:
                section = s
                break
        
        if not section:
            await query.message.reply_text(f"❌ Секция '{category}' не найдена в дайджесте.")
            return
        
        # Разделяем содержимое секции на части для пагинации
        section_parts = []
        current_part = ""
        for paragraph in section["text"].split("\n\n"):
            if len(current_part) + len(paragraph) + 2 <= 3500:  # Лимит Telegram на длину сообщения
                if current_part:
                    current_part += "\n\n" + paragraph
                else:
                    current_part = paragraph
            else:
                section_parts.append(current_part)
                current_part = paragraph
        
        if current_part:
            section_parts.append(current_part)
        
        # Если есть несколько частей, реализуем пагинацию
        if len(section_parts) > 1:
            # Сохраняем в context_data информацию о текущей странице и секции
            pagination_key = f"digest_{digest_id}_{short_category_id}"
            if not context.user_data.get("pagination"):
                context.user_data["pagination"] = {}
            
            context.user_data["pagination"][pagination_key] = {
                "current_page": 0,
                "total_pages": len(section_parts),
                "parts": section_parts,
                "category": category  # Сохраняем полное название категории
            }
            
            # Формируем заголовок с информацией о пагинации
            digest_type = "краткий" if digest["digest_type"] == "brief" else "подробный"
            date_str = digest["date"].strftime("%d.%m.%Y")
            
            header = f"📊 {digest_type.capitalize()} дайджест за {date_str}\n"
            header += f"📂 Категория: {category.capitalize()}\n"
            header += f"📄 Страница 1/{len(section_parts)}\n\n"
            
            # Конвертируем текст для корректного отображения в Telegram
            from utils.text_utils import TextUtils
            content = TextUtils.convert_to_html(header + section_parts[0])
            
            # Создаем клавиатуру с кнопками навигации
            keyboard = []
            
            # Если есть следующая страница, добавляем кнопку
            if len(section_parts) > 1:
                keyboard.append([
                    InlineKeyboardButton(
                        "Следующая страница ➡️", 
                        callback_data=f"pg_{digest_id}_{short_category_id}_n"
                    )
                ])
            
            # Добавляем кнопку возврата к оглавлению дайджеста
            keyboard.append([
                InlineKeyboardButton(
                    "🔙 К оглавлению", 
                    callback_data=f"view_digest_{digest_id}"
                )
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Отправляем секцию с кнопками пагинации
            await query.message.edit_text(
                content,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            # Если только одна часть, отправляем ее без пагинации
            digest_type = "краткий" if digest["digest_type"] == "brief" else "подробный" # Re-evaluate type for clarity
            date_str = digest["date"].strftime("%d.%m.%Y")
            
            header = f"📊 {digest_type.capitalize()} дайджест за {date_str}\n"
            header += f"📂 Категория: {category.capitalize()}\n\n"
            
            # Конвертируем текст для корректного отображения в Telegram
            from utils.text_utils import TextUtils
            content = TextUtils.convert_to_html(header + section["text"])
            
            # Создаем клавиатуру с кнопкой возврата
            keyboard = [[
                InlineKeyboardButton(
                    "🔙 К оглавлению", 
                    callback_data=f"view_digest_{digest_id}"
                )
            ]]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Отправляем секцию
            await query.message.edit_text(
                content,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    except Exception as e:
        logger.error(f"Ошибка при просмотре секции дайджеста: {str(e)}", exc_info=True)
        # В случае ошибки отправляем новое сообщение вместо редактирования
        await query.message.reply_text(
            f"Произошла ошибка при загрузке секции дайджеста: {str(e)}\n"
            "Пожалуйста, попробуйте использовать команду /list для просмотра списка дайджестов."
        )

async def page_navigation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик колбэка для пагинации секций дайджеста с короткими ID"""
    query = update.callback_query
    await query.answer()
    
    try:
        # Извлекаем информацию из callback_data
        # Формат: pg_DIGEST_ID_SHORT_CATEGORY_ID_ACTION
        parts = query.data.split("_")
        if len(parts) < 4:
            await query.message.reply_text("❌ Неверный формат callback_data для пагинации")
            return
        
        digest_id = int(parts[1])
        short_category_id = parts[2]
        action = parts[3]  # n (next) или p (prev)
        
        # Получаем данные пагинации из user_data
        pagination_key = f"digest_{digest_id}_{short_category_id}"
        if not context.user_data.get("pagination") or pagination_key not in context.user_data["pagination"]:
            await query.message.reply_text(
                "❌ Данные пагинации не найдены. Пожалуйста, начните просмотр заново."
            )
            return
        
        pagination_data = context.user_data["pagination"][pagination_key]
        current_page = pagination_data["current_page"]
        total_pages = pagination_data["total_pages"]
        category = pagination_data["category"]  # Получаем полное название категории
        
        # Обновляем текущую страницу в зависимости от действия
        if action == "n" and current_page < total_pages - 1:
            current_page += 1
        elif action == "p" and current_page > 0:
            current_page -= 1
        else:
            # Если действие некорректно, игнорируем
            return
        
        # Обновляем текущую страницу в context_data
        pagination_data["current_page"] = current_page
        
        # Получаем дайджест для формирования заголовка
        digest = db_manager.get_digest_by_id_with_sections(digest_id)
        
        if not digest:
            await query.message.reply_text("❌ Дайджест не найден или был удален.")
            return
        
        # Формируем заголовок с информацией о пагинации
        digest_type = "краткий" if digest["digest_type"] == "brief" else "подробный"
        date_str = digest["date"].strftime("%d.%m.%Y")
        
        header = f"📊 {digest_type.capitalize()} дайджест за {date_str}\n"
        header += f"📂 Категория: {category.capitalize()}\n"
        header += f"📄 Страница {current_page + 1}/{total_pages}\n\n"
        
        # Конвертируем текст для корректного отображения в Telegram
        from utils.text_utils import TextUtils
        content = TextUtils.convert_to_html(header + pagination_data["parts"][current_page])
        
        # Создаем клавиатуру с кнопками навигации
        keyboard = []
        
        # Добавляем кнопки пагинации
        pagination_buttons = []
        if current_page > 0:
            pagination_buttons.append(
                InlineKeyboardButton(
                    "⬅️ Предыдущая", 
                    callback_data=f"pg_{digest_id}_{short_category_id}_p"
                )
            )
        if current_page < total_pages - 1:
            pagination_buttons.append(
                InlineKeyboardButton(
                    "Следующая ➡️", 
                    callback_data=f"pg_{digest_id}_{short_category_id}_n"
                )
            )
        
        if pagination_buttons:
            keyboard.append(pagination_buttons)
        
        # Добавляем кнопку возврата к оглавлению дайджеста
        keyboard.append([
            InlineKeyboardButton(
                "🔙 К оглавлению", 
                callback_data=f"view_digest_{digest_id}"
            )
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем обновленную секцию с кнопками пагинации
        await query.message.edit_text(
            content,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Ошибка при навигации по страницам: {str(e)}", exc_info=True)
        # В случае ошибки отправляем новое сообщение вместо редактирования
        await query.message.reply_text(
            f"Произошла ошибка при навигации: {str(e)}\n"
            "Пожалуйста, попробуйте использовать команду /list для просмотра списка дайджестов."
        )

async def show_full_digest(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager):
    """Обработчик колбэка для просмотра полного текста дайджеста"""
    query = update.callback_query
    await query.answer()
    
    try:
        # Извлекаем ID дайджеста из callback_data
        # Формат: df_DIGEST_ID
        digest_id = int(query.data.replace("df_", ""))
        
        # Получаем дайджест по ID
        digest = db_manager.get_digest_by_id_with_sections(digest_id)
        
        if not digest:
            await query.message.reply_text("❌ Дайджест не найден или был удален.")
            return
        
        # Очищаем текст от проблемных символов
        from utils.text_utils import TextUtils
        safe_text = TextUtils.clean_markdown_text(digest["text"])
        
        # Разбиваем на части, чтобы не превышать лимит Telegram
        chunks = TextUtils.split_text(safe_text, max_length=3500)
        
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
        
        # Для первой части добавляем заголовок и информацию о частях
        first_chunk = f"{header}\n\n{chunks[0]}"
        if len(chunks) > 1:
            first_chunk += f"\n\n(Продолжение следует, всего {len(chunks)} части)"
        
        # Конвертируем текст для корректного отображения в Telegram
        first_chunk_html = TextUtils.convert_to_html(first_chunk)
        
        # Создаем клавиатуру с кнопкой возврата только для первой части
        keyboard = [[
            InlineKeyboardButton(
                "🔙 К оглавлению", 
                callback_data=f"view_digest_{digest_id}"
            )
        ]]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем первую часть
        await query.message.edit_text(
            first_chunk_html,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        # Если есть дополнительные части, отправляем их новыми сообщениями
        for i in range(1, len(chunks)):
            part_text = chunks[i]
            
            # Для последней части добавляем обратную ссылку
            if i == len(chunks) - 1:
                part_text += "\n\n[Вернуться к оглавлению дайджеста]"
            
            # Конвертируем текст для корректного отображения в Telegram
            part_html = TextUtils.convert_to_html(part_text)
            
            # Отправляем часть
            await query.message.reply_text(
                part_html,
                parse_mode='HTML'
            )
    except Exception as e:
        logger.error(f"Ошибка при просмотре полного дайджеста: {str(e)}", exc_info=True)
        # В случае ошибки отправляем новое сообщение вместо редактирования
        await query.message.reply_text(
            f"Произошла ошибка при загрузке полного дайджеста: {str(e)}\n"
            "Пожалуйста, попробуйте использовать команду /list для просмотра списка дайджестов."
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
