"""
Улучшенный обработчик текстовых сообщений для бота
"""
import logging
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

async def improved_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, db_manager, llm_model):
    """
    Улучшенный обработчик текстовых сообщений с подробным логированием и диагностикой
    
    Args:
        update (Update): Объект сообщения от Telegram
        context (ContextTypes.DEFAULT_TYPE): Контекст Telegram
        db_manager: Менеджер базы данных
        llm_model: Модель для генерации ответов
    """
    user_message = update.message.text
    user_id = update.effective_user.id
    
    logger.info(f"Получено сообщение от пользователя {user_id}: {user_message[:50]}...")
    
    # Проверяем, ждем ли мы от пользователя конкретный ввод 
    # (например, диапазон дат или название категории)
    if context.user_data.get("awaiting_date_range"):
        logger.info(f"Обработка ожидаемого ввода (дата): {user_message}")
        # Логика обработки диапазона дат
        # ...
        return
    
    if context.user_data.get("awaiting_category_period"):
        logger.info(f"Обработка ожидаемого ввода (категория): {user_message}")
        # Логика обработки периода для категории
        # ...
        return
    
    if context.user_data.get("awaiting_channel_period"):
        logger.info(f"Обработка ожидаемого ввода (канал): {user_message}")
        # Логика обработки периода для канала
        # ...
        return
    
    # Если нет особых ожиданий, рассматриваем как вопрос к боту
    try:
        # Отправляем индикатор набора текста
        await update.message.chat.send_action(action="typing")
        
        # Получаем контекст для ответа - последний доступный дайджест
        logger.info("Поиск дайджеста для контекста...")
        brief_digest = db_manager.get_latest_digest_with_sections(digest_type="brief")
        detailed_digest = db_manager.get_latest_digest_with_sections(digest_type="detailed")
        
        # Используем подробный дайджест для контекста, если он есть, иначе краткий
        digest = detailed_digest or brief_digest
        
        if digest:
            logger.info(f"Найден дайджест ID={digest['id']} от {digest['date'].strftime('%Y-%m-%d')} для контекста")
        else:
            logger.warning("Дайджест не найден. Используем контекст по умолчанию.")
            # Отправляем информацию пользователю
            await update.message.reply_text(
                "К сожалению, у меня пока нет информации для ответа на ваш вопрос. "
                "Дайджест еще не сформирован. Вы можете сгенерировать его с помощью команды /generate"
            )
            return
        
        # Определяем текущую дату для поиска свежих новостей
        current_date = datetime.now()
        date_from = current_date - timedelta(days=7)  # Последние 7 дней
        
        # Получаем свежие данные за последние 7 дней
        recent_messages = db_manager.get_messages_by_date_range(
            start_date=date_from,
            end_date=current_date
        )
        
        recent_data = ""
        if recent_messages:
            # Добавляем информацию из недавних сообщений (ограничиваем объем)
            max_recent_msgs = min(5, len(recent_messages))
            recent_data = "\n\nНедавние новости:\n"
            for i, msg in enumerate(recent_messages[:max_recent_msgs]):
                recent_data += f"{i+1}. Канал {msg.channel}, {msg.date.strftime('%d.%m.%Y')}: "
                # Ограничиваем размер каждого сообщения
                msg_preview = msg.text[:150] + "..." if len(msg.text) > 150 else msg.text
                recent_data += f"{msg_preview}\n\n"
        
        logger.info(f"Подготовлено {len(recent_messages[:5])} недавних сообщений для контекста")
        
        # Формируем запрос к модели
        prompt = f"""
        Вопрос: {user_message}
        
        Контекст (дайджест правовых новостей):
        {digest["text"]}
        {recent_data}
        
        Дай краткий и точный ответ на вопрос на основе представленного контекста.
        Если информации недостаточно, так и скажи.
        Если вопрос касается определенной категории новостей, укажи, что пользователь может 
        получить более подробную информацию по этой категории с помощью команды /category.
        """
        
        logger.info(f"Отправка запроса к LLM, длина промпта: {len(prompt)} символов")
        
        # Получаем ответ от модели с таймаутом
        try:
            # Устанавливаем увеличенные параметры для запроса
            response = llm_model.generate(
                prompt=prompt,
                max_tokens=500,
                temperature=0.7  # Немного случайности для более естественных ответов
            )
            
            logger.info(f"Получен ответ от LLM, длина: {len(response)} символов")
            
            # Проверка качества ответа
            if len(response.strip()) < 10:
                logger.warning(f"Подозрительно короткий ответ: '{response}'")
                response = "Извините, возникла проблема при обработке вашего запроса. Пожалуйста, задайте вопрос иначе или попробуйте позже."
            
            # Логируем первые 100 символов ответа для отладки
            logger.info(f"Ответ (первые 100 символов): {response[:100]}...")
            
            # Отправляем ответ пользователю
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка при генерации ответа: {str(e)}", exc_info=True)
            
            # Отправляем сообщение об ошибке
            await update.message.reply_text(
                "Извините, произошла ошибка при обработке вашего запроса. "
                "Пожалуйста, попробуйте позже или воспользуйтесь командами /digest или /category."
            )
    
    except Exception as e:
        logger.error(f"Необработанная ошибка в обработчике сообщений: {str(e)}", exc_info=True)
        
        # Отправляем общее сообщение об ошибке
        await update.message.reply_text(
            "Произошла непредвиденная ошибка при обработке вашего сообщения. "
            "Пожалуйста, попробуйте позже."
        )