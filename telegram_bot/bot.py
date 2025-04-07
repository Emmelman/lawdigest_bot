# telegram_bot/bot.py
"""
Telegram-бот для взаимодействия с пользователями
"""
import logging
import re
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
import asyncio
from telegram.ext import (
    Application, 
    CommandHandler, 
    MessageHandler, 
    CallbackQueryHandler,
    ContextTypes, 
    filters
)
from config.settings import TELEGRAM_BOT_TOKEN, CATEGORIES, BOT_USERNAME, TELEGRAM_CHANNELS

from database.db_manager import DatabaseManager
from llm.gemma_model import GemmaLLM

logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self, db_manager, llm_model=None): 
        """
        Инициализация бота
        """
        self.db_manager = db_manager
        self.llm_model = llm_model or GemmaLLM()
        self.application = None
        self.menu_commands = [
            ("digest", "Краткий дайджест новостей"),
            ("detail", "Подробный дайджест"),
            ("cat", "Выбрать категорию новостей"),
            ("date", "Дайджест за дату (формат: дд.мм.гггг)"),
            ("generate", "Сгенерировать новый дайджест"),
            ("list", "Список доступных дайджестов"),
            ("help", "Справка")
        ]
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user = update.effective_user
        
        # Проверяем, есть ли параметры в команде start
        if context.args and context.args[0].startswith('msg_'):
            # Извлекаем ID сообщения
            try:
                message_id = int(context.args[0].replace('msg_', ''))
                message = self.db_manager.get_message_by_id(message_id)
                
                if message:
                    await update.message.reply_text(
                        f"Сообщение из канала {message.channel} от {message.date.strftime('%d.%m.%Y')}:\n\n{message.text}"
                    )
                    return
            except (ValueError, Exception) as e:
                logger.error(f"Ошибка при обработке параметра start: {str(e)}")
        
        # Обычная команда /start без параметров
        await update.message.reply_text(
            f"Здравствуйте, {user.first_name}! Я бот для дайджеста правовых новостей.\n\n"
            "Доступные команды:\n"
            "/digest - получить краткий дайджест\n"
            "/digest_detailed - получить подробный дайджест\n"
            "/category - выбрать категорию новостей\n"
            "/help - получить справку"
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        await update.message.reply_text(
            "Я могу предоставить вам дайджест правовых новостей.\n\n"
            "Доступные команды:\n"
            "/digest - получить краткий дайджест\n"
            "/digest_detailed - получить подробный дайджест\n"
            "/category - выбрать категорию новостей\n"
            "/help - получить справку\n\n"
            "Вы также можете задать мне вопрос по правовым новостям."
        )
    
    async def digest_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /digest - краткий дайджест"""
        # Получаем последний краткий дайджест
        digest = self.db_manager.get_latest_digest_with_sections(digest_type="brief")
        
        if not digest:
            # Если краткого нет, пробуем получить любой
            digest = self.db_manager.get_latest_digest_with_sections()
        
        if not digest:
            await update.message.reply_text("К сожалению, дайджест еще не сформирован.")
            return
        
        # Очищаем текст от проблемных Markdown-сущностей
        safe_text = self._clean_markdown_text(digest["text"])
        
        # Отправляем дайджест по частям, так как Telegram ограничивает длину сообщения
        chunks = self._split_text(safe_text)
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                text_html = self._convert_to_html(chunk)
                await update.message.reply_text(
                    f"Дайджест за {digest['date'].strftime('%d.%m.%Y')} (краткая версия):\n\n{text_html}",
                    parse_mode='HTML'  # Отключаем Markdown parsing
                )
            else:
                await update.message.reply_text(chunk, parse_mode='HTML')

    async def digest_detailed_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /digest_detailed - подробный дайджест"""
        # Получаем последний подробный дайджест
        digest = self.db_manager.get_latest_digest_with_sections(digest_type="detailed")
        
        if not digest:
            # Если подробного нет, пробуем получить любой
            digest = self.db_manager.get_latest_digest_with_sections()
        
        if not digest:
            await update.message.reply_text("К сожалению, подробный дайджест еще не сформирован.")
            return
        
        # Очищаем текст от проблемных Markdown-сущностей
        safe_text = self._clean_markdown_text(digest["text"])
        
        # Отправляем дайджест по частям, так как Telegram ограничивает длину сообщения
        chunks = self._split_text(safe_text)
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                text_html = self._convert_to_html(chunk)
                await update.message.reply_text(
                    f"Дайджест за {digest['date'].strftime('%d.%m.%Y')} (подробная версия):\n\n{text_html}",
                    parse_mode='HTML'  # Отключаем Markdown parsing
                )
            else:
                await update.message.reply_text(chunk, parse_mode='HTML')

    def _clean_for_html(self, text):
        """
        Подготавливает текст для отправки с HTML-форматированием в Telegram
        """
        import re
        
        # Экранируем HTML-символы
        text = text.replace('&', '&amp;')  # Должно быть первым!
        text = text.replace('<', '&lt;')
        text = text.replace('>', '&gt;')
        
        # Заменяем маркеры форматирования на HTML-теги
        text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)  # **жирный** -> <b>жирный</b>
        text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)      # *курсив* -> <i>курсив</i>
        
        # Исправляем экранированные точки после цифр
        text = re.sub(r'(\d+)\\\.\s*', r'\1. ', text)
        
        return text
    
    async def category_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /category"""
        keyboard = []
        
        # Для каждой категории создаем две кнопки с сокращённым текстом
        for cat in CATEGORIES:
            # Сокращаем название категории, если оно длинное
            short_name = cat[:15] + "..." if len(cat) > 15 else cat
            keyboard.append([
                InlineKeyboardButton(f"{short_name} (кратко)", callback_data=f"cat_brief_{cat}"),
                InlineKeyboardButton(f"{short_name} (подр.)", callback_data=f"cat_detailed_{cat}")
            ])
        
        # Добавляем кнопку для категории "другое"
        keyboard.append([
            InlineKeyboardButton("другое (кратко)", callback_data="cat_brief_другое"),
            InlineKeyboardButton("другое (подр.)", callback_data="cat_detailed_другое")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Выберите категорию и тип обзора:", 
            reply_markup=reply_markup
        )
    async def generate_digest_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /generate - запуск генерации дайджеста"""
        keyboard = [
            [InlineKeyboardButton("За сегодня", callback_data="gen_digest_today")],
            [InlineKeyboardButton("За вчера", callback_data="gen_digest_yesterday")],
            [InlineKeyboardButton("За период", callback_data="gen_digest_range")],
            [InlineKeyboardButton("С фокусом на категорию", callback_data="gen_digest_category")],
            [InlineKeyboardButton("С фильтрацией по каналам", callback_data="gen_digest_channels")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Выберите тип дайджеста для генерации:", 
            reply_markup=reply_markup
        )
    
    async def list_digests_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /list - список доступных дайджестов"""
        # Получаем последние 10 дайджестов (вместо 5)
        digests = self.db_manager.find_digests_by_parameters(limit=10)
        
        if not digests:
            await update.message.reply_text("Дайджесты еще не сформированы.")
            return
        
        keyboard = []
        for digest in digests:
            # Формируем описание дайджеста с большим количеством деталей
            if digest["date_range_start"] and digest["date_range_end"]:
                days_diff = (digest["date_range_end"] - digest["date_range_start"]).days
                if days_diff > 0:
                    start_date = digest["date_range_start"].strftime("%d.%m.%Y")
                    end_date = digest["date_range_end"].strftime("%d.%m.%Y")
                    date_text = f"{start_date} - {end_date} ({days_diff+1} дн.)"
                else:
                    date_text = digest["date"].strftime("%d.%m.%Y")
            else:
                date_text = digest["date"].strftime("%d.%m.%Y")
            
            # Добавляем информацию о фокусе, если есть
            focus_text = ""
            if digest["focus_category"]:
                focus_text = f" - {digest['focus_category']}"
            
            # Добавляем время создания
            created_at = ""
            if digest.get("created_at"):
                created_at = f" ({digest['created_at'].strftime('%H:%M')})"
            
            button_text = f"{date_text}{focus_text} ({digest['digest_type']}){created_at}"
            keyboard.append([
                InlineKeyboardButton(button_text, callback_data=f"show_digest_{digest['id']}")
            ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Выберите дайджест для просмотра:", 
            reply_markup=reply_markup
        )
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
    
        """Обработчик нажатий на кнопки"""
        query = update.callback_query
        await query.answer()
        
        # Обработка запросов на генерацию дайджеста
        if query.data.startswith("gen_digest_"):
            action = query.data.replace("gen_digest_", "")
            
            if action == "today":
                # Генерация за сегодня
                today = datetime.now()
                await self._handle_digest_generation(
                    query, today, today, "За сегодня"
                )
            elif action == "yesterday":
                # Генерация за вчера
                yesterday = datetime.now() - timedelta(days=1)
                await self._handle_digest_generation(
                    query, yesterday, yesterday, "За вчера"
                )
            elif action == "range":
                # Запрашиваем диапазон дат
                context.user_data["awaiting_date_range"] = True
                await query.message.reply_text(
                    "Укажите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, например: 01.04.2025-07.04.2025"
                )
            elif action == "category":
                # Выбор категории для фокуса
                keyboard = []
                for category in CATEGORIES + ["другое"]:
                    keyboard.append([
                        InlineKeyboardButton(
                            category, callback_data=f"gen_digest_cat_{category}"
                        )
                    ])
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.message.reply_text(
                    "Выберите категорию для формирования фокусированного дайджеста:", 
                    reply_markup=reply_markup
                )
            
            elif action == "channels":
                # Выбор каналов для фильтрации
                keyboard = []
                for channel in TELEGRAM_CHANNELS:
                    display_name = channel
                    if channel.startswith("@"):
                        display_name = channel[1:]
                        keyboard.append([
                        InlineKeyboardButton(
                            display_name, callback_data=f"gen_digest_chan_{channel}"
                        )
                    ])
                        
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.message.reply_text(
                    "Выберите канал для формирования дайджеста:", 
                    reply_markup=reply_markup
                )
        # В методе button_callback, для ветки обработки каналов:
        elif query.data.startswith("gen_digest_chan_"):
            channel = query.data.replace("gen_digest_chan_", "")
            
            # Запрашиваем период для канала
            context.user_data["focus_channel"] = channel
            # Сохраняем каналы в списке, а не просто строкой
            context.user_data["channels"] = [channel]
            await query.message.reply_text(
                f"Выбран канал: {channel}\n"
                "Теперь укажите период в формате ДД.ММ.ГГГГ или ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, "
                "или напишите 'сегодня' или 'вчера'"
            )
            context.user_data["awaiting_channel_period"] = True
        # Обработка выбора категории для фокуса
        elif query.data.startswith("gen_digest_cat_"):
            category = query.data.replace("gen_digest_cat_", "")
            
            # Запрашиваем период для категории
            context.user_data["focus_category"] = category
            await query.message.reply_text(
                f"Выбрана категория: {category}\n"
                "Теперь укажите период в формате ДД.ММ.ГГГГ или ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, "
                "или напишите 'сегодня' или 'вчера'"
            )
            context.user_data["awaiting_category_period"] = True
        
        # Обработка выбора канала для фильтрации
        elif query.data.startswith("gen_digest_chan_"):
            channel = query.data.replace("gen_digest_chan_", "")
            
            # Запрашиваем период для канала
            context.user_data["focus_channel"] = channel
            await query.message.reply_text(
                f"Выбран канал: {channel}\n"
                "Теперь укажите период в формате ДД.ММ.ГГГГ или ДД.ММ.ГГГГ-ДД.ММ.ГГГГ, "
                "или напишите 'сегодня' или 'вчера'"
            )
            context.user_data["awaiting_channel_period"] = True
        
        # Обработка просмотра дайджеста из списка
        elif query.data.startswith("show_digest_"):
            digest_id = int(query.data.replace("show_digest_", ""))
            await self._show_digest_by_id(query.message, digest_id)
        
        # Существующая обработка категорий
        elif query.data.startswith("cat_"):
            # Формат: cat_[тип]_[категория]
            parts = query.data.split("_", 2)
            if len(parts) == 3:
                digest_type = parts[1]  # brief или detailed
                category = parts[2]     # название категории
            
                # Получаем последний дайджест нужного типа
                digest = self.db_manager.get_latest_digest_with_sections(digest_type=digest_type)
                
                if not digest:
                    # Если дайджеста такого типа нет, берем любой
                    digest = self.db_manager.get_latest_digest_with_sections()
                
                if not digest:
                    await query.message.reply_text(f"К сожалению, дайджест еще не сформирован.")
                    return
                
                # Ищем соответствующую секцию в дайджесте
                section = next(
                    (s for s in digest["sections"] if s["category"] == category), 
                    None
                )
                
                if not section:
                    await query.message.reply_text(
                        f"Информация по категории '{category}' отсутствует в последнем дайджесте.",
                        parse_mode='HTML'
                    )
                    return
                
                # Подготавливаем текст для ответа
                digest_type_name = "Краткий обзор" if digest_type == "brief" else "Подробный обзор"
                header = f"Дайджест за {digest['date'].strftime('%d.%m.%Y')}\n{digest_type_name} категории: {category}\n\n"
                
                # Отправляем секцию (возможно, разбитую на части)
                full_text = header + section["text"]
                chunks = self._split_text(full_text)
                
                for chunk in chunks:
                    await query.message.reply_text(chunk, parse_mode='HTML')
    
    # В файле telegram_bot/bot.py
    async def _handle_digest_generation(self, query, start_date, end_date, description, 
                                   focus_category=None, channels=None, keywords=None):
        """
        Запуск процесса генерации дайджеста с параметрами
        """
        await query.message.reply_text(
            f"Запущена генерация дайджеста {description}.\n"
            f"Период: с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}\n"
            f"{'Фокус на категории: ' + focus_category if focus_category else ''}\n"
            f"{'Каналы: ' + ', '.join(channels) if channels else ''}\n"
            f"{'Ключевые слова: ' + ', '.join(keywords) if keywords else ''}\n\n"
            "Это может занять несколько минут..."
        )
        
        # Запускаем генерацию в отдельном потоке
        import threading
        
        def generate_and_notify():
            try:
                # Этот код внутри функции, поэтому self, query и т.д. доступны из окружения
                from agents.digester import DigesterAgent
                from agents.data_collector import DataCollectorAgent
                from agents.analyzer import AnalyzerAgent
                
                # Создаем агенты
                collector = DataCollectorAgent(self.db_manager)
                analyzer = AnalyzerAgent(self.db_manager)
                digester = DigesterAgent(self.db_manager, self.llm_model)
                
                # Вычисляем количество дней
                days_back = (end_date - start_date).days + 1
                
                # Отправляем сообщение через прокси-функцию
                self._send_sync_message(query.message.chat_id, 
                    f"Собираем данные за последние {days_back} дней...")
                
                # Запускаем сбор данных
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                collect_result = loop.run_until_complete(
                    collector._collect_all_channels_parallel(days_back=days_back)
                )
                
                total_messages = sum(collect_result.values())
                self._send_sync_message(query.message.chat_id, 
                    f"Собрано {total_messages} новых сообщений")
                
                # Запускаем анализ сообщений
                self._send_sync_message(query.message.chat_id, 
                    "Выполняем анализ и категоризацию сообщений...")
                
                from agents.analyzer import AnalyzerAgent
                analyzer = AnalyzerAgent(self.db_manager)
                analyzer.fast_check = True  # Включаем быструю проверку критиком
                analyze_result = analyzer.analyze_messages(limit=200)  # увеличиваем лимит
                
                # Запускаем критика для всех сообщений с низкой уверенностью
                self._send_sync_message(query.message.chat_id, 
                    "Проверяем сообщения с низкой уверенностью...")

                from agents.critic import CriticAgent
                critic = CriticAgent(self.db_manager)
                review_result = critic.review_recent_categorizations(
                    confidence_threshold=2,  # Проверять сообщения с уверенностью <= 2
                    limit=50
                )

                self._send_sync_message(query.message.chat_id, 
                    f"Проверка завершена. Проверено: {review_result.get('total', 0)}, "
                    f"обновлено: {review_result.get('updated', 0)} сообщений.")

                # Запускаем генерацию дайджеста
                self._send_sync_message(query.message.chat_id, 
                    "Формируем дайджест...")
                
                result = digester.create_digest(
                    date=end_date,
                    days_back=days_back,
                    digest_type="both",
                    focus_category=focus_category,
                    channels=channels,
                    keywords=keywords
                )
                
                # Проверяем результат
                if result.get("status") == "no_messages":
                    self._send_sync_message(query.message.chat_id, 
                        "Не найдено сообщений, соответствующих критериям фильтрации.")
                    return
                
                # Отправляем уведомление об успешной генерации
                self._send_sync_message(query.message.chat_id, 
                    f"Дайджест {description} успешно сгенерирован!\n"
                    "Используйте команду /list для просмотра доступных дайджестов.")
                
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                logger.error(f"Ошибка при генерации дайджеста: {str(e)}\n{tb}")
                self._send_sync_message(query.message.chat_id, 
                    f"Произошла ошибка при генерации дайджеста: {str(e)}")
        
        # Запускаем в отдельном потоке
        thread = threading.Thread(target=generate_and_notify)
        thread.daemon = True
        thread.start()

    # Этот метод должен быть на уровне класса, а не внутри другого метода
    def _send_sync_message(self, chat_id, text):
        """Синхронная отправка сообщения через requests"""
        import requests
        
        token = TELEGRAM_BOT_TOKEN
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        
        try:
            response = requests.post(url, data=data)
            response.raise_for_status()
            logger.info(f"Сообщение отправлено: {text[:30]}...")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения: {str(e)}")
            
            
    
    async def _show_digest_by_id(self, message, digest_id):
        """
        Показывает дайджест по его ID
        """
        # Получаем дайджест с секциями
        digest = self.db_manager.get_digest_by_id_with_sections(digest_id)
        
        if not digest:
            await message.reply_text("Дайджест не найден.")
            return
        
        # Очищаем текст от проблемных символов
        safe_text = self._clean_markdown_text(digest["text"])
        
        # Отправляем дайджест по частям
        chunks = self._split_text(safe_text)
        
        # Формируем заголовок в зависимости от параметров дайджеста
        header = f"Дайджест за {digest['date'].strftime('%d.%m.%Y')}"
        
        if digest.get("date_range_start") and digest.get("date_range_end"):
            start_date = digest["date_range_start"].strftime("%d.%m.%Y")
            end_date = digest["date_range_end"].strftime("%d.%m.%Y")
            if start_date != end_date:
                header = f"Дайджест за период с {start_date} по {end_date}"
        
        if digest.get("focus_category"):
            header += f" (фокус: {digest['focus_category']})"
            
        if digest.get("digest_type"):
            header += f" - {digest['digest_type']}"
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                text_html = self._convert_to_html(chunk)
                await message.reply_text(
                    f"{header}\n\n{text_html}",
                    parse_mode='HTML'
                )
            else:
                await message.reply_text(chunk, parse_mode='HTML')

    async def message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений"""
        user_message = update.message.text
        
        # Получаем контекст для ответа
        brief_digest = self.db_manager.get_latest_digest_with_sections(digest_type="brief")
        detailed_digest = self.db_manager.get_latest_digest_with_sections(digest_type="detailed")
        
        # Используем подробный дайджест для контекста, если он есть
        digest = detailed_digest or brief_digest
        
        if not digest:
            await update.message.reply_text(
                "К сожалению, у меня пока нет информации для ответа на ваш вопрос. "
                "Дайджест еще не сформирован."
            )
            return
        
        # Формируем запрос к модели
        prompt = f"""
        Вопрос: {user_message}
        
        Контекст (дайджест правовых новостей):
        {digest["text"]}
        
        Дай краткий и точный ответ на вопрос на основе представленного контекста.
        Если информации недостаточно, так и скажи.
        Если вопрос касается определенной категории новостей, укажи, что пользователь может 
        получить более подробную информацию по этой категории с помощью команды /category.
        """
        
        # Получаем ответ от модели
        try:
            response = self.llm_model.generate(prompt, max_tokens=500)
            await update.message.reply_text(response)
        except Exception as e:
            logger.error(f"Ошибка при генерации ответа: {str(e)}")
            await update.message.reply_text(
                "Извините, произошла ошибка при обработке вашего запроса. "
                "Пожалуйста, попробуйте позже или воспользуйтесь командами /digest или /category.",
                parse_mode='HTML'
            )
    
    async def date_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /date - получение дайджеста за определенную дату"""
        if not context.args or len(context.args) != 1:
            await update.message.reply_text(
                "Пожалуйста, укажите дату в формате ДД.ММ.ГГГГ, например: /date 01.04.2025"
            )
            return
        
        date_str = context.args[0]
        try:
            # Парсим дату из строки
            date_parts = date_str.split(".")
            if len(date_parts) != 3:
                raise ValueError("Неверный формат даты")
            
            day, month, year = map(int, date_parts)
            target_date = datetime(year, month, day)
            
            # Получаем дайджест по дате
            digest = self.db_manager.get_digest_by_date_with_sections(target_date)
            
            if not digest:
                await update.message.reply_text(
                    f"Дайджест за {date_str} не найден. Возможно, он еще не был сформирован."
                )
                return
            
            # Отправляем дайджест по частям
            chunks = self._split_text(digest["text"])
            
            for i, chunk in enumerate(chunks):
                if i == 0:
                    text_html = self._convert_to_html(chunk)
                    await update.message.reply_text(
                        f"Дайджест за {digest['date'].strftime('%d.%m.%Y')}:\n\n{text_html}",
                        parse_mode='HTML'
                    )
                else:
                    await update.message.reply_text(chunk)
                    
        except ValueError as e:
            await update.message.reply_text(
                f"Ошибка в формате даты. Пожалуйста, используйте формат ДД.ММ.ГГГГ, например: 01.04.2025"
            )
        except Exception as e:
            logger.error(f"Ошибка при обработке команды /date: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте позже."
            )
    
    def _split_text(self, text, max_length=4000):
        """
        Разбивает длинный текст на части, учитывая ограничения Telegram
        
        Args:
            text (str): Исходный текст
            max_length (int): Максимальная длина части
            
        Returns:
            list: Список частей текста
        """
        if len(text) <= max_length:
            return [text]
        
        parts = []
        current_part = ""
        
        for paragraph in text.split("\n\n"):
            # Если абзац сам по себе слишком длинный
            if len(paragraph) > max_length:
                # Добавляем предыдущий фрагмент, если есть
                if current_part:
                    parts.append(current_part)
                    current_part = ""
                
                # Разбиваем длинный абзац на предложения
                sentences = paragraph.split(". ")
                sentence_part = ""
                
                for sentence in sentences:
                    if len(sentence_part) + len(sentence) + 2 <= max_length:
                        if sentence_part:
                            sentence_part += ". " + sentence
                        else:
                            sentence_part = sentence
                    else:
                        parts.append(sentence_part.strip())
                        sentence_part = sentence
                
                if sentence_part:
                    parts.append(sentence_part.strip())
            
            # Обычный случай - проверяем, можно ли добавить абзац к текущей части
            elif len(current_part) + len(paragraph) + 2 <= max_length:
                if current_part:
                    current_part += "\n\n" + paragraph
                else:
                    current_part = paragraph
            else:
                parts.append(current_part)
                current_part = paragraph
        
        if current_part:
            parts.append(current_part)
        
        return parts
    def _clean_markdown_text(self, text):
        """
        Корректная обработка Markdown текста
        """
        import re
        
        # Специальная функция для экранирования только специфических символов
        def escape_markdown(text):
            escape_chars = r'_*[]()~`>#+-=|{}.!'
            return ''.join(['\\' + char if char in escape_chars else char for char in text])
        
        # Обработка ссылок, чтобы они корректно отображались
        def process_links(match):
            text, url = match.groups()
            # Экранируем текст внутри квадратных скобок
            safe_text = escape_markdown(text)
            return f'[{safe_text}]({url})'
        
        # Обработка жирного текста
        def process_bold(match):
            text = match.group(1)
            # Экранируем текст внутри жирного форматирования
            safe_text = escape_markdown(text)
            return f'*{safe_text}*'
        
        # Сначала обрабатываем ссылки
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', process_links, text)
        
        # Затем обрабатываем жирный текст
        text = re.sub(r'\*\*([^*]+)\*\*', process_bold, text)
        
        return text
    def _convert_to_html(self, text):
        """Конвертирует Markdown-подобный синтаксис в HTML"""
        # Заменяем звездочки на HTML-теги
        text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)  # **жирный** -> <b>жирный</b>
        text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)      # *курсив* -> <i>курсив</i>
        
        # Удаляем экранирующие символы
        text = re.sub(r'\\([.()[\]{}])', r'\1', text)  # \.() -> .()
        
        return text
    async def setup_commands(self):
        """Регистрация обработчиков команд"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("digest", self.digest_command))
        self.application.add_handler(CommandHandler("brief", self.digest_command))
        self.application.add_handler(CommandHandler("digest_detailed", self.digest_detailed_command))
        self.application.add_handler(CommandHandler("detail", self.digest_detailed_command))
        self.application.add_handler(CommandHandler("category", self.category_command))
        self.application.add_handler(CommandHandler("cat", self.category_command))
        self.application.add_handler(CommandHandler("date", self.date_command))
        
        # Новые команды
        self.application.add_handler(CommandHandler("generate", self.generate_digest_command))
        self.application.add_handler(CommandHandler("gen", self.generate_digest_command))
        self.application.add_handler(CommandHandler("list", self.list_digests_command))
        
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.message_handler))
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
        # Заменяем вызов asyncio.run() на более надежный метод создания event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.setup_commands())
        
        # Используем job queue для установки команд при запуске
        async def setup_commands_job(context):
            await context.bot.set_my_commands(commands)
        
        # Добавляем задачу в очередь при запуске
        self.application.job_queue.run_once(setup_commands_job, 1)
        
        # Запускаем бота
        self.application.run_polling()
        
        return self.application
        
    