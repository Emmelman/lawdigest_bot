"""
Агент-критик для проверки и исправления категоризации сообщений
"""
import logging
from crewai import Agent
from langchain.tools import Tool

from config.settings import CATEGORIES
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

logger = logging.getLogger(__name__)

class CriticAgent:
    """Агент для проверки и исправления категоризации сообщений"""
    
    def __init__(self, db_manager, llm_model=None):
        """
        Инициализация агента
        
        Args:
            db_manager (DatabaseManager): Менеджер БД
            llm_model (GemmaLLM, optional): Модель для обработки текста
        """
        self.db_manager = db_manager
        
        # Импорт здесь, чтобы избежать циклических импортов
        from llm.gemma_model import GemmaLLM
        self.llm_model = llm_model or GemmaLLM()
        
        # Создаем инструмент для проверки категоризации
        review_tool = Tool(
            name="review_categorization",
            func=self.review_recent_categorizations,
            description="Проверяет и исправляет категоризацию последних сообщений"
        )
        
        # Создаем агента CrewAI
        self.agent = Agent(
            name="Critic",
            role="Критик-аналитик",
            goal="Проверять и улучшать категоризацию сообщений для повышения точности дайджеста",
            backstory="Я проверяю результаты классификации и исправляю ошибки, чтобы обеспечить высокое качество дайджеста.",
            verbose=True,
            tools=[review_tool]
        )
    def review_categorization_batch(self, messages_batch):
        """
        Обработка пакета сообщений
        
        Args:
            messages_batch (list): Список сообщений для проверки
            
        Returns:
            list: Результаты проверки
        """
        results = []
        for message in messages_batch:
            result = self.review_categorization(message.id, message.category)
            results.append(result)
        return results
    def get_message_by_id(self, message_id):
        """
        Получение сообщения по ID через менеджер БД
        
        Args:
            message_id (int): ID сообщения
            
        Returns:
            Message: Объект сообщения или None
        """
        return self.db_manager.get_message_by_id(message_id)
    
    def review_categorization(self, message_id, original_category):
        """
        Проверяет и при необходимости исправляет категорию сообщения
        
        Args:
            message_id (int): ID сообщения
            original_category (str): Текущая категория
            
        Returns:
            dict: Результат проверки
        """
        # Получаем сообщение из базы данных
        message = self.get_message_by_id(message_id)
        if not message:
            logger.warning(f"Сообщение с ID {message_id} не найдено")
            return {"status": "error", "message": "Сообщение не найдено"}
        
        # Формируем детальный промпт для критика
        prompt = f"""
        Ты - эксперт по анализу правовых новостей. Проверь, правильно ли категоризировано следующее сообщение.
        
        Сообщение:
        {message.text}
        
        Текущая категория: {original_category}
        
        Возможные категории:
        1. Законодательные инициативы - предложения о создании новых законов или нормативных актов, находящиеся на стадии обсуждения, внесения или рассмотрения в Госдуме. Обычно содержат фразы: "законопроект", "проект закона", "внесен на рассмотрение", "планируется принять".

        2. Новая судебная практика - решения, определения, постановления судов, создающие прецеденты или разъясняющие применение норм права. Признаки: упоминание судов (ВС, Верховный Суд, КС, арбитражный суд), номеров дел, дат решений, слов "решение", "определение", "постановление", "практика".

        3. Новые законы - недавно принятые и вступившие в силу законодательные акты. Признаки: "закон принят", "закон подписан", "вступает в силу", "вступил в силу", указание номера федерального закона.

        4. Поправки к законам - изменения в существующих законах, внесенные или вступившие в силу. Признаки: "внесены изменения", "поправки", "новая редакция", "дополнен статьей", указания на изменение конкретных статей существующих законов.

        5. Другое - не относящееся к вышеперечисленным категориям
        
        Особые указания для точной категоризации:
        - Если описывается решение суда, определение суда, обзор практики - это "новая судебная практика"
        - Если указаны названия судов (ВС, КС) и описываются их решения - это "новая судебная практика"
        - Если упоминаются номера дел или определений - это "новая судебная практика"
        - Если говорится о внесении законопроекта или его рассмотрении, но не о принятии - это "законодательные инициативы"
        - Если упоминается о принятии закона в третьем чтении, о подписании Президентом, принятии закона или законов - это "новые законы"
        
        Верни ответ СТРОГО в формате:
        Правильная категория: [категория]
        Обоснование: [краткое объяснение]
        """
        
        try:
            response = self.llm_model.generate(prompt, max_tokens=300)
            logger.debug(f"Ответ критика для сообщения {message_id}: {response}")
            
            # Парсим ответ
            lines = response.strip().split("\n")
            new_category = None
            justification = ""
            
            for line in lines:
                if line.startswith("Правильная категория:"):
                    category_text = line.replace("Правильная категория:", "").strip()
                    # Находим наиболее близкую категорию
                    for category in CATEGORIES + ["другое"]:
                        if category.lower() in category_text.lower():
                            new_category = category
                            break
                
                if line.startswith("Обоснование:"):
                    justification = line.replace("Обоснование:", "").strip()
            
            # Если категория изменилась, обновляем её в БД
            if new_category and new_category != original_category:
                success = self.db_manager.update_message_category(message_id, new_category)
                logger.info(f"Категория сообщения {message_id} изменена с '{original_category}' на '{new_category}'. Обоснование: {justification}")
                return {
                    "status": "updated",
                    "message_id": message_id,
                    "old_category": original_category,
                    "new_category": new_category,
                    "justification": justification
                }
            else:
                logger.debug(f"Категория сообщения {message_id} оставлена без изменений: '{original_category}'")
                return {
                    "status": "unchanged",
                    "message_id": message_id,
                    "category": original_category or new_category,
                    "justification": justification
                }
        except Exception as e:
            logger.error(f"Ошибка при проверке категории сообщения {message_id}: {str(e)}")
            return {
                "status": "error",
                "message_id": message_id,
                "error": str(e)
            }
    
    def review_recent_categorizations(self, confidence_threshold=3, limit=30, batch_size=5, max_workers=3):
        """
        Проверяет категоризацию сообщений с низкой уверенностью
        
        Args:
            confidence_threshold (int): Проверять только сообщения с уверенностью <= этого значения
            limit (int): Максимальное количество сообщений для проверки
            batch_size (int): Размер пакета для параллельной обработки
            max_workers (int): Максимальное количество потоков
            
        Returns:
            dict: Результаты проверки
        """
        logger.info(f"Запуск проверки категоризации сообщений с уверенностью <= {confidence_threshold}")
        
        # Получаем сообщения с низкой уверенностью
        messages = self.db_manager.get_messages_with_low_confidence(
            confidence_threshold=confidence_threshold, 
            limit=limit
        )
        
        if not messages:
            logger.info("Нет сообщений с низкой уверенностью для проверки")
            return {
                "status": "success",
                "total": 0,
                "details": []
            }
        
        logger.info(f"Получено {len(messages)} сообщений с низкой уверенностью")
        
        # Разбиваем на пакеты для параллельной обработки
        batches = [messages[i:i+batch_size] for i in range(0, len(messages), batch_size)]
        
        all_results = []
        # Используем ThreadPoolExecutor для параллельной обработки
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(self.review_categorization_batch, batch): batch 
                for batch in batches
            }
            
            for future in concurrent.futures.as_completed(future_to_batch):
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                except Exception as e:
                    logger.error(f"Ошибка при обработке пакета сообщений: {str(e)}")
        
        # Подсчет статистики
        updated = sum(1 for r in all_results if r.get("status") == "updated")
        unchanged = sum(1 for r in all_results if r.get("status") == "unchanged")
        errors = sum(1 for r in all_results if r.get("status") == "error")
        
        logger.info(f"Проверка категоризации завершена. Всего: {len(messages)}, обновлено: {updated}, "
                f"без изменений: {unchanged}, ошибок: {errors}")
        
        return {
            "status": "success",
            "total": len(messages),
            "updated": updated,
            "unchanged": unchanged,
            "errors": errors,
            "details": all_results
        }