"""
Агент-критик для проверки и исправления категоризации сообщений
"""
import logging
import json
import os
from datetime import datetime
from crewai import Agent
from langchain.tools import Tool
from utils.learning_manager import LearningExamplesManager
from config.settings import CATEGORIES
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

logger = logging.getLogger(__name__)

class CriticAgent:
    def __init__(self, db_manager, llm_model=None):
        """
        Инициализация агента
        
        Args:
            db_manager (DatabaseManager): Менеджер БД
            llm_model (GemmaLLM, optional): Модель для обработки текста
        """
        self.db_manager = db_manager
        
        # Импорт здесь, чтобы избежать циклических импортов
        from llm.gemma_model import GemmaLLM # Changed to lazy import
        
        # Инициализируем менеджер обучающих примеров
        self.learning_manager = LearningExamplesManager()
        
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
        self.llm_model = llm_model or GemmaLLM() # Initialize after lazy import
    def _save_learning_example(self, text, category, justification):
        """Сохраняет примеры для обучения аналитика"""
        try:
            # Используем менеджер обучающих примеров
            success = self.learning_manager.save_example(text, category, justification)
            if success:
                logger.info(f"Сохранен обучающий пример для категории '{category}'")
            return success
        except Exception as e:
            logger.error(f"Не удалось сохранить обучающий пример: {str(e)}")
            return False
        
    def get_message_by_id(self, message_id):
        """
        Получение сообщения по ID через менеджер БД
        
        Args:
            message_id (int): ID сообщения
            
        Returns:
            Message: Объект сообщения или None
        """
        return self.db_manager.get_message_by_id(message_id)
    
    # В agents/critic.py - улучшенный метод review_categorization
# Заменить существующий метод на этот

    # В agents/critic.py - улучшенный метод review_categorization
# Заменить существующий метод на этот

    def review_categorization(self, message_id, original_category):
        """
        Проверяет и при необходимости исправляет категорию сообщения
        с использованием многоперспективного анализа
        
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
        
        logger.info(f"Начинаю многоперспективный анализ сообщения {message_id}")
        
        # ЭТАП 1: ПРАВОВАЯ ЭКСПЕРТИЗА
        legal_analysis = self._perform_legal_accuracy_review(message.text, original_category)
        logger.debug(f"Правовая экспертиза: {legal_analysis}")
        
        # ЭТАП 2: ЛОГИЧЕСКАЯ КОНСИСТЕНТНОСТЬ
        consistency_analysis = self._perform_consistency_review(message.text, original_category)
        logger.debug(f"Анализ консистентности: {consistency_analysis}")
        
        # ЭТАП 3: КОНТЕКСТНЫЙ АНАЛИЗ
        context_analysis = self._perform_context_review(message.text, message.channel, original_category)
        logger.debug(f"Контекстный анализ: {context_analysis}")
        
        # ЭТАП 4: СИНТЕЗ И ФИНАЛЬНОЕ РЕШЕНИЕ
        final_decision = self._synthesize_multi_perspective_decision(
            message.text, original_category, 
            legal_analysis, consistency_analysis, context_analysis
        )
        
        # ЛОГИРОВАНИЕ МНОГОПЕРСПЕКТИВНОГО REASONING
        self._log_multi_perspective_reasoning(
            message_id, message.text, original_category,
            legal_analysis, consistency_analysis, context_analysis, final_decision
        )
        
        # Применяем решение
        return self._apply_review_decision(message_id, message, original_category, final_decision)

    def _perform_legal_accuracy_review(self, message_text, current_category):
        """ЭТАП 1: Проверка правовой точности"""
        
        legal_prompt = f"""
    Ты - эксперт по правовой терминологии и российскому законодательству.

    ЗАДАЧА: Проверить правовую точность категоризации сообщения.

    СООБЩЕНИЕ: {message_text}
    ТЕКУЩАЯ КАТЕГОРИЯ: {current_category}

    ПРАВОВАЯ ЭКСПЕРТИЗА:

    1. ТЕРМИНОЛОГИЧЕСКИЙ АНАЛИЗ:
    - Правильно ли использованы правовые термины?
    - Соответствует ли терминология российской правовой системе?
    - Нет ли ошибок в понимании правовых процедур?

    2. ПРОЦЕДУРНЫЙ АНАЛИЗ:
    - Законодательные инициативы: проекты, внесение, рассмотрение
    - Новые законы: принятие, подписание, опубликование, вступление в силу
    - Поправки к законам: изменения, дополнения существующих актов
    - Судебная практика: решения, постановления, разъяснения судов

    3. ПРАВОВАЯ ОЦЕНКА:
    На какой стадии правового процесса находится описываемое событие?
    Соответствует ли категория этой стадии?

    ОТВЕТ в формате:
    Правовая оценка: [правильно/неправильно/спорно]
    Стадия процесса: [описание стадии]
    Рекомендация: [подтвердить категорию или предложить другую]
    """
        
        try:
            response = self.llm_model.generate(legal_prompt, max_tokens=300, temperature=0.2)
            return self._parse_review_response(response, "legal")
        except Exception as e:
            logger.error(f"Ошибка в правовой экспертизе: {str(e)}")
            return {"status": "error", "recommendation": "подтвердить"}

    def _perform_consistency_review(self, message_text, current_category):
        """ЭТАП 2: Проверка логической консистентности"""
        
        consistency_prompt = f"""
    Ты - аналитик логической последовательности и качества классификации.

    ЗАДАЧА: Проверить внутреннюю логику категоризации.

    СООБЩЕНИЕ: {message_text}
    ТЕКУЩАЯ КАТЕГОРИЯ: {current_category}

    АНАЛИЗ КОНСИСТЕНТНОСТИ:

    1. ЛОГИЧЕСКАЯ ПРОВЕРКА:
    - Есть ли явные признаки указанной категории?
    - Нет ли признаков других категорий?
    - Логично ли решение о категоризации?

    2. ПРИЗНАКИ КАТЕГОРИЙ:
    Законодательные инициативы: "проект", "предложение", "рассмотрение", "внесен"
    Новые законы: "принят", "подписан", "вступает в силу", "федеральный закон №"
    Поправки: "изменения", "внесены в", "дополнен", существующий закон
    Судебная практика: "суд", "решение", "постановление", "разъяснение"

    3. АЛЬТЕРНАТИВНЫЕ КАТЕГОРИИ:
    Могло ли сообщение относиться к другой категории?
    Какие признаки за это говорят?

    ОТВЕТ в формате:
    Логическая оценка: [логично/нелогично/спорно]
    Альтернатива: [другая возможная категория или "нет"]
    Уверенность: [1-5]
    """
        
        try:
            response = self.llm_model.generate(consistency_prompt, max_tokens=300, temperature=0.2)
            return self._parse_review_response(response, "consistency")
        except Exception as e:
            logger.error(f"Ошибка в анализе консистентности: {str(e)}")
            return {"status": "error", "recommendation": "подтвердить"}

    def _perform_context_review(self, message_text, channel, current_category):
        """ЭТАП 3: Контекстный анализ"""
        
        context_prompt = f"""
    Ты - эксперт по контекстному анализу и медиа-источникам.

    ЗАДАЧА: Учесть контекст источника при проверке категоризации.

    СООБЩЕНИЕ: {message_text}
    ИСТОЧНИК: {channel}
    ТЕКУЩАЯ КАТЕГОРИЯ: {current_category}

    КОНТЕКСТНЫЙ АНАЛИЗ:

    1. АНАЛИЗ ИСТОЧНИКА:
    @dumainfo → часто законодательные инициативы и принятые законы
    @sovfedinfo → федеральное законодательство, одобрения СФ
    @vsrf_ru → судебная практика, разъяснения ВС
    @kremlininfo → подписанные президентом законы
    @governmentru → правительственные решения

    2. ТИПИЧНОСТЬ ДЛЯ ИСТОЧНИКА:
    Типично ли такое сообщение для данного канала?
    Соответствует ли категория специализации источника?

    3. ВРЕМЕННЫЕ МАРКЕРЫ:
    Есть ли указания на время (прошлое/настоящее/будущее)?
    Как это влияет на категорию?

    ОТВЕТ в формате:
    Контекст: [соответствует/не соответствует источнику]
    Типичность: [типично/нетипично для канала]
    Временной аспект: [актуальное/историческое/планируемое]
    """
        
        try:
            response = self.llm_model.generate(context_prompt, max_tokens=250, temperature=0.2)
            return self._parse_review_response(response, "context")
        except Exception as e:
            logger.error(f"Ошибка в контекстном анализе: {str(e)}")
            return {"status": "error", "recommendation": "подтвердить"}

    def _synthesize_multi_perspective_decision(self, message_text, original_category, 
                                            legal_analysis, consistency_analysis, context_analysis):
        """ЭТАП 4: Синтез всех анализов и принятие решения"""
        
        synthesis_prompt = f"""
    Ты - старший эксперт-аналитик, принимающий финальное решение.

    СООБЩЕНИЕ: {message_text}
    ТЕКУЩАЯ КАТЕГОРИЯ: {original_category}

    РЕЗУЛЬТАТЫ ЭКСПЕРТИЗ:
    1. Правовая экспертиза: {legal_analysis}
    2. Логическая консистентность: {consistency_analysis}  
    3. Контекстный анализ: {context_analysis}

    ПРИНЯТИЕ РЕШЕНИЯ:

    Проанализируй все три экспертизы и прими взвешенное решение:

    1. Если 2+ экспертизы рекомендуют изменение → ИЗМЕНИТЬ
    2. Если 2+ экспертизы подтверждают категорию → ПОДТВЕРДИТЬ
    3. Если мнения разделились → учесть уверенность экспертиз

    КАТЕГОРИИ НА ВЫБОР:
    - законодательные инициативы
    - новые законы  
    - поправки к законам
    - новая судебная практика
    - другое

    ФИНАЛЬНОЕ РЕШЕНИЕ в формате:
    Решение: [подтвердить/изменить]
    Новая категория: [если изменить - укажи какую]
    Уверенность: [1-5]
    Обоснование: [краткое объяснение решения]
    """
        
        try:
            response = self.llm_model.generate(synthesis_prompt, max_tokens=400, temperature=0.3)
            return self._parse_final_decision(response)
        except Exception as e:
            logger.error(f"Ошибка в синтезе решения: {str(e)}")
            return {
                "action": "подтвердить",
                "category": original_category,
                "confidence": 3,
                "reasoning": f"Ошибка анализа: {str(e)}"
            }

    def _parse_review_response(self, response, review_type):
        """Парсинг ответов промежуточных экспертиз"""
        result = {
            "response": response,
            "recommendation": "подтвердить",  # по умолчанию
            "confidence": 3
        }
        
        response_lower = response.lower()
        
        # Определяем рекомендацию на основе ключевых слов
        if any(word in response_lower for word in ["неправильно", "нелогично", "не соответствует", "изменить"]):
            result["recommendation"] = "изменить"
        elif any(word in response_lower for word in ["правильно", "логично", "соответствует", "подтвердить"]):
            result["recommendation"] = "подтвердить"
        elif any(word in response_lower for word in ["спорно", "неоднозначно"]):
            result["recommendation"] = "спорно"
        
        # Ищем уверенность
        import re
        confidence_match = re.search(r'уверенность[:\s]*(\d+)', response_lower)
        if confidence_match:
            result["confidence"] = min(5, max(1, int(confidence_match.group(1))))
        
        return result

    def _parse_final_decision(self, response):
        """Парсинг финального решения"""
        result = {
            "action": "подтвердить",
            "category": None,
            "confidence": 3,
            "reasoning": ""
        }
        
        lines = response.strip().split('\n')
        
        for line in lines:
            line_clean = line.strip().lower()
            
            if line_clean.startswith("решение:"):
                if "изменить" in line_clean:
                    result["action"] = "изменить"
                else:
                    result["action"] = "подтвердить"
            
            elif line_clean.startswith("новая категория:"):
                category_text = line.split(":", 1)[1].strip()
                # Найти подходящую категорию
                from config.settings import CATEGORIES
                for cat in CATEGORIES + ["другое"]:
                    if cat.lower() in category_text.lower():
                        result["category"] = cat
                        break
            
            elif line_clean.startswith("уверенность:"):
                import re
                conf_match = re.search(r'\d+', line)
                if conf_match:
                    result["confidence"] = min(5, max(1, int(conf_match.group())))
            
            elif line_clean.startswith("обоснование:"):
                result["reasoning"] = line.split(":", 1)[1].strip()
        
        return result

    def _apply_review_decision(self, message_id, message, original_category, decision):
        """Применение решения многоперспективного анализа"""
        
        if decision["action"] == "изменить" and decision["category"]:
            new_category = decision["category"]
            confidence = decision["confidence"]
            reasoning = decision["reasoning"]
            
            # Обновляем категорию в БД
            success = self.db_manager.update_message_category(message_id, new_category, confidence)
            
            if success:
                # Сохраняем пример для обучения
                self._save_learning_example(message.text, new_category, reasoning)
                
                logger.info(f"Многоперспективный анализ: категория сообщения {message_id} "
                        f"изменена с '{original_category}' на '{new_category}' "
                        f"с уверенностью {confidence}")
                
                return {
                    "status": "updated",
                    "original_category": original_category,
                    "new_category": new_category,
                    "confidence": confidence,
                    "reasoning": reasoning,
                    "method": "multi_perspective_analysis"
                }
            else:
                logger.error(f"Не удалось обновить категорию сообщения {message_id}")
                return {"status": "error", "message": "Ошибка обновления БД"}
        
        else:
            # Подтверждаем текущую категорию
            logger.info(f"Многоперспективный анализ: категория '{original_category}' "
                    f"для сообщения {message_id} подтверждена")
            
            return {
                "status": "confirmed",
                "category": original_category,
                "confidence": decision["confidence"],
                "reasoning": decision["reasoning"],
                "method": "multi_perspective_analysis"
            }
    
    def review_recent_categorizations(self, confidence_threshold=3, limit=30, batch_size=5, max_workers=3, start_date=None, end_date=None):
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
        limit=limit,
        start_date=start_date,
        end_date=end_date
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
            future_to_message = {executor.submit(self.review_categorization, msg.id, msg.category): msg for msg_batch in batches for msg in msg_batch}
            
            for future in concurrent.futures.as_completed(future_to_message):
                message = future_to_message[future] # Это сообщение, а не пакет
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    logger.error(f"Ошибка при обработке сообщения {message.id}: {str(e)}")
        
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
    def _log_multi_perspective_reasoning(self, message_id, message_text, original_category, 
                                   legal_analysis, consistency_analysis, context_analysis, final_decision):
        """Логирование многоперспективного reasoning критика"""
        
        logger.info("🔍 MULTI-PERSPECTIVE REASONING КРИТИКА:")
        logger.info(f"   📝 Сообщение ID: {message_id}")
        logger.info(f"   📄 Текст: {message_text[:100]}{'...' if len(message_text) > 100 else ''}")
        logger.info(f"   📂 Исходная категория: {original_category}")
        logger.info("")
        
        # ЭТАП 1: Правовая экспертиза
        logger.info("   🏛️ ЭТАП 1 - ПРАВОВАЯ ЭКСПЕРТИЗА:")
        legal_rec = legal_analysis.get('recommendation', 'нет данных')
        legal_conf = legal_analysis.get('confidence', 'нет данных')
        logger.info(f"     Рекомендация: {legal_rec}")
        logger.info(f"     Уверенность: {legal_conf}")
        
        # ЭТАП 2: Логическая консистентность
        logger.info("   🧠 ЭТАП 2 - ЛОГИЧЕСКАЯ КОНСИСТЕНТНОСТЬ:")
        consistency_rec = consistency_analysis.get('recommendation', 'нет данных')
        consistency_conf = consistency_analysis.get('confidence', 'нет данных')
        logger.info(f"     Рекомендация: {consistency_rec}")
        logger.info(f"     Уверенность: {consistency_conf}")
        
        # ЭТАП 3: Контекстный анализ
        logger.info("   🌐 ЭТАП 3 - КОНТЕКСТНЫЙ АНАЛИЗ:")
        context_rec = context_analysis.get('recommendation', 'нет данных')
        context_conf = context_analysis.get('confidence', 'нет данных')
        logger.info(f"     Рекомендация: {context_rec}")
        logger.info(f"     Уверенность: {context_conf}")
        
        # ФИНАЛЬНОЕ РЕШЕНИЕ
        logger.info("   ⚖️ ФИНАЛЬНОЕ РЕШЕНИЕ:")
        action = final_decision.get('action', 'нет данных')
        new_category = final_decision.get('category', original_category)
        final_conf = final_decision.get('confidence', 'нет данных')
        reasoning = final_decision.get('reasoning', 'нет обоснования')
        
        logger.info(f"     Действие: {action}")
        if action == "изменить":
            logger.info(f"     Новая категория: {new_category}")
        logger.info(f"     Финальная уверенность: {final_conf}")
        logger.info(f"     Обоснование: {reasoning}")
        
        logger.info("   " + "═" * 60)
    