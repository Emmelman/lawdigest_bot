# agents/collaborative_crew.py - ИСПОЛЬЗУЕМ СУЩЕСТВУЮЩИЕ АГЕНТЫ
"""
Настоящая система коллаборации агентов через CrewAI
Использует СУЩЕСТВУЮЩИЕ self.agent из analyzer.py, critic.py, digester.py
"""

import logging
from datetime import datetime
from typing import Dict, List, Any
from enum import Enum

from crewai import Task, Crew, Process

logger = logging.getLogger(__name__)

class TrueCrewAICollaboration:
    """
    Настоящая коллаборация агентов через CrewAI
    Использует существующие self.agent из агентов системы
    """
    
    def __init__(self, agent_registry):
        """
        Инициализация системы коллаборации
        
        Args:
            agent_registry: Реестр агентов системы
        """
        self.agent_registry = agent_registry
        self.collaboration_history = []
        
        # Получаем СУЩЕСТВУЮЩИХ CrewAI агентов из наших классов
        self._get_existing_crewai_agents()
        
        logger.info("🤝 CrewAI коллаборация с СУЩЕСТВУЮЩИМИ агентами инициализирована")
    
    def _get_existing_crewai_agents(self):
        """Получаем существующие CrewAI агенты из наших классов"""
        
        try:
            # Получаем наших агентов из реестра
            analyzer_instance = self.agent_registry.get_agent("analyzer")
            critic_instance = self.agent_registry.get_agent("critic") 
            digester_instance = self.agent_registry.get_agent("digester")
            
            # Используем ИХ self.agent (CrewAI агенты)
            self.crewai_analyzer = analyzer_instance.agent
            self.crewai_critic = critic_instance.agent
            self.crewai_digester = digester_instance.agent
            
            # Получаем LLM из анализатора
            self.llm_model = analyzer_instance.llm_model
            
            logger.info("✅ Получены существующие CrewAI агенты:")
            logger.info(f"   🧠 Анализатор: {self.crewai_analyzer.role}")
            logger.info(f"   🔍 Критик: {self.crewai_critic.role}")
            logger.info(f"   📋 Дайджестер: {self.crewai_digester.role}")
            
        except Exception as e:
            logger.error(f"Ошибка получения существующих агентов: {str(e)}")
            self.crewai_analyzer = None
            self.crewai_critic = None  
            self.crewai_digester = None
            self.llm_model = None
    
    async def collaborate_on_difficult_categorization(self, message_id: int, message_text: str, 
                                                    initial_category: str, confidence: float) -> Dict[str, Any]:
        """
        CrewAI коллаборация для сложной категоризации с СУЩЕСТВУЮЩИМИ агентами
        """
        logger.info(f"🤝 CREWAI КОЛЛАБОРАЦИЯ: Сложная категоризация сообщения {message_id}")
        logger.info(f"   📝 Текст: {message_text[:100]}...")
        logger.info(f"   📂 Первоначальная категория: {initial_category} (уверенность: {confidence})")
        
        if not all([self.crewai_analyzer, self.crewai_critic]):
            logger.error("❌ Не все агенты доступны для коллаборации")
            return {
                "status": "error", 
                "final_category": initial_category,
                "final_confidence": confidence,
                "reasoning": "Агенты недоступны"
            }
        
        try:
            # ЗАДАЧА 1: Углубленный анализ СУЩЕСТВУЮЩИМ анализатором
            deep_analysis_task = Task(
                description=f"""
                Проанализируй этот сложный правовой текст и определи наиболее точную категорию.
                
                ТЕКСТ СООБЩЕНИЯ:
                {message_text}
                
                ПЕРВОНАЧАЛЬНАЯ КАТЕГОРИЯ: {initial_category} (уверенность: {confidence})
                
                ДОСТУПНЫЕ КАТЕГОРИИ:
                1. законодательные инициативы - проекты, предложения, рассмотрение в Госдуме
                2. новые законы - принятые и подписанные законы, вступающие в силу
                3. поправки к законам - изменения в существующие законы
                4. новая судебная практика - решения, постановления судов
                5. другое - не относится к правовым вопросам
                
                Проведи детальный анализ:
                1. Извлеки ключевые правовые понятия
                2. Определи стадию правового процесса  
                3. Найди признаки каждой категории
                4. Выбери наиболее точную категорию
                5. Оцени уверенность от 1 до 5
                
                ФОРМАТ ОТВЕТА:
                Категория: [точная категория]
                Уверенность: [1-5]
                Ключевые признаки: [что указывает на категорию]
                Обоснование: [подробное объяснение]
                """,
                agent=self.crewai_analyzer,  # ИСПОЛЬЗУЕМ СУЩЕСТВУЮЩЕГО!
                expected_output="Детальный анализ с категорией, уверенностью и обоснованием"
            )
            
            # ЗАДАЧА 2: Экспертная проверка СУЩЕСТВУЮЩИМ критиком
            expert_review_task = Task(
                description=f"""
                Проведи экспертную проверку категоризации этого правового сообщения.
                
                ИСХОДНЫЕ ДАННЫЕ:
                - Текст: {message_text}
                - Первоначальная категория: {initial_category}
                - Результат углубленного анализа будет предоставлен
                
                Выполни многоперспективный анализ:
                1. Правовая экспертиза: соответствие категории правовой природе
                2. Логическая консистентность: нет ли противоречий
                3. Контекстный анализ: учет источника и временных маркеров
                
                Дай окончательную экспертную оценку.
                
                ФОРМАТ ОТВЕТА:
                Экспертная категория: [финальная категория]
                Экспертная уверенность: [1-5]
                Правовое обоснование: [детальное объяснение]
                Рекомендации: [предложения]
                """,
                agent=self.crewai_critic,  # ИСПОЛЬЗУЕМ СУЩЕСТВУЮЩЕГО!
                expected_output="Экспертная оценка с правовым обоснованием",
                context=[deep_analysis_task]
            )
            
            # СОЗДАЕМ CREW С СУЩЕСТВУЮЩИМИ АГЕНТАМИ
            categorization_crew = Crew(
                agents=[self.crewai_analyzer, self.crewai_critic],
                tasks=[deep_analysis_task, expert_review_task],
                process=Process.sequential,
                verbose=True
            )
            
            logger.info("🚀 Запуск CrewAI с существующими агентами...")
            
            # ВЫПОЛНЯЕМ через CrewAI, но с нашей LLM
            result = await self._execute_crew_with_existing_agents(categorization_crew)
            
            # Парсим результат
            collaboration_result = self._parse_categorization_result(
                result, message_text, initial_category, confidence
            )
            
            self._log_crewai_collaboration("difficult_categorization", collaboration_result)
            
            return collaboration_result
            
        except Exception as e:
            logger.error(f"Ошибка при CrewAI коллаборации с существующими агентами: {str(e)}")
            return {
                "status": "error",
                "final_category": initial_category,
                "final_confidence": confidence,
                "reasoning": f"Ошибка коллаборации: {str(e)}",
                "method": "crewai_existing_agents_error"
            }
    
    async def collaborate_on_quality_assurance(self, digest_content: Dict[str, str], 
                                             digest_type: str, categories_data: Dict) -> Dict[str, Any]:
        """
        CrewAI проверка качества дайджеста с СУЩЕСТВУЮЩИМИ агентами
        """
        logger.info(f"🤝 CREWAI КОЛЛАБОРАЦИЯ: Проверка качества дайджеста ({digest_type})")
        
        if not all([self.crewai_digester, self.crewai_critic]):
            logger.error("❌ Не все агенты доступны для проверки качества")
            return {"status": "error", "overall_score": 3.0}
        
        try:
            # ЗАДАЧА 1: Анализ качества СУЩЕСТВУЮЩИМ дайджестером  
            content_analysis_task = Task(
                description=f"""
                Проанализируй качество дайджеста с точки зрения контентной стратегии.
                
                ТИП ДАЙДЖЕСТА: {digest_type}
                СОДЕРЖИМОЕ: {str(digest_content)[:500]}...
                
                Оцени:
                1. Структурную организацию контента
                2. Соответствие типу дайджеста 
                3. Логичность порядка категорий
                4. Качество стратегического планирования
                5. Читательскую ценность
                
                ФОРМАТ ОТВЕТА:
                Оценка структуры: [1-5]
                Оценка соответствия: [1-5]
                Оценка контента: [1-5] 
                Стратегическая оценка: [1-5]
                Рекомендации: [предложения по улучшению]
                """,
                agent=self.crewai_digester,  # ИСПОЛЬЗУЕМ СУЩЕСТВУЮЩЕГО!
                expected_output="Детальная оценка качества дайджеста"
            )
            
            # ЗАДАЧА 2: Экспертная проверка СУЩЕСТВУЮЩИМ критиком
            expert_quality_check = Task(
                description=f"""
                Проведи экспертную проверку качества правового дайджеста.
                
                Проверь:
                1. Фактическую точность правовой информации
                2. Соответствие юридической терминологии
                3. Полноту освещения важных вопросов
                4. Стилистическое соответствие
                5. Общее качество и профессионализм
                
                ФОРМАТ ОТВЕТА:
                Правовая точность: [1-5]
                Полнота освещения: [1-5]
                Ясность изложения: [1-5]
                Профессионализм: [1-5]
                Общая оценка: [1-5]
                Критические замечания: [что требует исправления]
                """,
                agent=self.crewai_critic,  # ИСПОЛЬЗУЕМ СУЩЕСТВУЮЩЕГО!
                expected_output="Экспертная оценка качества",
                context=[content_analysis_task]
            )
            
            # СОЗДАЕМ CREW ДЛЯ ПРОВЕРКИ КАЧЕСТВА
            quality_crew = Crew(
                agents=[self.crewai_digester, self.crewai_critic],
                tasks=[content_analysis_task, expert_quality_check],
                process=Process.sequential,
                verbose=True
            )
            
            logger.info("🚀 Запуск CrewAI проверки качества с существующими агентами...")
            
            # ВЫПОЛНЯЕМ
            result = await self._execute_crew_with_existing_agents(quality_crew)
            
            # Парсим результат
            quality_result = self._parse_quality_result(result, digest_type)
            
            self._log_crewai_collaboration("quality_assurance", quality_result)
            
            return quality_result
            
        except Exception as e:
            logger.error(f"Ошибка при CrewAI проверке качества: {str(e)}")
            return {
                "status": "error",
                "overall_score": 3.0,
                "method": "crewai_existing_agents_error",
                "error": str(e)
            }
    
    async def _execute_crew_with_existing_agents(self, crew: Crew) -> str:
        """
        Выполнение Crew с существующими агентами и нашей локальной LLM
        """
        logger.info("🔄 Выполнение CrewAI с существующими агентами и локальной LLM...")
        
        try:
            # Получаем задачи
            tasks = crew.tasks
            results = []
            
            for i, task in enumerate(tasks):
                agent_role = task.agent.role if hasattr(task.agent, 'role') else 'Unknown'
                logger.info(f"📋 Выполнение задачи {i+1}/{len(tasks)}: {agent_role}")
                
                # Строим контекст из предыдущих результатов
                context = ""
                if hasattr(task, 'context') and task.context:
                    for ctx_task in task.context:
                        task_index = tasks.index(ctx_task)
                        if task_index < len(results):
                            context += f"\nКонтекст от предыдущей задачи:\n{results[task_index]}\n"
                
                # Формируем полный промпт для нашей LLM
                full_prompt = f"""
                РОЛЬ: {getattr(task.agent, 'role', 'Специалист')}
                ЦЕЛЬ: {getattr(task.agent, 'goal', 'Выполнить задачу качественно')}
                ПРЕДЫСТОРИЯ: {getattr(task.agent, 'backstory', 'Опытный специалист')}
                
                {context}
                
                ЗАДАЧА:
                {task.description}
                
                ОЖИДАЕМЫЙ РЕЗУЛЬТАТ: {task.expected_output}
                
                Выполни задачу в соответствии с твоей ролью и целью. Будь конкретен и структурирован.
                """
                
                # Используем нашу локальную LLM
                result = self.llm_model.generate(
                    full_prompt,
                    max_tokens=800,
                    temperature=0.3
                )
                
                results.append(result)
                logger.info(f"✅ Задача {i+1} выполнена: {len(result)} символов")
            
            # Объединяем результаты
            final_result = "\n\n=== РЕЗУЛЬТАТ КОЛЛАБОРАЦИИ ===\n\n".join(results)
            
            logger.info(f"🎉 CrewAI коллаборация с существующими агентами завершена")
            return final_result
            
        except Exception as e:
            logger.error(f"Ошибка выполнения CrewAI с существующими агентами: {str(e)}")
            raise
    
    def _parse_categorization_result(self, result: str, message_text: str, 
                                   initial_category: str, confidence: float) -> Dict[str, Any]:
        """Парсинг результата коллаборации по категоризации"""
        
        lines = result.split('\n')
        
        final_category = initial_category
        final_confidence = confidence
        reasoning = ""
        
        # Парсим результат
        for line in lines:
            line_clean = line.strip()
            
            # Ищем категорию
            if line_clean.lower().startswith('категория:') or line_clean.lower().startswith('экспертная категория:'):
                category_text = line_clean.split(':', 1)[1].strip().lower()
                categories = ['законодательные инициативы', 'новые законы', 'поправки к законам', 
                             'новая судебная практика', 'другое']
                for cat in categories:
                    if cat in category_text:
                        final_category = cat
                        break
            
            # Ищем уверенность
            elif line_clean.lower().startswith('уверенность:') or line_clean.lower().startswith('экспертная уверенность:'):
                try:
                    import re
                    conf_match = re.search(r'\d+', line_clean)
                    if conf_match:
                        final_confidence = float(conf_match.group())
                        final_confidence = max(1.0, min(5.0, final_confidence))
                except:
                    pass
            
            # Ищем обоснование
            elif line_clean.lower().startswith('обоснование:') or line_clean.lower().startswith('правовое обоснование:'):
                reasoning = line_clean.split(':', 1)[1].strip()
        
        # Метрики
        category_changed = final_category != initial_category
        confidence_improved = final_confidence > confidence
        
        return {
            "status": "success",
            "final_category": final_category,
            "final_confidence": final_confidence,
            "category_changed": category_changed,
            "confidence_improved": confidence_improved,
            "reasoning": reasoning or f"CrewAI коллаборация с существующими агентами",
            "method": "crewai_existing_agents",
            "raw_result": result[:300] + "..." if len(result) > 300 else result
        }
    
    def _parse_quality_result(self, result: str, digest_type: str) -> Dict[str, Any]:
        """Парсинг результата проверки качества"""
        
        scores = {"structure": 3.0, "content": 3.0, "accuracy": 3.0, "completeness": 3.0, "overall": 3.0}
        recommendations = []
        
        lines = result.split('\n')
        for line in lines:
            line_clean = line.strip()
            
            # Ищем оценки
            import re
            if 'оценка' in line_clean.lower():
                score_match = re.search(r'(\d+(?:\.\d+)?)', line_clean)
                if score_match:
                    score = float(score_match.group())
                    if 'структур' in line_clean.lower():
                        scores["structure"] = score
                    elif 'контент' in line_clean.lower():
                        scores["content"] = score
                    elif 'точность' in line_clean.lower():
                        scores["accuracy"] = score
                    elif 'полнота' in line_clean.lower():
                        scores["completeness"] = score
                    elif 'общая' in line_clean.lower():
                        scores["overall"] = score
            
            # Ищем рекомендации
            if 'рекомендац' in line_clean.lower() and ':' in line_clean:
                rec = line_clean.split(':', 1)[1].strip()
                if rec:
                    recommendations.append(rec)
        
        # Рассчитываем общую оценку
        if scores["overall"] == 3.0:
            scores["overall"] = sum(scores[k] for k in ["structure", "content", "accuracy", "completeness"]) / 4
        
        return {
            "status": "success",
            "overall_score": scores["overall"],
            "component_scores": scores,
            "recommendations": recommendations,
            "method": "crewai_existing_agents_quality",
            "digest_type": digest_type
        }
    
    def _log_crewai_collaboration(self, scenario: str, result: Dict[str, Any]):
        """Логирование результата CrewAI коллаборации"""
        logger.info(f"🤝 CREWAI КОЛЛАБОРАЦИЯ С СУЩЕСТВУЮЩИМИ АГЕНТАМИ ЗАВЕРШЕНА ({scenario}):")
        logger.info(f"   📊 Статус: {result.get('status', 'unknown')}")
        logger.info(f"   🔧 Метод: {result.get('method', 'unknown')}")
        
        if scenario == "difficult_categorization":
            logger.info(f"   🎯 Финальная категория: {result.get('final_category', 'unknown')}")
            logger.info(f"   📈 Уверенность: {result.get('final_confidence', 0):.1f}/5")
            logger.info(f"   🔄 Изменения: категория={'Да' if result.get('category_changed') else 'Нет'}, "
                       f"уверенность={'Да' if result.get('confidence_improved') else 'Нет'}")
        
        elif scenario == "quality_assurance":
            logger.info(f"   📊 Общая оценка: {result.get('overall_score', 0):.1f}/5")
            logger.info(f"   📋 Рекомендаций: {len(result.get('recommendations', []))}")
        
        logger.info("   " + "=" * 50)
        
        # Сохраняем в историю
        self.collaboration_history.append({
            "timestamp": datetime.now(),
            "scenario": scenario,
            "result": result,
            "method": "crewai_existing_agents"
        })