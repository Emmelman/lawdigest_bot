"""
Orchestrator Agents - оригинальный и intelligent оркестраторы

Содержит:
Intelligent Orchestrator Agent - планировщик с использованием CrewAI для принятия решений
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum
from dataclasses import dataclass
from crewai import Agent, Task, Crew

logger = logging.getLogger(__name__)

class TaskType(Enum):
    """Типы задач в системе"""
    DATA_COLLECTION = "data_collection"
    MESSAGE_ANALYSIS = "message_analysis"
    CATEGORIZATION_REVIEW = "categorization_review"
    DIGEST_CREATION = "digest_creation"
    DIGEST_UPDATE = "digest_update"

class TaskPriority(Enum):
    """Приоритеты задач"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

class TaskStatus(Enum):
    """Статусы выполнения задач"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class TaskRequest:
    """Запрос на выполнение задачи"""
    task_type: TaskType
    priority: TaskPriority = TaskPriority.NORMAL
    params: Dict[str, Any] = None
    dependencies: List[str] = None
    timeout: int = 300
    retry_count: int = 3
    created_at: datetime = None
    reasoning: str = ""  # Объяснение, почему задача нужна
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.params is None:
            self.params = {}
        if self.dependencies is None:
            self.dependencies = []

@dataclass
class TaskResult:
    """Результат выполнения задачи"""
    task_id: str
    task_type: TaskType
    status: TaskStatus
    result: Any = None
    error: str = None
    execution_time: float = 0.0
    completed_at: datetime = None

class OrchestratorAgent:
    """
    Оригинальный агент-оркестратор для обратной совместимости
    """
    
    def __init__(self, db_manager, agent_registry=None):
        """Инициализация оркестратора"""
        self.db_manager = db_manager
        self.agent_registry = agent_registry
        self.task_queue = []
        self.active_tasks = {}
        self.completed_tasks = []
        self.context = {}
        
        # Создаем CrewAI агента
        self.agent = Agent(
            name="Orchestrator",
            role="Главный координатор и планировщик",
            goal="Эффективно планировать и координировать работу всех агентов системы",
            backstory="Я являюсь главным координатором системы, который анализирует текущую ситуацию, "
                     "планирует оптимальную последовательность действий и управляет выполнением задач.",
            verbose=True
        )
        
        # Стратегии выполнения для разных сценариев
        self.execution_strategies = {
            "daily_workflow": self._create_daily_workflow_strategy,
            "urgent_update": self._create_urgent_update_strategy,
            "full_analysis": self._create_full_analysis_strategy,
            "digest_only": self._create_digest_only_strategy
        }
        
        logger.info("Orchestrator Agent инициализирован")
    
    async def plan_and_execute(self, scenario: str = "daily_workflow", **kwargs) -> Dict[str, Any]:
        """Главный метод планирования и выполнения"""
        logger.info(f"Запуск планирования и выполнения сценария: {scenario}")
        
        try:
            # Простая логика для оригинального оркестратора
            context = await self._analyze_current_state(**kwargs)
            execution_plan = await self._create_execution_plan(scenario, context, **kwargs)
            results = await self._execute_plan(execution_plan)
            final_result = await self._analyze_results_and_decide(results, scenario)
            
            return final_result
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении сценария {scenario}: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "scenario": scenario
            }
    
    async def _analyze_current_state(self, **kwargs) -> Dict[str, Any]:
        """Анализ текущего состояния системы (упрощенный)"""
        context = {
            "timestamp": datetime.now(),
            "scenario_params": kwargs,
            "unanalyzed_count": 0,
            "low_confidence_count": 0,
            "today_digests_count": 0,
            "needs_data_collection": True
        }
        
        try:
            # Упрощенные проверки для оригинального оркестратора
            unanalyzed_messages = self.db_manager.get_unanalyzed_messages(limit=100)
            context["unanalyzed_count"] = len(unanalyzed_messages)
            
            # Проверяем дайджесты за сегодня
            today_digests = self.db_manager.find_digests_by_parameters(is_today=True)
            context["today_digests_count"] = len(today_digests)
            
        except Exception as e:
            logger.error(f"Ошибка при анализе состояния: {str(e)}")
        
        return context
    
    async def _create_execution_plan(self, scenario: str, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Создание плана выполнения (упрощенный)"""
        if scenario not in self.execution_strategies:
            logger.warning(f"Неизвестный сценарий {scenario}, используем daily_workflow")
            scenario = "daily_workflow"
        
        strategy_func = self.execution_strategies[scenario]
        return await strategy_func(context, **kwargs)
    
    async def _create_daily_workflow_strategy(self, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Стратегия для ежедневного рабочего процесса (упрощенная)"""
        tasks = []
        
        # Базовые задачи для daily_workflow
        tasks.append(TaskRequest(
            task_type=TaskType.DATA_COLLECTION,
            priority=TaskPriority.HIGH,
            params={"days_back": kwargs.get("days_back", 1),
                    "force_update": True
                    },
        reasoning="Необходим сбор свежих данных из Telegram каналов"
        ))
        
        tasks.append(TaskRequest(
            task_type=TaskType.MESSAGE_ANALYSIS,
            priority=TaskPriority.NORMAL,
            params={"limit": 100},
            dependencies=["data_collection"]
        ))
        
        tasks.append(TaskRequest(
            task_type=TaskType.DIGEST_CREATION,
            priority=TaskPriority.HIGH,
            params={"days_back": kwargs.get("days_back", 1)},
            dependencies=["message_analysis"]
        ))
        
        return tasks
    
    async def _create_urgent_update_strategy(self, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Стратегия для срочного обновления"""
        return await self._create_daily_workflow_strategy(context, **kwargs)
    
    async def _create_full_analysis_strategy(self, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Стратегия для полного анализа"""
        return await self._create_daily_workflow_strategy(context, **kwargs)
    
    async def _create_digest_only_strategy(self, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Стратегия только для создания дайджеста"""
        tasks = []
        tasks.append(TaskRequest(
            task_type=TaskType.DIGEST_CREATION,
            priority=TaskPriority.HIGH,
            params={"days_back": kwargs.get("days_back", 1)}
        ))
        return tasks
    
    async def _execute_plan(self, execution_plan: List[TaskRequest]) -> List[TaskResult]:
        """Упрощенное выполнение плана"""
        results = []
        
        for task in execution_plan:
            try:
                start_time = datetime.now()
                result = await self._execute_single_task(task)
                execution_time = (datetime.now() - start_time).total_seconds()
                
                task_result = TaskResult(
                    task_id=task.task_type.value,
                    task_type=task.task_type,
                    status=TaskStatus.COMPLETED,
                    result=result,
                    execution_time=execution_time,
                    completed_at=datetime.now()
                )
                
                results.append(task_result)
                
            except Exception as e:
                logger.error(f"Ошибка при выполнении задачи {task.task_type.value}: {str(e)}")
                
                task_result = TaskResult(
                    task_id=task.task_type.value,
                    task_type=task.task_type,
                    status=TaskStatus.FAILED,
                    error=str(e),
                    execution_time=0,
                    completed_at=datetime.now()
                )
                
                results.append(task_result)
        
        return results
    
    async def _execute_single_task(self, task: TaskRequest) -> Any:
        """Выполнение одной задачи (упрощенное)"""
        if not self.agent_registry:
            raise Exception("Agent registry не инициализирован")
        
        if task.task_type == TaskType.DATA_COLLECTION:
            collector = self.agent_registry.get_agent("data_collector")
            return await collector.collect_data(**task.params)
            
        elif task.task_type == TaskType.MESSAGE_ANALYSIS:
            analyzer = self.agent_registry.get_agent("analyzer")
            return await analyzer.analyze_messages(**task.params)
            
        elif task.task_type == TaskType.DIGEST_CREATION:
            digester = self.agent_registry.get_agent("digester")
            # Используем синхронный метод в executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: digester.create_digest(**task.params))
            
        else:
            raise Exception(f"Неизвестный тип задачи: {task.task_type}")
    
    async def _analyze_results_and_decide(self, results: List[TaskResult], scenario: str) -> Dict[str, Any]:
        """Анализ результатов выполнения (упрощенный)"""
        successful_tasks = [r for r in results if r.status == TaskStatus.COMPLETED]
        failed_tasks = [r for r in results if r.status == TaskStatus.FAILED]
        
        total_execution_time = sum(r.execution_time for r in results)
        success_rate = len(successful_tasks) / len(results) if results else 0
        
        summary = {
            "collected_messages": 0,
            "analyzed_messages": 0,
            "created_digests": []
        }
        
        metrics = {
            "total_tasks": len(results),
            "successful_tasks": len(successful_tasks),
            "failed_tasks": len(failed_tasks),
            "success_rate": success_rate,
            "total_execution_time": total_execution_time,
            "scenario": scenario
        }
        
        return {
            "status": "success" if not failed_tasks else "partial_success",
            "metrics": metrics,
            "summary": summary,
            "recommendations": []
        }
    
    def create_task(self) -> Task:
        """Создание задачи CrewAI для интеграции"""
        return Task(
            description="Спланировать и скоординировать выполнение рабочего процесса системы",
            agent=self.agent,
            expected_output="Результаты планирования и выполнения с метриками и рекомендациями"
        )

class IntelligentOrchestratorAgent:
    """
    Intelligent агент-оркестратор, использующий CrewAI для принятия решений
    """
    
    def __init__(self, db_manager, agent_registry=None):
        """Инициализация оркестратора"""
        self.db_manager = db_manager
        self.agent_registry = agent_registry
        self.task_queue = []
        self.active_tasks = {}
        self.completed_tasks = []
        self.context = {}
        
        # Инициализируем локальную LLM как остальные агенты
        from llm.gemma_model import GemmaLLM
        self.llm_model = GemmaLLM()  # Используем ту же LLM что и другие агенты
        
        # Создаем CrewAI агента с локальной LLM
        try:
            # Создаем простую функцию-wrapper для LLM
            def llm_generate(prompt):
                return self.llm_model.generate(prompt, max_tokens=1000, temperature=0.7)
            
            self.planning_agent = Agent(
                name="IntelligentPlanner",
                role="Intelligent системный планировщик и аналитик",
                goal="Анализировать текущее состояние системы и принимать оптимальный решения",
                backstory="""Я — продвинутый ИИ-планировщик, который понимает архитектуру системы 
                            обработки новостных сообщений и принимает intelligent решения.""",
                verbose=True,
                allow_delegation=False,
                llm=None  # Пока отключаем, используем свой llm_model
            )
            
            self.planning_task = Task(
                description="Анализировать систему и создать оптимальный план выполнения",
                agent=self.planning_agent,
                expected_output="JSON план выполнения с задачами и обоснованием"
            )
            
            self.planning_crew = None  # Временно отключаем Crew
            
        except Exception as e:
            logger.warning(f"Ошибка при создании CrewAI компонентов: {e}")
            self.planning_agent = None
            self.planning_crew = None
        
        logger.info("Intelligent Orchestrator Agent инициализирован")
    
    async def plan_and_execute(self, scenario: str = "daily_workflow", **kwargs) -> Dict[str, Any]:
        """
        Главный метод intelligent планирования и выполнения
        
        Args:
            scenario: Сценарий выполнения
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами выполнения
        """
        logger.info(f"Запуск intelligent планирования и выполнения сценария: {scenario}")
        
        try:
            # ДОБАВИТЬ ЭТУ ОТЛАДКУ:
            logger.info("Шаг 1: Начинаем сбор контекста")

            # Этап 1: Сбор контекста о текущем состоянии
            context = await self._gather_system_context(**kwargs)
            logger.info(f"Контекст собран: {len(context.get('unanalyzed_messages', []))} неанализированных сообщений")
            
            # ДОБАВИТЬ ЭТУ ОТЛАДКУ:
            logger.info("Шаг 2: Начинаем intelligent планирование")

            # Этап 2: Intelligent планирование через CrewAI
            execution_plan = await self._intelligent_planning(scenario, context, **kwargs)
            logger.info(f"Intelligent план создан: {len(execution_plan)} задач")
            
             # ДОБАВИТЬ ЭТУ ОТЛАДКУ:
            logger.info("Шаг 3: Начинаем выполнение плана")

            # Этап 3: Выполнение плана
            results = await self._execute_intelligent_plan(execution_plan)
            logger.info(f"Выполнение завершено: {len(results)} результатов")
            
            # ДОБАВИТЬ ЭТУ ОТЛАДКУ:
            logger.info("Шаг 4: Анализ результатов")

            # Этап 4: Анализ результатов
            final_result = await self._analyze_execution_results(results, scenario, context)
            
            return final_result
            
        except Exception as e:
            logger.error(f"Ошибка при intelligent выполнении сценария {scenario}: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "scenario": scenario
            }
    
    async def _gather_system_context(self, **kwargs) -> Dict[str, Any]:
        """Сбор полного контекста о состоянии системы"""
        context = {
            "timestamp": datetime.now(),
            "scenario_params": kwargs
        }
        
        try:
            # Получаем количество неанализированных сообщений
            unanalyzed_messages = self.db_manager.get_unanalyzed_messages(limit=1000)
            context["unanalyzed_count"] = len(unanalyzed_messages)
            context["unanalyzed_messages"] = unanalyzed_messages[:5]  # Примеры для анализа
            
            # Получаем сообщения с низкой уверенностью
            
            try:
                low_confidence_messages = self.db_manager.get_messages_with_low_confidence(confidence_threshold=2)
                context["low_confidence_count"] = len(low_confidence_messages)
            except (AttributeError, TypeError):
                # Fallback если метод не существует или параметры неправильные
                context["low_confidence_count"] = 0
            
            # Проверяем наличие дайджестов за сегодня
            today_digests = self.db_manager.find_digests_by_parameters(is_today=True)
            context["today_digests_count"] = len(today_digests)
            context["has_brief_digest"] = any(d.digest_type == "brief" for d in today_digests)
            context["has_detailed_digest"] = any(d.digest_type == "detailed" for d in today_digests)
            
            # Получаем статистику по категориям
            category_stats = {}
            if unanalyzed_messages:
                for msg in unanalyzed_messages[:20]:  # Анализируем выборку
                    cat = getattr(msg, 'category', 'неизвестно')
                    conf = getattr(msg, 'confidence', 0)
                    category_stats[cat] = category_stats.get(cat, {'count': 0, 'avg_confidence': 0})
                    category_stats[cat]['count'] += 1
                    category_stats[cat]['avg_confidence'] = (category_stats[cat]['avg_confidence'] + conf) / 2
            
            context["category_statistics"] = category_stats
            
            # Информация о временных рамках
            days_back = kwargs.get("days_back", 1)
            force_update = kwargs.get("force_update", False)
            context["days_back"] = days_back
            context["force_update"] = force_update
            
            # Оценка потребности в сборе данных
            last_collection_time = getattr(self.db_manager, '_last_collection_time', None)
            if last_collection_time:
                time_since_collection = (datetime.now() - last_collection_time).total_seconds() / 3600
                context["hours_since_last_collection"] = time_since_collection
                context["needs_data_collection"] = time_since_collection > 2 or force_update
            else:
                context["needs_data_collection"] = True
                context["hours_since_last_collection"] = 999
            
            logger.info(f"Система контекст: неанализированных={context['unanalyzed_count']}, "
                       f"низкая уверенность={context['low_confidence_count']}, "
                       f"дайджестов сегодня={context['today_digests_count']}")
            
        except Exception as e:
            logger.error(f"Ошибка при сборе контекста: {str(e)}")
            # Возвращаем базовый контекст
            context.update({
                "unanalyzed_count": 0,
                "low_confidence_count": 0,
                "today_digests_count": 0,
                "needs_data_collection": True,
                "error": str(e)
            })
        
        return context
    
    async def _intelligent_planning(self, scenario: str, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Intelligent планирование с использованием локальной LLM"""
        
        # Используем полный детальный контекст
        planning_context = f"""
        АНАЛИЗ ТЕКУЩЕЙ СИТУАЦИИ:
        
        Сценарий: {scenario}
        Временная метка: {context['timestamp']}
        
        СОСТОЯНИЕ ДАННЫХ:
        - Неанализированных сообщений: {context.get('unanalyzed_count', 0)}
        - Сообщений с низкой уверенностью: {context.get('low_confidence_count', 0)}
        - Дайджестов за сегодня: {context.get('today_digests_count', 0)}
        - Есть краткий дайджест: {context.get('has_brief_digest', False)}
        - Есть подробный дайджест: {context.get('has_detailed_digest', False)}
        
        ПАРАМЕТРЫ СБОРА:
        - Дней назад: {context.get('days_back', 1)}
        - Принудительное обновление: {context.get('force_update', False)}
        - Часов с последнего сбора: {context.get('hours_since_last_collection', 'неизвестно')}
        - Нужен сбор данных: {context.get('needs_data_collection', True)}
        
        СТАТИСТИКА КАТЕГОРИЙ:
        {context.get('category_statistics', {})}
        
        ДОСТУПНЫЕ ТИПЫ ЗАДАЧ:
        1. DATA_COLLECTION - сбор новых сообщений из Telegram каналов
        2. MESSAGE_ANALYSIS - анализ и категоризация сообщений
        3. CATEGORIZATION_REVIEW - проверка и улучшение категоризации критиком
        4. DIGEST_CREATION - создание новых дайджестов
        5. DIGEST_UPDATE - обновление существующих дайджестов
        
        ТВОЯ ЗАДАЧА:
        Проанализируй ситуацию и создай optimal план выполнения для сценария "{scenario}".
        Для каждой задачи объясни WHY она нужна и в каком ПОРЯДКЕ должна выполняться.
        
        ОСОБЕННОСТИ СЦЕНАРИЕВ:
        - daily_workflow: полный цикл (сбор → анализ → проверка → дайджест)
        - urgent_update: быстрое обновление критически важной информации
        - full_analysis: глубокий анализ с акцентом на качество
        - digest_only: только создание дайджеста из существующих данных
        
        ВАЖНЫЕ ПРАВИЛА:
        1. Если есть неанализированные сообщения → нужен MESSAGE_ANALYSIS
        2. Если много сообщений с низкой уверенностью → нужен CATEGORIZATION_REVIEW
        3. Всегда объясняй свои решения
        4. Учитывай зависимости между задачами
        5. Оптимизируй для конкретного сценария
        
        Ответь в формате: ЗАДАЧА_1: reasoning, ЗАДАЧА_2: reasoning, и т.д.
        """
        
        try:
            # Используем вашу локальную LLM для intelligent планирования
            logger.info("Начинаю intelligent планирование через локальную LLM...")
            
            response = self.llm_model.generate(
                planning_context, 
                max_tokens=800,  # Увеличиваем для подробного ответа
                temperature=0.3   # Низкая температура для логичных решений
            )
            
            logger.info(f"LLM планирование завершено. Результат: {response}")
            
            # Парсим ответ и создаем план
            tasks = await self._parse_llm_planning_result(response, context, **kwargs)
            
            return tasks
            
        except Exception as e:
            logger.error(f"Ошибка при LLM планировании: {str(e)}")
            # Fallback на проверенное планирование
            return await self._fallback_planning(scenario, context, **kwargs)
    
    async def _parse_llm_planning_result(self, llm_response: str, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Парсинг ответа LLM в план задач"""
        tasks = []
        dependencies = []
        
        # Простой анализ ответа LLM
        response_lower = llm_response.lower()
        from datetime import datetime, timedelta

        # Рассчитываем правильные даты
        days_back = kwargs.get("days_back", 1)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back-1)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # Определяем нужные задачи на основе ответа LLM и логики
        if "data_collection" in response_lower or context.get('needs_data_collection', True):
            tasks.append(TaskRequest(
                task_type=TaskType.DATA_COLLECTION,
                priority=TaskPriority.HIGH,
                params={
                    "days_back": kwargs.get("days_back", 1),
                    "force_update": True,
                    "start_date": start_date,
                    "end_date": end_date
                    },
                reasoning="LLM: Необходим сбор данных"
            ))
            dependencies.append("data_collection")
        
        if context.get('unanalyzed_count', 0) > 0 or "message_analysis" in response_lower:
            tasks.append(TaskRequest(
                task_type=TaskType.MESSAGE_ANALYSIS,
                priority=TaskPriority.NORMAL,
                params={"limit": 100},
                dependencies=dependencies.copy(),
                reasoning="LLM: Анализ неанализированных сообщений"
            ))
            dependencies.append("message_analysis")
        
        if context.get('low_confidence_count', 0) > 0 or "categorization_review" in response_lower:
            tasks.append(TaskRequest(
                task_type=TaskType.CATEGORIZATION_REVIEW,
                priority=TaskPriority.NORMAL,
                params={"confidence_threshold": 3},
                dependencies=dependencies.copy(),
                reasoning="LLM: Проверка категоризации"
            ))
            dependencies.append("categorization_review")
        
        if "digest" in response_lower:
            tasks.append(TaskRequest(
                task_type=TaskType.DIGEST_CREATION,
                priority=TaskPriority.HIGH,
                params={
                    "days_back": kwargs.get("days_back", 1)
                    },
                dependencies=dependencies.copy(),
                reasoning="LLM: Создание дайджестов"
            ))
        
        return tasks

    async def _parse_planning_result(self, planning_result: str, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Парсинг результата планирования CrewAI в список задач"""
        tasks = []
        
        # Простой анализ текста результата планирования
        planning_text = str(planning_result).lower()
        
        # Определяем необходимые задачи на основе анализа
        dependencies = []
        
        # 1. Проверяем необходимость сбора данных
        if ("data_collection" in planning_text or 
            "сбор данных" in planning_text or 
            context.get('needs_data_collection', True) or
            context.get('unanalyzed_count', 0) == 0):
            
            from datetime import datetime, timedelta

            # Рассчитываем правильные даты
            days_back = kwargs.get("days_back", 1)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back-1)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

            tasks.append(TaskRequest(
                task_type=TaskType.DATA_COLLECTION,
                priority=TaskPriority.HIGH,
                params={
                    "days_back": kwargs.get("days_back", 1),
                    "force_update": True,
                    "start_date": start_date, 
                    "end_date": end_date 
                },
                reasoning="Необходим сбор свежих данных из Telegram каналов"
            ))
            dependencies.append("data_collection")
        
        # 2. Проверяем необходимость анализа
        if (context.get('unanalyzed_count', 0) > 0 or 
            "message_analysis" in planning_text or
            "анализ сообщений" in planning_text):
            
            tasks.append(TaskRequest(
                task_type=TaskType.MESSAGE_ANALYSIS,
                priority=TaskPriority.NORMAL,
                params={
                    "limit": min(context.get('unanalyzed_count', 100), 200),
                    "batch_size": 10
                },
                dependencies=dependencies.copy(),
                reasoning=f"Нужно проанализировать {context.get('unanalyzed_count', 0)} неанализированных сообщений"
            ))
            dependencies.append("message_analysis")
        
        # 3. Проверяем необходимость проверки категоризации
        if (context.get('low_confidence_count', 0) > 0 or
            "categorization_review" in planning_text or
            "проверка категоризации" in planning_text or
            "критик" in planning_text):
            
            tasks.append(TaskRequest(
                task_type=TaskType.CATEGORIZATION_REVIEW,
                priority=TaskPriority.NORMAL,
                params={
                    "confidence_threshold": 3,
                    "limit": min(context.get('low_confidence_count', 50), 100)
                },
                dependencies=dependencies.copy(),
                reasoning=f"Нужно улучшить категоризацию {context.get('low_confidence_count', 0)} сообщений с низкой уверенностью"
            ))
            dependencies.append("categorization_review")
        
        # 4. Определяем задачи по дайджестам
        today = datetime.now().date()
        has_digests = context.get('today_digests_count', 0) > 0
        
        if has_digests and ("digest_update" in planning_text or "обновление дайджеста" in planning_text):
            tasks.append(TaskRequest(
                task_type=TaskType.DIGEST_UPDATE,
                priority=TaskPriority.HIGH,
                params={
                    "date": today,
                    "digest_type": "both"
                },
                dependencies=dependencies.copy(),
                reasoning="Обновляем существующие дайджесты новыми проанализированными данными"
            ))
        else:
            tasks.append(TaskRequest(
                task_type=TaskType.DIGEST_CREATION,
                priority=TaskPriority.HIGH,
                params={
                    "date": today,
                    "days_back": kwargs.get("days_back", 1),
                    "digest_type": "both"
                },
                dependencies=dependencies.copy(),
                reasoning="Создаем новые дайджесты на основе собранных и проанализированных данных"
            ))
        
        logger.info(f"Intelligent план создан: {len(tasks)} задач")
        for i, task in enumerate(tasks, 1):
            deps_str = f" (зависит от: {', '.join(task.dependencies)})" if task.dependencies else ""
            logger.info(f"  {i}. {task.task_type.value}{deps_str} - {task.reasoning}")
        
        return tasks
    
    async def _fallback_planning(self, scenario: str, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Fallback планирование без CrewAI"""
        logger.info("Используем fallback планирование")
        
        tasks = []
        dependencies = []
        
        # Базовая логика для daily_workflow
        if scenario == "daily_workflow":
            # Сбор данных
            tasks.append(TaskRequest(
                task_type=TaskType.DATA_COLLECTION,
                priority=TaskPriority.HIGH,
                params={"days_back": kwargs.get("days_back", 1),
                        "force_update": True
                        },
                reasoning="Fallback: сбор данных для daily_workflow"
            ))
            dependencies.append("data_collection")
            
            # Анализ (всегда, так как есть проблемы с confidence)
            tasks.append(TaskRequest(
                task_type=TaskType.MESSAGE_ANALYSIS,
                priority=TaskPriority.NORMAL,
                params={"limit": 100, "batch_size": 10},
                dependencies=dependencies.copy(),
                reasoning="Fallback: анализ сообщений (fix для нулевой уверенности)"
            ))
            dependencies.append("message_analysis")
            
            # Критик (всегда для улучшения качества)
            tasks.append(TaskRequest(
                task_type=TaskType.CATEGORIZATION_REVIEW,
                priority=TaskPriority.NORMAL,
                params={"confidence_threshold": 3, "limit": 50},
                dependencies=dependencies.copy(),
                reasoning="Fallback: проверка категоризации критиком"
            ))
            dependencies.append("categorization_review")
            
            # Дайджест
            tasks.append(TaskRequest(
                task_type=TaskType.DIGEST_CREATION,
                priority=TaskPriority.HIGH,
                params={
                    "date": datetime.now().date(),
                    "days_back": kwargs.get("days_back", 1),
                    "digest_type": "both"
                },
                dependencies=dependencies.copy(),
                reasoning="Fallback: создание дайджестов"
            ))
        
        return tasks
    
    async def _execute_intelligent_plan(self, execution_plan: List[TaskRequest]) -> List[TaskResult]:
        """Выполнение intelligent плана с подробным логированием"""
        results = []
        completed_tasks = set()
        failed_tasks = set()
        
        logger.info(f"Начинаю выполнение intelligent плана из {len(execution_plan)} задач")
        
        # Выводим план выполнения
        for i, task in enumerate(execution_plan, 1):
            deps_str = f" (зависит от: {', '.join(task.dependencies)})" if task.dependencies else ""
            logger.info(f"  План {i}: {task.task_type.value}{deps_str}")
            logger.info(f"    Обоснование: {task.reasoning}")
        
        # Выполняем задачи с учетом зависимостей
        while len(completed_tasks) + len(failed_tasks) < len(execution_plan):
            ready_tasks = []
            
            for task in execution_plan:
                task_id = task.task_type.value
                
                if task_id in completed_tasks or task_id in failed_tasks:
                    continue
                
                dependencies_met = all(dep in completed_tasks for dep in task.dependencies)
                if dependencies_met:
                    ready_tasks.append(task)
            
            if not ready_tasks:
                remaining = [t.task_type.value for t in execution_plan 
                           if t.task_type.value not in completed_tasks and t.task_type.value not in failed_tasks]
                logger.error(f"Deadlock: нет готовых задач. Оставшиеся: {remaining}")
                break
            
            # Выполняем готовые задачи
            for task in ready_tasks:
                logger.info(f"Выполнение задачи: {task.task_type.value}")
                logger.info(f"  Обоснование: {task.reasoning}")
                
                try:
                    start_time = datetime.now()
                    result = await self._execute_single_task(task)
                    execution_time = (datetime.now() - start_time).total_seconds()
                    
                    task_result = TaskResult(
                        task_id=task.task_type.value,
                        task_type=task.task_type,
                        status=TaskStatus.COMPLETED,
                        result=result,
                        execution_time=execution_time,
                        completed_at=datetime.now()
                    )
                    
                    results.append(task_result)
                    completed_tasks.add(task.task_type.value)
                    
                    logger.info(f"Задача {task.task_type.value} выполнена успешно за {execution_time:.2f}с")
                    
                except Exception as e:
                    logger.error(f"Ошибка при выполнении задачи {task.task_type.value}: {str(e)}")
                    
                    task_result = TaskResult(
                        task_id=task.task_type.value,
                        task_type=task.task_type,
                        status=TaskStatus.FAILED,
                        error=str(e),
                        execution_time=0,
                        completed_at=datetime.now()
                    )
                    
                    results.append(task_result)
                    failed_tasks.add(task.task_type.value)
        
        logger.info(f"Выполнение плана завершено. Успешно: {len(completed_tasks)}, с ошибками: {len(failed_tasks)}")
        return results
    
    async def _execute_single_task(self, task: TaskRequest) -> Any:
        """Выполнение одной задачи"""
        if not self.agent_registry:
            raise Exception("Agent registry не инициализирован")
        
        if task.task_type == TaskType.DATA_COLLECTION:
            collector = self.agent_registry.get_agent("data_collector")
            return await collector.collect_data(**task.params)
            
        elif task.task_type == TaskType.MESSAGE_ANALYSIS:
            analyzer = self.agent_registry.get_agent("analyzer")
            return analyzer.analyze_messages(**task.params)
            
        elif task.task_type == TaskType.CATEGORIZATION_REVIEW:
            critic = self.agent_registry.get_agent("critic")
            return critic.review_recent_categorizations(**task.params)
            
        elif task.task_type == TaskType.DIGEST_CREATION:
            digester = self.agent_registry.get_agent("digester")
            return await self._execute_digest_creation_async(digester, task.params)
            
        elif task.task_type == TaskType.DIGEST_UPDATE:
            digester = self.agent_registry.get_agent("digester")
            # DigesterAgent не имеет отдельного метода update_digest, используем create_digest с update_existing=True
            task_params = task.params.copy()
            task_params['update_existing'] = True
            return await self._execute_digest_creation_async(digester, task_params)
            
        else:
            raise Exception(f"Неизвестный тип задачи: {task.task_type}")
            
    
    async def _analyze_execution_results(self, results: List[TaskResult], scenario: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Анализ результатов выполнения"""
        successful_tasks = [r for r in results if r.status == TaskStatus.COMPLETED]
        failed_tasks = [r for r in results if r.status == TaskStatus.FAILED]
        
        total_execution_time = sum(r.execution_time for r in results)
        success_rate = len(successful_tasks) / len(results) if results else 0
        
        # Собираем детальную сводку
        summary = {
            "collected_messages": 0,
            "analyzed_messages": 0,
            "reviewed_messages": 0,
            "created_digests": [],
            "updated_digests": []
        }
        
        for result in successful_tasks:
            if result.task_type == TaskType.DATA_COLLECTION and result.result:
                summary["collected_messages"] = result.result.get("total_new_messages", 0)
            elif result.task_type == TaskType.MESSAGE_ANALYSIS and result.result:
                summary["analyzed_messages"] = result.result.get("analyzed_count", 0)
            elif result.task_type == TaskType.CATEGORIZATION_REVIEW and result.result:
                summary["reviewed_messages"] = result.result.get("reviewed_count", 0)
            elif result.task_type == TaskType.DIGEST_CREATION and result.result:
                if isinstance(result.result, dict) and "brief_id" in result.result:
                    summary["created_digests"].append(f"brief (ID: {result.result['brief_id']})")
                if isinstance(result.result, dict) and "detailed_id" in result.result:
                    summary["created_digests"].append(f"detailed (ID: {result.result['detailed_id']})")
            elif result.task_type == TaskType.DIGEST_UPDATE and result.result:
                summary["updated_digests"].append(str(result.result))
        
        # Генерируем рекомендации
        recommendations = []
        
        if failed_tasks:
            recommendations.append({
                "type": "retry_failed",
                "description": f"Повторить {len(failed_tasks)} неудачных задач: {', '.join(t.task_type.value for t in failed_tasks)}"
            })
        
        if summary["collected_messages"] > 50:
            recommendations.append({
                "type": "high_volume",
                "description": f"Собрано {summary['collected_messages']} сообщений - много активности"
            })
        
        if summary["reviewed_messages"] > 20:
            recommendations.append({
                "type": "quality_issues",
                "description": f"Критик исправил {summary['reviewed_messages']} сообщений - нужна настройка анализатора"
            })
        
        if context.get('unanalyzed_count', 0) > 0 and summary["analyzed_messages"] == 0:
            recommendations.append({
                "type": "analysis_needed",
                "description": f"Остались неанализированные сообщения ({context['unanalyzed_count']}) - требуется повторный анализ"
            })
        
        metrics = {
            "total_tasks": len(results),
            "successful_tasks": len(successful_tasks),
            "failed_tasks": len(failed_tasks),
            "success_rate": success_rate,
            "total_execution_time": total_execution_time,
            "scenario": scenario,
            "intelligent_planning": True
        }
        
        final_result = {
            "status": "success" if not failed_tasks else "partial_success",
            "metrics": metrics,
            "summary": summary,
            "recommendations": recommendations,
            "planning_context": {
                "original_unanalyzed": context.get('unanalyzed_count', 0),
                "original_low_confidence": context.get('low_confidence_count', 0),
                "original_digests_count": context.get('today_digests_count', 0)
            },
            "task_results": [
                {
                    "task": r.task_type.value,
                    "status": r.status.value,
                    "execution_time": r.execution_time,
                    "error": r.error
                }
                for r in results
            ]
        }
        
        logger.info(f"Анализ результатов завершен: {success_rate:.1%} успешности, "
                   f"{total_execution_time:.1f}с общего времени выполнения")
        
        return final_result
    async def _execute_digest_creation_async(self, agent, params: Dict[str, Any]) -> Dict[str, Any]:
        """Асинхронное выполнение создания дайджеста"""
        try:
            # create_digest - синхронный метод, выполняем в executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: agent.create_digest(
                    date=params.get("date"),
                    days_back=params.get("days_back", 1),
                    digest_type=params.get("digest_type", "both"),
                    update_existing=params.get("update_existing", False),
                    focus_category=params.get("focus_category"),
                    channels=params.get("channels"),
                    keywords=params.get("keywords"),
                    digest_id=params.get("digest_id")
                )
            )
            
            digest_info = []
            if result.get("brief_digest_id"):
                digest_info.append(f"краткий (ID: {result['brief_digest_id']})")
            if result.get("detailed_digest_id"):
                digest_info.append(f"подробный (ID: {result['detailed_digest_id']})")
            
            action = "обновлен" if params.get("update_existing") else "создан"
            logger.info(f"Дайджест {action}: {', '.join(digest_info)}")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при работе с дайджестом: {str(e)}")
            return {"status": "error", "error": str(e)}
        
    def create_task(self) -> Task:
        """Создание задачи CrewAI для интеграции"""
        return Task(
            description="Выполнить intelligent планирование и координацию рабочего процесса системы",
            agent=self.planning_agent,
            expected_output="Результаты intelligent планирования и выполнения с метриками и рекомендациями"
        )
