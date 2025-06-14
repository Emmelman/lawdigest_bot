"""
Orchestrator Agent - главный планировщик и координатор системы
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum
from dataclasses import dataclass
from crewai import Agent, Task

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
    timeout: int = 300  # 5 минут по умолчанию
    retry_count: int = 3
    created_at: datetime = None
    
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
    Главный агент-оркестратор для планирования и координации работы всех агентов
    """
    
    def __init__(self, db_manager, agent_registry=None):
        """
        Инициализация оркестратора
        
        Args:
            db_manager: Менеджер базы данных
            agent_registry: Реестр доступных агентов
        """
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
        """
        Главный метод планирования и выполнения
        
        Args:
            scenario: Сценарий выполнения
            **kwargs: Дополнительные параметры
            
        Returns:
            Dict с результатами выполнения
        """
        logger.info(f"Запуск планирования и выполнения сценария: {scenario}")
        
        try:
            # Этап 1: Анализ текущего состояния
            context = await self._analyze_current_state(**kwargs)
            logger.info(f"Анализ состояния завершен: {len(context.get('unanalyzed_messages', []))} неанализированных сообщений")
            
            # Этап 2: Создание плана выполнения
            execution_plan = await self._create_execution_plan(scenario, context, **kwargs)
            logger.info(f"План создан: {len(execution_plan)} задач")
            
            # Этап 3: Выполнение плана
            results = await self._execute_plan(execution_plan)
            logger.info(f"Выполнение завершено: {len(results)} результатов")
            
            # Этап 4: Анализ результатов и принятие решений
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
        """Анализ текущего состояния системы"""
        context = {
            "timestamp": datetime.now(),
            "scenario_params": kwargs
        }
        
        try:
            # Проверяем количество неанализированных сообщений
            unanalyzed_messages = self.db_manager.get_unanalyzed_messages(limit=1000)
            context["unanalyzed_messages"] = unanalyzed_messages
            context["unanalyzed_count"] = len(unanalyzed_messages)
            
            # Проверяем сообщения с низкой уверенностью
            low_confidence_messages = self.db_manager.get_messages_with_low_confidence(
                confidence_threshold=2, limit=100
            )
            context["low_confidence_messages"] = low_confidence_messages
            context["low_confidence_count"] = len(low_confidence_messages)
            
            # Проверяем последний дайджест
            latest_digest = self.db_manager.get_latest_digest()
            context["latest_digest"] = latest_digest
            context["latest_digest_date"] = latest_digest.date if latest_digest else None
            
            # Проверяем сегодняшние дайджесты
            today_digests = self.db_manager.find_digests_by_parameters(is_today=True, limit=10)
            context["today_digests"] = today_digests
            context["today_digests_count"] = len(today_digests)
            
            # Определяем, нужно ли собирать новые данные
            last_collection_time = kwargs.get("last_collection_time")
            if last_collection_time:
                time_since_collection = datetime.now() - last_collection_time
                context["needs_data_collection"] = time_since_collection.total_seconds() > 1800  # 30 минут
            else:
                context["needs_data_collection"] = True
            
            logger.debug(f"Состояние системы: {context['unanalyzed_count']} неанализированных, "
                        f"{context['low_confidence_count']} с низкой уверенностью, "
                        f"{context['today_digests_count']} дайджестов за сегодня")
            
            return context
            
        except Exception as e:
            logger.error(f"Ошибка при анализе состояния: {str(e)}")
            return context
    
    async def _create_execution_plan(self, scenario: str, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Создание плана выполнения на основе сценария и контекста"""
        
        if scenario not in self.execution_strategies:
            logger.warning(f"Неизвестный сценарий {scenario}, используем daily_workflow")
            scenario = "daily_workflow"
        
        strategy_func = self.execution_strategies[scenario]
        return await strategy_func(context, **kwargs)
    
    async def _create_daily_workflow_strategy(self, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Стратегия для ежедневного рабочего процесса"""
        tasks = []
        
        # Задача 1: Сбор данных (если нужно)
        if context.get("needs_data_collection", True):
            tasks.append(TaskRequest(
                task_type=TaskType.DATA_COLLECTION,
                priority=TaskPriority.HIGH,
                params={
                    "days_back": kwargs.get("days_back", 1),
                    "force_update": kwargs.get("force_update", False)
                }
            ))
        
        # Задача 2: Анализ сообщений (если есть неанализированные)
        if context.get("unanalyzed_count", 0) > 0:
            tasks.append(TaskRequest(
                task_type=TaskType.MESSAGE_ANALYSIS,
                priority=TaskPriority.NORMAL,
                params={
                    "limit": min(context["unanalyzed_count"], 100),
                    "batch_size": 10
                },
                dependencies=["data_collection"] if context.get("needs_data_collection") else []
            ))
        
        # Задача 3: Проверка категоризации (если есть сообщения с низкой уверенностью)
        if context.get("low_confidence_count", 0) > 0:
            tasks.append(TaskRequest(
                task_type=TaskType.CATEGORIZATION_REVIEW,
                priority=TaskPriority.NORMAL,
                params={
                    "confidence_threshold": 2,
                    "limit": min(context["low_confidence_count"], 50)
                },
                dependencies=["message_analysis"] if context.get("unanalyzed_count", 0) > 0 else []
            ))
        
        # Задача 4: Создание/обновление дайджеста
        today = datetime.now().date()
        has_today_digest = context.get("today_digests_count", 0) > 0
        
        if has_today_digest:
            # Обновляем существующий дайджест
            tasks.append(TaskRequest(
                task_type=TaskType.DIGEST_UPDATE,
                priority=TaskPriority.HIGH,
                params={
                    "date": today,
                    "digest_type": "both"
                },
                dependencies=[task.task_type.value for task in tasks[-2:]]  # Зависит от последних задач
            ))
        else:
            # Создаем новый дайджест
            tasks.append(TaskRequest(
                task_type=TaskType.DIGEST_CREATION,
                priority=TaskPriority.HIGH,
                params={
                    "date": today,
                    "days_back": kwargs.get("days_back", 1),
                    "digest_type": "both"
                },
                dependencies=[task.task_type.value for task in tasks]  # Зависит от всех предыдущих
            ))
        
        logger.info(f"Создан план daily_workflow: {len(tasks)} задач")
        return tasks
    
    async def _create_urgent_update_strategy(self, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Стратегия для срочного обновления"""
        tasks = []
        
        # Быстрый сбор только свежих данных
        tasks.append(TaskRequest(
            task_type=TaskType.DATA_COLLECTION,
            priority=TaskPriority.CRITICAL,
            params={
                "days_back": 1,
                "force_update": True
            },
            timeout=180  # 3 минуты
        ))
        
        # Быстрый анализ только новых сообщений
        if context.get("unanalyzed_count", 0) > 0:
            tasks.append(TaskRequest(
                task_type=TaskType.MESSAGE_ANALYSIS,
                priority=TaskPriority.CRITICAL,
                params={
                    "limit": 50,  # Ограничиваем для скорости
                    "batch_size": 5
                },
                dependencies=["data_collection"],
                timeout=120  # 2 минуты
            ))
        
        # Обновление дайджеста
        tasks.append(TaskRequest(
            task_type=TaskType.DIGEST_UPDATE,
            priority=TaskPriority.CRITICAL,
            params={
                "date": datetime.now().date(),
                "digest_type": "brief"  # Только краткий для скорости
            },
            dependencies=["message_analysis"] if context.get("unanalyzed_count", 0) > 0 else ["data_collection"],
            timeout=120
        ))
        
        logger.info(f"Создан план urgent_update: {len(tasks)} задач")
        return tasks
    
    async def _create_full_analysis_strategy(self, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Стратегия для полного анализа"""
        tasks = []
        
        # Полный сбор данных за указанный период
        tasks.append(TaskRequest(
            task_type=TaskType.DATA_COLLECTION,
            priority=TaskPriority.NORMAL,
            params={
                "days_back": kwargs.get("days_back", 7),
                "force_update": kwargs.get("force_update", True)
            },
            timeout=600  # 10 минут
        ))
        
        # Анализ всех неанализированных сообщений
        tasks.append(TaskRequest(
            task_type=TaskType.MESSAGE_ANALYSIS,
            priority=TaskPriority.NORMAL,
            params={
                "limit": kwargs.get("analysis_limit", 500),
                "batch_size": 20
            },
            dependencies=["data_collection"],
            timeout=900  # 15 минут
        ))
        
        # Полная проверка категоризации
        tasks.append(TaskRequest(
            task_type=TaskType.CATEGORIZATION_REVIEW,
            priority=TaskPriority.NORMAL,
            params={
                "confidence_threshold": 3,  # Более строгий порог
                "limit": 200
            },
            dependencies=["message_analysis"],
            timeout=600  # 10 минут
        ))
        
        # Создание детального дайджеста
        tasks.append(TaskRequest(
            task_type=TaskType.DIGEST_CREATION,
            priority=TaskPriority.NORMAL,
            params={
                "date": datetime.now().date(),
                "days_back": kwargs.get("days_back", 7),
                "digest_type": "both"
            },
            dependencies=["categorization_review"]
        ))
        
        logger.info(f"Создан план full_analysis: {len(tasks)} задач")
        return tasks
    
    async def _create_digest_only_strategy(self, context: Dict[str, Any], **kwargs) -> List[TaskRequest]:
        """Стратегия только для создания дайджеста"""
        tasks = []
        
        # Только создание дайджеста на основе существующих данных
        tasks.append(TaskRequest(
            task_type=TaskType.DIGEST_CREATION,
            priority=TaskPriority.HIGH,
            params={
                "date": kwargs.get("date", datetime.now().date()),
                "days_back": kwargs.get("days_back", 1),
                "digest_type": kwargs.get("digest_type", "both"),
                "focus_category": kwargs.get("focus_category"),
                "channels": kwargs.get("channels"),
                "keywords": kwargs.get("keywords")
            }
        ))
        
        logger.info(f"Создан план digest_only: {len(tasks)} задач")
        return tasks
    async def _execute_plan(self, execution_plan: List[TaskRequest]) -> List[TaskResult]:
        """Выполнение плана задач с учетом зависимостей"""
        results = []
        completed_tasks = set()
        failed_tasks = set()
        
        logger.info(f"Начало выполнения плана из {len(execution_plan)} задач")
        
        # Создаем мапу задач для удобства
        tasks_map = {task.task_type.value: task for task in execution_plan}
        
        # Выполняем задачи, учитывая зависимости
        while len(completed_tasks) + len(failed_tasks) < len(execution_plan):
            # Находим задачи, готовые к выполнению
            ready_tasks = []
            for task in execution_plan:
                task_id = task.task_type.value
                
                # Пропускаем уже выполненные или неудачные задачи
                if task_id in completed_tasks or task_id in failed_tasks:
                    continue
                
                # Проверяем зависимости
                dependencies_met = all(
                    dep in completed_tasks for dep in task.dependencies
                )
                
                if dependencies_met:
                    ready_tasks.append(task)
            
            if not ready_tasks:
                # Если нет готовых задач, но есть невыполненные - проблема с зависимостями
                remaining_tasks = [
                    task.task_type.value for task in execution_plan 
                    if task.task_type.value not in completed_tasks and task.task_type.value not in failed_tasks
                ]
                logger.error(f"Deadlock: нет готовых к выполнению задач. Остаются: {remaining_tasks}")
                break
            
            # Выполняем готовые задачи (можно параллельно, если нет конфликтов)
            for task in ready_tasks:
                try:
                    logger.info(f"Выполнение задачи: {task.task_type.value}")
                    start_time = datetime.now()
                    
                    result = await self._execute_single_task(task)
                    
                    execution_time = (datetime.now() - start_time).total_seconds()
                    
                    task_result = TaskResult(
                        task_id=task.task_type.value,
                        task_type=task.task_type,
                        status=TaskStatus.COMPLETED if result.get("status") == "success" else TaskStatus.FAILED,
                        result=result,
                        execution_time=execution_time,
                        completed_at=datetime.now()
                    )
                    
                    results.append(task_result)
                    
                    if task_result.status == TaskStatus.COMPLETED:
                        completed_tasks.add(task.task_type.value)
                        logger.info(f"Задача {task.task_type.value} выполнена успешно за {execution_time:.2f}с")
                    else:
                        failed_tasks.add(task.task_type.value)
                        logger.error(f"Задача {task.task_type.value} завершилась с ошибкой: {result.get('error')}")
                        
                except Exception as e:
                    logger.error(f"Исключение при выполнении задачи {task.task_type.value}: {str(e)}")
                    
                    task_result = TaskResult(
                        task_id=task.task_type.value,
                        task_type=task.task_type,
                        status=TaskStatus.FAILED,
                        error=str(e),
                        completed_at=datetime.now()
                    )
                    
                    results.append(task_result)
                    failed_tasks.add(task.task_type.value)
        
        logger.info(f"Выполнение плана завершено. Успешно: {len(completed_tasks)}, "
                   f"с ошибками: {len(failed_tasks)}")
        
        return results
    
    async def _execute_single_task(self, task: TaskRequest) -> Dict[str, Any]:
        """Выполнение отдельной задачи"""
        
        if not self.agent_registry:
            logger.error("Agent registry не инициализирован")
            return {"status": "error", "error": "Agent registry не доступен"}
        
        try:
            # Получаем соответствующий агент из реестра
            agent = self.agent_registry.get_agent(task.task_type)
            
            if not agent:
                return {"status": "error", "error": f"Агент для задачи {task.task_type.value} не найден"}
            
            # Выполняем задачу в зависимости от типа
            if task.task_type == TaskType.DATA_COLLECTION:
                return await self._execute_data_collection(agent, task.params)
            
            elif task.task_type == TaskType.MESSAGE_ANALYSIS:
                return await self._execute_message_analysis(agent, task.params)
            
            elif task.task_type == TaskType.CATEGORIZATION_REVIEW:
                return await self._execute_categorization_review(agent, task.params)
            
            elif task.task_type == TaskType.DIGEST_CREATION:
                return await self._execute_digest_creation(agent, task.params)
            
            elif task.task_type == TaskType.DIGEST_UPDATE:
                return await self._execute_digest_update(agent, task.params)
            
            else:
                return {"status": "error", "error": f"Неизвестный тип задачи: {task.task_type.value}"}
                
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи {task.task_type.value}: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _execute_data_collection(self, agent, params: Dict[str, Any]) -> Dict[str, Any]:
        """Выполнение сбора данных"""
        try:
            result = await agent.collect_data(
                days_back=params.get("days_back", 1),
                force_update=params.get("force_update", False),
                start_date=params.get("start_date"),
                end_date=params.get("end_date")
            )
            
            logger.info(f"Сбор данных завершен: {result.get('total_new_messages', 0)} новых сообщений")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при сборе данных: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _execute_message_analysis(self, agent, params: Dict[str, Any]) -> Dict[str, Any]:
        """Выполнение анализа сообщений"""
        try:
            # Этот метод синхронный, выполняем в executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: agent.analyze_messages(
                    limit=params.get("limit", 50),
                    batch_size=params.get("batch_size", 10)
                )
            )
            
            logger.info(f"Анализ сообщений завершен: {result.get('analyzed_count', 0)} проанализировано")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при анализе сообщений: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _execute_categorization_review(self, agent, params: Dict[str, Any]) -> Dict[str, Any]:
        """Выполнение проверки категоризации"""
        try:
            # Этот метод синхронный, выполняем в executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: agent.review_recent_categorizations(
                    confidence_threshold=params.get("confidence_threshold", 2),
                    limit=params.get("limit", 50),
                    batch_size=params.get("batch_size", 5),
                    max_workers=params.get("max_workers", 3),
                    start_date=params.get("start_date"),
                    end_date=params.get("end_date")
                )
            )
            
            logger.info(f"Проверка категоризации завершена: {result.get('updated', 0)} обновлено")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при проверке категоризации: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _execute_digest_creation(self, agent, params: Dict[str, Any]) -> Dict[str, Any]:
        """Выполнение создания дайджеста"""
        try:
            # Этот метод синхронный, выполняем в executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: agent.create_digest(
                    date=params.get("date"),
                    days_back=params.get("days_back", 1),
                    digest_type=params.get("digest_type", "both"),
                    update_existing=params.get("update_existing", True),
                    focus_category=params.get("focus_category"),
                    channels=params.get("channels"),
                    keywords=params.get("keywords")
                )
            )
            
            digest_info = []
            if result.get("brief_digest_id"):
                digest_info.append(f"краткий (ID: {result['brief_digest_id']})")
            if result.get("detailed_digest_id"):
                digest_info.append(f"подробный (ID: {result['detailed_digest_id']})")
            
            logger.info(f"Создание дайджеста завершено: {', '.join(digest_info)}")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при создании дайджеста: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _execute_digest_update(self, agent, params: Dict[str, Any]) -> Dict[str, Any]:
        """Выполнение обновления дайджеста"""
        try:
            # Получаем дату для обновления
            date = params.get("date", datetime.now())
            
            # Этот метод синхронный, выполняем в executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: agent.update_digests_for_date(date)
            )
            
            # ИСПРАВЛЕНИЕ: Проверяем наличие обновленных дайджестов и формируем правильный статус
            updated_digests = result.get("updated_digests", [])
            updated_count = len(updated_digests)
            
            if updated_count > 0:
                # Если обновили дайджесты - это успех
                logger.info(f"Обновление дайджестов завершено: {updated_count} дайджестов обновлено")
                return {
                    "status": "success",  # ДОБАВЛЯЕМ ЯВНЫЙ СТАТУС УСПЕХА
                    "updated_digests": updated_digests,
                    "updated_count": updated_count
                }
            else:
                # Если ничего не обновили - это тоже может быть нормально
                logger.info("Обновление дайджестов завершено: нет дайджестов для обновления")
                return {
                    "status": "success",  # УСПЕХ, даже если нечего было обновлять
                    "updated_digests": [],
                    "updated_count": 0
                }
                
        except Exception as e:
            logger.error(f"Ошибка при обновлении дайджеста: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def _analyze_results_and_decide(self, results: List[TaskResult], scenario: str) -> Dict[str, Any]:
        """Анализ результатов выполнения и принятие решений о дальнейших действиях"""
        
        # Подсчитываем статистику
        successful_tasks = [r for r in results if r.status == TaskStatus.COMPLETED]
        failed_tasks = [r for r in results if r.status == TaskStatus.FAILED]
        
        total_execution_time = sum(r.execution_time for r in results)
        
        # Собираем ключевые метрики
        metrics = {
            "total_tasks": len(results),
            "successful_tasks": len(successful_tasks),
            "failed_tasks": len(failed_tasks),
            "success_rate": len(successful_tasks) / len(results) if results else 0,
            "total_execution_time": total_execution_time,
            "scenario": scenario
        }
        
        # Извлекаем важную информацию из результатов
        summary = {
            "collected_messages": 0,
            "analyzed_messages": 0,
            "reviewed_messages": 0,
            "created_digests": [],
            "updated_digests": []
        }
        
        for result in successful_tasks:
            if result.task_type == TaskType.DATA_COLLECTION:
                summary["collected_messages"] = result.result.get("total_new_messages", 0)
            
            elif result.task_type == TaskType.MESSAGE_ANALYSIS:
                summary["analyzed_messages"] = result.result.get("analyzed_count", 0)
            
            elif result.task_type == TaskType.CATEGORIZATION_REVIEW:
                summary["reviewed_messages"] = result.result.get("updated", 0)
            
            elif result.task_type == TaskType.DIGEST_CREATION:
                if result.result.get("brief_digest_id"):
                    summary["created_digests"].append(f"краткий (ID: {result.result['brief_digest_id']})")
                if result.result.get("detailed_digest_id"):
                    summary["created_digests"].append(f"подробный (ID: {result.result['detailed_digest_id']})")
            
            elif result.task_type == TaskType.DIGEST_UPDATE:
                updated_digests = result.result.get("updated_digests", [])
                summary["updated_digests"].extend([
                    f"{d['digest_type']} (ID: {d['digest_id']})" for d in updated_digests
                ])
        
        # Определяем рекомендации для следующих действий
        recommendations = []
        
        # Если есть неудачные задачи
        if failed_tasks:
            recommendations.append({
                "type": "retry_failed",
                "description": f"Повторить {len(failed_tasks)} неудачных задач",
                "tasks": [t.task_type.value for t in failed_tasks]
            })
        
        # Если собрано много новых сообщений, рекомендуем дополнительный анализ
        if summary["collected_messages"] > 50:
            recommendations.append({
                "type": "extended_analysis",
                "description": f"Собрано {summary['collected_messages']} новых сообщений, рекомендуется расширенный анализ"
            })
        
        # Если много сообщений было улучшено критиком
        if summary["reviewed_messages"] > 10:
            recommendations.append({
                "type": "quality_check",
                "description": f"Критик улучшил {summary['reviewed_messages']} сообщений, возможно нужна настройка анализатора"
            })
        
        final_result = {
            "status": "success" if not failed_tasks else "partial_success",
            "metrics": metrics,
            "summary": summary,
            "recommendations": recommendations,
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
        
        logger.info(f"Анализ результатов завершен: {metrics['success_rate']:.1%} успешности, "
                   f"{total_execution_time:.1f}с общего времени выполнения")
        
        return final_result
    
    def create_task(self) -> Task:
        """Создание задачи CrewAI для интеграции"""
        return Task(
            description="Спланировать и скоординировать выполнение рабочего процесса системы",
            agent=self.agent,
            expected_output="Результаты планирования и выполнения с метриками и рекомендациями"
        )