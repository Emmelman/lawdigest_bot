"""
Очередь задач для управления выполнением задач в системе
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import heapq
import uuid

from .orchestrator import TaskRequest, TaskResult, TaskStatus, TaskPriority

logger = logging.getLogger(__name__)

@dataclass
class QueuedTask:
    """Задача в очереди с дополнительной информацией"""
    task_id: str
    request: TaskRequest
    priority_score: int = field(init=False)
    created_at: datetime = field(default_factory=datetime.now)
    scheduled_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    retry_count: int = 0
    
    def __post_init__(self):
        # Вычисляем приоритетный счет для сортировки
        self.priority_score = self._calculate_priority_score()
    
    def _calculate_priority_score(self) -> int:
        """Вычисление приоритетного счета для сортировки в очереди"""
        base_score = self.request.priority.value * 1000
        
        # Добавляем временной фактор (старые задачи получают больший приоритет)
        age_minutes = (datetime.now() - self.created_at).total_seconds() / 60
        age_bonus = min(int(age_minutes), 100)  # Максимум 100 баллов за возраст
        
        # Штрафуем за количество попыток
        retry_penalty = self.retry_count * 50
        
        return base_score + age_bonus - retry_penalty
    
    def __lt__(self, other):
        """Сравнение для приоритетной очереди (больший счет = выше приоритет)"""
        return self.priority_score > other.priority_score

class TaskQueue:
    """
    Очередь задач с поддержкой приоритетов, зависимостей и повторных попыток
    """
    
    def __init__(self, max_concurrent_tasks: int = 3):
        """
        Инициализация очереди задач
        
        Args:
            max_concurrent_tasks: Максимальное количество одновременно выполняемых задач
        """
        self.max_concurrent_tasks = max_concurrent_tasks
        self.pending_queue = []  # Приоритетная очередь
        self.active_tasks = {}   # Активно выполняемые задачи
        self.completed_tasks = {}  # Завершенные задачи
        self.failed_tasks = {}   # Неудачные задачи
        self.blocked_tasks = {}  # Заблокированные задачи (ждут зависимости)
        
        self._lock = asyncio.Lock()
        self._running = False
        self._worker_task = None
        
        logger.info(f"Очередь задач инициализирована (макс. одновременных задач: {max_concurrent_tasks})")
    
    async def add_task(self, request: TaskRequest) -> str:
        """
        Добавление задачи в очередь
        
        Args:
            request: Запрос на выполнение задачи
            
        Returns:
            ID задачи
        """
        async with self._lock:
            task_id = str(uuid.uuid4())
            queued_task = QueuedTask(task_id=task_id, request=request)
            
            # Проверяем зависимости
            if self._check_dependencies(queued_task):
                heapq.heappush(self.pending_queue, queued_task)
                logger.info(f"Задача {request.task_type.value} добавлена в очередь (ID: {task_id})")
            else:
                self.blocked_tasks[task_id] = queued_task
                logger.info(f"Задача {request.task_type.value} заблокирована из-за зависимостей (ID: {task_id})")
            
            return task_id
    
    def _check_dependencies(self, task: QueuedTask) -> bool:
        """
        Проверка выполнения зависимостей задачи
        
        Args:
            task: Задача для проверки
            
        Returns:
            True если все зависимости выполнены
        """
        if not task.request.dependencies:
            return True
        
        # Проверяем, что все зависимости успешно завершены
        for dep in task.request.dependencies:
            # Ищем задачи по типу в завершенных
            dep_completed = any(
                completed_task.request.task_type.value == dep and 
                completed_task.task_id in self.completed_tasks
                for completed_task in self.completed_tasks.values()
            )
            
            if not dep_completed:
                return False
        
        return True
    
    async def start_processing(self, executor_func):
        """
        Запуск обработки очереди
        
        Args:
            executor_func: Асинхронная функция для выполнения задач
        """
        if self._running:
            logger.warning("Обработка очереди уже запущена")
            return
        
        self._running = True
        self._executor_func = executor_func
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("Запущена обработка очереди задач")
    
    async def stop_processing(self):
        """Остановка обработки очереди"""
        self._running = False
        
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Обработка очереди остановлена")
    
    async def _worker_loop(self):
        """Основной цикл обработки задач"""
        try:
            while self._running:
                await self._process_pending_tasks()
                await self._check_blocked_tasks()
                await self._cleanup_completed_tasks()
                
                # Небольшая пауза между итерациями
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("Рабочий цикл очереди отменен")
        except Exception as e:
            logger.error(f"Ошибка в рабочем цикле очереди: {str(e)}")
    
    async def _process_pending_tasks(self):
        """Обработка ожидающих задач"""
        async with self._lock:
            # Запускаем новые задачи, если есть свободные слоты
            while (len(self.active_tasks) < self.max_concurrent_tasks and 
                   self.pending_queue):
                
                task = heapq.heappop(self.pending_queue)
                
                # Проверяем зависимости еще раз (могли измениться)
                if not self._check_dependencies(task):
                    self.blocked_tasks[task.task_id] = task
                    continue
                
                # Проверяем таймаут
                if self._is_task_expired(task):
                    logger.warning(f"Задача {task.task_id} просрочена, пропускаем")
                    continue
                
                # Запускаем задачу
                task.started_at = datetime.now()
                self.active_tasks[task.task_id] = task
                
                # Создаем корутину для выполнения
                asyncio.create_task(self._execute_task(task))
                
                logger.info(f"Запущена задача {task.request.task_type.value} (ID: {task.task_id})")
    
    async def _execute_task(self, task: QueuedTask):
        """
        Выполнение отдельной задачи
        
        Args:
            task: Задача для выполнения
        """
        try:
            # Выполняем задачу с таймаутом
            result = await asyncio.wait_for(
                self._executor_func(task.request),
                timeout=task.request.timeout
            )
            
            # Обрабатываем результат
            await self._handle_task_completion(task, result)
            
        except asyncio.TimeoutError:
            logger.error(f"Таймаут выполнения задачи {task.task_id}")
            await self._handle_task_failure(task, "Превышено время выполнения")
            
        except Exception as e:
            logger.error(f"Ошибка выполнения задачи {task.task_id}: {str(e)}")
            await self._handle_task_failure(task, str(e))
    
    async def _handle_task_completion(self, task: QueuedTask, result: Dict[str, Any]):
        """Обработка успешного завершения задачи"""
        async with self._lock:
            # Удаляем из активных
            if task.task_id in self.active_tasks:
                del self.active_tasks[task.task_id]
            
            # Добавляем в завершенные
            task_result = TaskResult(
                task_id=task.task_id,
                task_type=task.request.task_type,
                status=TaskStatus.COMPLETED,
                result=result,
                execution_time=(datetime.now() - task.started_at).total_seconds(),
                completed_at=datetime.now()
            )
            
            self.completed_tasks[task.task_id] = task_result
            logger.info(f"Задача {task.request.task_type.value} успешно завершена (ID: {task.task_id})")
    
    async def _handle_task_failure(self, task: QueuedTask, error: str):
        """Обработка неудачного выполнения задачи"""
        async with self._lock:
            # Удаляем из активных
            if task.task_id in self.active_tasks:
                del self.active_tasks[task.task_id]
            
            # Проверяем возможность повторного выполнения
            if task.retry_count < task.request.retry_count:
                task.retry_count += 1
                task.priority_score = task._calculate_priority_score()  # Пересчитываем приоритет
                
                # Добавляем обратно в очередь с задержкой
                delay = min(2 ** task.retry_count, 60)  # Экспоненциальная задержка до 60 сек
                task.scheduled_at = datetime.now() + timedelta(seconds=delay)
                
                heapq.heappush(self.pending_queue, task)
                logger.info(f"Задача {task.task_id} будет повторена через {delay}с (попытка {task.retry_count + 1})")
            else:
                # Исчерпаны попытки, помечаем как неудачную
                task_result = TaskResult(
                    task_id=task.task_id,
                    task_type=task.request.task_type,
                    status=TaskStatus.FAILED,
                    error=error,
                    completed_at=datetime.now()
                )
                
                self.failed_tasks[task.task_id] = task_result
                logger.error(f"Задача {task.task_id} окончательно не выполнена: {error}")
    
    async def _check_blocked_tasks(self):
        """Проверка заблокированных задач на готовность"""
        async with self._lock:
            ready_tasks = []
            
            for task_id, task in list(self.blocked_tasks.items()):
                if self._check_dependencies(task):
                    ready_tasks.append(task_id)
            
            # Перемещаем готовые задачи в основную очередь
            for task_id in ready_tasks:
                task = self.blocked_tasks.pop(task_id)
                heapq.heappush(self.pending_queue, task)
                logger.info(f"Задача {task.task_id} разблокирована и добавлена в очередь")
    
    async def _cleanup_completed_tasks(self):
        """Очистка старых завершенных задач"""
        cutoff_time = datetime.now() - timedelta(hours=24)  # Храним 24 часа
        
        async with self._lock:
            # Очищаем старые завершенные задачи
            old_completed = [
                task_id for task_id, result in self.completed_tasks.items()
                if result.completed_at and result.completed_at < cutoff_time
            ]
            
            for task_id in old_completed:
                del self.completed_tasks[task_id]
            
            # Очищаем старые неудачные задачи
            old_failed = [
                task_id for task_id, result in self.failed_tasks.items()
                if result.completed_at and result.completed_at < cutoff_time
            ]
            
            for task_id in old_failed:
                del self.failed_tasks[task_id]
            
            if old_completed or old_failed:
                logger.debug(f"Очищено {len(old_completed)} завершенных и {len(old_failed)} неудачных задач")
    
    def _is_task_expired(self, task: QueuedTask) -> bool:
        """Проверка на просроченность задачи"""
        if not hasattr(task.request, 'expires_at'):
            return False
        
        return datetime.now() > task.request.expires_at
    
    async def get_status(self) -> Dict[str, Any]:
        """Получение статуса очереди"""
        async with self._lock:
            return {
                "running": self._running,
                "pending_tasks": len(self.pending_queue),
                "active_tasks": len(self.active_tasks),
                "blocked_tasks": len(self.blocked_tasks),
                "completed_tasks": len(self.completed_tasks),
                "failed_tasks": len(self.failed_tasks),
                "max_concurrent": self.max_concurrent_tasks
            }
    
    async def cancel_task(self, task_id: str) -> bool:
        """
        Отмена задачи
        
        Args:
            task_id: ID задачи для отмены
            
        Returns:
            True если задача была отменена
        """
        async with self._lock:
            # Ищем задачу в разных очередях
            
            # В ожидающих
            for i, task in enumerate(self.pending_queue):
                if task.task_id == task_id:
                    del self.pending_queue[i]
                    heapq.heapify(self.pending_queue)
                    logger.info(f"Задача {task_id} отменена из очереди ожидания")
                    return True
            
            # В заблокированных
            if task_id in self.blocked_tasks:
                del self.blocked_tasks[task_id]
                logger.info(f"Задача {task_id} отменена из заблокированных")
                return True
            
            # Активные задачи сложнее отменить (нужна поддержка в executor)
            if task_id in self.active_tasks:
                logger.warning(f"Задача {task_id} активна, отмена не поддерживается")
                return False
            
            logger.warning(f"Задача {task_id} не найдена для отмены")
            return False
    
    async def get_task_status(self, task_id: str) -> Optional[str]:
        """
        Получение статуса конкретной задачи
        
        Args:
            task_id: ID задачи
            
        Returns:
            Статус задачи или None если не найдена
        """
        async with self._lock:
            if task_id in self.active_tasks:
                return "running"
            elif task_id in self.completed_tasks:
                return "completed"
            elif task_id in self.failed_tasks:
                return "failed"
            elif task_id in self.blocked_tasks:
                return "blocked"
            else:
                # Проверяем в ожидающих
                for task in self.pending_queue:
                    if task.task_id == task_id:
                        return "pending"
                
                return None