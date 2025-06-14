"""
Менеджер контекста для управления состоянием системы и обмена данными между агентами
"""
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)

class ContextScope(Enum):
    """Область видимости контекста"""
    GLOBAL = "global"           # Глобальный контекст
    SESSION = "session"         # Контекст сессии выполнения
    TASK = "task"              # Контекст отдельной задачи
    AGENT = "agent"            # Контекст агента

@dataclass
class ContextEntry:
    """Запись в контексте"""
    key: str
    value: Any
    scope: ContextScope
    created_at: datetime
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

class ContextManager:
    """
    Менеджер контекста для управления состоянием и обмена данными
    """
    
    def __init__(self, db_manager):
        """
        Инициализация менеджера контекста
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db_manager = db_manager
        self.context_store = {}  # В памяти для быстрого доступа
        self.session_id = None
        
        # Инициализируем глобальный контекст
        self._initialize_global_context()
        
        logger.info("Менеджер контекста инициализирован")
    
    def _initialize_global_context(self):
        """Инициализация глобального контекста"""
        # Состояние системы
        self.set_global("system_started_at", datetime.now())
        self.set_global("system_status", "running")
        
        # Конфигурация
        from config.settings import CATEGORIES, TELEGRAM_CHANNELS
        self.set_global("categories", CATEGORIES)
        self.set_global("channels", TELEGRAM_CHANNELS)
        
        # Кэш для часто используемых данных
        self.set_global("stats_cache", {})
    
    def start_session(self, session_id: str = None) -> str:
        """
        Начало новой сессии выполнения
        
        Args:
            session_id: ID сессии (если не указан, генерируется автоматически)
            
        Returns:
            ID сессии
        """
        if session_id is None:
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.session_id = session_id
        self.set_session("started_at", datetime.now())
        self.set_session("status", "active")
        
        logger.info(f"Начата сессия: {session_id}")
        return session_id
    
    def end_session(self):
        """Завершение текущей сессии"""
        if self.session_id:
            self.set_session("ended_at", datetime.now())
            self.set_session("status", "completed")
            logger.info(f"Завершена сессия: {self.session_id}")
            
            # Очистка временных данных сессии
            self._cleanup_session_context()
            self.session_id = None
    
    def set_global(self, key: str, value: Any, expires_in_hours: int = None):
        """Установка значения в глобальном контексте"""
        expires_at = None
        if expires_in_hours:
            expires_at = datetime.now() + timedelta(hours=expires_in_hours)
        
        self._set_context(key, value, ContextScope.GLOBAL, expires_at)
    
    def set_session(self, key: str, value: Any, expires_in_hours: int = None):
        """Установка значения в контексте сессии"""
        if not self.session_id:
            logger.warning("Сессия не активна, использую глобальный контекст")
            return self.set_global(key, value, expires_in_hours)
        
        expires_at = None
        if expires_in_hours:
            expires_at = datetime.now() + timedelta(hours=expires_in_hours)
        
        session_key = f"session:{self.session_id}:{key}"
        self._set_context(session_key, value, ContextScope.SESSION, expires_at)
    
    def set_task(self, task_id: str, key: str, value: Any, expires_in_minutes: int = 60):
        """Установка значения в контексте задачи"""
        expires_at = datetime.now() + timedelta(minutes=expires_in_minutes)
        task_key = f"task:{task_id}:{key}"
        self._set_context(task_key, value, ContextScope.TASK, expires_at)
    
    def set_agent(self, agent_type: str, key: str, value: Any, expires_in_hours: int = 24):
        """Установка значения в контексте агента"""
        expires_at = datetime.now() + timedelta(hours=expires_in_hours)
        agent_key = f"agent:{agent_type}:{key}"
        self._set_context(agent_key, value, ContextScope.AGENT, expires_at)
    
    def get_global(self, key: str, default: Any = None) -> Any:
        """Получение значения из глобального контекста"""
        return self._get_context(key, default)
    
    def get_session(self, key: str, default: Any = None) -> Any:
        """Получение значения из контекста сессии"""
        if not self.session_id:
            return default
        
        session_key = f"session:{self.session_id}:{key}"
        return self._get_context(session_key, default)
    
    def get_task(self, task_id: str, key: str, default: Any = None) -> Any:
        """Получение значения из контекста задачи"""
        task_key = f"task:{task_id}:{key}"
        return self._get_context(task_key, default)
    
    def get_agent(self, agent_type: str, key: str, default: Any = None) -> Any:
        """Получение значения из контекста агента"""
        agent_key = f"agent:{agent_type}:{key}"
        return self._get_context(agent_key, default)
    
    def _set_context(self, key: str, value: Any, scope: ContextScope, expires_at: datetime = None):
        """Внутренний метод установки значения"""
        entry = ContextEntry(
            key=key,
            value=value,
            scope=scope,
            created_at=datetime.now(),
            expires_at=expires_at
        )
        
        self.context_store[key] = entry
        logger.debug(f"Установлено значение контекста: {key} (scope: {scope.value})")
    
    def _get_context(self, key: str, default: Any = None) -> Any:
        """Внутренний метод получения значения"""
        entry = self.context_store.get(key)
        
        if entry is None:
            return default
        
        # Проверяем срок действия
        if entry.expires_at and datetime.now() > entry.expires_at:
            del self.context_store[key]
            logger.debug(f"Значение контекста устарело и удалено: {key}")
            return default
        
        return entry.value
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Получение статистики системы с кэшированием"""
        cache_key = "system_stats"
        cached_stats = self.get_global(cache_key)
        
        # Проверяем кэш (обновляем раз в 5 минут)
        cache_time = self.get_global(f"{cache_key}_time")
        if cached_stats and cache_time:
            if (datetime.now() - cache_time).total_seconds() < 300:  # 5 минут
                return cached_stats
        
        # Обновляем статистику
        try:
            stats = {
                "unanalyzed_messages": len(self.db_manager.get_unanalyzed_messages(limit=1000)),
                "low_confidence_messages": len(self.db_manager.get_messages_with_low_confidence(limit=100)),
                "latest_digest": None,
                "today_digests_count": 0
            }
            
            # Информация о последнем дайджесте
            latest_digest = self.db_manager.get_latest_digest()
            if latest_digest:
                stats["latest_digest"] = {
                    "date": latest_digest.date.strftime("%Y-%m-%d"),
                    "type": latest_digest.digest_type
                }
            
            # Количество дайджестов за сегодня
            today_digests = self.db_manager.find_digests_by_parameters(is_today=True, limit=10)
            stats["today_digests_count"] = len(today_digests)
            
            # Кэшируем результат
            self.set_global(cache_key, stats, expires_in_hours=1)
            self.set_global(f"{cache_key}_time", datetime.now())
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики системы: {str(e)}")
            return {}
    
    def update_agent_status(self, agent_type: str, status: str, metadata: Dict[str, Any] = None):
        """Обновление статуса агента"""
        status_data = {
            "status": status,
            "updated_at": datetime.now(),
            "metadata": metadata or {}
        }
        
        self.set_agent(agent_type, "status", status_data)
        logger.debug(f"Обновлен статус агента {agent_type}: {status}")
    
    def get_agent_status(self, agent_type: str) -> Dict[str, Any]:
        """Получение статуса агента"""
        return self.get_agent(agent_type, "status", {
            "status": "unknown",
            "updated_at": None,
            "metadata": {}
        })
    
    def record_task_metrics(self, task_id: str, metrics: Dict[str, Any]):
        """Запись метрик выполнения задачи"""
        self.set_task(task_id, "metrics", metrics, expires_in_minutes=120)
        
        # Также обновляем агрегированные метрики
        self._update_aggregated_metrics(metrics)
    
    def _update_aggregated_metrics(self, metrics: Dict[str, Any]):
        """Обновление агрегированных метрик"""
        current_metrics = self.get_global("aggregated_metrics", {})
        
        # Простое агрегирование - подсчет задач и времени выполнения
        current_metrics["total_tasks"] = current_metrics.get("total_tasks", 0) + 1
        current_metrics["total_execution_time"] = current_metrics.get("total_execution_time", 0) + metrics.get("execution_time", 0)
        
        if metrics.get("status") == "success":
            current_metrics["successful_tasks"] = current_metrics.get("successful_tasks", 0) + 1
        
        self.set_global("aggregated_metrics", current_metrics, expires_in_hours=24)
    
    def _cleanup_session_context(self):
        """Очистка контекста сессии"""
        if not self.session_id:
            return
        
        session_prefix = f"session:{self.session_id}:"
        keys_to_remove = [key for key in self.context_store.keys() if key.startswith(session_prefix)]
        
        for key in keys_to_remove:
            del self.context_store[key]
        
        logger.debug(f"Очищен контекст сессии {self.session_id}: {len(keys_to_remove)} записей")
    
    def cleanup_expired(self):
        """Очистка устаревших записей"""
        now = datetime.now()
        expired_keys = []
        
        for key, entry in self.context_store.items():
            if entry.expires_at and now > entry.expires_at:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.context_store[key]
        
        if expired_keys:
            logger.debug(f"Очищено {len(expired_keys)} устаревших записей контекста")
    
    def get_context_summary(self) -> Dict[str, Any]:
        """Получение сводки по контексту"""
        summary = {
            "total_entries": len(self.context_store),
            "by_scope": {},
            "session_id": self.session_id,
            "global_keys": []
        }
        
        for entry in self.context_store.values():
            scope = entry.scope.value
            summary["by_scope"][scope] = summary["by_scope"].get(scope, 0) + 1
            
            if entry.scope == ContextScope.GLOBAL and not entry.key.startswith(("session:", "task:", "agent:")):
                summary["global_keys"].append(entry.key)
        
        return summary