"""
Реестр агентов для управления доступными агентами системы
"""
import logging
from typing import Dict, Optional, Any
from enum import Enum

from .orchestrator import TaskType

logger = logging.getLogger(__name__)

class AgentType(Enum):
    """Типы агентов в системе"""
    DATA_COLLECTOR = "data_collector"
    ANALYZER = "analyzer"
    CRITIC = "critic"
    DIGESTER = "digester"
    ORCHESTRATOR = "orchestrator"

class AgentRegistry:
    """
    Реестр для управления агентами системы
    """
    
    def __init__(self, db_manager):
        """
        Инициализация реестра агентов
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db_manager = db_manager
        self.agents = {}
        self.agent_capabilities = {}
        
        # Инициализируем агентов
        self._initialize_agents()
        
        logger.info(f"Реестр агентов инициализирован с {len(self.agents)} агентами")
    
    def _initialize_agents(self):
        """Инициализация всех агентов системы"""
        
        try:
            # Импортируем агентов
            from .data_collector import DataCollectorAgent
            from .analyzer import AnalyzerAgent
            from .critic import CriticAgent
            from .digester import DigesterAgent
            from llm.qwen_model import QwenLLM
            from llm.gemma_model import GemmaLLM
            
            # Инициализируем LLM модели
            qwen_model = QwenLLM()
            gemma_model = GemmaLLM()
            
            # Создаем агентов
            self.agents[AgentType.DATA_COLLECTOR] = DataCollectorAgent(self.db_manager)
            self.agents[AgentType.ANALYZER] = AnalyzerAgent(self.db_manager, qwen_model)
            self.agents[AgentType.CRITIC] = CriticAgent(self.db_manager, gemma_model)
            self.agents[AgentType.DIGESTER] = DigesterAgent(self.db_manager, gemma_model)
            
            # Определяем возможности агентов
            self.agent_capabilities = {
                AgentType.DATA_COLLECTOR: [TaskType.DATA_COLLECTION],
                AgentType.ANALYZER: [TaskType.MESSAGE_ANALYSIS],
                AgentType.CRITIC: [TaskType.CATEGORIZATION_REVIEW],
                AgentType.DIGESTER: [TaskType.DIGEST_CREATION, TaskType.DIGEST_UPDATE]
            }
            
            logger.info("Все агенты успешно инициализированы")
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации агентов: {str(e)}")
            raise
    
    def get_agent(self, task_type: TaskType) -> Optional[Any]:
        """
        Получение агента по типу задачи
        
        Args:
            task_type: Тип задачи
            
        Returns:
            Агент, способный выполнить задачу, или None
        """
        for agent_type, capabilities in self.agent_capabilities.items():
            if task_type in capabilities:
                return self.agents.get(agent_type)
        
        logger.warning(f"Агент для задачи {task_type.value} не найден")
        return None
    
    def get_agent_by_type(self, agent_type: AgentType) -> Optional[Any]:
        """
        Получение агента по типу
        
        Args:
            agent_type: Тип агента
            
        Returns:
            Агент или None
        """
        return self.agents.get(agent_type)
    
    def get_all_agents(self) -> Dict[AgentType, Any]:
        """Получение всех агентов"""
        return self.agents.copy()
    
    def get_agent_capabilities(self, agent_type: AgentType) -> list:
        """
        Получение списка возможностей агента
        
        Args:
            agent_type: Тип агента
            
        Returns:
            Список TaskType, которые может выполнять агент
        """
        return self.agent_capabilities.get(agent_type, [])
    
    def register_agent(self, agent_type: AgentType, agent: Any, capabilities: list):
        """
        Регистрация нового агента
        
        Args:
            agent_type: Тип агента
            agent: Экземпляр агента
            capabilities: Список TaskType, которые может выполнять агент
        """
        self.agents[agent_type] = agent
        self.agent_capabilities[agent_type] = capabilities
        logger.info(f"Зарегистрирован агент {agent_type.value} с возможностями: {[c.value for c in capabilities]}")
    
    def unregister_agent(self, agent_type: AgentType):
        """
        Удаление агента из реестра
        
        Args:
            agent_type: Тип агента для удаления
        """
        if agent_type in self.agents:
            del self.agents[agent_type]
            del self.agent_capabilities[agent_type]
            logger.info(f"Агент {agent_type.value} удален из реестра")
        else:
            logger.warning(f"Агент {agent_type.value} не найден в реестре")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Получение статуса всех агентов
        
        Returns:
            Словарь со статусом агентов
        """
        status = {
            "total_agents": len(self.agents),
            "agents": {}
        }
        
        for agent_type, agent in self.agents.items():
            capabilities = self.agent_capabilities.get(agent_type, [])
            status["agents"][agent_type.value] = {
                "available": agent is not None,
                "capabilities": [c.value for c in capabilities],
                "type": type(agent).__name__ if agent else None
            }
        
        return status