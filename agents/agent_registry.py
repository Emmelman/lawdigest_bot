"""
Обновленный реестр агентов для работы с Intelligent Orchestrator
"""
import logging
from typing import Dict, Any, Optional
from enum import Enum

logger = logging.getLogger(__name__)

class AgentType(Enum):
    """Типы агентов в системе"""
    DATA_COLLECTOR = "data_collector"
    ANALYZER = "analyzer" 
    CRITIC = "critic"
    DIGESTER = "digester"

class AgentRegistry:
    """
    Реестр агентов системы с поддержкой intelligent оркестратора
    """
    
    def __init__(self, db_manager):
        """
        Инициализация реестра агентов
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db_manager = db_manager
        self.agents = {}
        self._initialize_agents()
        
        logger.info(f"Реестр агентов инициализирован с {len(self.agents)} агентами")
    
    def _initialize_agents(self):
        """Инициализация всех агентов"""
        try:
            # Импортируем агентов
            from agents.data_collector import DataCollectorAgent
            from agents.analyzer import AnalyzerAgent
            from agents.critic import CriticAgent
            from agents.digester import DigesterAgent
            
            # Создаем экземпляры агентов
            self.agents[AgentType.DATA_COLLECTOR] = DataCollectorAgent(self.db_manager)
            self.agents[AgentType.ANALYZER] = AnalyzerAgent(self.db_manager)
            self.agents[AgentType.CRITIC] = CriticAgent(self.db_manager)
            self.agents[AgentType.DIGESTER] = DigesterAgent(self.db_manager)
            
            logger.info("Все агенты успешно инициализированы")
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации агентов: {str(e)}")
            raise
    
    def get_agent(self, agent_name: str):
        """
        Получение агента по имени
        
        Args:
            agent_name: Название агента
            
        Returns:
            Экземпляр агента
        """
        # Поддерживаем как строковые названия, так и enum
        if isinstance(agent_name, str):
            agent_map = {
                "data_collector": AgentType.DATA_COLLECTOR,
                "analyzer": AgentType.ANALYZER,
                "critic": AgentType.CRITIC,
                "digester": AgentType.DIGESTER
            }
            agent_type = agent_map.get(agent_name)
        else:
            agent_type = agent_name
        
        if agent_type not in self.agents:
            raise ValueError(f"Агент {agent_name} не найден в реестре")
        
        return self.agents[agent_type]
    
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
            try:
                # Проверяем базовые атрибуты агента
                agent_status = {
                    "initialized": True,
                    "type": agent_type.value,
                    "class": agent.__class__.__name__
                }
                
                # Добавляем специфичную информацию если доступна
                if hasattr(agent, 'get_status'):
                    agent_status.update(agent.get_status())
                
                status["agents"][agent_type.value] = agent_status
                
            except Exception as e:
                status["agents"][agent_type.value] = {
                    "initialized": False,
                    "error": str(e)
                }
        
        return status
    
    def validate_agents(self) -> Dict[str, bool]:
        """
        Валидация всех агентов
        
        Returns:
            Словарь с результатами валидации
        """
        validation_results = {}
        
        for agent_type, agent in self.agents.items():
            try:
                # Проверяем наличие основных методов
                required_methods = {
                    AgentType.DATA_COLLECTOR: ['collect_data'],
                    AgentType.ANALYZER: ['analyze_messages'],
                    AgentType.CRITIC: ['review_recent_categorizations'],
                    AgentType.DIGESTER: ['create_digest']  # Убрали update_digest, только create_digest
                }
                
                methods_to_check = required_methods.get(agent_type, [])
                
                for method_name in methods_to_check:
                    if not hasattr(agent, method_name):
                        raise AttributeError(f"Отсутствует метод {method_name}")
                    
                    method = getattr(agent, method_name)
                    if not callable(method):
                        raise AttributeError(f"Атрибут {method_name} не является методом")
                
                validation_results[agent_type.value] = True
                
            except Exception as e:
                logger.error(f"Валидация агента {agent_type.value} не пройдена: {str(e)}")
                validation_results[agent_type.value] = False
        
        return validation_results
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Проверка здоровья всех агентов
        
        Returns:
            Подробный отчет о состоянии агентов
        """
        health_report = {
            "timestamp": logger.info("Запуск проверки здоровья агентов"),
            "overall_status": "healthy",
            "agents": {}
        }
        
        failed_agents = []
        
        for agent_type, agent in self.agents.items():
            agent_health = {
                "status": "unknown",
                "details": {}
            }
            
            try:
                # Базовая проверка инициализации
                if agent is None:
                    raise Exception("Агент не инициализирован")
                
                # Проверка подключения к БД
                if hasattr(agent, 'db_manager') and agent.db_manager:
                    agent_health["details"]["database"] = "connected"
                else:
                    agent_health["details"]["database"] = "not_connected"
                
                # Специфичные проверки для каждого типа агента
                if agent_type == AgentType.DATA_COLLECTOR:
                    # Проверяем Telegram сессию если доступно
                    if hasattr(agent, 'session_manager'):
                        agent_health["details"]["telegram"] = "available"
                
                elif agent_type == AgentType.ANALYZER:
                    # Проверяем LLM конфигурацию
                    if hasattr(agent, 'llm_client'):
                        agent_health["details"]["llm"] = "configured"
                
                elif agent_type == AgentType.CRITIC:
                    # Проверяем learning manager
                    if hasattr(agent, 'learning_manager'):
                        agent_health["details"]["learning"] = "loaded"
                
                elif agent_type == AgentType.DIGESTER:
                    # Проверяем шаблоны дайджестов
                    if hasattr(agent, 'templates'):
                        agent_health["details"]["templates"] = "loaded"
                
                agent_health["status"] = "healthy"
                
            except Exception as e:
                agent_health["status"] = "unhealthy"
                agent_health["error"] = str(e)
                failed_agents.append(agent_type.value)
            
            health_report["agents"][agent_type.value] = agent_health
        
        # Определяем общий статус
        if failed_agents:
            health_report["overall_status"] = "degraded" if len(failed_agents) < len(self.agents) else "critical"
            health_report["failed_agents"] = failed_agents
        
        logger.info(f"Проверка здоровья завершена. Статус: {health_report['overall_status']}")
        
        return health_report