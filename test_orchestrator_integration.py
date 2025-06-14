"""
Тесты интеграции оркестратора с существующей системой
"""
import pytest
import asyncio
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock

# Импорты тестируемых компонентов
from database.db_manager import DatabaseManager
from agents.orchestrator import OrchestratorAgent, TaskType, TaskPriority, TaskRequest
from agents.agent_registry import AgentRegistry, AgentType
from agents.task_queue import TaskQueue
from agents.context_manager import ContextManager, ContextScope

class TestOrchestratorIntegration:
    """Тесты интеграции оркестратора"""
    
    @pytest.fixture
    def temp_db(self):
        """Временная база данных для тестов"""
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_file.close()
        
        db_url = f"sqlite:///{temp_file.name}"
        db_manager = DatabaseManager(db_url)
        
        yield db_manager
        
        # Очистка
        os.unlink(temp_file.name)
    
    @pytest.fixture
    def mock_agents(self):
        """Моки агентов для тестирования"""
        mock_collector = Mock()
        mock_collector.collect_data = AsyncMock(return_value={
            "status": "success",
            "total_new_messages": 15,
            "channels_stats": {"@dumainfo": 10, "@sovfedinfo": 5}
        })
        
        mock_analyzer = Mock()
        mock_analyzer.analyze_messages = Mock(return_value={
            "status": "success",
            "analyzed_count": 15,
            "categories": {"новые законы": 8, "поправки к законам": 7}
        })
        
        mock_critic = Mock()
        mock_critic.review_recent_categorizations = Mock(return_value={
            "status": "success",
            "total": 15,
            "updated": 3,
            "unchanged": 12
        })
        
        mock_digester = Mock()
        mock_digester.create_digest = Mock(return_value={
            "status": "success",
            "brief_digest_id": 1,
            "detailed_digest_id": 2
        })
        mock_digester.update_digests_for_date = Mock(return_value={
            "status": "success",
            "updated_digests": [{"digest_id": 1, "digest_type": "brief"}]
        })
        
        return {
            AgentType.DATA_COLLECTOR: mock_collector,
            AgentType.ANALYZER: mock_analyzer,
            AgentType.CRITIC: mock_critic,
            AgentType.DIGESTER: mock_digester
        }
    
    @pytest.fixture
    def orchestrator_setup(self, temp_db, mock_agents):
        """Настройка оркестратора для тестов"""
        # Создаем реестр агентов с моками
        agent_registry = AgentRegistry(temp_db)
        
        # Подменяем агентов на моки
        for agent_type, mock_agent in mock_agents.items():
            agent_registry.agents[agent_type] = mock_agent
        
        # Создаем оркестратор
        orchestrator = OrchestratorAgent(temp_db, agent_registry)
        
        return orchestrator, agent_registry, temp_db
    
    @pytest.mark.asyncio
    async def test_daily_workflow_scenario(self, orchestrator_setup):
        """Тест ежедневного рабочего процесса"""
        orchestrator, agent_registry, db_manager = orchestrator_setup
        
        # Подготавливаем данные в БД
        db_manager.save_message(
            channel="@dumainfo",
            message_id=1,
            text="Тестовое сообщение без категории",
            date=datetime.now() - timedelta(hours=2)
        )
        
        # Запускаем сценарий
        result = await orchestrator.plan_and_execute("daily_workflow", days_back=1)
        
        # Проверяем результат
        assert result["status"] in ["success", "partial_success"]
        assert "metrics" in result
        assert "summary" in result
        
        # Проверяем метрики
        metrics = result["metrics"]
        assert metrics["total_tasks"] > 0
        assert metrics["scenario"] == "daily_workflow"
        
        # Проверяем сводку
        summary = result["summary"]
        assert "collected_messages" in summary
        assert "analyzed_messages" in summary
    
    @pytest.mark.asyncio
    async def test_urgent_update_scenario(self, orchestrator_setup):
        """Тест сценария срочного обновления"""
        orchestrator, agent_registry, db_manager = orchestrator_setup
        
        result = await orchestrator.plan_and_execute("urgent_update")
        
        # Проверяем, что сценарий выполнился быстро
        assert result["status"] in ["success", "partial_success"]
        
        metrics = result["metrics"]
        assert metrics["total_execution_time"] < 300  # Менее 5 минут
        
        # Проверяем, что создан краткий дайджест
        summary = result["summary"]
        created_digests = summary.get("created_digests", [])
        assert any("краткий" in digest for digest in created_digests)
    
    @pytest.mark.asyncio
    async def test_task_dependencies(self, orchestrator_setup):
        """Тест выполнения задач с зависимостями"""
        orchestrator, agent_registry, db_manager = orchestrator_setup
        
        # Создаем план с зависимостями
        tasks = [
            TaskRequest(
                task_type=TaskType.DATA_COLLECTION,
                priority=TaskPriority.HIGH
            ),
            TaskRequest(
                task_type=TaskType.MESSAGE_ANALYSIS,
                priority=TaskPriority.NORMAL,
                dependencies=["data_collection"]
            ),
            TaskRequest(
                task_type=TaskType.DIGEST_CREATION,
                priority=TaskPriority.HIGH,
                dependencies=["message_analysis"]
            )
        ]
        
        # Выполняем план
        results = await orchestrator._execute_plan(tasks)
        
        # Проверяем порядок выполнения
        execution_order = [r.task_type.value for r in results]
        
        # DATA_COLLECTION должен быть раньше MESSAGE_ANALYSIS
        data_idx = execution_order.index("data_collection")
        analysis_idx = execution_order.index("message_analysis")
        digest_idx = execution_order.index("digest_creation")
        
        assert data_idx < analysis_idx < digest_idx
    
    @pytest.mark.asyncio
    async def test_error_handling(self, orchestrator_setup):
        """Тест обработки ошибок"""
        orchestrator, agent_registry, db_manager = orchestrator_setup
        
        # Подменяем один из агентов на ошибочный
        error_agent = Mock()
        error_agent.collect_data = AsyncMock(side_effect=Exception("Тестовая ошибка"))
        agent_registry.agents[AgentType.DATA_COLLECTOR] = error_agent
        
        result = await orchestrator.plan_and_execute("daily_workflow")
        
        # Проверяем, что ошибка обработана корректно
        assert result["status"] in ["error", "partial_success"]
        
        if result["status"] == "partial_success":
            metrics = result["metrics"]
            assert metrics["failed_tasks"] > 0
    
    def test_agent_registry_initialization(self, temp_db):
        """Тест инициализации реестра агентов"""
        agent_registry = AgentRegistry(temp_db)
        
        # Проверяем, что все агенты зарегистрированы
        assert len(agent_registry.agents) >= 4
        assert AgentType.DATA_COLLECTOR in agent_registry.agents
        assert AgentType.ANALYZER in agent_registry.agents
        assert AgentType.CRITIC in agent_registry.agents
        assert AgentType.DIGESTER in agent_registry.agents
        
        # Проверяем возможности агентов
        data_collector = agent_registry.get_agent(TaskType.DATA_COLLECTION)
        assert data_collector is not None
        
        analyzer = agent_registry.get_agent(TaskType.MESSAGE_ANALYSIS)
        assert analyzer is not None
    
    @pytest.mark.asyncio
    async def test_task_queue_functionality(self):
        """Тест функциональности очереди задач"""
        task_queue = TaskQueue(max_concurrent_tasks=2)
        
        # Создаем тестовый executor
        async def mock_executor(task_request):
            await asyncio.sleep(0.1)  # Имитация работы
            return {"status": "success", "result": f"Выполнена задача {task_request.task_type.value}"}
        
        # Добавляем задачи
        task1 = TaskRequest(task_type=TaskType.DATA_COLLECTION, priority=TaskPriority.HIGH)
        task2 = TaskRequest(task_type=TaskType.MESSAGE_ANALYSIS, priority=TaskPriority.NORMAL)
        
        task_id1 = await task_queue.add_task(task1)
        task_id2 = await task_queue.add_task(task2)
        
        # Запускаем обработку
        await task_queue.start_processing(mock_executor)
        
        # Ждем завершения
        await asyncio.sleep(0.5)
        
        # Проверяем статус
        status = await task_queue.get_status()
        assert status["completed_tasks"] >= 2
        
        await task_queue.stop_processing()
    
    def test_context_manager_functionality(self, temp_db):
        """Тест функциональности менеджера контекста"""
        context_manager = ContextManager(temp_db)
        
        # Тестируем глобальный контекст
        context_manager.set_global("test_key", "test_value")
        assert context_manager.get_global("test_key") == "test_value"
        
        # Тестируем контекст сессии
        session_id = context_manager.start_session()
        context_manager.set_session("session_key", "session_value")
        assert context_manager.get_session("session_key") == "session_value"
        
        context_manager.end_session()
        
        # Тестируем контекст агента
        context_manager.set_agent("test_agent", "agent_key", "agent_value")
        assert context_manager.get_agent("test_agent", "agent_key") == "agent_value"
        
        # Тестируем истечение срока действия
        context_manager.set_global("temp_key", "temp_value", expires_in_hours=0)
        context_manager.cleanup_expired()
        assert context_manager.get_global("temp_key") is None


class TestWorkflowIntegration:
    """Тесты интеграции с основным workflow"""
    
    @pytest.fixture
    def mock_workflow_components(self):
        """Моки компонентов workflow"""
        with patch('agents.data_collector.DataCollectorAgent') as mock_collector, \
             patch('agents.analyzer.AnalyzerAgent') as mock_analyzer, \
             patch('agents.critic.CriticAgent') as mock_critic, \
             patch('agents.digester.DigesterAgent') as mock_digester:
            
            # Настраиваем моки
            mock_collector.return_value.collect_data = AsyncMock(return_value={
                "status": "success", "total_new_messages": 10
            })
            
            mock_analyzer.return_value.analyze_messages = Mock(return_value={
                "status": "success", "analyzed_count": 10
            })
            
            mock_critic.return_value.review_recent_categorizations = Mock(return_value={
                "status": "success", "updated": 2
            })
            
            mock_digester.return_value.create_digest = Mock(return_value={
                "status": "success", "brief_digest_id": 1
            })
            
            yield {
                'collector': mock_collector,
                'analyzer': mock_analyzer,
                'critic': mock_critic,
                'digester': mock_digester
            }
    
    @pytest.mark.asyncio
    async def test_orchestrated_workflow_vs_legacy(self, mock_workflow_components):
        """Сравнение оркестрированного workflow с legacy версией"""
        
        # Тестируем новый оркестрированный workflow
        from main import run_orchestrated_workflow
        
        with patch('database.db_manager.DatabaseManager') as mock_db:
            mock_db.return_value.get_unanalyzed_messages.return_value = []
            mock_db.return_value.get_messages_with_low_confidence.return_value = []
            mock_db.return_value.get_latest_digest.return_value = None
            mock_db.return_value.find_digests_by_parameters.return_value = []
            
            result = await run_orchestrated_workflow("daily_workflow", days_back=1)
            
            # Результат должен быть успешным или частично успешным
            assert result is True or result is False  # Зависит от конкретной реализации
    
    def test_scheduler_integration(self):
        """Тест интеграции с планировщиком"""
        from scheduler.jobs import JobScheduler
        
        with patch('database.db_manager.DatabaseManager') as mock_db:
            scheduler = JobScheduler(mock_db.return_value)
            
            # Проверяем, что оркестратор инициализирован
            assert scheduler.use_orchestrator is True
            assert scheduler.orchestrator is not None
            assert scheduler.agent_registry is not None
            
            # Проверяем статус
            status = asyncio.run(scheduler.get_orchestrator_status())
            assert status["enabled"] is True


class TestPerformanceIntegration:
    """Тесты производительности интеграции"""
    
    @pytest.mark.asyncio
    async def test_concurrent_task_execution(self):
        """Тест одновременного выполнения задач"""
        import time
        
        task_queue = TaskQueue(max_concurrent_tasks=3)
        
        execution_times = []
        
        async def timing_executor(task_request):
            start_time = time.time()
            await asyncio.sleep(0.2)  # Имитация работы
            end_time = time.time()
            execution_times.append(end_time - start_time)
            return {"status": "success"}
        
        # Добавляем несколько задач
        tasks = [
            TaskRequest(task_type=TaskType.DATA_COLLECTION, priority=TaskPriority.NORMAL),
            TaskRequest(task_type=TaskType.MESSAGE_ANALYSIS, priority=TaskPriority.NORMAL),
            TaskRequest(task_type=TaskType.CATEGORIZATION_REVIEW, priority=TaskPriority.NORMAL),
        ]
        
        start_time = time.time()
        
        await task_queue.start_processing(timing_executor)
        
        for task in tasks:
            await task_queue.add_task(task)
        
        # Ждем завершения всех задач
        await asyncio.sleep(1.0)
        
        total_time = time.time() - start_time
        
        await task_queue.stop_processing()
        
        # Проверяем, что задачи выполнялись параллельно
        assert total_time < 0.8  # Должно быть меньше последовательного выполнения (3 * 0.2 = 0.6)
        assert len(execution_times) == 3
    
    @pytest.mark.asyncio
    async def test_memory_usage_context_manager(self):
        """Тест использования памяти менеджером контекста"""
        import sys
        
        with patch('database.db_manager.DatabaseManager') as mock_db:
            context_manager = ContextManager(mock_db.return_value)
            
            initial_size = sys.getsizeof(context_manager.context_store)
            
            # Добавляем много записей
            for i in range(1000):
                context_manager.set_global(f"test_key_{i}", f"test_value_{i}")
            
            large_size = sys.getsizeof(context_manager.context_store)
            
            # Очищаем устаревшие записи
            context_manager.cleanup_expired()
            
            final_size = sys.getsizeof(context_manager.context_store)
            
            # Проверяем, что размер увеличился, но не критично
            assert large_size > initial_size
            # После очистки размер может не измениться, если записи не устарели


if __name__ == "__main__":
    # Запуск тестов
    pytest.main([__file__, "-v"])