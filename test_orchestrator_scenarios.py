"""
Тесты сценариев работы оркестратора
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch

from agents.orchestrator import OrchestratorAgent, TaskType, TaskPriority
from agents.agent_registry import AgentRegistry
from database.db_manager import DatabaseManager

class TestOrchestratorScenarios:
    """Тесты различных сценариев работы оркестратора"""
    
    @pytest.fixture
    def setup_orchestrator(self):
        """Настройка оркестратора для тестов сценариев"""
        mock_db = Mock(spec=DatabaseManager)
        mock_registry = Mock(spec=AgentRegistry)
        
        # Настраиваем моки для анализа состояния
        mock_db.get_unanalyzed_messages.return_value = [Mock() for _ in range(25)]
        mock_db.get_messages_with_low_confidence.return_value = [Mock() for _ in range(10)]
        mock_db.get_latest_digest.return_value = None
        mock_db.find_digests_by_parameters.return_value = []
        
        orchestrator = OrchestratorAgent(mock_db, mock_registry)
        return orchestrator, mock_db, mock_registry
    
    @pytest.mark.asyncio
    async def test_daily_workflow_strategy(self, setup_orchestrator):
        """Тест стратегии ежедневного рабочего процесса"""
        orchestrator, mock_db, mock_registry = setup_orchestrator
        
        # Контекст с неанализированными сообщениями
        context = {
            "unanalyzed_count": 25,
            "low_confidence_count": 10,
            "today_digests_count": 0,
            "needs_data_collection": True
        }
        
        tasks = await orchestrator._create_daily_workflow_strategy(context, days_back=1)
        
        # Проверяем, что созданы правильные задачи
        task_types = [task.task_type for task in tasks]
        
        assert TaskType.DATA_COLLECTION in task_types
        assert TaskType.MESSAGE_ANALYSIS in task_types
        assert TaskType.CATEGORIZATION_REVIEW in task_types
        assert TaskType.DIGEST_CREATION in task_types
        
        # Проверяем зависимости
        analysis_task = next(t for t in tasks if t.task_type == TaskType.MESSAGE_ANALYSIS)
        assert "data_collection" in analysis_task.dependencies
    
    @pytest.mark.asyncio
    async def test_urgent_update_strategy(self, setup_orchestrator):
        """Тест стратегии срочного обновления"""
        orchestrator, mock_db, mock_registry = setup_orchestrator
        
        context = {
            "unanalyzed_count": 5,
            "low_confidence_count": 2,
            "today_digests_count": 1,
            "needs_data_collection": True
        }
        
        tasks = await orchestrator._create_urgent_update_strategy(context)
        
        # Проверяем приоритеты и таймауты
        for task in tasks:
            assert task.priority == TaskPriority.CRITICAL
            assert task.timeout <= 180  # Максимум 3 минуты
        
        # Проверяем, что создается только краткий дайджест
        digest_task = next((t for t in tasks if t.task_type == TaskType.DIGEST_UPDATE), None)
        if digest_task:
            assert digest_task.params.get("digest_type") == "brief"
    
    @pytest.mark.asyncio
    async def test_full_analysis_strategy(self, setup_orchestrator):
        """Тест стратегии полного анализа"""
        orchestrator, mock_db, mock_registry = setup_orchestrator
        
        context = {
            "unanalyzed_count": 100,
            "low_confidence_count": 50,
            "today_digests_count": 0,
            "needs_data_collection": True
        }
        
        tasks = await orchestrator._create_full_analysis_strategy(context, days_back=7, analysis_limit=500)
        
        # Проверяем параметры для полного анализа
        data_task = next(t for t in tasks if t.task_type == TaskType.DATA_COLLECTION)
        assert data_task.params["days_back"] == 7
        assert data_task.params["force_update"] is True
        
        analysis_task = next(t for t in tasks if t.task_type == TaskType.MESSAGE_ANALYSIS)
        assert analysis_task.params["limit"] == 500
        
        review_task = next(t for t in tasks if t.task_type == TaskType.CATEGORIZATION_REVIEW)
        assert review_task.params["confidence_threshold"] == 3  # Более строгий
    
    @pytest.mark.asyncio
    async def test_digest_only_strategy(self, setup_orchestrator):
        """Тест стратегии только создания дайджеста"""
        orchestrator, mock_db, mock_registry = setup_orchestrator
        
        context = {}
        
        tasks = await orchestrator._create_digest_only_strategy(
            context, 
            date=datetime(2024, 1, 15),
            days_back=3,
            digest_type="detailed",
            focus_category="новые законы"
        )
        
        # Должна быть только одна задача
        assert len(tasks) == 1
        
        digest_task = tasks[0]
        assert digest_task.task_type == TaskType.DIGEST_CREATION
        assert digest_task.params["days_back"] == 3
        assert digest_task.params["digest_type"] == "detailed"
        assert digest_task.params["focus_category"] == "новые законы"
    
    @pytest.mark.asyncio
    async def test_adaptive_planning_no_new_messages(self, setup_orchestrator):
        """Тест адаптивного планирования при отсутствии новых сообщений"""
        orchestrator, mock_db, mock_registry = setup_orchestrator
        
        # Контекст без новых сообщений
        mock_db.get_unanalyzed_messages.return_value = []
        mock_db.get_messages_with_low_confidence.return_value = []
        
        context = {
            "unanalyzed_count": 0,
            "low_confidence_count": 0,
            "today_digests_count": 1,
            "needs_data_collection": False
        }
        
        tasks = await orchestrator._create_daily_workflow_strategy(context)
        
        # Не должно быть задач анализа или проверки
        task_types = [task.task_type for task in tasks]
        assert TaskType.MESSAGE_ANALYSIS not in task_types
        assert TaskType.CATEGORIZATION_REVIEW not in task_types
        
        # Должно быть обновление существующего дайджеста
        assert TaskType.DIGEST_UPDATE in task_types
    
    @pytest.mark.asyncio
    async def test_error_recovery_planning(self, setup_orchestrator):
        """Тест планирования при ошибках выполнения"""
        orchestrator, mock_db, mock_registry = setup_orchestrator
        
        # Симулируем ошибку в одной из задач
        def failing_executor(task_request):
            if task_request.task_type == TaskType.DATA_COLLECTION:
                raise Exception("Ошибка сбора данных")
            return {"status": "success"}
        
        mock_registry.get_agent.return_value.collect_data = AsyncMock(side_effect=Exception("Тест ошибки"))
        
        context = {
            "unanalyzed_count": 10,
            "low_confidence_count": 5,
            "today_digests_count": 0,
            "needs_data_collection": True
        }
        
        result = await orchestrator.plan_and_execute("daily_workflow")
        
        # Проверяем, что система обработала ошибку
        assert result["status"] in ["error", "partial_success"]
        
        if result["status"] == "partial_success":
            # Должны быть рекомендации по повторному выполнению
            recommendations = result.get("recommendations", [])
            retry_recommendations = [r for r in recommendations if r.get("type") == "retry_failed"]
            assert len(retry_recommendations) > 0


class TestOrchestratorDecisionMaking:
    """Тесты принятия решений оркестратором"""
    
    @pytest.fixture
    def decision_orchestrator(self):
        """Оркестратор для тестов принятия решений"""
        mock_db = Mock()
        mock_registry = Mock()
        
        orchestrator = OrchestratorAgent(mock_db, mock_registry)
        return orchestrator
    
    @pytest.mark.asyncio
    async def test_priority_based_execution(self, decision_orchestrator):
        """Тест выполнения задач на основе приоритетов"""
        from agents.orchestrator import TaskRequest, TaskType, TaskPriority
        
        # Создаем задачи с разными приоритетами
        high_priority_task = TaskRequest(
            task_type=TaskType.DIGEST_CREATION,
            priority=TaskPriority.CRITICAL
        )
        
        low_priority_task = TaskRequest(
            task_type=TaskType.CATEGORIZATION_REVIEW,
            priority=TaskPriority.LOW
        )
        
        normal_priority_task = TaskRequest(
            task_type=TaskType.MESSAGE_ANALYSIS,
            priority=TaskPriority.NORMAL
        )
        
        # Симулируем успешное выполнение
        decision_orchestrator.agent_registry.get_agent = Mock(return_value=Mock())
        decision_orchestrator._execute_single_task = AsyncMock(return_value={"status": "success"})
        
        tasks = [low_priority_task, high_priority_task, normal_priority_task]
        results = await decision_orchestrator._execute_plan(tasks)
        
        # Проверяем, что задачи выполнились
        assert len(results) == 3
        
        # Проверяем, что все задачи помечены как выполненные
        successful_results = [r for r in results if r.status.value == "completed"]
        assert len(successful_results) == 3
    
    @pytest.mark.asyncio
    async def test_context_aware_decision_making(self, decision_orchestrator):
        """Тест принятия решений на основе контекста"""
        
        # Контекст с большим количеством неанализированных сообщений
        high_load_context = {
            "unanalyzed_count": 200,
            "low_confidence_count": 50,
            "today_digests_count": 0,
            "needs_data_collection": True
        }
        
        tasks_high_load = await decision_orchestrator._create_daily_workflow_strategy(high_load_context)
        
        # Должны быть созданы задачи с большими лимитами
        analysis_task = next(t for t in tasks_high_load if t.task_type == TaskType.MESSAGE_ANALYSIS)
        assert analysis_task.params["limit"] >= 100
        
        # Контекст с малым количеством сообщений
        low_load_context = {
            "unanalyzed_count": 5,
            "low_confidence_count": 2,
            "today_digests_count": 1,
            "needs_data_collection": False
        }
        
        tasks_low_load = await decision_orchestrator._create_daily_workflow_strategy(low_load_context)
        
        # Должно быть меньше задач
        assert len(tasks_low_load) <= len(tasks_high_load)
    
    @pytest.mark.asyncio
    async def test_recommendations_generation(self, decision_orchestrator):
        """Тест генерации рекомендаций"""
        from agents.orchestrator import TaskResult, TaskStatus, TaskType
        
        # Создаем результаты с разными статусами
        results = [
            TaskResult(
                task_id="1",
                task_type=TaskType.DATA_COLLECTION,
                status=TaskStatus.COMPLETED,
                result={"total_new_messages": 100}  # Много новых сообщений
            ),
            TaskResult(
                task_id="2",
                task_type=TaskType.MESSAGE_ANALYSIS,
                status=TaskStatus.FAILED,
                error="Ошибка анализа"
            ),
            TaskResult(
                task_id="3",
                task_type=TaskType.CATEGORIZATION_REVIEW,
                status=TaskStatus.COMPLETED,
                result={"updated": 25}  # Много обновлений критиком
            )
        ]
        
        final_result = await decision_orchestrator._analyze_results_and_decide(results, "daily_workflow")
        
        recommendations = final_result.get("recommendations", [])
        
        # Должна быть рекомендация повторить неудачную задачу
        retry_recommendations = [r for r in recommendations if r.get("type") == "retry_failed"]
        assert len(retry_recommendations) > 0
        
        # Должна быть рекомендация расширенного анализа (много новых сообщений)
        extended_analysis = [r for r in recommendations if r.get("type") == "extended_analysis"]
        assert len(extended_analysis) > 0
        
        # Должна быть рекомендация проверки качества (много исправлений критиком)
        quality_check = [r for r in recommendations if r.get("type") == "quality_check"]
        assert len(quality_check) > 0


class TestOrchestratorRealWorldScenarios:
   """Тесты реальных сценариев использования оркестратора"""
   
   @pytest.fixture
   def real_world_setup(self):
       """Настройка для реальных сценариев"""
       mock_db = Mock()
       mock_registry = Mock()
       
       # Создаем реалистичные моки агентов
       mock_agents = {
           'data_collector': Mock(),
           'analyzer': Mock(),
           'critic': Mock(),
           'digester': Mock()
       }
       
       # Настраиваем realistic поведение
       mock_agents['data_collector'].collect_data = AsyncMock()
       mock_agents['analyzer'].analyze_messages = Mock()
       mock_agents['critic'].review_recent_categorizations = Mock()
       mock_agents['digester'].create_digest = Mock()
       mock_agents['digester'].update_digests_for_date = Mock()
       
       mock_registry.get_agent.side_effect = lambda task_type: {
           TaskType.DATA_COLLECTION: mock_agents['data_collector'],
           TaskType.MESSAGE_ANALYSIS: mock_agents['analyzer'],
           TaskType.CATEGORIZATION_REVIEW: mock_agents['critic'],
           TaskType.DIGEST_CREATION: mock_agents['digester'],
           TaskType.DIGEST_UPDATE: mock_agents['digester']
       }.get(task_type)
       
       orchestrator = OrchestratorAgent(mock_db, mock_registry)
       return orchestrator, mock_db, mock_registry, mock_agents
   
   @pytest.mark.asyncio
   async def test_morning_startup_scenario(self, real_world_setup):
       """Тест сценария утреннего запуска системы"""
       orchestrator, mock_db, mock_registry, mock_agents = real_world_setup
       
       # Симулируем состояние после ночного перерыва
       mock_db.get_unanalyzed_messages.return_value = [Mock() for _ in range(45)]  # Накопилось сообщений
       mock_db.get_messages_with_low_confidence.return_value = [Mock() for _ in range(15)]
       mock_db.get_latest_digest.return_value = Mock(date=datetime.now() - timedelta(days=1))
       mock_db.find_digests_by_parameters.return_value = []  # Нет сегодняшних дайджестов
       
       # Настраиваем ответы агентов
       mock_agents['data_collector'].collect_data.return_value = {
           "status": "success",
           "total_new_messages": 35,
           "channels_stats": {"@dumainfo": 20, "@sovfedinfo": 15}
       }
       
       mock_agents['analyzer'].analyze_messages.return_value = {
           "status": "success",
           "analyzed_count": 45,
           "categories": {"новые законы": 20, "поправки к законам": 15, "другое": 10}
       }
       
       mock_agents['critic'].review_recent_categorizations.return_value = {
           "status": "success",
           "total": 45,
           "updated": 8,
           "unchanged": 37
       }
       
       mock_agents['digester'].create_digest.return_value = {
           "status": "success",
           "brief_digest_id": 1,
           "detailed_digest_id": 2,
           "total_messages": 45
       }
       
       # Запускаем утренний сценарий
       result = await orchestrator.plan_and_execute("daily_workflow", days_back=1)
       
       # Проверяем результаты
       assert result["status"] == "success"
       
       summary = result["summary"]
       assert summary["collected_messages"] == 35
       assert summary["analyzed_messages"] == 45
       assert summary["reviewed_messages"] == 8
       assert len(summary["created_digests"]) == 2
       
       # Проверяем, что все агенты были вызваны
       mock_agents['data_collector'].collect_data.assert_called_once()
       mock_agents['analyzer'].analyze_messages.assert_called_once()
       mock_agents['critic'].review_recent_categorizations.assert_called_once()
       mock_agents['digester'].create_digest.assert_called_once()
   
   @pytest.mark.asyncio
   async def test_midday_update_scenario(self, real_world_setup):
       """Тест сценария обновления в середине дня"""
       orchestrator, mock_db, mock_registry, mock_agents = real_world_setup
       
       # Симулируем состояние в середине дня (есть утренний дайджест)
       mock_db.get_unanalyzed_messages.return_value = [Mock() for _ in range(12)]  # Немного новых
       mock_db.get_messages_with_low_confidence.return_value = [Mock() for _ in range(3)]
       mock_db.find_digests_by_parameters.return_value = [
           {"id": 1, "digest_type": "brief", "date": datetime.now()}
       ]
       
       # Настраиваем ответы для обновления
       mock_agents['data_collector'].collect_data.return_value = {
           "status": "success",
           "total_new_messages": 8,
           "channels_stats": {"@dumainfo": 5, "@sovfedinfo": 3}
       }
       
       mock_agents['analyzer'].analyze_messages.return_value = {
           "status": "success",
           "analyzed_count": 12,
           "categories": {"поправки к законам": 7, "другое": 5}
       }
       
       mock_agents['critic'].review_recent_categorizations.return_value = {
           "status": "success",
           "total": 12,
           "updated": 2,
           "unchanged": 10
       }
       
       mock_agents['digester'].update_digests_for_date.return_value = {
           "status": "success",
           "updated_digests": [{"digest_id": 1, "digest_type": "brief"}]
       }
       
       # Запускаем срочное обновление
       result = await orchestrator.plan_and_execute("urgent_update")
       
       # Проверяем результаты
       assert result["status"] == "success"
       
       summary = result["summary"]
       assert summary["collected_messages"] == 8
       assert summary["analyzed_messages"] == 12
       assert len(summary["updated_digests"]) == 1
       
       # Проверяем, что использовалось обновление, а не создание
       mock_agents['digester'].update_digests_for_date.assert_called_once()
   
   @pytest.mark.asyncio
   async def test_weekend_maintenance_scenario(self, real_world_setup):
       """Тест сценария выходного обслуживания"""
       orchestrator, mock_db, mock_registry, mock_agents = real_world_setup
       
       # Симулируем накопление за выходные
       mock_db.get_unanalyzed_messages.return_value = [Mock() for _ in range(150)]
       mock_db.get_messages_with_low_confidence.return_value = [Mock() for _ in range(40)]
       mock_db.get_latest_digest.return_value = Mock(date=datetime.now() - timedelta(days=2))
       
       # Настраиваем ответы для полного анализа
       mock_agents['data_collector'].collect_data.return_value = {
           "status": "success",
           "total_new_messages": 120,
           "channels_stats": {"@dumainfo": 60, "@sovfedinfo": 40, "@vsrf_ru": 20}
       }
       
       mock_agents['analyzer'].analyze_messages.return_value = {
           "status": "success",
           "analyzed_count": 150,
           "categories": {
               "новые законы": 45,
               "поправки к законам": 35,
               "судебная практика": 25,
               "законодательные инициативы": 30,
               "другое": 15
           }
       }
       
       mock_agents['critic'].review_recent_categorizations.return_value = {
           "status": "success",
           "total": 150,
           "updated": 25,
           "unchanged": 125
       }
       
       mock_agents['digester'].create_digest.return_value = {
           "status": "success",
           "brief_digest_id": 3,
           "detailed_digest_id": 4,
           "total_messages": 150
       }
       
       # Запускаем полный анализ
       result = await orchestrator.plan_and_execute("full_analysis", days_back=3, analysis_limit=200)
       
       # Проверяем результаты
       assert result["status"] == "success"
       
       summary = result["summary"]
       assert summary["collected_messages"] == 120
       assert summary["analyzed_messages"] == 150
       assert summary["reviewed_messages"] == 25
       
       # Проверяем рекомендации
       recommendations = result.get("recommendations", [])
       extended_analysis_recs = [r for r in recommendations if r.get("type") == "extended_analysis"]
       assert len(extended_analysis_recs) > 0  # Должна быть рекомендация из-за большого объема
   
   @pytest.mark.asyncio
   async def test_system_overload_scenario(self, real_world_setup):
       """Тест сценария перегрузки системы"""
       orchestrator, mock_db, mock_registry, mock_agents = real_world_setup
       
       # Симулируем перегрузку
       mock_db.get_unanalyzed_messages.return_value = [Mock() for _ in range(500)]
       mock_db.get_messages_with_low_confidence.return_value = [Mock() for _ in range(100)]
       
       # Симулируем медленную работу агентов
       mock_agents['data_collector'].collect_data = AsyncMock(side_effect=asyncio.TimeoutError())
       mock_agents['analyzer'].analyze_messages.side_effect = Exception("Переполнение буфера")
       
       result = await orchestrator.plan_and_execute("daily_workflow")
       
       # Система должна обработать ошибки
       assert result["status"] in ["error", "partial_success"]
       
       # Должны быть рекомендации по восстановлению
       recommendations = result.get("recommendations", [])
       assert len(recommendations) > 0
   
   @pytest.mark.asyncio
   async def test_holiday_period_scenario(self, real_world_setup):
       """Тест сценария праздничного периода (мало активности)"""
       orchestrator, mock_db, mock_registry, mock_agents = real_world_setup
       
       # Симулируем низкую активность
       mock_db.get_unanalyzed_messages.return_value = [Mock() for _ in range(3)]
       mock_db.get_messages_with_low_confidence.return_value = []
       mock_db.find_digests_by_parameters.return_value = [
           {"id": 1, "digest_type": "brief", "date": datetime.now()}
       ]
       
       mock_agents['data_collector'].collect_data.return_value = {
           "status": "success",
           "total_new_messages": 2,
           "channels_stats": {"@dumainfo": 2}
       }
       
       mock_agents['analyzer'].analyze_messages.return_value = {
           "status": "success",
           "analyzed_count": 3,
           "categories": {"другое": 3}
       }
       
       # Критик не должен запускаться (нет сообщений с низкой уверенностью)
       
       mock_agents['digester'].update_digests_for_date.return_value = {
           "status": "success",
           "updated_digests": [{"digest_id": 1, "digest_type": "brief"}]
       }
       
       result = await orchestrator.plan_and_execute("daily_workflow")
       
       # Проверяем оптимизированное выполнение
       assert result["status"] == "success"
       
       # Критик не должен вызываться
       mock_agents['critic'].review_recent_categorizations.assert_not_called()
       
       # Должно быть обновление, а не создание нового дайджеста
       mock_agents['digester'].update_digests_for_date.assert_called_once()
       mock_agents['digester'].create_digest.assert_not_called()


if __name__ == "__main__":
   pytest.main([__file__, "-v", "--tb=short"])