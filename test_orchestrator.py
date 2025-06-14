import asyncio
from database.db_manager import DatabaseManager
from agents.agent_registry import AgentRegistry
from agents.orchestrator import OrchestratorAgent
from config.settings import DATABASE_URL

async def test_orchestrator():
    try:
        print('🎭 Тестирование оркестратора...')
        
        # Инициализация
        db = DatabaseManager(DATABASE_URL)
        registry = AgentRegistry(db)
        orchestrator = OrchestratorAgent(db, registry)
        print('✅ Оркестратор инициализирован')
        
        # Анализ состояния
        print('🔍 Анализ состояния системы...')
        context = await orchestrator._analyze_current_state()
        
        print(f'📊 Неанализированных сообщений: {context.get("unanalyzed_count", 0)}')
        print(f'📊 С низкой уверенностью: {context.get("low_confidence_count", 0)}')
        print(f'📊 Дайджестов за сегодня: {context.get("today_digests_count", 0)}')
        
        # Создание плана
        print('📋 Создание плана выполнения...')
        plan = await orchestrator._create_execution_plan("daily_workflow", context)
        
        print(f'📝 Запланировано задач: {len(plan)}')
        for i, task in enumerate(plan, 1):
            deps = f' (зависит от: {", ".join(task.dependencies)})' if task.dependencies else ''
            print(f'  {i}. {task.task_type.value}{deps}')
        
        print('✅ Тест оркестратора пройден успешно!')
        return True
        
    except Exception as e:
        print(f'❌ Ошибка в тесте оркестратора: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    result = asyncio.run(test_orchestrator())
    if result:
        print('\n🎉 ВСЕ БАЗОВЫЕ ТЕСТЫ ПРОЙДЕНЫ!')
    else:
        print('\n💥 Есть проблемы, требующие исправления')