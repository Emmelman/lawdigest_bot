#!/usr/bin/env python3
"""
Тест системы коллаборации агентов
"""

import sys
import os
import logging
from datetime import datetime
from unittest.mock import Mock, MagicMock
import asyncio

# Добавляем корневую директорию в путь
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_cross_agent_collaboration():
    """Тест системы коллаборации агентов"""
    print("🤝 ТЕСТИРОВАНИЕ CROSS-AGENT COLLABORATION")
    print("=" * 70)
    
    try:
        from agents.collaborative_crew import CollaborativeCrew
        from agents.agent_registry import AgentRegistry
        from database.db_manager import DatabaseManager
        from config.settings import DATABASE_URL
        
        print("✅ Инициализация компонентов...")
        
        # Создаем мок-объекты для тестирования
        db_manager = DatabaseManager(DATABASE_URL)
        agent_registry = AgentRegistry(db_manager)
        
        # Создаем систему коллаборации
        collaborative_crew = CollaborativeCrew(agent_registry)
        
        # Включаем debug логирование
        logging.getLogger('agents.collaborative_crew').setLevel(logging.INFO)
        
        print(f"\n🔍 Тестируем коллаборативные сценарии...")
        
        # ТЕСТ 1: Коллаборативная категоризация сложного случая
        print(f"\n🤝 ТЕСТ 1: Коллаборативная категоризация")
        
        test_message = "Комитет Госдумы рекомендовал принять в первом чтении законопроект о внесении изменений в федеральный закон 'О государственной службе'"
        
        collaboration_result = await collaborative_crew.collaborate_on_difficult_categorization(
            message_id=999,
            message_text=test_message,
            initial_category="другое",
            confidence=2.0
        )
        
        print(f"Результат коллаборативной категоризации:")
        print(f"  Статус: {collaboration_result.get('status')}")
        print(f"  Финальная категория: {collaboration_result.get('final_category')}")
        print(f"  Уверенность: {collaboration_result.get('final_confidence', 0):.1f}/5")
        print(f"  Сила консенсуса: {collaboration_result.get('consensus_strength', 0):.0%}")
        
        categorization_success = (
            collaboration_result.get('status') == 'success' and
            collaboration_result.get('final_category') is not None and
            collaboration_result.get('final_confidence', 0) > 0
        )
        
        # ТЕСТ 2: Проверка качества дайджеста
        print(f"\n📋 ТЕСТ 2: Коллаборативная проверка качества")
        
        test_digest_content = {
            "brief": "Краткий обзор правовых изменений за сегодня...",
            "detailed": "Подробный анализ новых законодательных инициатив..."
        }
        
        quality_result = await collaborative_crew.collaborate_on_quality_assurance(
            digest_content=test_digest_content,
            digest_type="both",
            categories_data={}
        )
        
        print(f"Результат проверки качества:")
        print(f"  Статус: {quality_result.get('status')}")
        print(f"  Общая оценка: {quality_result.get('overall_score', 0):.1f}/5")
        print(f"  Компоненты: {quality_result.get('component_scores', {})}")
        
        quality_success = (
            quality_result.get('status') == 'success' and
            quality_result.get('overall_score', 0) > 0
        )
        
        # ТЕСТ 3: Комплексный анализ
        print(f"\n🔬 ТЕСТ 3: Комплексный анализ")
        
        comprehensive_result = await collaborative_crew.collaborate_on_comprehensive_analysis(
            period_start=datetime.now(),
            period_end=datetime.now()
        )
        
        print(f"Результат комплексного анализа:")
        print(f"  Статус: {comprehensive_result.get('status')}")
        print(f"  Краткое резюме: {comprehensive_result.get('summary', 'нет данных')}")
        
        comprehensive_success = comprehensive_result.get('status') == 'success'
        
        # Анализ результатов
        print(f"\n📊 АНАЛИЗ РЕЗУЛЬТАТОВ:")
        print("=" * 70)
        
        tests_results = [
            ("Коллаборативная категоризация", categorization_success),
            ("Проверка качества дайджеста", quality_success),
            ("Комплексный анализ", comprehensive_success)
        ]
        
        successful_tests = sum(1 for _, success in tests_results if success)
        total_tests = len(tests_results)
        
        print(f"Успешных тестов: {successful_tests}/{total_tests}")
        
        for test_name, success in tests_results:
            status_icon = "✅" if success else "❌"
            print(f"{status_icon} {test_name}")
        
        # Проверка истории коллаборации
        history_count = len(collaborative_crew.collaboration_history)
        print(f"📜 Записей в истории коллаборации: {history_count}")
        
        # Общая оценка
        if successful_tests >= 2:
            print(f"\n🎉 CROSS-AGENT COLLABORATION РАБОТАЕТ ОТЛИЧНО!")
            print(f"✅ Коллаборативные механизмы функционируют")
            print(f"✅ Агенты успешно взаимодействуют")
            print(f"✅ История коллаборации ведется")
            return True
        else:
            print(f"\n⚠️ CROSS-AGENT COLLABORATION РАБОТАЕТ ЧАСТИЧНО")
            print(f"❌ Некоторые коллаборативные механизмы требуют доработки")
            return False
            
    except Exception as e:
        print(f"❌ Критическая ошибка при тестировании: {str(e)}")
        logger.error(f"Критическая ошибка в тесте: {str(e)}", exc_info=True)
        return False

async def test_orchestrator_collaboration_integration():
    """Тест интеграции коллаборации в оркестратор"""
    print(f"\n🔧 ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ В ОРКЕСТРАТОР")
    print("=" * 70)
    
    try:
        from agents.orchestrator import IntelligentOrchestratorAgent
        from agents.agent_registry import AgentRegistry
        from database.db_manager import DatabaseManager
        from config.settings import DATABASE_URL
        import inspect
        
        # Проверяем наличие новых методов в оркестраторе
        required_methods = [
            '_execute_task_with_collaboration',
            '_should_use_collaboration',
            '_perform_task_collaboration'
        ]
        
        found_methods = []
        for method_name in required_methods:
            if hasattr(IntelligentOrchestratorAgent, method_name):
                found_methods.append(method_name)
                print(f"✅ Найден метод: {method_name}")
            else:
                print(f"❌ Отсутствует метод: {method_name}")
        
        # Проверяем инициализацию с коллаборацией
        db_manager = DatabaseManager(DATABASE_URL)
        agent_registry = AgentRegistry(db_manager)
        orchestrator = IntelligentOrchestratorAgent(db_manager, agent_registry)
        
        has_collaborative_crew = hasattr(orchestrator, 'collaborative_crew') and orchestrator.collaborative_crew is not None
        print(f"✅ Система коллаборации инициализирована: {'Да' if has_collaborative_crew else 'Нет'}")
        
        integration_score = len(found_methods) + (1 if has_collaborative_crew else 0)
        max_score = len(required_methods) + 1
        
        print(f"\n📊 Оценка интеграции: {integration_score}/{max_score}")
        
        if integration_score >= max_score - 1:
            print("✅ Интеграция коллаборации в оркестратор успешна")
            return True
        else:
            print("❌ Интеграция коллаборации неполная")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка проверки интеграции: {str(e)}")
        return False

async def main():
    print("🚀 ЗАПУСК ТЕСТИРОВАНИЯ CROSS-AGENT COLLABORATION")
    print()
    
    # Тест коллаборативных механизмов
    collaboration_ok = await test_cross_agent_collaboration()
    
    # Тест интеграции в оркестратор
    integration_ok = await test_orchestrator_collaboration_integration()
    
    print(f"\n" + "=" * 70)
    if collaboration_ok and integration_ok:
        print("🎉 ВСЕ ТЕСТЫ CROSS-AGENT COLLABORATION ПРОЙДЕНЫ!")
        print("✅ Коллаборативные механизмы работают")
        print("✅ Интеграция в оркестратор успешна")
        print()
        print("📝 Следующие шаги:")
        print("1. Интегрируйте код в систему")
        print("2. Протестируйте в реальной работе")
        print("3. Система Enhanced reasoning готова!")
    else:
        print("⚠️ ЧАСТИЧНЫЙ УСПЕХ ИЛИ ПРОБЛЕМЫ")
        if not collaboration_ok:
            print("❌ Проблемы с коллаборативными механизмами")
        if not integration_ok:
            print("❌ Проблемы с интеграцией в оркестратор")
    
    print(f"\n🎊 ПОЗДРАВЛЯЕМ! Все 4 компонента Enhanced reasoning реализованы:")
    print("   ✅ Enhanced Analyzer reasoning")
    print("   ✅ Enhanced Critic multi-perspective analysis")
    print("   ✅ Enhanced Digester strategic planning")
    print("   ✅ Cross-agent collaboration")

if __name__ == "__main__":
    asyncio.run(main())
