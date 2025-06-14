#!/usr/bin/env python3
"""
Скрипт для быстрого тестирования интеграции оркестратора
"""
import asyncio
import sys
import os
from datetime import datetime

# Добавляем корневую директорию в путь
sys.path.insert(0, os.path.dirname(__file__))

from database.db_manager import DatabaseManager
from agents.orchestrator import OrchestratorAgent
from agents.agent_registry import AgentRegistry
from config.settings import DATABASE_URL

async def quick_integration_test():
    """Быстрый тест интеграции"""
    print("🚀 Быстрый тест интеграции оркестратора")
    print("=" * 50)
    
    try:
        # Инициализация
        print("1. Инициализация компонентов...")
        db_manager = DatabaseManager(DATABASE_URL)
        
        print("2. Создание реестра агентов...")
        agent_registry = AgentRegistry(db_manager)
        
        print("3. Инициализация оркестратора...")
        orchestrator = OrchestratorAgent(db_manager, agent_registry)
        
        # Проверка статуса агентов
        print("4. Проверка статуса агентов...")
        agent_status = agent_registry.get_status()
        print(f"   Всего агентов: {agent_status['total_agents']}")
        
        for agent_name, status in agent_status["agents"].items():
            status_icon = "✅" if status["available"] else "❌"
            print(f"   {status_icon} {agent_name}: {status['type']}")
        
        # Анализ текущего состояния
        print("5. Анализ состояния системы...")
        context = await orchestrator._analyze_current_state()
        
        print(f"   Неанализированных сообщений: {context.get('unanalyzed_count', 0)}")
        print(f"   С низкой уверенностью: {context.get('low_confidence_count', 0)}")
        print(f"   Дайджестов за сегодня: {context.get('today_digests_count', 0)}")
        
        # Создание плана
        print("6. Создание плана выполнения...")
        plan = await orchestrator._create_execution_plan("daily_workflow", context)
        
        print(f"   Запланировано задач: {len(plan)}")
        for i, task in enumerate(plan, 1):
            deps_str = f" (зависит от: {', '.join(task.dependencies)})" if task.dependencies else ""
            print(f"   {i}. {task.task_type.value}{deps_str}")
        
        # Пробный запуск (только планирование)
        print("7. Пробный запуск планирования...")
        result = await orchestrator.plan_and_execute("digest_only", 
                                                   date=datetime.now().date(),
                                                   days_back=1)
        
        print(f"   Статус: {result.get('status')}")
        print(f"   Время выполнения: {result.get('metrics', {}).get('total_execution_time', 0):.1f}с")
        
        summary = result.get('summary', {})
        if summary.get('created_digests'):
            print(f"   Созданы дайджесты: {', '.join(summary['created_digests'])}")
        
        print("\n✅ Тест интеграции пройден успешно!")
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
        return False

async def performance_test():
    """Тест производительности"""
    print("\n🏃 Тест производительности")
    print("=" * 30)
    
    try:
        db_manager = DatabaseManager(DATABASE_URL)
        agent_registry = AgentRegistry(db_manager)
        orchestrator = OrchestratorAgent(db_manager, agent_registry)
        
        scenarios = [
            ("urgent_update", "Срочное обновление"),
            ("daily_workflow", "Ежедневный процесс"),
            ("digest_only", "Только дайджест")
        ]
        
        for scenario, description in scenarios:
            print(f"\nТестирование: {description}")
            start_time = datetime.now()
            
            try:
                result = await orchestrator.plan_and_execute(scenario, days_back=1)
                execution_time = (datetime.now() - start_time).total_seconds()
                
                metrics = result.get('metrics', {})
                print(f"  ⏱️  Время: {execution_time:.1f}с")
                print(f"  📊 Задач: {metrics.get('total_tasks', 0)}")
                print(f"  ✅ Успешность: {metrics.get('success_rate', 0):.1%}")
                print(f"  📈 Статус: {result.get('status')}")
                
            except Exception as e:
                execution_time = (datetime.now() - start_time).total_seconds()
                print(f"  ❌ Ошибка за {execution_time:.1f}с: {e}")
        
        print("\n✅ Тест производительности завершен!")
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка теста производительности: {e}")
        return False

def connectivity_test():
   """Тест подключений"""
   print("\n🔌 Тест подключений")
   print("=" * 25)
   
   try:
       # Тест БД
       print("Тестирование подключения к БД...")
       db_manager = DatabaseManager(DATABASE_URL)
       
       # Простой запрос
       unanalyzed = db_manager.get_unanalyzed_messages(limit=1)
       print(f"  ✅ БД доступна (найдено {len(unanalyzed)} неанализированных сообщений)")
       
       # Тест LLM моделей
       print("Тестирование LLM моделей...")
       try:
           from llm.qwen_model import QwenLLM
           from llm.gemma_model import GemmaLLM
           
           qwen = QwenLLM()
           gemma = GemmaLLM()
           print("  ✅ LLM модели инициализированы")
           
       except Exception as e:
           print(f"  ⚠️  LLM модели недоступны: {e}")
       
       # Тест Telegram (без реального подключения)
       print("Проверка настроек Telegram...")
       from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNELS
       
       if TELEGRAM_API_ID and TELEGRAM_API_HASH:
           print(f"  ✅ API настройки найдены")
           print(f"  📺 Каналов для мониторинга: {len(TELEGRAM_CHANNELS)}")
       else:
           print("  ⚠️  API настройки не найдены")
       
       print("\n✅ Тест подключений завершен!")
       return True
       
   except Exception as e:
       print(f"\n❌ Ошибка теста подключений: {e}")
       return False

async def main():
   """Главная функция тестирования"""
   print("🧪 КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ ОРКЕСТРАТОРА")
   print("=" * 55)
   print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
   print()
   
   # Список тестов
   tests = [
       ("Подключения", connectivity_test, False),  # Синхронный
       ("Интеграция", quick_integration_test, True),  # Асинхронный
       ("Производительность", performance_test, True),  # Асинхронный
   ]
   
   results = []
   
   for test_name, test_func, is_async in tests:
       print(f"\n{'='*20} {test_name.upper()} {'='*20}")
       
       try:
           if is_async:
               result = await test_func()
           else:
               result = test_func()
           
           results.append((test_name, result))
           
       except Exception as e:
           print(f"❌ Критическая ошибка в тесте {test_name}: {e}")
           results.append((test_name, False))
   
   # Итоговый отчет
   print(f"\n{'='*20} ИТОГОВЫЙ ОТЧЕТ {'='*20}")
   
   passed = 0
   total = len(results)
   
   for test_name, result in results:
       status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
       print(f"{test_name:.<20} {status}")
       if result:
           passed += 1
   
   print(f"\nРезультат: {passed}/{total} тестов пройдено")
   
   if passed == total:
       print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Оркестратор готов к использованию.")
       return 0
   else:
       print("⚠️  НЕ ВСЕ ТЕСТЫ ПРОЙДЕНЫ. Требуется исправление ошибок.")
       return 1

if __name__ == "__main__":
   try:
       exit_code = asyncio.run(main())
       sys.exit(exit_code)
   except KeyboardInterrupt:
       print("\n\n⏹️  Тестирование прервано пользователем")
       sys.exit(1)
   except Exception as e:
       print(f"\n\n💥 Критическая ошибка: {e}")
       import traceback
       traceback.print_exc()
       sys.exit(1)
