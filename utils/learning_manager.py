# utils/learning_manager.py
"""
Менеджер обучающих примеров для работы с примерами категоризации
"""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import threading

logger = logging.getLogger(__name__)

class LearningExamplesManager:
    """Менеджер для работы с обучающими примерами категоризации новостей"""
    
    def __init__(self, examples_dir="learning_examples", max_examples_per_category=200):
        self.examples_dir = examples_dir
        self.max_examples_per_category = max_examples_per_category
        self.examples_file = os.path.join(examples_dir, "examples.jsonl")
        self.examples_by_category = {}  # Кэш примеров по категориям
        self.last_loaded = None  # Время последней загрузки
        self.lock = threading.Lock()  # Для потокобезопасности
        
        # Инициализация директории и начальная загрузка примеров
        os.makedirs(examples_dir, exist_ok=True)
        self._load_examples()
    
    def _load_examples(self) -> None:
        """Загружает примеры из файла в кэш по категориям"""
        with self.lock:
            self.examples_by_category = {}
            
            if not os.path.exists(self.examples_file):
                logger.info(f"Файл примеров {self.examples_file} не существует, будет создан.")
                return
            
            try:
                examples = []
                with open(self.examples_file, "r", encoding="utf-8") as f:
                    for line in f:
                        example = json.loads(line)
                        examples.append(example)
                
                # Группируем примеры по категориям
                for example in examples:
                    category = example.get("category", "другое")
                    if category not in self.examples_by_category:
                        self.examples_by_category[category] = []
                    self.examples_by_category[category].append(example)
                
                # Ограничиваем количество примеров в каждой категории
                for category in self.examples_by_category:
                    if len(self.examples_by_category[category]) > self.max_examples_per_category:
                        # Сортируем по времени и оставляем только последние
                        self.examples_by_category[category].sort(
                            key=lambda x: x.get("timestamp", ""), reverse=True
                        )
                        self.examples_by_category[category] = self.examples_by_category[category][:self.max_examples_per_category]
                
                self.last_loaded = datetime.now()
                logger.info(f"Загружено {sum(len(examples) for examples in self.examples_by_category.values())} примеров из {len(self.examples_by_category)} категорий")
            
            except Exception as e:
                logger.error(f"Ошибка при загрузке примеров: {str(e)}")
                # Создаем пустой кэш в случае ошибки
                self.examples_by_category = {}
    
    def save_example(self, text: str, category: str, justification: str) -> bool:
        """
        Сохраняет новый пример в кэш и файл
        
        Args:
            text (str): Текст примера
            category (str): Категория
            justification (str): Обоснование категоризации
            
        Returns:
            bool: True если сохранение успешно
        """
        with self.lock:
            try:
                # Создаем пример
                example = {
                    "text": text,
                    "category": category,
                    "justification": justification,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Добавляем в кэш
                if category not in self.examples_by_category:
                    self.examples_by_category[category] = []
                
                self.examples_by_category[category].append(example)
                
                # Ограничиваем количество примеров в категории
                if len(self.examples_by_category[category]) > self.max_examples_per_category:
                    self.examples_by_category[category].sort(
                        key=lambda x: x.get("timestamp", ""), reverse=True
                    )
                    self.examples_by_category[category] = self.examples_by_category[category][:self.max_examples_per_category]
                
                # Записываем в файл с ротацией при необходимости
                if self._should_rotate_file():
                    self._rotate_examples_file()
                
                # Добавляем пример в файл
                with open(self.examples_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(example, ensure_ascii=False) + "\n")
                
                logger.debug(f"Сохранен обучающий пример для категории '{category}'")
                return True
            
            except Exception as e:
                logger.error(f"Ошибка при сохранении обучающего примера: {str(e)}")
                return False
    
    def get_examples(self, category=None, limit=5):
        """
        Возвращает примеры для указанной категории или все примеры
        с оптимизацией производительности
        
        Args:
            category (str, optional): Категория для фильтрации примеров
            limit (int): Максимальное количество примеров
            
        Returns:
            list: Список оптимизированных примеров
        """
        # Проверяем, нужно ли обновить кэш (увеличено до 30 минут)
        current_time = datetime.now()
        cache_needs_update = (self.last_loaded is None or 
                            (current_time - self.last_loaded).total_seconds() > 1800)  # 30 минут
        
        # Блокируем только если требуется обновление кэша
        if cache_needs_update:
            with self.lock:
                # Повторная проверка после получения блокировки
                if self.last_loaded is None or (current_time - self.last_loaded).total_seconds() > 1800:
                    self._load_examples()
        
        # Проверяем наличие категории в кэше - этот блок не требует блокировки
        if category and category in self.examples_by_category:
            # Быстрый путь - возвращаем последние примеры из указанной категории
            raw_examples = self.examples_by_category[category][-limit:]
        elif category is None:
            # Получаем примеры из всех категорий (оптимизированная логика)
            raw_examples = []
            categories = list(self.examples_by_category.keys())
            
            if not categories:
                return []
                
            # Определяем количество примеров из каждой категории
            examples_per_category = max(1, limit // len(categories))
            
            # Сразу собираем базовое количество примеров
            for cat in categories:
                if cat in self.examples_by_category and self.examples_by_category[cat]:
                    # Берем только последние примеры из каждой категории
                    cat_examples = self.examples_by_category[cat][-examples_per_category:]
                    raw_examples.extend(cat_examples)
            
            # Если собрали меньше чем нужно, дополняем
            if len(raw_examples) < limit:
                # Собираем категории с наибольшим количеством примеров
                sorted_categories = sorted(
                    categories, 
                    key=lambda c: len(self.examples_by_category.get(c, [])),
                    reverse=True
                )
                
                # Добавляем примеры из категорий с наибольшим количеством примеров
                for cat in sorted_categories:
                    if len(raw_examples) >= limit:
                        break
                        
                    # Находим примеры, которые ещё не добавлены
                    used = set(id(ex) for ex in raw_examples if ex in self.examples_by_category.get(cat, []))
                    available = [ex for ex in self.examples_by_category.get(cat, []) 
                                if id(ex) not in used]
                    
                    # Добавляем нужное количество
                    need_more = limit - len(raw_examples)
                    raw_examples.extend(available[-need_more:] if need_more <= len(available) else available)
            
            # Обрезаем до нужного лимита
            raw_examples = raw_examples[:limit]
        else:
            # Если категория указана, но не найдена - возвращаем пустой список
            return []
        
        # Создаем оптимизированные копии примеров - без лишних циклов
        optimized_examples = []
        for ex in raw_examples:
            # Оптимизируем копирование - копируем только нужные поля
            optimized = {
                'category': ex.get('category', ''),
                'text': ex.get('text', '')[:200] + ('...' if len(ex.get('text', '')) > 200 else ''),
                'justification': ex.get('justification', '')[:100] + ('...' if len(ex.get('justification', '')) > 100 else '')
            }
            
            optimized_examples.append(optimized)
        
        # Возвращаем результат без лишнего логирования
        return optimized_examples
    
    def _should_rotate_file(self) -> bool:
        """Проверяет, нужно ли создать новый файл примеров"""
        if not os.path.exists(self.examples_file):
            return False
        
        # Проверяем размер файла (более 5 МБ)
        file_size = os.path.getsize(self.examples_file)
        return file_size > 5 * 1024 * 1024
    
    def _rotate_examples_file(self) -> None:
        """Создает новую версию файла примеров и архивирует старую"""
        if not os.path.exists(self.examples_file):
            return
        
        try:
            # Создаем имя архивного файла с датой
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            archive_path = os.path.join(self.examples_dir, f"examples_{timestamp}.jsonl.bak")
            
            # Переименовываем текущий файл в архив
            os.rename(self.examples_file, archive_path)
            
            # Записываем текущий кэш в новый файл
            with open(self.examples_file, "w", encoding="utf-8") as f:
                for category, examples in self.examples_by_category.items():
                    for example in examples:
                        f.write(json.dumps(example, ensure_ascii=False) + "\n")
            
            logger.info(f"Создан новый файл примеров, старый архивирован как {archive_path}")
        
        except Exception as e:
            logger.error(f"Ошибка при ротации файла примеров: {str(e)}")

    