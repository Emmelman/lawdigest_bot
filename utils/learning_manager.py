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
        с оптимизацией размера данных
        """
        with self.lock:
            # Если кэш устарел (более 5 минут), перезагружаем примеры
            current_time = datetime.now()
            if self.last_loaded is None or (current_time - self.last_loaded).total_seconds() > 300:
                logger.debug("Перезагрузка примеров из-за устаревшего кэша")
                self._load_examples()
            else:
                logger.debug("Использование кэша примеров")
            
            # Получаем примеры
            if category and category in self.examples_by_category:
                # Берем последние примеры из указанной категории
                raw_examples = self.examples_by_category[category][-limit:]
            elif category is None:
                # Берем примеры из всех категорий с равномерным распределением
                raw_examples = []
                categories = list(self.examples_by_category.keys())
                examples_per_category = max(1, limit // len(categories)) if categories else 0
                
                for cat in categories:
                    cat_examples = self.examples_by_category[cat]
                    raw_examples.extend(cat_examples[-examples_per_category:])
                
                # Если мест еще осталось, добавляем из наиболее заполненных категорий
                while len(raw_examples) < limit and categories:
                    # Находим категорию с наибольшим числом примеров
                    max_cat = max(categories, key=lambda c: len(self.examples_by_category[c]))
                    if self.examples_by_category[max_cat]:
                        # Добавляем еще один пример из этой категории
                        used_indices = [raw_examples.index(ex) for ex in raw_examples if ex in self.examples_by_category[max_cat]]
                        available = [ex for i, ex in enumerate(self.examples_by_category[max_cat]) 
                                    if i not in used_indices]
                        
                        if available:
                            raw_examples.append(available[-1])
                        
                        # Удаляем категорию из списка, если все примеры уже использованы
                        if len(used_indices) >= len(self.examples_by_category[max_cat]):
                            categories.remove(max_cat)
                    else:
                        categories.remove(max_cat)
                
                raw_examples = raw_examples[:limit]
            else:
                # Если категория указана, но не найдена
                logger.warning(f"Запрошена несуществующая категория примеров: {category}")
                return []
            
            # Создаем копии примеров с оптимизированным размером
            optimized_examples = []
            for ex in raw_examples:
                # Создаем копию примера
                optimized = ex.copy()
                
                # Ограничиваем размер текста и обоснования
                if len(optimized['text']) > 200:
                    optimized['text'] = optimized['text'][:197] + '...'
                
                if 'justification' in optimized and len(optimized['justification']) > 100:
                    optimized['justification'] = optimized['justification'][:97] + '...'
                
                optimized_examples.append(optimized)
            
            logger.debug(f"Возвращено {len(optimized_examples)} примеров")
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