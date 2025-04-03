# test_llm_models.py
import os
from dotenv import load_dotenv

from llm.qwen_model import QwenLLM
from llm.gemma_model import GemmaLLM

# Загрузка переменных окружения
load_dotenv()

def test_qwen_model():
    """Тестирование модели Qwen для классификации"""
    qwen_model = QwenLLM()
    
    test_text = """
    Государственная Дума приняла в первом чтении законопроект, вносящий изменения 
    в статью 12.18 КоАП. Поправки предусматривают увеличение штрафа за непропуск 
    пешеходов на пешеходном переходе.
    """
    
    categories = [
        'законодательные инициативы',
        'новая судебная практика',
        'новые законы',
        'поправки к законам',
        'другое'
    ]
    
    result = qwen_model.classify(test_text, categories)
    print(f"Qwen классифицировал текст как: {result}")

def test_gemma_model():
    """Тестирование модели Gemma для генерации текста"""
    gemma_model = GemmaLLM()
    
    test_text = """
    Верховный суд РФ опубликовал обзор судебной практики по делам, связанным с защитой прав потребителей 
    финансовых услуг. В обзоре подчеркивается, что банки обязаны информировать клиентов обо всех 
    комиссиях и условиях договора доступным языком, а также разъяснены особенности применения 
    положений закона о защите прав потребителей к финансовым организациям.
    """
    
    result = gemma_model.summarize(test_text)
    print(f"Gemma создал резюме:\n{result}")

if __name__ == "__main__":
    print("Тестирование моделей LLM...")
    test_qwen_model()
    print("\n" + "-"*50 + "\n")
    test_gemma_model()