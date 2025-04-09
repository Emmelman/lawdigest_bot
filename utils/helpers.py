# В файле utils/helpers.py
from datetime import datetime, timedelta
def normalize_date(date_obj):
    """
    Приводит дату к нормализованному виду (без часового пояса)
    
    Args:
        date_obj (datetime|date): Объект даты или datetime для нормализации
        
    Returns:
        datetime: Нормализованный объект datetime без часового пояса
    """
    if isinstance(date_obj, datetime):
        # Если это datetime с часовым поясом, убираем его
        if date_obj.tzinfo is not None:
            return date_obj.replace(tzinfo=None)
        return date_obj
    elif hasattr(date_obj, 'year') and hasattr(date_obj, 'month') and hasattr(date_obj, 'day'):
        # Если это date, преобразуем в datetime
        return datetime(date_obj.year, date_obj.month, date_obj.day)
    else:
        raise ValueError(f"Невозможно нормализовать объект типа {type(date_obj)}")

def date_to_start_of_day(date_obj):
    """
    Преобразует дату в начало дня (00:00:00)
    
    Args:
        date_obj (datetime|date): Объект даты или datetime
        
    Returns:
        datetime: datetime с временем 00:00:00
    """
    normalized = normalize_date(date_obj)
    return normalized.replace(hour=0, minute=0, second=0, microsecond=0)

def date_to_end_of_day(date_obj):
    """
    Преобразует дату в конец дня (23:59:59)
    
    Args:
        date_obj (datetime|date): Объект даты или datetime
        
    Returns:
        datetime: datetime с временем 23:59:59
    """
    normalized = normalize_date(date_obj)
    return normalized.replace(hour=23, minute=59, second=59, microsecond=999999)

def parse_date_string(date_str, format="%d.%m.%Y"):
    """
    Парсит строку с датой в объект datetime
    
    Args:
        date_str (str): Строка с датой
        format (str): Формат даты
        
    Returns:
        datetime: Объект datetime
    """
    try:
        return datetime.strptime(date_str, format)
    except ValueError:
        raise ValueError(f"Невозможно распознать дату '{date_str}' в формате '{format}'")