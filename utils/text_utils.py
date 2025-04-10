# utils/text_utils.py
"""
Утилиты для обработки текста
"""
import re
import logging

logger = logging.getLogger(__name__)

class TextUtils:
    @staticmethod
    def clean_markdown_text(text):
        """Корректная обработка Markdown текста"""
        # Обработка ссылок и экранирование
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', 
                     lambda m: f'[{m.group(1)}]({m.group(2)})', text)
        
        # Обработка жирного текста
        text = re.sub(r'\*\*([^*]+)\*\*', r'<b>\1</b>', text)
        
        return text
    
    @staticmethod
    def convert_to_html(text):
        """Конвертирует Markdown-подобный синтаксис в HTML"""
        text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)  # **жирный** -> <b>жирный</b>
        text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)      # *курсив* -> <i>курсив</i>
        
        # Удаляем экранирующие символы
        text = re.sub(r'\\([.()[\]{}])', r'\1', text)
        
        return text
    
    @staticmethod
    def split_text(text, max_length=4000):
        """Разбивает длинный текст на части для Telegram"""
        if len(text) <= max_length:
            return [text]
        
        parts = []
        paragraphs = text.split("\n\n")
        current_part = ""
        
        for paragraph in paragraphs:
            if len(current_part) + len(paragraph) + 2 <= max_length:
                if current_part:
                    current_part += "\n\n" + paragraph
                else:
                    current_part = paragraph
            else:
                parts.append(current_part)
                current_part = paragraph
        
        if current_part:
            parts.append(current_part)
        
        return parts