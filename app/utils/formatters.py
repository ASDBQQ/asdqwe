# app/utils/formatters.py
def format_rubles(n: int) -> str:
    """Форматирует числа: 12500 → '12 500'."""
    return f"{n:,}".replace(",", " ")
