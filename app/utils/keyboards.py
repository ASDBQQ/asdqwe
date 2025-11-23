# app/utils/keyboards.py
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)


# ============================
#   ГЛАВНОЕ НИЖНЕЕ МЕНЮ
# ============================

def bottom_menu() -> ReplyKeyboardMarkup:
    """Главное меню, которое всегда под сообщениями."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🕹 Игры"),
                KeyboardButton(text="💼 Баланс"),
            ],
            [
                KeyboardButton(text="🎁 Розыгрыш"),
                KeyboardButton(text="👤 Профиль"),
            ],
            [KeyboardButton(text="🌐 Поддержка")],
        ],
        resize_keyboard=True,
    )


# ============================
#   МЕНЮ ИГР (КОСТИ)
# ============================

def games_menu_keyboard() -> InlineKeyboardMarkup:
    """Меню раздела 'Игры' (кости)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✔️ Создать игру", callback_data="game_create"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data="game_refresh"),
        ],
        [
            InlineKeyboardButton(text="📝 Мои игры", callback_data="game_my"),
            InlineKeyboardButton(text="🏆 Рейтинг", callback_data="game_rating"),
        ],
        [
            InlineKeyboardButton(text="🎮 Игры", callback_data="menu_games"),
            InlineKeyboardButton(text="🐼 Помощь", callback_data="help_menu"),
        ],
        [
            InlineKeyboardButton(text="⬅ Назад", callback_data="menu_start"),
        ],
    ])


# ============================
#   МЕНЮ РОЗЫГРЫША (БАНКИР)
# ============================

def raffle_help_button() -> InlineKeyboardMarkup:
    """Одна кнопка помощи — универсальная."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🐼 Помощь", callback_data="help_menu")]
    ])


# ============================
#   МЕНЮ БАЛАНСА
# ============================

def balance_menu_keyboard() -> InlineKeyboardMarkup:
    """Меню управления балансом."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💳 Пополнить", callback_data="balance_deposit"),
        ],
        [
            InlineKeyboardButton(text="🐼 Помощь", callback_data="help_menu"),
        ],
        [
            InlineKeyboardButton(text="⬅ Назад", callback_data="menu_start"),
        ],
    ])


# ============================
#   МЕНЮ ПОМОЩИ
# ============================

def help_menu_keyboard() -> InlineKeyboardMarkup:
    """Главное меню помощи — выбор раздела."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кости", callback_data="help_dice")],
        [InlineKeyboardButton(text="🎩 Банкир", callback_data="help_banker")],
        [InlineKeyboardButton(text="💳 Баланс/Вывод", callback_data="help_balance")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="help_back")],
    ])
