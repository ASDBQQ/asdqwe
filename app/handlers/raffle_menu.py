# app/handlers/raffle_menu.py
from aiogram import F
from aiogram.types import CallbackQuery

from app.bot import dp
from app.config import RAFFLE_MIN_BET
from app.services.raffle import (
    pending_raffle_bet_input,
    _process_raffle_bet,
    send_raffle_menu,
    cancel_user_bets,
    build_raffle_rating_text,
)
from app.services.games import pending_bet_input


@dp.callback_query(F.data == "mode_banker")
async def cb_mode_banker(callback: CallbackQuery):
    """Переход в меню Банкира."""
    await send_raffle_menu(callback.message.chat.id, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "raffle_make_bet")
async def cb_raffle_make_bet(callback: CallbackQuery):
    """
    Кнопка «Сделать ставку».
    Если это первая ставка в раунде — просим ввести сумму первой ставки.
    Если раунд уже идёт — просим ввести сумму (кратную базовой ставке).
    """
    uid = callback.from_user.id
    pending_raffle_bet_input[uid] = True
    pending_bet_input.pop(uid, None)

    await callback.message.answer(
        "Введите сумму ₽ для участия в Банкире.\n"
        f"Минимальная первая ставка: {RAFFLE_MIN_BET} ₽.\n"
        "Если раунд уже идёт, сумма должна быть кратной фиксированной ставке (бот подскажет)."
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("raffle_quick:"))
async def cb_raffle_quick(callback: CallbackQuery):
    """Быстрые суммы (1/3/7 долей)."""
    uid = callback.from_user.id
    chat_id = callback.message.chat.id

    try:
        amount = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректная сумма.", show_alert=True)
        return

    msg_text = await _process_raffle_bet(uid, chat_id, amount)
    await callback.message.answer(msg_text)
    await callback.answer()


@dp.callback_query(F.data == "raffle_refresh")
async def cb_raffle_refresh(callback: CallbackQuery):
    """Обновление информации о текущем раунде."""
    await send_raffle_menu(callback.message.chat.id, callback.from_user.id)
    await callback.answer("Обновлено!")


@dp.callback_query(F.data == "raffle_cancel")
async def cb_raffle_cancel(callback: CallbackQuery):
    """Отмена ставок пользователя в текущем раунде (до 10 минут)."""
    uid = callback.from_user.id
    text = await cancel_user_bets(uid)
    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "raffle_rating")
async def cb_raffle_rating(callback: CallbackQuery):
    """Отдельный рейтинг Банкира."""
    text = await build_raffle_rating_text(callback.from_user.id)
    await callback.message.answer(text)
    await callback.answer()


