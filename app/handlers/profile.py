# app/handlers/profile.py
from aiogram import F, types

from app.bot import dp
from app.services.balances import register_user
from app.db.users import get_user_registered_at
from app.db.games import get_user_dice_games_count
from app.db.raffle import get_user_raffle_bets_count


@dp.message(F.text == "👤 Профиль")
async def msg_profile(m: types.Message):
    register_user(m.from_user)
    uid = m.from_user.id

    reg_date_dt = await get_user_registered_at(uid)
    reg_date_str = (
        reg_date_dt.strftime("%d.%m.%Y %H:%M:%S") if reg_date_dt else "Неизвестно"
    )

    dice_games_count = await get_user_dice_games_count(uid)
    raffle_rounds_count = await get_user_raffle_bets_count(uid)

    text = (
        "👤 Ваш Профиль:\n\n"
        f"🆔 ID Пользователя: <code>{uid}</code>\n"
        f"🗓 Дата регистрации: {reg_date_str}\n\n"
        f"🎲 Всего игр в Кости: {dice_games_count}\n"
        f"🎩 Всего игр в Банкир: {raffle_rounds_count}"
    )

    await m.answer(text)
