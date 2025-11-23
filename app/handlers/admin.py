# app/handlers/admin.py
from aiogram import types
from aiogram.filters import Command

from app.bot import dp
from app.config import ADMIN_IDS, MAIN_ADMIN_ID
from app.services.balances import (
    register_user,
    change_balance,
    set_balance,
    get_balance,
)
from app.services.ton import get_ton_rub_rate
from app.utils.formatters import format_rubles


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


@dp.message(Command("addbalance"))
async def cmd_addbalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Нет прав.")
    parts = (m.text or "").split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /addbalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    change_balance(uid, amount)
    await m.answer(
        f"✅ Баланс {uid} увеличен на {format_rubles(amount)} ₽. "
        f"Теперь: {format_rubles(get_balance(uid))} ₽"
    )


@dp.message(Command("removebalance"))
async def cmd_removebalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Нет прав.")
    parts = (m.text or "").split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /removebalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    change_balance(uid, -amount)
    await m.answer(
        f"✅ Баланс {uid} уменьшен на {format_rubles(amount)} ₽. "
        f"Теперь: {format_rubles(get_balance(uid))} ₽"
    )


@dp.message(Command("setbalance"))
async def cmd_setbalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Нет прав.")
    parts = (m.text or "").split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /setbalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    set_balance(uid, amount)
    await m.answer(f"✅ Баланс {uid} установлен на {format_rubles(amount)} ₽")


@dp.message(Command("adminprofit"))
async def cmd_adminprofit(m: types.Message):
    register_user(m.from_user)
    if m.from_user.id != MAIN_ADMIN_ID:
        return await m.answer("⛔ Только основной админ.")
    bal = get_balance(MAIN_ADMIN_ID)
    rate = await get_ton_rub_rate()
    ton_equiv = bal / rate if rate > 0 else 0
    await m.answer(
        "💸 Баланс админа (накопленная комиссия и игры): "
        f"{format_rubles(bal)} ₽.\n"
        f"≈ {ton_equiv:.4f} TON по текущему курсу ({rate:.2f} ₽ за 1 TON).\n"
        "Эти ₽ можно вывести, обменяв TON на рубли."
    )
