# app/handlers/balance.py

from typing import Dict, Any, Optional

from aiogram import F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from app.bot import dp
from app.config import TON_WALLET_ADDRESS
from app.services.balances import (
    register_user,
    get_balance,
    user_usernames,
)
from app.services.ton import get_ton_rub_rate
from app.utils.formatters import format_rubles
from app.utils.keyboards import bottom_menu


# ---------- СОСТОЯНИЯ ДЛЯ ВЫВОДА ----------
pending_withdraw_step: Dict[int, str] = {}
temp_withdraw: Dict[int, Dict[str, Any]] = {}

# ---------- СОСТОЯНИЯ ДЛЯ ПЕРЕВОДОВ ----------
pending_transfer_step: Dict[int, str] = {}     # await_username / await_amount
temp_transfer: Dict[int, Dict[str, Any]] = {}  # target_id, amount


# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------

async def format_balance_text(uid: int) -> str:
    bal = get_balance(uid)
    rate = await get_ton_rub_rate()
    ton_equiv = bal / rate if rate > 0 else 0
    return (
        f"💼 Ваш баланс: {ton_equiv:.4f} TON\n"
        f"≈ {format_rubles(bal)} ₽\n"
        f"Курс: 1 TON ≈ {rate:.2f} ₽"
    )


def resolve_user_by_username(username_str: str) -> Optional[int]:
    """
    Нужна для переводов — ищем user_id по @username.
    """
    uname = username_str.strip().lstrip("@").lower()
    if not uname:
        return None

    for uid, stored in user_usernames.items():
        if stored and stored.lower() == uname:
            return uid

    return None


# ---------- ГЛАВНОЕ МЕНЮ БАЛАНСА ----------

@dp.message(F.text == "💼 Баланс")
async def msg_balance(m: types.Message):
    register_user(m.from_user)
    uid = m.from_user.id

    pending_withdraw_step.pop(uid, None)
    temp_withdraw.pop(uid, None)
    pending_transfer_step.pop(uid, None)
    temp_transfer.pop(uid, None)

    text = await format_balance_text(uid)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💎 Пополнить (TON)", callback_data="deposit_menu")],
            [InlineKeyboardButton(text="🔄 Перевод", callback_data="transfer_menu")],
            [InlineKeyboardButton(text="💸 Вывод TON", callback_data="withdraw_menu")],
            [InlineKeyboardButton(text="🐼 Помощь", callback_data="help_balance")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="balance_back")],
        ]
    )

    await m.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "balance_back")
async def cb_balance_back(callback: CallbackQuery):
    uid = callback.from_user.id

    pending_withdraw_step.pop(uid, None)
    temp_withdraw.pop(uid, None)
    pending_transfer_step.pop(uid, None)
    temp_transfer.pop(uid, None)

    await callback.message.answer("Главное меню:", reply_markup=bottom_menu())
    await callback.answer()


# ---------- ПОПОЛНЕНИЕ TON ----------

@dp.callback_query(F.data == "deposit_menu")
async def cb_deposit_menu(callback: CallbackQuery):
    uid = callback.from_user.id
    rate = await get_ton_rub_rate()

    half_ton = int(rate * 0.5)
    one_ton = int(rate * 1)

    ton_link = f"ton://transfer/{TON_WALLET_ADDRESS}?text=ID{uid}"

    text = (
        "💎 Пополнение через TON\n\n"
        f"1 TON ≈ {rate:.2f} ₽\n"
        f"0.5 TON ≈ {format_rubles(half_ton)} ₽\n"
        f"1 TON  ≈ {format_rubles(one_ton)} ₽\n\n"
        "1️⃣ Откройте TON-кошелёк.\n"
        f"2️⃣ Отправьте TON на адрес: <code>{TON_WALLET_ADDRESS}</code>\n"
        f"3️⃣ В комментарии укажите: <code>ID{uid}</code>\n\n"
        "После получения TON бот автоматически зачислит ₽."
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💎 Оплатить через Tonkeeper", url=ton_link)],
        ]
    )

    await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


# ---------- ВЫВОД TON (ШАГ 1: СУММА) ----------

@dp.callback_query(F.data == "withdraw_menu")
async def cb_withdraw_menu(callback: CallbackQuery):
    uid = callback.from_user.id
    bal = get_balance(uid)

    if bal <= 0:
        return await callback.answer("Баланс 0 ₽.", show_alert=True)

    pending_withdraw_step[uid] = "amount"
    temp_withdraw[uid] = {}

    rate = await get_ton_rub_rate()
    ton_equiv = bal / rate if rate > 0 else 0

    await callback.message.answer(
        "💸 Вывод TON\n"
        f"Ваш баланс: {format_rubles(bal)} ₽ (≈ {ton_equiv:.4f} TON)\n\n"
        "Введите сумму (₽):"
    )
    await callback.answer()


# ---------- ПЕРЕВОД (ШАГ 1: ВВОД ПОЛУЧАТЕЛЯ) ----------

@dp.callback_query(F.data == "transfer_menu")
async def cb_transfer_menu(callback: CallbackQuery):
    uid = callback.from_user.id

    pending_transfer_step.pop(uid, None)
    temp_transfer.pop(uid, None)

    pending_transfer_step[uid] = "await_username"
    temp_transfer[uid] = {}

    await callback.message.answer(
        "🔄 Перевод ₽\n"
        "Введите ID или @username получателя.\n"
        "Получатель должен хотя бы раз написать боту."
    )
    await callback.answer()


# ---------- ПОМОЩЬ ----------

@dp.callback_query(F.data == "help_balance")
async def cb_help_balance(callback: CallbackQuery):
    text = (
        "💳 *Помощь по балансу*\n\n"
        "• Пополнение только через TON.\n"
        "• Вывод выполняется администратором.\n"
        "• Переводы работают мгновенно.\n"
        "• Получатель должен хотя бы раз написать боту."
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()












