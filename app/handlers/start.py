# app/handlers/start.py
from aiogram import F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from app.bot import dp
from app.services.balances import register_user, get_balance
from app.utils.keyboards import bottom_menu
from app.services.games import send_games_list
from app.services.raffle import send_raffle_menu


@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    register_user(m.from_user)
    get_balance(m.from_user.id)
    await m.answer(
        "Добро пожаловать в игровой бот TON!\n"
        "Здесь вы найдёте кости, розыгрыши и честные игры на ₽.\n"
        "Пополняйте TON, играйте — выигрывайте!",
        reply_markup=bottom_menu(),
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кости", callback_data="mode_dice")],
            [InlineKeyboardButton(text="🎩 Банкир", callback_data="mode_banker")],
        ]
    )
    await m.answer("Выберите режим игры:", reply_markup=kb)


@dp.message(F.text == "🕹 Игры")
async def msg_games(m: types.Message):
    register_user(m.from_user)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кости", callback_data="mode_dice")],
            [InlineKeyboardButton(text="🎩 Банкир", callback_data="mode_banker")],
        ]
    )
    await m.answer("Выберите режим игры:", reply_markup=kb)


@dp.message(F.text == "🎁 Розыгрыш")
async def msg_raffle_main(m: types.Message):
    # как в оригинале — заглушка
    register_user(m.from_user)
    await m.answer("Розыгрыши скоро появятся.")


@dp.message(F.text == "🌐 Поддержка")
async def msg_support(m: types.Message):
    register_user(m.from_user)
    await m.answer("Поддержка: @Btcbqq")


@dp.callback_query(F.data == "mode_dice")
async def cb_mode_dice(callback: CallbackQuery):
    await send_games_list(callback.message.chat.id, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "mode_banker")
async def cb_mode_banker(callback: CallbackQuery):
    await send_raffle_menu(callback.message.chat.id, callback.from_user.id)
    await callback.answer()
