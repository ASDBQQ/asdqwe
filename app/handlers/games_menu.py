# app/handlers/games_menu.py

from aiogram import F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from datetime import datetime, timezone

from app.bot import dp
from app.utils.formatters import format_rubles
from app.services.games import (
    games,
    pending_bet_input,
    send_games_list,
    build_games_text,
    build_games_keyboard,
    build_user_stats_and_history,
    build_history_keyboard,
    build_rating_text,
    play_game
)
from app.services.raffle import pending_raffle_bet_input
from app.services.state_reset import reset_user_state
from app.services.balances import get_balance, change_balance
from app.config import DICE_MIN_BET, DICE_BET_MIN_CANCEL_AGE


@dp.callback_query(F.data == "menu_games")
async def cb_menu_games(callback: CallbackQuery):
    reset_user_state(callback.from_user.id)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кости", callback_data="mode_dice")],
            [InlineKeyboardButton(text="🎩 Банкир", callback_data="mode_banker")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back_main")],
        ]
    )
    await callback.message.answer("Выберите режим игры:", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data == "back_main")
async def back_main(callback):
    from app.utils.keyboards import bottom_menu
    reset_user_state(callback.from_user.id)
    await callback.message.answer("Главное меню:", reply_markup=bottom_menu())
    await callback.answer()


# ---------------------------------------------------------
#                 СОЗДАНИЕ ИГРЫ (КОСТИ)
# ---------------------------------------------------------

@dp.callback_query(F.data == "create_game")
async def cb_create_game(callback: CallbackQuery):
    uid = callback.from_user.id
    pending_bet_input[uid] = True
    pending_raffle_bet_input.pop(uid, None)

    await callback.message.answer(
        f"Введите ставку (числом, в ₽). Минимум {DICE_MIN_BET} ₽:"
    )
    await callback.answer()


# ---------------------------------------------------------
#             ОТКРЫТИЕ КОНКРЕТНОЙ ИГРЫ
# ---------------------------------------------------------

@dp.callback_query(F.data.startswith("game_open:"))
async def cb_game_open(callback: CallbackQuery):
    gid = int(callback.data.split(":")[1])
    g = games.get(gid)

    if not g:
        return await callback.answer("Игра не найдена.", show_alert=True)
    if g["opponent_id"] is not None:
        return await callback.answer("Кто-то уже вступил!", show_alert=True)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔ Вступить", callback_data=f"join_confirm:{gid}")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="menu_games")],
    ])

    await callback.message.answer(
        f"🎲 Игра №{gid}\n"
        f"💰 Ставка: {format_rubles(g['bet'])} ₽\n\nХотите вступить?",
        reply_markup=kb,
    )
    await callback.answer()


# ---------------------------------------------------------
#               МОИ СОЗДАННЫЕ ИГРЫ (КОСТИ)
# ---------------------------------------------------------

@dp.callback_query(F.data.startswith("game_my:"))
async def cb_game_my(callback: CallbackQuery):
    uid = callback.from_user.id
    gid = int(callback.data.split(":")[1])

    g = games.get(gid)
    if not g:
        return await callback.answer("Игра не найдена.", show_alert=True)
    if g["creator_id"] != uid:
        return await callback.answer("Это не ваша игра.", show_alert=True)
    if g["opponent_id"] is not None:
        return await callback.answer("Уже есть соперник.", show_alert=True)

    rows = []
    time_passed = datetime.now(timezone.utc) - g["created_at"]

    if time_passed < DICE_BET_MIN_CANCEL_AGE:
        rows.append([
            InlineKeyboardButton(text="❌ Отменить ставку", callback_data=f"cancel_game:{gid}")
        ])

    rows.append([InlineKeyboardButton(text="⬅ Назад", callback_data="menu_games")])

    await callback.message.answer(
        f"🎲 Ваша игра №{gid}\n"
        f"💰 Ставка: {format_rubles(g['bet'])} ₽\n\nОжидание соперника...",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


# ---------------------------------------------------------
#               ОТМЕНА СТАВКИ (КОСТИ)
# ---------------------------------------------------------

@dp.callback_query(F.data.startswith("cancel_game:"))
async def cb_cancel_game(callback: CallbackQuery):
    uid = callback.from_user.id
    gid = int(callback.data.split(":")[1])

    g = games.get(gid)
    if not g:
        return await callback.answer("Игра не найдена.", show_alert=True)
    if g["creator_id"] != uid:
        return await callback.answer("Это не ваша игра.", show_alert=True)
    if g["opponent_id"] is not None:
        return await callback.answer("Уже есть соперник.", show_alert=True)

    created_at = g["created_at"]
    if datetime.now(timezone.utc) - created_at > DICE_BET_MIN_CANCEL_AGE:
        return await callback.answer(
            "Ставку можно отменить только в течение первой минуты.",
            show_alert=True,
        )

    change_balance(uid, g["bet"])
    del games[gid]

    await callback.message.answer(
        f"❌ Ставка №{gid} отменена. {format_rubles(g['bet'])} ₽ возвращены."
    )
    await send_games_list(callback.message.chat.id, uid)
    await callback.answer()


# ---------------------------------------------------------
#                 ПРИСОЕДИНЕНИЕ К ИГРЕ
# ---------------------------------------------------------

@dp.callback_query(F.data.startswith("join_confirm:"))
async def cb_join_confirm(callback: CallbackQuery):
    uid = callback.from_user.id
    gid = int(callback.data.split(":")[1])

    g = games.get(gid)
    if not g:
        return await callback.answer("Игра не найдена.", show_alert=True)
    if g["opponent_id"] is not None:
        return await callback.answer("Кто-то уже вступил!", show_alert=True)

    if get_balance(uid) < g["bet"]:
        return await callback.answer("Недостаточно ₽.", show_alert=True)

    g["opponent_id"] = uid
    change_balance(uid, -g["bet"])

    from app.db.games import upsert_game
    await upsert_game(g)

    await callback.message.answer(f"✅ Вы вступили в игру №{gid}!")
    await callback.answer()

    await play_game(gid)


# ---------------------------------------------------------
#                  ИСТОРИЯ ИГР
# ---------------------------------------------------------

@dp.callback_query(F.data.startswith("my_games"))
async def cb_my_games(callback: CallbackQuery):
    uid = callback.from_user.id
    page = int(callback.data.split(":")[1])

    stats, history = await build_user_stats_and_history(uid)
    kb = build_history_keyboard(history, page)

    await callback.message.answer(stats, reply_markup=kb)
    await callback.answer()


# ---------------------------------------------------------
#                    ОБНОВЛЕНИЕ СПИСКА ИГР
# ---------------------------------------------------------

@dp.callback_query(F.data == "refresh_games")
async def cb_refresh_games(callback: CallbackQuery):
    uid = callback.from_user.id
    try:
        await callback.message.edit_text(
            build_games_text(),
            reply_markup=build_games_keyboard(uid),
        )
    except:
        await callback.message.answer(
            build_games_text(),
            reply_markup=build_games_keyboard(uid),
        )
    await callback.answer("Обновлено!")


# ---------------------------------------------------------
#                    РЕЙТИНГ КОСТЕЙ
# ---------------------------------------------------------

@dp.callback_query(F.data == "rating")
async def cb_rating(callback: CallbackQuery):
    text = await build_rating_text(callback.from_user.id)
    await callback.message.answer(text)
    await callback.answer()


# ---------------------------------------------------------
#                    МЕНЮ ПОМОЩИ
# ---------------------------------------------------------

@dp.callback_query(F.data == "help_menu")
async def cb_help_menu(callback: CallbackQuery):
    await callback.message.answer(
        "🐼 Выберите раздел помощи:",
        reply_markup=help_menu_keyboard()
    )
    await callback.answer()


# ---------------------------------------------------------
#                  ПОМОЩЬ ПО КОСТЯМ
# ---------------------------------------------------------

@dp.callback_query(F.data == "help_dice")
async def cb_help_dice(callback: CallbackQuery):
    text = (
        "🎲 *Помощь: Кости 1x1*\n\n"
        "• Соперники бросают кубики.\n"
        "• При ничьей ― переброс.\n"
        "• Комиссия: 1%.\n"
        "• Рейтинг считается только по играм в кости."
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()


# ---------------------------------------------------------
#               ПОМОЩЬ ПО БАНКИРУ
# ---------------------------------------------------------

@dp.callback_query(F.data == "help_banker")
async def cb_help_banker(callback: CallbackQuery):
    text = (
        "🎩 *Помощь: Банкир*\n\n"
        "1️⃣ Первая ставка задаёт цену доли.\n"
        "2️⃣ До 10 ставок на игрока.\n"
        "3️⃣ Чем больше ставок — тем выше шанс.\n"
        "4️⃣ Таймер 60 секунд после 2 участников.\n"
        "5️⃣ Победитель получает банк минус 1%."
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()


# ---------------------------------------------------------
#             ПОМОЩЬ ПО БАЛАНСУ
# ---------------------------------------------------------

@dp.callback_query(F.data == "help_balance")
async def cb_help_balance(callback: CallbackQuery):
    text = (
        "💳 *Помощь: Баланс*\n\n"
        "• Пополнение только через TON.\n"
        "• Средства поступают за 5–30 секунд.\n"
        "• Если TON не пришёл — обратитесь в поддержку."
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()


# ---------------------------------------------------------
#                 ИГНОР КОЛЛБЕКОВ
# ---------------------------------------------------------

@dp.callback_query(F.data == "ignore")
async def cb_ignore(callback: CallbackQuery):
    await callback.answer()







