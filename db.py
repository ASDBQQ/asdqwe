import asyncio
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

import aiohttp
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# Импортируем обновленные функции из db.py
from db import (
    init_db,
    upsert_user,
    upsert_game,
    get_user_games,
    add_ton_deposit,
    add_transfer,
    get_user_registered_at,
    get_user_dice_games_count,
    get_user_raffle_bets_count,
    get_users_profit_and_games_30_days,
    get_game,
    get_banker_rating_30_days,
)

# ========================
#      НАСТРОЙКИ
# ========================

BOT_TOKEN = "8589113961:AAH8bF8umtdtYhkhbBB5oW8NoMBMxI4bLxk"

# TON кошелёк для пополнений
TON_WALLET_ADDRESS = "UQCzzlkNLsCGqHTUj1zkD_3CVBMoXw-9Od3dRKGgHaBxysYe"

TONAPI_RATES_URL = "https://tonapi.io/v2/rates?tokens=ton&currencies=rub"
TON_RUB_CACHE_TTL = 60  # секунд кэша курса

START_BALANCE_COINS = 0
DICE_MIN_BET = 10
BANKER_MAX_JOINERS = 5 # Максимум игроков против Банкира
COMMISSION_RATE = 0.01 # 1% комиссии

MAIN_ADMIN_ID = 7106398341
ADMIN_IDS = {MAIN_ADMIN_ID, 783924834}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========================
#      FSM STATES
# ========================

class DiceGame(StatesGroup):
    """Состояния для создания игры "Кости"."""
    waiting_for_bet = State()

class BankerGame(StatesGroup):
    """Состояния для создания игры "Банкир"."""
    waiting_for_bet = State()

class Transfer(StatesGroup):
    """Состояния для перевода средств."""
    waiting_for_recipient = State()
    waiting_for_amount = State()

class Withdraw(StatesGroup):
    """Состояния для вывода средств."""
    waiting_for_amount = State()
    waiting_for_details = State()

# ========================
#      ДАННЫЕ В ПАМЯТИ
# ========================

user_balances: dict[int, int] = {}
user_usernames: dict[int, str] = {}
games: dict[int, dict] = {} # Используется только для активных игр, остальное - БД
next_game_id = 1 # Используется только для инициализации
processed_ton_tx: set[str] = set()

# кэш курса TON→RUB
_ton_rate_cache: dict[str, float | datetime] = {
    "value": 0.0,
    "updated": datetime.fromtimestamp(0, tz=timezone.utc),
}

# ========================
#      УТИЛИТЫ
# ========================

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def get_balance(uid: int) -> int:
    """Возвращает баланс в рублях/монетах, загруженный из БД."""
    if uid not in user_balances:
        user_balances[uid] = START_BALANCE_COINS
    return user_balances[uid]

# ИСПРАВЛЕНО: Функция _schedule_upsert_user теперь передает полный баланс и использует keyword аргумент
def _schedule_upsert_user(uid: int, registered_at: datetime | None = None):
    """Фоновое сохранение пользователя в БД (баланс + username + registered_at)."""
    username = user_usernames.get(uid)
    
    # ИСПРАВЛЕНИЕ: Получаем актуальный баланс из памяти
    current_balance = user_balances.get(uid, 0)
    
    try:
        # ИСПРАВЛЕНИЕ: Передаем актуальный баланс и используем registered_at как keyword аргумент
        asyncio.create_task(
            upsert_user(
                uid, 
                username, 
                current_balance, 
                registered_at=registered_at
            )
        )
    except RuntimeError:
        pass

# ИСПРАВЛЕНО: Функция change_balance теперь передает полный новый баланс
def change_balance(uid: int, delta: int):
    """Обновляет баланс в памяти и запускает фоновое сохранение."""
    get_balance(uid)
    user_balances[uid] += delta
    
    username = user_usernames.get(uid)
    new_balance = user_balances[uid] # <--- Используем новый полный баланс
    try:
        # Сразу сохраняем изменение в БД
        asyncio.create_task(upsert_user(uid, username, new_balance))
    except RuntimeError:
        pass

# ИСПРАВЛЕНО: Функция set_balance теперь передает полный новый баланс
def set_balance(uid: int, value: int):
    """Устанавливает баланс в памяти и запускает фоновое сохранение."""
    user_balances[uid] = value
    
    username = user_usernames.get(uid)
    new_balance = user_balances[uid] # <--- Используем новый полный баланс
    try:
        # Обновление через новый баланс
        asyncio.create_task(upsert_user(uid, username, new_balance))
    except RuntimeError:
        pass

def format_rubles(n: int) -> str:
    return f"{n:,}".replace(",", " ")

async def get_ton_rub_rate() -> float:
    """Получить курс TON→RUB через tonapi.io (с простым кэшем)."""
    now = datetime.now(timezone.utc)
    cached_value = _ton_rate_cache["value"]
    updated: datetime = _ton_rate_cache["updated"]  # type: ignore

    if cached_value and (now - updated).total_seconds() < TON_RUB_CACHE_TTL:
        return float(cached_value)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(TONAPI_RATES_URL, timeout=10) as resp:
                data = await resp.json()
        rate = float(data["rates"]["TON"]["prices"]["RUB"])
        _ton_rate_cache["value"] = rate
        _ton_rate_cache["updated"] = now
        return rate
    except Exception:
        return float(cached_value or 100.0)

async def format_balance_text(uid: int) -> str:
    bal = get_balance(uid)
    rate = await get_ton_rub_rate()
    ton_equiv = bal / rate if rate > 0 else 0
    return (
        f"💼 Ваш баланс: {ton_equiv:.4f} TON\n"
        f"≈ {format_rubles(bal)} ₽\n"
        f"Текущий курс: 1 TON ≈ {rate:.2f} ₽"
    )

def bottom_menu():
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(text="🕹 Игры"),
                types.KeyboardButton(text="💼 Баланс"),
            ],
            [
                types.KeyboardButton(text="🏆 Рейтинг"),
                types.KeyboardButton(text="👤 Профиль"),
            ],
            [
                types.KeyboardButton(text="🌐 Поддержка"),
            ],
        ],
        resize_keyboard=True
    )

def register_user(user: types.User):
    uid = user.id
    if uid not in user_balances:
        user_balances[uid] = START_BALANCE_COINS
        # Передаем дату регистрации
        _schedule_upsert_user(uid, datetime.now(timezone.utc))
    
    if user.username:
        user_usernames[uid] = user.username
        # Обновляем username, дату регистрации не передаем
        _schedule_upsert_user(uid)

def resolve_user_by_username(username_str: str) -> int | None:
    uname = username_str.strip().lstrip("@").lower()
    for uid, uname_stored in user_usernames.items():
        if uname_stored and uname_stored.lower() == uname:
            return uid
    return None

def calculate_profit(uid: int, g: dict) -> int:
    """Рассчитывает профит игрока в игре 'dice' (для статистики)."""
    bet = g.get("bet_amount", 0)
    winner_id = g.get("winner_id")

    # Dice game logic
    if g.get('game_type') == 'dice':
        # creator_id, opponent_id must be in the game data
        creator_id = g.get("creator_id")
        opponent_id = g.get("opponent_id")

        if winner_id is None and g.get('finished') == 1:
            # Отмененная игра (winner_id=0 for cancelled in db)
            return 0 
        
        commission = int(2 * bet * COMMISSION_RATE)
        
        if winner_id == creator_id or winner_id == opponent_id:
            # Чистый выигрыш = bet - commission
            profit = bet - commission 
            if uid == winner_id:
                return profit
            else:
                return -bet # Проигрыш = потеря ставки
        
        # Ничья (Rolls are equal, usually winner_id is None/0, and funds returned)
        if winner_id == 0: # If finished=1 and winner_id=0, it means cancelled or draw with refund
            # Since dice game logic refunds on a draw, profit is 0 (no loss/gain)
            return 0 
        
        return 0 # Should not happen if logic is sound

    return 0


# ========================
#      ОБЩИЕ ХЕНДЛЕРЫ
# ========================

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    register_user(m.from_user)
    await m.answer(
        "👋 Добро пожаловать в игровой бот TON!\n"
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

@dp.message(F.text == "🌐 Поддержка")
async def msg_support(m: types.Message):
    register_user(m.from_user)
    await m.answer("Поддержка: @Btcbqq")
    
@dp.callback_query(F.data == "ignore")
async def cb_ignore(callback: CallbackQuery):
    await callback.answer()

# ========================
#      МЕНЮ ИГР (КОСТИ/БАНКИР)
# ========================

def build_games_keyboard(uid: int) -> InlineKeyboardMarkup:
    """Клавиатура для списка активных игр 'dice'."""
    rows = []
    # Загружаем активные игры из глобального кэша 'games'
    active = [g for g in games.values() if g["opponent_id"] is None and g.get('game_type') == 'dice']
    active.sort(key=lambda x: x["id"], reverse=True)

    rows.append([
        InlineKeyboardButton(text="✅Создать игру", callback_data="create_dice_game"),
        InlineKeyboardButton(text="🔄Обновить", callback_data="refresh_games"),
    ])

    for g in active:
        txt = f"🎲Игра #{g['id']} | {format_rubles(g['bet'])} ₽"
        if g["creator_id"] == uid:
            rows.append([
                InlineKeyboardButton(text=f"{txt} (Вы)", callback_data=f"game_my:{g['id']}")
            ])
        else:
            rows.append([
                InlineKeyboardButton(text=txt, callback_data=f"game_open:{g['id']}")
            ])

    rows.append([
        InlineKeyboardButton(text="📋 Мои игры", callback_data="my_games:0"),
        InlineKeyboardButton(text="🏆 Рейтинг", callback_data="rating"),
    ])
    rows.append([
        InlineKeyboardButton(text="🎮 Игры", callback_data="menu_games"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

async def send_games_list(chat_id: int, uid: int, message_id: Optional[int] = None):
    text = "Создайте игру или выберите уже имеющуюся:"
    kb = build_games_keyboard(uid)
    if message_id:
        try:
            await bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)
        except Exception:
            await bot.send_message(chat_id, text, reply_markup=kb)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb)


@dp.callback_query(F.data == "mode_dice")
async def cb_mode_dice(callback: CallbackQuery):
    # Убираем старую клавиатуру 'Кости'/'Банкир'
    await callback.message.edit_reply_markup(reply_markup=None)
    await send_games_list(callback.message.chat.id, callback.from_user.id)
    await callback.answer()

@dp.callback_query(F.data == "mode_banker")
async def cb_mode_banker_start(callback: CallbackQuery, state: FSMContext):
    """Начало создания игры "Банкир"."""
    await state.set_state(BankerGame.waiting_for_bet)
    text = "🎩 **Создание игры 'Банкир'**\n\n" \
           "Введите сумму вашей ставки (это также будет ставка для присоединившихся). " \
           f"Вы можете принять до **{BANKER_MAX_JOINERS}** ставок. Максимальный риск: {BANKER_MAX_JOINERS}x ваша ставка."
    await callback.message.edit_text(text)
    await callback.answer()

# ==================================
#      ИГРА "КОСТИ" (Dice) - FSM-логика
# ==================================

async def telegram_roll(uid: int) -> int:
    msg = await bot.send_dice(uid, emoji="🎲")
    await asyncio.sleep(3)
    return msg.dice.value

async def play_dice_game(gid: int):
    """Выполняет броски, расчеты и отправляет результат игры 'Кости'."""
    g = games.get(gid)
    if not g or g["opponent_id"] is None:
        return

    c = g["creator_id"]
    o = g["opponent_id"]
    bet = g["bet"]

    # Броски
    await bot.send_message(c, f"🎲 Бросок в игре №{gid}!")
    await bot.send_message(o, f"🎲 Бросок в игре №{gid}!")
    cr = await telegram_roll(c)
    orr = await telegram_roll(o)

    # Расчет
    bank = bet * 2
    commission = int(bank * COMMISSION_RATE)
    prize = bank - commission
    
    winner = "draw"
    winner_id = 0 # 0 for draw/cancelled
    
    if cr > orr:
        winner = "creator"
        winner_id = c
        change_balance(c, prize)
        change_balance(MAIN_ADMIN_ID, commission)
    elif orr > cr:
        winner = "opponent"
        winner_id = o
        change_balance(o, prize)
        change_balance(MAIN_ADMIN_ID, commission)
    else:
        winner = "draw"
        # Возвращаем ставки при ничьей
        change_balance(c, bet)
        change_balance(o, bet)
        commission = 0 # Комиссия возвращается при ничьей

    # Обновление кэша и БД
    g["creator_roll"] = cr
    g["opponent_roll"] = orr
    g["winner"] = winner
    g["finished"] = True
    g["finished_at"] = datetime.now(timezone.utc)
    
    # rolls = [creator_roll, opponent_roll]
    # Замените на корректный вызов upsert_game, если он требует winner_id, rolls, opponent_id
    # Предполагаем, что upsert_game принимает game_id, creator_id, game_type, bet_amount, target_score, finished, [winner_id, rolls, opponent_id...]
    # NOTE: В вашем upsert_game нет сигнатуры с таким количеством аргументов. Используем словарь, как в вашем db.py, предполагая, что вы добавите нужные ключи.
    game_update = {
        "id": gid,
        "creator_id": c,
        "opponent_id": o,
        "bet": bet,
        "creator_roll": cr,
        "opponent_roll": orr,
        "winner": winner,
        "finished": 1,
        "created_at": g["created_at"], # Сохраняем начальную дату
        "finished_at": datetime.now(timezone.utc)
    }
    
    await upsert_game(game_update)
    
    if gid in games:
        del games[gid] # Удаляем из активных

    # Уведомления
    for user in (c, o):
        is_creator = (user == c)
        your = cr if is_creator else orr
        their = orr if is_creator else cr
        
        creator_username = user_usernames.get(c, f"ID{c}")
        opponent_username = user_usernames.get(o, f"ID{o}")

        if winner == "draw":
            result_text = "🤝 Ничья!"
            bank_text = f"💰 Банк: {format_rubles(bank)} ₽ (вернули ставки)"
        else:
            winner_username = creator_username if winner == "creator" else opponent_username
            if (winner == "creator" and is_creator) or (winner == "opponent" and not is_creator):
                result_text = f"🥳 **Победа!** (+{format_rubles(prize)} ₽)"
            else:
                result_text = "😔 **Проигрыш!**"
            
            bank_text = (
                f"🏆 Победитель: @{winner_username}\n"
                f"💰 Банк: {format_rubles(bank)} ₽\n"
                f"💸 Комиссия: {format_rubles(commission)} ₽ ({COMMISSION_RATE*100}%)"
            )

        txt = (
            f"🏁 **Игра 'Кости' №{gid} завершена!**\n"
            f"@{creator_username} vs @{opponent_username}\n\n"
            f"{bank_text}\n\n"
            f"🫵 Ваш результат: **{your}**\n"
            f"🧑‍🤝‍🧑 Результат соперника: **{their}**\n\n"
            f"{result_text}\n"
            f"💼 **Баланс:** {format_rubles(get_balance(user))} ₽"
        )
        try:
            await bot.send_message(user, txt)
        except Exception:
            pass # Если пользователь заблокировал бота

@dp.callback_query(F.data == "create_dice_game")
async def cb_create_game(callback: CallbackQuery, state: FSMContext):
    """Начало создания игры "Кости" (FSM)."""
    uid = callback.from_user.id
    await state.set_state(DiceGame.waiting_for_bet)
    
    await callback.message.answer(
        f"Введите ставку (числом, в ₽). Минимум {DICE_MIN_BET} ₽:"
    )
    await callback.answer()

@dp.message(DiceGame.waiting_for_bet, F.text.regexp(r"^\d+$"))
async def handle_dice_bet(message: types.Message, state: FSMContext):
    """Обработка ставки и создание игры "Кости"."""
    uid = message.from_user.id
    bet = int(message.text)
    await state.clear()
    
    if bet < DICE_MIN_BET:
        return await message.answer(f"Минимальная ставка {DICE_MIN_BET} ₽.")
    if bet > get_balance(uid):
        return await message.answer(f"Недостаточно ₽ на балансе! Ваш баланс: {format_rubles(get_balance(uid))} ₽.")

    global next_game_id
    gid = next_game_id
    next_game_id += 1
    
    # Снимаем ставку (обновленная change_balance сохраняет новый баланс)
    change_balance(uid, -bet)

    created_at = datetime.now(timezone.utc)
    
    # Создаем игру в памяти (для быстрой работы)
    game_data = {
        "id": gid,
        "creator_id": uid,
        "opponent_id": None,
        "game_type": 'dice',
        "bet": bet,
        "creator_roll": None,
        "opponent_roll": None,
        "winner": None,
        "finished": 0,
        "created_at": created_at,
        "finished_at": None,
    }
    games[gid] = game_data

    # Сохраняем игру в БД
    await upsert_game(game_data)

    await message.answer(f"✅ **Игра 'Кости' №{gid} создана!** Ставка: {format_rubles(bet)} ₽.")
    await send_games_list(message.chat.id, uid)

@dp.message(DiceGame.waiting_for_bet)
async def handle_dice_bet_invalid(message: types.Message):
    await message.answer("Неверный формат ставки. Введите целое число.")

@dp.callback_query(F.data == "refresh_games")
async def cb_refresh_games(callback: CallbackQuery):
    await send_games_list(callback.message.chat.id, callback.from_user.id, callback.message.message_id)
    await callback.answer("Обновлено!")

@dp.callback_query(F.data.startswith("game_open:"))
async def cb_game_open(callback: CallbackQuery):
    """Окно чужой игры (Кости)"""
    gid = int(callback.data.split(":", 1)[1])
    g = games.get(gid)

    if not g or g["opponent_id"] is not None:
        return await callback.answer("Игра не найдена или уже занята.", show_alert=True)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✔ Вступить", callback_data=f"join_confirm:{gid}")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="refresh_games")],
        ]
    )

    creator_username = user_usernames.get(g["creator_id"], f"ID{g['creator_id']}")
    await callback.message.answer(
        f"🎲 Игра №{gid}\n"
        f"👤 Создатель: @{creator_username}\n"
        f"💰 Ставка: {format_rubles(g['bet'])} ₽\n\n"
        f"Хотите вступить?",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("join_confirm:"))
async def cb_join_confirm(callback: CallbackQuery):
    """Подтверждение вступления в игру Кости."""
    uid = callback.from_user.id
    gid = int(callback.data.split(":", 1)[1])

    g = games.get(gid)
    if not g or g["opponent_id"] is not None or g["creator_id"] == uid:
        return await callback.answer("Игра недоступна.", show_alert=True)

    bet = g["bet"]
    if get_balance(uid) < bet:
        return await callback.answer("Недостаточно ₽.", show_alert=True)

    # Присоединение (обновленная change_balance сохраняет новый баланс)
    g["opponent_id"] = uid
    user_usernames[uid] = callback.from_user.username or user_usernames.get(uid) or f"ID{uid}"
    change_balance(uid, -bet)

    # Обновляем кэш и БД
    game_update = g.copy()
    game_update["finished"] = 0 # Еще не завершена
    await upsert_game(game_update)

    await callback.message.answer(f"✅ Вы присоединились к игре №{gid}! Ожидайте бросков.")
    await callback.answer()

    await play_dice_game(gid)

@dp.callback_query(F.data.startswith("game_my:"))
async def cb_game_my(callback: CallbackQuery):
    """Окно своей игры (Кости)"""
    uid = callback.from_user.id
    gid = int(callback.data.split(":", 1)[1])

    g = games.get(gid)
    if not g or g["creator_id"] != uid or g["opponent_id"] is not None:
        return await callback.answer("Игра не найдена или уже занята.", show_alert=True)

    # Кнопка "Отменить" доступна только в первую минуту
    time_passed = datetime.now(timezone.utc) - g["created_at"]
    DICE_BET_MIN_CANCEL_AGE = timedelta(minutes=1) # Константа из вашего кода
    
    rows = []
    if time_passed < DICE_BET_MIN_CANCEL_AGE:
        rows.append([InlineKeyboardButton(text="❌ Отменить ставку", callback_data=f"cancel_dice_game:{gid}")])
    
    rows.append([InlineKeyboardButton(text="⬅ Назад", callback_data="refresh_games")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    await callback.message.answer(
        f"🎲 Ваша игра №{gid}\n"
        f"💰 Ставка: {format_rubles(g['bet'])} ₽\n\n"
        f"Ожидание соперника...",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("cancel_dice_game:"))
async def cb_cancel_game(callback: CallbackQuery):
    """Отмена ставки (Кости)"""
    uid = callback.from_user.id
    gid = int(callback.data.split(":", 1)[1])

    g = games.get(gid)
    if not g or g["creator_id"] != uid or g["opponent_id"] is not None:
        return await callback.answer("Отмена невозможна.", show_alert=True)

    DICE_BET_MIN_CANCEL_AGE = timedelta(minutes=1)
    if (datetime.now(timezone.utc) - g["created_at"]) > DICE_BET_MIN_CANCEL_AGE:
        return await callback.answer(
            f"Ставку можно отменить только в течение первой минуты после создания.", 
            show_alert=True
        )

    bet = g["bet"]
    # Возвращаем ставку (обновленная change_balance сохраняет новый баланс)
    change_balance(uid, bet)
    
    # Завершаем игру в БД (finished=1, winner=None/draw)
    game_update = g.copy()
    game_update["finished"] = 1
    game_update["finished_at"] = datetime.now(timezone.utc)
    game_update["winner"] = "draw"
    await upsert_game(game_update)
    
    if gid in games:
        del games[gid]
    
    await callback.message.answer(
        f"❌ Ставка №{gid} отменена. {format_rubles(bet)} ₽ возвращены на баланс."
    )
    await send_games_list(callback.message.chat.id, uid)
    await callback.answer()

# ==================================
#      ИГРА "БАНКИР" (Banker) - FSM-логика
# ==================================

def get_banker_game_kb(game_id: int, joiners_count: int) -> InlineKeyboardMarkup:
    """Клавиатура для игры "Банкир"."""
    buttons = []
    if joiners_count < BANKER_MAX_JOINERS:
        buttons.append(InlineKeyboardButton(text="🤝 Присоединиться", callback_data=f"banker_join_{game_id}"))
    
    return InlineKeyboardMarkup(
        inline_keyboard=[
            buttons,
            [InlineKeyboardButton(text="🎲 Начать бросок", callback_data=f"banker_roll_start_{game_id}")],
            [InlineKeyboardButton(text="🚫 Отмена", callback_data=f"banker_cancel_{game_id}")]
        ]
    )

def get_joiner_roll_kb(game_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для присоединившегося в Банкире (в личке)."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Бросить кости", callback_data=f"banker_roll_joiner_{game_id}")],
            [InlineKeyboardButton(text="🚪 Выход", callback_data="ignore")]
        ]
    )


@dp.message(BankerGame.waiting_for_bet, F.text.regexp(r"^\d+$"))
async def handle_banker_bet(message: types.Message, state: FSMContext):
    """Обработка ставки и создание игры "Банкир"."""
    uid = message.from_user.id
    bet_amount = int(message.text)
    await state.clear()
    
    if bet_amount <= 0:
        return await message.answer("Ставка должна быть положительным числом.")
    if bet_amount > get_balance(uid):
        return await message.answer(f"Недостаточно средств. Ваш баланс: {format_rubles(get_balance(uid))} ₽")

    # 1. Списываем ставку у "Банкира" (обновленная change_balance сохраняет новый баланс)
    change_balance(uid, -bet_amount)

    global next_game_id
    gid = next_game_id
    next_game_id += 1

    # 2. Создаем игру в памяти
    game_data = {
        "id": gid,
        "creator_id": uid,
        "game_type": 'banker',
        "bet": bet_amount,
        "creator_roll": None,
        "opponent_id": None, # Не используется в этой игре
        "opponent_roll": None, # Не используется в этой игре
        "winner": None,
        "target_score": 0, # Бросок Банкира
        "joiners": [], # Список присоединившихся
        "finished": 0,
        "created_at": datetime.now(timezone.utc),
        "finished_at": None,
    }
    games[gid] = game_data
    
    # 3. Сохраняем игру в БД
    await upsert_game(game_data)
    
    # 4. Отправляем сообщение о создании
    text = f"🎩 **Игра 'Банкир' №{gid} создана!**\n\n" \
           f"**Банкир:** @{message.from_user.username or f'ID{uid}'}\n" \
           f"**Ставка:** {format_rubles(bet_amount)} ₽\n" \
           f"**Слоты:** 0/{BANKER_MAX_JOINERS}\n" \
           "Нажмите **'Начать бросок'** или ожидайте присоединившихся."
    
    await message.answer(text, reply_markup=get_banker_game_kb(gid, 0))

@dp.message(BankerGame.waiting_for_bet)
async def handle_banker_bet_invalid(message: types.Message):
    await message.answer("Неверный формат ставки. Введите целое число.")


@dp.callback_query(F.data.startswith("banker_join_"))
async def cb_banker_join(callback: CallbackQuery):
    """Присоединение к игре "Банкир"."""
    game_id = int(callback.data.split('_')[-1])
    joiner_id = callback.from_user.id
    
    # Получаем игру из кэша, т.к. get_game не была реализована в db.py
    game = games.get(game_id)
    if not game or game.get('finished') != 0 or game.get('game_type') != 'banker' or game.get('target_score') != 0:
        await callback.answer("Игра недоступна для присоединения.", show_alert=True)
        return

    if game['creator_id'] == joiner_id:
        return await callback.answer("Вы не можете присоединиться к своей игре.", show_alert=True)

    joiners_list = game.get('joiners', [])
    if joiner_id in [j['user_id'] for j in joiners_list]:
        return await callback.answer("Вы уже присоединились к этой игре.", show_alert=True)

    if len(joiners_list) >= BANKER_MAX_JOINERS:
        return await callback.answer("Все слоты заняты.", show_alert=True)

    # Проверяем баланс
    bet_amount = game['bet']
    if get_balance(joiner_id) < bet_amount:
        return await callback.answer(f"Недостаточно средств. Ваш баланс: {format_rubles(get_balance(joiner_id))} ₽", show_alert=True)

    # 1. Списываем ставку у присоединившегося (обновленная change_balance сохраняет новый баланс)
    change_balance(joiner_id, -bet_amount)

    # 2. Обновляем joiners в кэше
    joiner_username = callback.from_user.username or f"ID{joiner_id}"
    joiners_list.append({
        'user_id': joiner_id,
        'username': joiner_username,
        'bet': bet_amount, # Изменено с bet_amount, чтобы соответствовать ключу в game
        'roll': None,
        'won': None,
    })
    
    # 3. Обновляем БД (используем upsert_game с полным словарем)
    game_update = game.copy()
    game_update['joiners'] = joiners_list
    await upsert_game(game_update)
    games[game_id]['joiners'] = joiners_list # Обновляем кэш

    await callback.answer("Вы успешно присоединились! Ожидайте броска Банкира.", show_alert=True)
    
    # Обновляем сообщение
    creator_user = user_usernames.get(game['creator_id'], f"ID{game['creator_id']}")
    joiners_count = len(joiners_list)
    
    # ИСПРАВЛЕНИЕ: Используем одинарные кавычки для 'username'
    text = f"🎩 **Игра 'Банкир' №{game_id}**\n\n" \
           f"**Банкир:** @{creator_user}\n" \
           f"**Ставка:** {format_rubles(bet_amount)} ₽\n" \
           f"**Слоты:** {joiners_count}/{BANKER_MAX_JOINERS}\n" \
          joined = ", ".join([f"@{j['username']}" for j in joiners_list])
return f"**Присоединились:** {joined}"

           "Ожидаем присоединившихся игроков или начала броска."
    
    await callback.message.edit_text(text, reply_markup=get_banker_game_kb(game_id, joiners_count))
    
    await callback.message.edit_text(text, reply_markup=get_banker_game_kb(game_id, joiners_count))


@dp.callback_query(F.data.startswith("banker_roll_start_"))
async def cb_banker_roll_start(callback: CallbackQuery):
    """Инициирует бросок костей Банкиром."""
    game_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    game = games.get(game_id)
    if not game or game.get('finished') != 0 or game.get('game_type') != 'banker':
        return await callback.answer("Игра недоступна.", show_alert=True)

    if game['creator_id'] != user_id:
        return await callback.answer("Только Банкир может начать бросок.", show_alert=True)

    joiners_list = game.get('joiners', [])
    if not joiners_list:
        return await callback.answer("Нет присоединившихся игроков.", show_alert=True)

    # 1. Банкир бросает
    creator_roll = random.randint(1, 6)
    
    # 2. Записываем бросок Банкира (target_score)
    game_update = game.copy()
    game_update['target_score'] = creator_roll
    game_update['creator_roll'] = creator_roll # Используем creator_roll
    
    # Обновляем БД и кэш
    await upsert_game(game_update)
    games[game_id].update(game_update)

    text = f"🎲 **Бросок Банкира в игре №{game_id}!**\n\n" \
           f"**Банкир** (@{user_usernames.get(user_id, f"ID{user_id}")}) бросил **{creator_roll}**\n\n" \
           "Теперь очередь присоединившихся бросать кости. Проверьте личные сообщения."
    
    # Отправляем сообщение в чат
    await callback.message.edit_text(text, reply_markup=None)

    # Отправляем личные сообщения присоединившимся
    for joiner in joiners_list:
        try:
            await bot.send_message(
                joiner['user_id'],
                f"🎩 В игре Банкир №{game_id} Банкир бросил **{creator_roll}**.\n" \
                "Ваша очередь бросить кости, чтобы попытаться выбросить больше!",
                reply_markup=get_joiner_roll_kb(game_id)
            )
        except Exception:
            pass
    
    await callback.answer("Игроки уведомлены.")


@dp.callback_query(F.data.startswith("banker_roll_joiner_"))
async def cb_banker_roll_joiner(callback: CallbackQuery):
    """Бросок присоединившегося в игре "Банкир" (в личке)."""
    game_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    game = games.get(game_id)
    if not game or game.get('finished') != 0 or game.get('game_type') != 'banker' or game.get('target_score') == 0:
        return await callback.answer("Игра недоступна или Банкир еще не бросил.", show_alert=True)

    joiners_list = game.get('joiners', [])
    joiner_info = next((j for j in joiners_list if j['user_id'] == user_id), None)
    
    if not joiner_info:
        return await callback.answer("Вы не присоединялись к этой игре.", show_alert=True)
    if joiner_info['roll'] is not None:
        return await callback.answer("Вы уже бросали кости в этой игре.", show_alert=True)

    # Присоединившийся бросает
    joiner_roll = random.randint(1, 6)
    
    # Обновляем roll в списке joiners
    for j in joiners_list:
        if j['user_id'] == user_id:
            j['roll'] = joiner_roll
            break

    # Обновляем игру в БД и кэш
    game_update = game.copy()
    game_update['joiners'] = joiners_list
    await upsert_game(game_update)
    games[game_id]['joiners'] = joiners_list 

    await callback.message.edit_text(
        f"🎲 Вы бросили **{joiner_roll}**! Банкир бросил **{game['target_score']}**.\nОжидаем других игроков."
    )
    await callback.answer("Вы бросили кости!")

    # Проверка на завершение
    if all(j['roll'] is not None for j in joiners_list):
        await finish_banker_game(game_id)

async def finish_banker_game(game_id: int):
    """Завершает игру "Банкир" и распределяет средства."""
    game = games.get(game_id)
    if not game or game.get('finished') != 0 or game.get('game_type') != 'banker':
        return

    creator_id = game['creator_id']
    banker_roll = game['target_score']
    bet_amount = game['bet']
    joiners_list = game['joiners']
    commission_rate = COMMISSION_RATE

    results_text = f"🎉 **Игра 'Банкир' №{game_id} завершена!**\n\n" \
                   f"**Бросок Банкира:** **{banker_roll}**\n\n" \
                   f"**Результаты игроков:**\n"
    
    banker_profit_before_commission = 0
    total_banker_commission = 0

    # 1. Расчеты для присоединившихся
    for j in joiners_list:
        joiner_id = j['user_id']
        joiner_roll = j.get('roll', 0)
        
        # Если игрок не бросил или бросил 0 (в случае если roll = 0, что невозможно с dice, но для безопасности)
        if joiner_roll is None or joiner_roll == 0:
            profit = -bet_amount
            j['won'] = False
            results_text += f"😔 @{j['username']} (Не бросил) - **Проигрыш!**\n"
            banker_profit_before_commission += bet_amount
            continue

        if joiner_roll > banker_roll:
            # Победа присоединившегося
            prize = bet_amount * 2
            commission = int(bet_amount * commission_rate) # Комиссия берется только с выигрыша (+ставка), т.е. с (bet_amount*2)
            net_prize = prize - commission
            
            # Выплата присоединившемуся (он уже заплатил ставку -bet_amount, поэтому +prize)
            change_balance(joiner_id, prize)
            
            # Комиссия идет админу
            change_balance(MAIN_ADMIN_ID, commission)
            total_banker_commission += commission
            
            # Убыток Банкира
            banker_profit_before_commission -= bet_amount
            j['won'] = True
            
            results_text += f"🥳 @{j['username']} ({joiner_roll}) - **Победа!** (+{format_rubles(net_prize)} ₽)\n"

        else:
            # Проигрыш присоединившегося (Банкир забирает ставку)
            j['won'] = False
            banker_profit_before_commission += bet_amount
            results_text += f"😔 @{j['username']} ({joiner_roll}) - **Проигрыш!**\n"
            # Средства уже списаны у игрока при присоединении

    # 2. Расчеты для Банкира
    final_banker_profit = banker_profit_before_commission - total_banker_commission
    
    # Возврат Банкиру его ставки + чистый доход/убыток
    change_balance(creator_id, bet_amount + final_banker_profit)
    
    results_text += f"\n**Итог Банкира:**\n" \
                    f"Начальная ставка: {format_rubles(bet_amount)} ₽\n" \
                    f"Прибыль/убыток (до комиссии): {format_rubles(banker_profit_before_commission)} ₽\n" \
                    f"Комиссия ({commission_rate*100}%): -{format_rubles(total_banker_commission)} ₽\n" \
                    f"Чистая выплата (Возврат ставки + Прибыль): **{format_rubles(bet_amount + final_banker_profit)} ₽**"

    # 3. Завершение игры в БД и удаление из кэша
    game_update = game.copy()
    game_update['finished'] = 1
    game_update['finished_at'] = datetime.now(timezone.utc)
    game_update['winner'] = "creator" # Банкир считается "победителем" раунда
    game_update['joiners'] = joiners_list # Обновляем финальный список с результатами
    
    await upsert_game(game_update)

    if game_id in games:
        del games[game_id] 
    
    # Уведомление в чат (отправляем Банкиру, чтобы он переслал или сообщил в чат)
    try:
        await bot.send_message(creator_id, results_text)
    except Exception:
        pass # Если Банкир заблокировал бота

@dp.callback_query(F.data.startswith("banker_cancel_"))
async def cb_banker_cancel(callback: CallbackQuery):
    """Отмена игры "Банкир" (только Банкиром)."""
    game_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    game = games.get(game_id)
    if not game or game.get('finished') != 0 or game.get('game_type') != 'banker':
        return await callback.answer("Игра не найдена или уже завершена.", show_alert=True)

    if game['creator_id'] != user_id:
        return await callback.answer("Только Банкир может отменить игру.", show_alert=True)

    # Возвращаем ставки Банкиру и всем присоединившимся (обновленная change_balance сохраняет новый баланс)
    change_balance(game['creator_id'], game['bet'])
    for joiner in game.get('joiners', []):
        change_balance(joiner['user_id'], joiner['bet'])

    # Завершаем игру (winner=draw/cancelled)
    game_update = game.copy()
    game_update['finished'] = 1
    game_update['finished_at'] = datetime.now(timezone.utc)
    game_update['winner'] = "draw"
    await upsert_game(game_update)

    if game_id in games:
        del games[game_id]

    await callback.message.edit_text(
        f"❌ Игра 'Банкир' №{game_id} отменена. Ставки возвращены всем участникам."
    )
    await callback.answer()


# ==================================
#      БАЛАНС / ПОПОЛНЕНИЕ / ПЕРЕВОД
# ==================================

@dp.message(F.text == "💼 Баланс")
@dp.message(Command("balance"))
async def msg_balance(m: types.Message):
    register_user(m.from_user)
    uid = m.from_user.id
    
    text = await format_balance_text(uid)
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📥 Пополнить", callback_data="deposit_menu")],
            [InlineKeyboardButton(text="📤 Вывести", callback_data="withdraw_menu")],
            [InlineKeyboardButton(text="🔄 Перевести", callback_data="transfer_start")],
        ]
    )
    await m.answer(text, reply_markup=kb)

# --- Переводы ---

@dp.callback_query(F.data == "transfer_start")
async def cb_transfer_start(callback: CallbackQuery, state: FSMContext):
    """Начало перевода (FSM)."""
    uid = callback.from_user.id
    bal = get_balance(uid)
    if bal <= 0:
        return await callback.answer("Баланс нулевой.", show_alert=True)
    
    await state.set_state(Transfer.waiting_for_recipient)
    await callback.message.answer(
        f"🔄 **Перевод средств**\n"
        f"Ваш баланс: {format_rubles(bal)} ₽\n"
        "Введите ID или @username получателя:"
    )
    await callback.answer()

@dp.message(Transfer.waiting_for_recipient)
async def handle_transfer_recipient(message: types.Message, state: FSMContext):
    """Обработка ID/юзернейма получателя."""
    uid = message.from_user.id
    input_str = message.text.strip()
    target_id = None
    
    if input_str.isdigit():
        target_id = int(input_str)
    elif input_str.startswith('@'):
        target_id = resolve_user_by_username(input_str)
    
    if target_id is None:
        return await message.answer("Пользователь не найден. Попробуйте ввести ID или юзернейм.")
    if target_id == uid:
        return await message.answer("Вы не можете перевести средства самому себе.")
    
    # Проверяем, существует ли пользователь в памяти (т.е. зарегистрирован)
    if target_id not in user_balances:
        return await message.answer(f"Пользователь с ID {target_id} не найден в системе. Попросите его написать /start.")

    await state.update_data(target_id=target_id)
    
    bal = get_balance(uid)
    await state.set_state(Transfer.waiting_for_amount)
    await message.answer(
        f"Получатель: ID `{target_id}`. Ваш баланс: {format_rubles(bal)} ₽\n"
        "Введите сумму ₽ для перевода (минимум 1):"
    )

@dp.message(Transfer.waiting_for_amount)
async def handle_transfer_amount(message: types.Message, state: FSMContext):
    """Обработка суммы перевода и его выполнение."""
    uid = message.from_user.id
    try:
        amount = int(message.text)
        if amount <= 0:
            return await message.answer("Сумма должна быть > 0.")
        bal = get_balance(uid)
        if amount > bal:
            return await message.answer(f"Недостаточно ₽. Ваш баланс: {format_rubles(bal)} ₽.")
        
        data = await state.get_data()
        target_id = data.get("target_id")
        
        # Выполнение перевода (обновленная change_balance сохраняет новый баланс)
        change_balance(uid, -amount)
        change_balance(target_id, amount)
        await add_transfer(uid, target_id, amount)

        await message.answer(
            f"✅ **Перевод выполнен.**\n"
            f"Вы отправили {format_rubles(amount)} ₽ пользователю ID {target_id}.\n"
            f"Ваш новый баланс: {format_rubles(get_balance(uid))} ₽.", 
            reply_markup=bottom_menu()
        )
        try:
            await bot.send_message(
                target_id, 
                f"🔄 Вам перевели {format_rubles(amount)} ₽ от пользователя ID {uid}.\n"
                f"Ваш новый баланс: {format_rubles(get_balance(target_id))} ₽."
            )
        except Exception:
            pass
    except ValueError:
        return await message.answer("Введите сумму числом.")
    finally:
        await state.clear()

# --- Вывод ---

@dp.callback_query(F.data == "withdraw_menu")
async def cb_withdraw_menu(callback: CallbackQuery, state: FSMContext):
    """Начало вывода (FSM)."""
    uid = callback.from_user.id
    bal = get_balance(uid)
    if bal <= 0:
        return await callback.answer("Баланс нулевой.", show_alert=True)
    
    await state.set_state(Withdraw.waiting_for_amount)
    rate = await get_ton_rub_rate()
    ton_equiv = bal / rate if rate > 0 else 0
    
    await callback.message.answer(
        f"💸 **Вывод средств в TON**\n"
        f"Ваш баланс: {format_rubles(bal)} ₽ (≈ {ton_equiv:.4f} TON)\n"
        f"1 TON ≈ {rate:.2f} ₽\n\n"
        "Введите сумму в ₽, которую хотите вывести (будет конвертирована в TON):"
    )
    await callback.answer()

@dp.message(Withdraw.waiting_for_amount)
async def handle_withdraw_amount(message: types.Message, state: FSMContext):
    """Обработка суммы вывода."""
    uid = message.from_user.id
    try:
        amount = int(message.text)
        if amount <= 0:
            return await message.answer("Сумма должна быть > 0.")
        bal = get_balance(uid)
        if amount > bal:
            return await message.answer(f"Недостаточно ₽. Ваш баланс: {format_rubles(bal)} ₽.")
        
        await state.update_data(amount=amount)
        await state.set_state(Withdraw.waiting_for_details)
        
        await message.answer(
            f"Сумма к выводу: {format_rubles(amount)} ₽.\n"
            "Введите адрес TON-кошелька и, при необходимости, любые комментарии:"
        )
    except ValueError:
        return await message.answer("Введите сумму числом.")

@dp.message(Withdraw.waiting_for_details)
async def handle_withdraw_details(message: types.Message, state: FSMContext):
    """Обработка реквизитов и отправка заявки админу."""
    uid = message.from_user.id
    details = message.text.strip()
    data = await state.get_data()
    amount = data.get("amount")
    
    # Списываем баланс сразу (обновленная change_balance сохраняет новый баланс)
    change_balance(uid, -amount)
    
    # Расчет эквивалента в TON
    rate = await get_ton_rub_rate()
    ton_equiv = amount / rate if rate > 0 else 0
    ton_text = f"{ton_equiv:.4f} TON"

    # Уведомление администраторов
    username = message.from_user.username
    link = f"tg://user?id={uid}"
    mention = f"@{username}" if username else f"ID {uid}"
    
    msg_admin = (
        f"💸 **НОВАЯ ЗАЯВКА НА ВЫВОД (TON)**\n\n"
        f"👤 Пользователь: {mention}\n"
        f"🆔 user_id: {uid}\n"
        f"🔗 Профиль: {link}\n\n"
        f"💰 Сумма: {format_rubles(amount)} ₽\n"
        f"💎 Эквивалент: {ton_text}\n"
        f"📄 Комментарий: {details}\n\n"
        f"После фактической отправки TON уменьшите баланс через /removebalance или /setbalance."
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, msg_admin)
        except Exception:
            pass

    await message.answer(
        "✅ **Заявка на вывод отправлена администратору.**\n"
        "После обработки вам отправят TON на указанные реквизиты."
    )
    await state.clear()


# ==================================
#      РЕЙТИНГ
# ==================================

@dp.message(F.text == "🏆 Рейтинг")
async def msg_rating(m: types.Message):
    register_user(m.from_user)
    await cb_menu_rating(m)

@dp.callback_query(F.data == "rating")
@dp.message(Command("rating"))
async def cb_menu_rating(m: types.Message | CallbackQuery):
    """Меню выбора типа рейтинга."""
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кости (Топ-10)", callback_data="rating_dice")],
            [InlineKeyboardButton(text="🎩 Банкир (Топ-10)", callback_data="rating_banker")],
        ]
    )
    text = "🏆 **Рейтинг**\n\nВыберите тип рейтинга:"
    if isinstance(m, CallbackQuery):
        await m.message.edit_text(text, reply_markup=kb)
        await m.answer()
    else:
        await m.answer(text, reply_markup=kb)

@dp.callback_query(F.data == "rating_dice")
async def cb_rating_dice(callback: CallbackQuery):
    """Показывает рейтинг игроков в "Кости"."""
    # Logic from the old code, simplified and merged with new db function
    finished_games, _ = await get_users_profit_and_games_30_days()
    now = datetime.now(timezone.utc)
    user_stats = {}
    
    for g in finished_games:
        # finished_at теперь должен быть datetime благодаря db.py
        finished_at = datetime.fromisoformat(g["finished_at"]) if isinstance(g["finished_at"], str) else g["finished_at"]
        if (now - finished_at) > timedelta(days=30):
            continue
        
        # NOTE: Ваш оригинальный код не сохраняет profit в БД, 
        # поэтому расчет делается здесь на основе calculate_profit
        for uid in (g.get("creator_id"), g.get("opponent_id")):
            if uid is None: continue
            
            # Предполагаем, что ключ bet_amount используется в calculate_profit
            game_data = g.copy()
            game_data['bet_amount'] = g['bet'] if 'bet' in g else 0
            
            p = calculate_profit(uid, game_data)
            user_stats.setdefault(uid, {"profit": 0, "games": 0})
            user_stats[uid]["profit"] += p
            user_stats[uid]["games"] += 1

    top_players = sorted(
        [
            {"uid": uid, "username": user_usernames.get(uid, f"ID{uid}"), "profit": data["profit"]}
            for uid, data in user_stats.items()
        ],
        key=lambda x: x["profit"],
        reverse=True,
    )[:10]

    text = "🏆 **Рейтинг Костей (30 дней):**\n\n"
    if not top_players:
        text += "Нет данных."
    else:
        for i, player in enumerate(top_players):
            rank = i + 1
            emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"{rank}.")
            profit_str = f"+{player['profit']}" if player['profit'] > 0 else f"{player['profit']}"
            text += f"{emoji} **@{player['username']}** — **{format_rubles(profit_str)} ₽**\n"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к рейтингам", callback_data="rating")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "rating_banker")
async def cb_rating_banker(callback: CallbackQuery):
    """Показывает рейтинг "Банкиров"."""
    
    # NOTE: Ваша функция get_banker_rating_30_days не была определена в db.py, 
    # используем заглушку, но импорт сохранен
    
    try:
        # Если get_banker_rating_30_days реализована, она должна возвращать список
        top_bankers = await get_banker_rating_30_days() 
    except (NameError, TypeError):
        # Заглушка, если функция не реализована или не работает
        top_bankers = []

    text = "🎩 **Рейтинг Банкиров (30 дней):**\n\n"
    if not top_bankers:
        text += "Нет данных или функция рейтинга не реализована."
    else:
        for i, banker in enumerate(top_bankers):
            rank = i + 1
            emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"{rank}.")
            profit_str = f"+{banker['profit']}" if banker['profit'] > 0 else f"{banker['profit']}"
            text += f"{emoji} **@{banker['username']}** — **{format_rubles(profit_str)} ₽**\n"
            
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к рейтингам", callback_data="rating")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()


# ==================================
#      ОСТАЛЬНЫЕ ХЕНДЛЕРЫ
# ==================================

@dp.message(F.text == "👤 Профиль")
async def msg_profile(m: types.Message):
    register_user(m.from_user)
    uid = m.from_user.id
    
    # Загружаем данные из БД
    reg_date_dt = await get_user_registered_at(uid)
    reg_date_str = reg_date_dt.strftime("%d.%m.%Y %H:%M:%S") if reg_date_dt else "Неизвестно"
    dice_games_count = await get_user_dice_games_count(uid)
    # Используем get_user_raffle_bets_count как заглушку для banker games count
    banker_games_count = await get_user_raffle_bets_count(uid) 
    
    text = (
        f"👤 Ваш Профиль:\n\n"
        f"🆔 ID Пользователя: <code>{uid}</code>\n"
        f"🗓 Дата регистрации: {reg_date_str}\n"
        f"🎲 Всего игр в Кости: {dice_games_count}\n"
        f"🎩 Всего игр в Банкир: {banker_games_count}"
    )
    await m.answer(text, parse_mode="HTML")

@dp.callback_query(F.data.startswith("my_games:"))
async def cb_my_games(callback: CallbackQuery):
    """Показывает статистику и историю игр пользователя (Кости)."""
    uid = callback.from_user.id
    # Получение статистики из вашего старого кода, но с использованием обновленной get_user_games
    now = datetime.now(timezone.utc)
    finished = await get_user_games(uid)
    
    stats = {"month": {"games": 0, "profit": 0}, "week": {"games": 0, "profit": 0}, "day": {"games": 0, "profit": 0}}
    
    for g in finished:
        if not g.get("finished_at"): continue
        
        # finished_at теперь должен быть datetime благодаря db.py
        finished_at = datetime.fromisoformat(g["finished_at"]) if isinstance(g["finished_at"], str) else g["finished_at"]
        delta = now - finished_at
        
        # Передаем данные в calculate_profit, как ожидается
        game_data = g.copy()
        game_data['bet_amount'] = g['bet'] if 'bet' in g else 0
        game_data['game_type'] = 'dice' # Добавляем тип игры для логики
        p = calculate_profit(uid, game_data) 

        if delta <= timedelta(days=30):
            stats["month"]["games"] += 1
            stats["month"]["profit"] += p
        if delta <= timedelta(days=7):
            stats["week"]["games"] += 1
            stats["week"]["profit"] += p
        if delta <= timedelta(days=1):
            stats["day"]["games"] += 1
            stats["day"]["profit"] += p

    def ps(v): return ("+" if v > 0 else "") + format_rubles(v)

    stats_text = (
        f"🎲 Кости за месяц: {stats['month']['games']}\n"
        f"└ 💸 Профит: {ps(stats['month']['profit'])} ₽\n\n"
        f"🎲 За неделю: {stats['week']['games']}\n"
        f"└ 💸 Профит: {ps(stats['week']['profit'])} ₽\n\n"
        f"🎲 За сутки: {stats['day']['games']}\n"
        f"└ 💸 Профит: {ps(stats['day']['profit'])} ₽"
    )

    history = []
    for g in finished[:30]:
        
        my = "?"
        opp = "?"
        # В db.py нет rolls, используем creator_roll и opponent_roll
        if uid == g["creator_id"]:
            my = g.get("creator_roll", "?")
            opp = g.get("opponent_roll", "?")
        else:
            my = g.get("opponent_roll", "?")
            opp = g.get("creator_roll", "?")
        
        game_data = g.copy()
        game_data['bet_amount'] = g['bet'] if 'bet' in g else 0
        game_data['game_type'] = 'dice' 
        profit = calculate_profit(uid, game_data)
        
        if profit > 0:
            emoji, text_res = "🟩", "Победа"
        elif profit < 0:
            emoji, text_res = "🟥", "Проигрыш"
        else:
            emoji, text_res = "⚪", "Ничья"

        history.append(
            f"{emoji} Игра #{g['id']}: {text_res} ({my}:{opp}), {ps(profit)} ₽"
        )
    
    text = f"📋 **Мои игры (Кости):**\n\n" \
           f"**Статистика:**\n{stats_text}\n\n" \
           f"**Последние 30 игр:**\n" + "\n".join(history)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Назад", callback_data="refresh_games")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "deposit_menu")
async def cb_deposit_menu(callback: CallbackQuery):
    uid = callback.from_user.id
    rate = await get_ton_rub_rate()
    half_ton = int(rate * 0.5)
    one_ton = int(rate * 1)
    
    ton_url = f"ton://transfer/{TON_WALLET_ADDRESS}?text=ID{uid}"
    
    text = (
        "💎 **Пополнение через TON**\n\n"
        f"1 TON ≈ {rate:.2f} ₽. Ваш ID для пополнения: `{uid}`\n\n"
        f"**Инструкция:**\n"
        "1. Отправьте TON на адрес кошелька ниже.\n"
        f"2. **Обязательно** укажите в комментарии свой ID в формате `ID{uid}`.\n"
        "3. Бот автоматически зачислит ₽ по текущему курсу.\n\n"
        f"**Адрес для пополнения:**\n"
        f"<code>{TON_WALLET_ADDRESS}</code>"
    )
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"0.5 TON ({format_rubles(half_ton)} ₽)", url=f"{ton_url}0.5"),
                InlineKeyboardButton(text=f"1 TON ({format_rubles(one_ton)} ₽)", url=f"{ton_url}1"),
            ],
            [
                InlineKeyboardButton(text="Копировать адрес", callback_data=f"copy_address:{TON_WALLET_ADDRESS}"),
                InlineKeyboardButton(text="Копировать ID", callback_data=f"copy_id:{uid}"),
            ],
        ]
    )
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data.startswith("copy_address:"))
async def cb_copy_address(callback: CallbackQuery):
    address = callback.data.split(":", 1)[1]
    await callback.answer(f"Адрес скопирован: {address}", show_alert=True)

@dp.callback_query(F.data.startswith("copy_id:"))
async def cb_copy_id(callback: CallbackQuery):
    uid = callback.data.split(":", 1)[1]
    await callback.answer(f"ID скопирован: {uid}", show_alert=True)

@dp.callback_query(F.data == "menu_games")
async def cb_menu_games(callback: CallbackQuery):
    """Возврат в меню выбора игры."""
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кости", callback_data="mode_dice")],
            [InlineKeyboardButton(text="🎩 Банкир", callback_data="mode_banker")],
        ]
    )
    await callback.message.edit_text("Выберите режим игры:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "help")
async def cb_help(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кости", callback_data="help_dice")],
            [InlineKeyboardButton(text="🎩 Банкир", callback_data="help_banker")],
            [InlineKeyboardButton(text="💸 Баланс/Вывод", callback_data="help_balance")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="menu_games")],
        ]
    )
    await callback.message.answer("🐼 Выберите раздел помощи:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "help_dice")
async def cb_help_dice(callback: CallbackQuery):
    text = (
        "🎲 Помощь: Кости (1 на 1)\n\n"
        "1. Игроки ставят в банк сумму первоначальной ставки.\n"
        "2. Игроки бросают кости, тот, кто выбросил больше - забирает весь банк (минус 1% комиссии). "
        "Результат при ничьей - возврат ставки.\n"
    )
    await callback.message.answer(text)
    await callback.answer()

@dp.callback_query(F.data == "help_banker")
async def cb_help_banker(callback: CallbackQuery):
    text = (
        "🎩 Помощь: Банкир (1 против N)\n\n"
        "1. Банкир (создатель игры) ставит ставку и принимает до 5 игроков.\n"
        "2. Банкир бросает кости (целевое число).\n"
        "3. Присоединившиеся игроки бросают кости. Если игрок выбрасывает больше, "
        "он выигрывает ставку Банкира (минус 1% комиссии).\n"
        "4. Если игрок выбрасывает меньше или равно, он проигрывает ставку в пользу Банкира.\n"
    )
    await callback.message.answer(text)
    await callback.answer()

@dp.callback_query(F.data == "help_balance")
async def cb_help_balance(callback: CallbackQuery):
    text = (
        "💸 Помощь: Баланс и Вывод\n\n"
        "1. Пополнение: Отправьте TON на указанный адрес, обязательно указав в комментарии "
        "свой ID (формат IDXXXXXX). Бот автоматически зачислит ₽ по текущему курсу.\n"
        "2. Вывод: Вывод осуществляется в TON по курсу. Заявка отправляется администратору.\n"
        "3. Переводы: Доступны между игроками в разделе 'Баланс'.\n"
        f"4. Комиссия: С каждой игры (Кости, Банкир) удерживается {COMMISSION_RATE*100}% комиссии."
    )
    await callback.message.answer(text)
    await callback.answer()

# ==================================
#      АДМИН-КОМАНДЫ
# ==================================

@dp.message(Command("addbalance"))
async def cmd_addbalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Нет прав.")
    
    parts = m.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /addbalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    
    # Обновленная change_balance корректно сохранит новый баланс
    change_balance(uid, amount)
    
    await m.answer(f"✅ Баланс {uid} увеличен на {format_rubles(amount)} ₽")

@dp.message(Command("removebalance"))
async def cmd_removebalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Нет прав.")
    
    parts = m.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /removebalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    
    # Обновленная change_balance корректно сохранит новый баланс (с отрицательной дельтой)
    change_balance(uid, -amount)
    
    await m.answer(f"✅ Баланс {uid} уменьшен на {format_rubles(amount)} ₽")

@dp.message(Command("setbalance"))
async def cmd_setbalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Нет прав.")
    
    parts = m.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /setbalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    
    # Обновленная set_balance корректно сохранит новый баланс
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
        f"💸 Баланс админа (накопленная комиссия и игры): {format_rubles(bal)} ₽.\n"
        f"≈ {ton_equiv:.4f} TON по текущему курсу ({rate:.2f} ₽ за 1 TON).\n"
        f"Эти ₽ можно вывести, обменяв TON на рубли."
    )

# === TON Worker ===

async def ton_deposit_worker():
    """Периодически опрашивает tonapi по адресу кошелька и ищет новые входящие переводы."""
    if not TON_WALLET_ADDRESS:
        return

    url = f"https://tonapi.io/v2/blockchain/accounts/{TON_WALLET_ADDRESS}/transactions?limit=50"
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    data = await resp.json()
            
            tx_list = data.get("transactions") or data.get("data") or []
            
            for tx in tx_list:
                tx_hash = tx.get("hash") or tx.get("transaction_id") or ""
                if not tx_hash or tx_hash in processed_ton_tx:
                    continue
                
                comment = ""
                in_msg = tx.get("in_msg") or tx.get("in_message") or {}
                if isinstance(in_msg, dict):
                    comment = in_msg.get("message") or ""

                # Проверка комментария на наличие ID
                match = re.search(r"ID(\d+)", comment, re.IGNORECASE)
                if not match:
                    continue # Игнорируем, если нет ID в комментарии
                
                user_id = int(match.group(1))
                
                # Получение TON-суммы
                if 'value' not in in_msg or not in_msg['value']:
                    continue

                value_nano = int(in_msg['value'])
                ton_amount = value_nano / 10**9 # Переводим из наноТОН
                
                if ton_amount <= 0:
                    continue

                # Конвертация в рубли/монеты
                rate = await get_ton_rub_rate()
                coins_amount = int(ton_amount * rate)
                
                if coins_amount <= 0:
                    continue

                # 1. Зачисление баланса (обновленная change_balance сохраняет новый баланс)
                change_balance(user_id, coins_amount)
                
                # 2. Запись в БД
                await add_ton_deposit(tx_hash, user_id, ton_amount, coins_amount, comment)
                processed_ton_tx.add(tx_hash)
                
                # 3. Уведомление пользователя
                try:
                    await bot.send_message(
                        user_id,
                        f"💰 **Пополнение баланса!**\n"
                        f"Зачислено: {ton_amount:.4f} TON\n"
                        f"Эквивалент: **{format_rubles(coins_amount)} ₽**\n"
                        f"Ваш новый баланс: {format_rubles(get_balance(user_id))} ₽"
                    )
                except Exception:
                    # Пользователь заблокировал бота
                    pass

            # Пауза перед следующим опросом (30 секунд)
            await asyncio.sleep(30) 

        except Exception as e:
            print(f"Ошибка в TON Worker: {e}")
            await asyncio.sleep(60) # Увеличиваем паузу при ошибке

# ========================
#      ЗАПУСК БОТА
# ========================

async def main():
    print("Бот запущен (TON + Кости + Банкир + FSM, PostgreSQL).")
    
    # инициализация БД и загрузка данных
    try:
        from db import pool 
        # Инициализируем БД, передавая ссылки на структуры в памяти
        await init_db(user_balances, user_usernames, processed_ton_tx)
        
        # Обновляем next_game_id для активных игр в кэше
        global next_game_id
        if pool:
            async with pool.acquire() as conn:
                # Находим максимальный ID в таблице games, чтобы продолжить нумерацию
                max_id = await conn.fetchval("SELECT MAX(id) FROM games")
                next_game_id = (max_id or 0) + 1
            
    except Exception as e:
        print(f"Критическая ошибка при инициализации БД: {e}")
        return # Выход, если БД не работает

    # Запускаем фоновый воркер для проверки депозитов TON
    asyncio.create_task(ton_deposit_worker())

    # Запускаем диспетчер
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен.")




