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
# ПРЕДПОЛАГАЕТСЯ, что эти функции в db.py существуют и корректны
from db import (
    init_db,
    upsert_user,
    upsert_game,
    get_user_games,
    add_ton_deposit,
    add_transfer,
    get_user_registered_at,
    get_user_dice_games_count,
    get_user_raffle_bets_count, # Временно используется для Банкира (должна быть get_user_banker_games_count)
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

# --- НОВАЯ КОНСТАНТА ---
DICE_BET_MIN_CANCEL_AGE = timedelta(minutes=1) 

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
# Кэш games используется ТОЛЬКО для активных игр "Кости" (opponent_id=None)
games: dict[int, dict] = {} 
next_game_id = 1 
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
    """Возвращает баланс в рублях/монетах, загруженный из БД (СИНХРОННАЯ)."""
    if uid not in user_balances:
        user_balances[uid] = START_BALANCE_COINS
    return user_balances[uid]

async def _schedule_upsert_user(uid: int, balance_delta: int = 0, registered_at: datetime | None = None):
    """Асинхронное сохранение пользователя в БД."""
    username = user_usernames.get(uid)
    await upsert_user(uid, username, balance_delta, registered_at)

async def change_balance(uid: int, delta: int):
    """Обновляет баланс в памяти и запускает АСИНХРОННОЕ сохранение (ИСПРАВЛЕНО: теперь await)."""
    get_balance(uid)
    user_balances[uid] += delta
    await _schedule_upsert_user(uid, delta)

async def set_balance(uid: int, value: int):
    """Устанавливает баланс в памяти и запускает АСИНХРОННОЕ сохранение (ИСПРАВЛЕНО: теперь await)."""
    current_balance = get_balance(uid)
    delta = value - current_balance
    user_balances[uid] = value
    await _schedule_upsert_user(uid, delta)

def format_rubles(n: int | str) -> str:
    """Форматирует число с разделителем тысяч (СИНХРОННАЯ)."""
    # Преобразуем str в int, если необходимо, для корректного форматирования
    n_int = int(n) if isinstance(n, str) and n.lstrip("+-").isdigit() else n
    return f"{n_int:,}".replace(",", " ")

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
        # Возвращаем кэшированное значение или дефолтное
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
    """Регистрация пользователя в памяти и фоновое сохранение в БД (СИНХРОННАЯ)."""
    uid = user.id
    # Используем create_task для фоновой операции, чтобы не блокировать хендлер
    if uid not in user_balances:
        user_balances[uid] = START_BALANCE_COINS
        asyncio.create_task(_schedule_upsert_user(uid, registered_at=datetime.now(timezone.utc)))
    
    if user.username:
        user_usernames[uid] = user.username
        asyncio.create_task(_schedule_upsert_user(uid))

def resolve_user_by_username(username_str: str) -> int | None:
    uname = username_str.strip().lstrip("@").lower()
    for uid, uname_stored in user_usernames.items():
        if uname_stored and uname_stored.lower() == uname:
            return uid
    return None

def calculate_profit(uid: int, g: dict) -> int:
    """Рассчитывает профит игрока в игре (для статистики)."""
    bet = g.get("bet_amount", g.get("bet", 0)) # Использование bet_amount для БД, bet для кэша
    winner_id = g.get("winner_id")
    game_type = g.get('game_type')

    # Dice game logic
    if game_type == 'dice':
        creator_id = g.get("creator_id")
        opponent_id = g.get("opponent_id")

        if winner_id is None and g.get('finished') in (1, True):
            # Отмененная игра (winner_id=0 for cancelled in db)
            return 0 
        
        commission = int(2 * bet * COMMISSION_RATE)
        
        if winner_id == creator_id or winner_id == opponent_id:
            profit = bet - commission 
            if uid == winner_id:
                return profit
            else:
                return -bet 
        
        if winner_id == 0: 
            # Ничья (Rolls are equal) или отмена с возвратом
            return 0 
        
        return 0 
    
    # Banker game logic (КРИТИЧНАЯ ПРОБЛЕМА: ОТСУТСТВОВАЛА ЛОГИКА)
    if game_type == 'banker':
        creator_id = g.get("creator_id")
        joiners_list = g.get('joiners', []) # joiners должен быть сохранен в БД
        
        if uid == creator_id:
            # Банкир
            banker_profit = 0
            # Если не завершена/отменена
            if g.get('finished') not in (1, True):
                return 0
            
            # Логика профита Банкира
            for joiner in joiners_list:
                if joiner.get('won') is True:
                    # Проигрыш: (ставка - комиссия)
                    banker_profit -= (bet - int(bet * COMMISSION_RATE)) 
                elif joiner.get('won') is False:
                    # Выигрыш: ставка
                    banker_profit += bet
            
            # Комиссия Банкира снимается с его чистого дохода
            if banker_profit > 0:
                banker_profit -= int(banker_profit * COMMISSION_RATE)
            
            # Профит = чистый доход/убыток (ставка возвращается, поэтому не входит в профит)
            return banker_profit 
        
        # Присоединившийся
        joiner_info = next((j for j in joiners_list if j['user_id'] == uid), None)
        if joiner_info:
            if joiner_info.get('won') is True:
                # Выигрыш: (ставка - комиссия)
                return bet - int(bet * COMMISSION_RATE)
            elif joiner_info.get('won') is False:
                # Проигрыш: -ставка
                return -bet
            # Если не выиграл/проиграл (отмена, например)
            return 0
    
    return 0


# ========================
#      ОБЩИЕ ХЕНДЛЕРЫ
# ========================

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    register_user(m.from_user)
    # ... (Остальной код start без изменений)
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
        await change_balance(c, prize) # ИСПРАВЛЕНО: await change_balance
        await change_balance(MAIN_ADMIN_ID, commission) # ИСПРАВЛЕНО: await change_balance
    elif orr > cr:
        winner = "opponent"
        winner_id = o
        await change_balance(o, prize) # ИСПРАВЛЕНО: await change_balance
        await change_balance(MAIN_ADMIN_ID, commission) # ИСПРАВЛЕНО: await change_balance
    else:
        winner = "draw"
        # Возвращаем ставки при ничьей
        await change_balance(c, bet) # ИСПРАВЛЕНО: await change_balance
        await change_balance(o, bet) # ИСПРАВЛЕНО: await change_balance
        commission = 0 # Комиссия возвращается при ничьей

    # Обновление кэша и БД
    g["creator_roll"] = cr
    g["opponent_roll"] = orr
    g["winner"] = winner
    g["finished"] = True
    g["finished_at"] = datetime.now(timezone.utc)
    
    # rolls = [creator_roll, opponent_roll]
    await upsert_game(g["id"], c, 'dice', bet, 0, 1, winner_id, [cr, orr], opponent_id=o)
    
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
    
    # Снимаем ставку (ИСПРАВЛЕНО: await change_balance)
    await change_balance(uid, -bet)

    # Сохраняем игру в БД
    await upsert_game(
        game_id=gid, creator_id=uid, game_type='dice', bet_amount=bet,
        target_score=0, finished=0
    )
    
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
        "finished": False,
        "created_at": datetime.now(timezone.utc),
        "finished_at": None,
    }
    games[gid] = game_data

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
        # Обновляем список игр, если игра занята/отменена, чтобы пользователь увидел актуальные данные
        await send_games_list(callback.message.chat.id, callback.from_user.id, callback.message.message_id)
        return await callback.answer("Игра не найдена или уже занята.", show_alert=True)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✔ Вступить", callback_data=f"join_confirm:{gid}")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="refresh_games")],
        ]
    )

    creator_username = user_usernames.get(g["creator_id"], f"ID{g['creator_id']}")
    # ИСПРАВЛЕНО: используем edit_message_text, чтобы не засорять чат
    await callback.message.edit_text(
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
        await send_games_list(callback.message.chat.id, callback.from_user.id, callback.message.message_id)
        return await callback.answer("Игра недоступна.", show_alert=True)

    bet = g["bet"]
    if get_balance(uid) < bet:
        return await callback.answer("Недостаточно ₽.", show_alert=True)

    # Присоединение
    g["opponent_id"] = uid
    user_usernames[uid] = callback.from_user.username or user_usernames.get(uid) or f"ID{uid}"
    await change_balance(uid, -bet) # ИСПРАВЛЕНО: await change_balance

    # Обновляем кэш и БД
    await upsert_game(
        g["id"], g["creator_id"], 'dice', bet, g.get("creator_roll", 0), 0, opponent_id=uid
    )

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
        await send_games_list(callback.message.chat.id, callback.from_user.id, callback.message.message_id)
        return await callback.answer("Игра не найдена или уже занята.", show_alert=True)

    # Кнопка "Отменить" доступна только в первую минуту
    time_passed = datetime.now(timezone.utc) - g["created_at"]
    
    rows = []
    if time_passed < DICE_BET_MIN_CANCEL_AGE:
        rows.append([InlineKeyboardButton(text="❌ Отменить ставку", callback_data=f"cancel_dice_game:{gid}")])
    
    rows.append([InlineKeyboardButton(text="⬅ Назад", callback_data="refresh_games")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    # ИСПРАВЛЕНО: используем edit_message_text, чтобы не засорять чат
    await callback.message.edit_text(
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

    if (datetime.now(timezone.utc) - g["created_at"]) > DICE_BET_MIN_CANCEL_AGE:
        return await callback.answer(
            f"Ставку можно отменить только в течение первой минуты после создания.", 
            show_alert=True
        )

    bet = g["bet"]
    await change_balance(uid, bet) # ИСПРАВЛЕНО: await change_balance
    
    # Завершаем игру в БД (finished=1, winner_id=0, rolls=[])
    await upsert_game(g["id"], g["creator_id"], 'dice', bet, 0, 1, winner_id=0)
    
    if gid in games:
        del games[gid]
    
    # ИСПРАВЛЕНО: используем edit_message_text
    await callback.message.edit_text(
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
            # Кнопка 'Начать бросок' доступна только Банкиру
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

    # 1. Списываем ставку у "Банкира" (ИСПРАВЛЕНО: await change_balance)
    await change_balance(uid, -bet_amount)

    # 2. Создаем игру в БД
    game_id = await upsert_game(
        game_id=None, creator_id=uid, game_type='banker', bet_amount=bet_amount,
        target_score=0, finished=0
    )
    
    # (ИСПРАВЛЕНО: Удален кэш games[game_id] для Банкира, т.к. он не обновлялся консистентно)
    
    # 3. Отправляем сообщение о создании
    text = f"🎩 **Игра 'Банкир' №{game_id} создана!**\n\n" \
           f"**Банкир:** @{message.from_user.username or f'ID{uid}'}\n" \
           f"**Ставка:** {format_rubles(bet_amount)} ₽\n" \
           f"**Слоты:** 0/{BANKER_MAX_JOINERS}\n" \
           "Нажмите **'Начать бросок'** или ожидайте присоединившихся."
    
    await message.answer(text, reply_markup=get_banker_game_kb(game_id, 0))

@dp.message(BankerGame.waiting_for_bet)
async def handle_banker_bet_invalid(message: types.Message):
    await message.answer("Неверный формат ставки. Введите целое число.")


@dp.callback_query(F.data.startswith("banker_join_"))
async def cb_banker_join(callback: CallbackQuery):
    """Присоединение к игре "Банкир"."""
    game_id = int(callback.data.split('_')[-1])
    joiner_id = callback.from_user.id
    
    # ИСПРАВЛЕНО: Получаем актуальные данные из БД
    game = await get_game(game_id)
    if not game or game['finished'] != 0 or game['game_type'] != 'banker' or game['target_score'] != 0:
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
    bet_amount = game['bet_amount']
    if get_balance(joiner_id) < bet_amount:
        return await callback.answer(f"Недостаточно средств. Ваш баланс: {format_rubles(get_balance(joiner_id))} ₽", show_alert=True)

    # 1. Списываем ставку у присоединившегося (ИСПРАВЛЕНО: await change_balance)
    await change_balance(joiner_id, -bet_amount)

    # 2. Обновляем joiners в БД
    joiners_list.append({
        'user_id': joiner_id,
        'username': callback.from_user.username or user_usernames.get(joiner_id) or f"ID{joiner_id}", # Загружаем из кэша памяти, если нет в ТГ
        'bet_amount': bet_amount,
        'roll': None,
        'won': None,
        'processed': False
    })
    
    # Сохраняем обратно в БД
    await upsert_game(
        game_id=game_id, creator_id=game['creator_id'], game_type='banker',
        bet_amount=bet_amount, target_score=game['target_score'], finished=0, joiners=joiners_list
    )
    # ИСПРАВЛЕНО: Удалил games[game_id]['joiners'] = joiners_list

    await callback.answer("Вы успешно присоединились! Ожидайте броска Банкира.", show_alert=True)
    
    # Обновляем сообщение
    creator_user = user_usernames.get(game['creator_id'], f"ID{game['creator_id']}")
    joiners_count = len(joiners_list)
    
    text = f"🎩 **Игра 'Банкир' №{game_id}**\n\n" \
           f"**Банкир:** @{creator_user}\n" \
           f"**Ставка:** {format_rubles(bet_amount)} ₽\n" \
           f"**Слоты:** {joiners_count}/{BANKER_MAX_JOINERS}\n" \
           f"**Присоединились:** {', '.join([f'@{j["username"]}' for j in joiners_list])}\n" \
           "Ожидаем присоединившихся игроков или начала броска."
    
    await callback.message.edit_text(text, reply_markup=get_banker_game_kb(game_id, joiners_count))


@dp.callback_query(F.data.startswith("banker_roll_start_"))
async def cb_banker_roll_start(callback: CallbackQuery):
    """Инициирует бросок костей Банкиром."""
    game_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    # ИСПРАВЛЕНО: Получаем актуальные данные из БД
    game = await get_game(game_id)
    if not game or game['finished'] != 0 or game['game_type'] != 'banker' or game['target_score'] != 0:
        return await callback.answer("Игра недоступна или уже начата.", show_alert=True)

    if game['creator_id'] != user_id:
        return await callback.answer("Только Банкир может начать бросок.", show_alert=True)

    joiners_list = game.get('joiners', [])
    if not joiners_list:
        return await callback.answer("Нет присоединившихся игроков.", show_alert=True)

    # 1. Банкир бросает
    creator_roll = random.randint(1, 6)
    
    # 2. Записываем бросок Банкира (target_score)
    # ИСПРАВЛЕНО: Обновляем только БД
    await upsert_game(
        game_id=game_id, creator_id=user_id, game_type='banker', bet_amount=game['bet_amount'],
        target_score=creator_roll, finished=0, rolls=[creator_roll], joiners=joiners_list
    )
    # ИСПРАВЛЕНО: Удалил games[game_id]['target_score'] = creator_roll 

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
                "Ваша очередь бросать!",
                reply_markup=get_joiner_roll_kb(game_id)
            )
        except Exception as e:
            # Не используем print в рабочем коде, лучше logging
            # print(f"Не удалось отправить сообщение игроку {joiner['user_id']}: {e}") 
            pass
            
    await callback.answer("Вы бросили кости! Игроки уведомлены.")


@dp.callback_query(F.data.startswith("banker_roll_joiner_"))
async def cb_banker_roll_joiner(callback: CallbackQuery):
    """Бросок присоединившегося в игре "Банкир" (в личке)."""
    game_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    # ИСПРАВЛЕНО: Получаем актуальные данные из БД
    game = await get_game(game_id)
    if not game or game['finished'] != 0 or game['game_type'] != 'banker' or game['target_score'] == 0:
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
    updated_joiners_list = []
    for j in joiners_list:
        if j['user_id'] == user_id:
            j['roll'] = joiner_roll
        updated_joiners_list.append(j)
    
    # Обновляем игру в БД
    await upsert_game(
        game_id=game_id, creator_id=game['creator_id'], game_type='banker',
        bet_amount=game['bet_amount'], target_score=game['target_score'], finished=0,
        rolls=game['rolls'], joiners=updated_joiners_list
    )
    # ИСПРАВЛЕНО: Удалил games[game_id]['joiners'] = joiners_list
    
    await callback.message.edit_text(
        f"🎲 Вы бросили **{joiner_roll}**! Банкир бросил **{game['target_score']}**.\nОжидаем других игроков."
    )
    await callback.answer("Вы бросили кости!")
    
    # Проверка на завершение (получаем свежий список joiners)
    if all(j['roll'] is not None for j in updated_joiners_list):
        await finish_banker_game(game_id)


async def finish_banker_game(game_id: int):
    """Завершает игру "Банкир" и распределяет средства."""
    # ИСПРАВЛЕНО: Получаем актуальные данные из БД
    game = await get_game(game_id)
    if not game or game['finished'] != 0 or game['game_type'] != 'banker':
        return

    creator_id = game['creator_id']
    banker_roll = game['target_score']
    bet_amount = game['bet_amount'] 
    joiners_list = game['joiners']
    
    # Проверка, что все бросили
    if not all(j['roll'] is not None for j in joiners_list):
         # ИСПРАВЛЕНО: Добавил защиту от случайного вызова finish
        return 

    commission_rate = COMMISSION_RATE

    results_text = f"🎉 **Игра 'Банкир' №{game_id} завершена!** 🎉\n\n"
    banker_username = user_usernames.get(creator_id, f"ID{creator_id}")
    results_text += f"**Банкир** (@{banker_username}) бросил **{banker_roll}**\n\n"
    
    banker_profit_before_commission = 0
    total_banker_commission = 0
    
    # 1. Обработка результатов присоединившихся
    for joiner in joiners_list:
        joiner_roll = joiner['roll']
        joiner_wins = joiner_roll > banker_roll
        joiner['won'] = joiner_wins
        
        if joiner_wins:
            # Присоединившийся выиграл
            commission_win = int(bet_amount * commission_rate)
            win_amount = bet_amount - commission_win
            # Возврат ставки + чистый выигрыш (ИСПРАВЛЕНО: await change_balance)
            await change_balance(joiner['user_id'], bet_amount + win_amount) 
            
            banker_profit_before_commission -= win_amount 
            total_banker_commission += commission_win # Комиссия с выигрыша игрока
            
            results_text += f"🔹 @{joiner['username']} бросил **{joiner_roll}** и **ВЫИГРАЛ** (+{win_amount} ₽)\n"
        else:
            # Присоединившийся проиграл (ставка уже списана)
            banker_profit_before_commission += bet_amount 
            results_text += f"🔸 @{joiner['username']} бросил **{joiner_roll}** и **ПРОИГРАЛ** (-{bet_amount} ₽)\n"
        
        joiner['processed'] = True

    # 2. Выплата Банкиру
    final_banker_profit = banker_profit_before_commission
    
    # Комиссия Банкира снимается только с его чистого дохода (Banker wins - Banker losses)
    if banker_profit_before_commission > 0:
        banker_commission_on_win = int(banker_profit_before_commission * commission_rate)
        final_banker_profit = banker_profit_before_commission - banker_commission_on_win
        total_banker_commission += banker_commission_on_win # Комиссия с выигрыша банкира
    
    # Зачисление комиссии админу (общая со всех выигрышей) (ИСПРАВЛЕНО: await change_balance)
    await change_balance(MAIN_ADMIN_ID, total_banker_commission)
    
    # Возврат Банкиру его ставки + чистый доход/убыток (ИСПРАВЛЕНО: await change_balance)
    await change_balance(creator_id, bet_amount + final_banker_profit) 
    
    results_text += f"\n**Итог Банкира:**\n" \
                    f"Начальная ставка: {format_rubles(bet_amount)} ₽\n" \
                    f"Прибыль/убыток (до комиссии): {format_rubles(banker_profit_before_commission)} ₽\n" \
                    f"Комиссия ({commission_rate*100}%): -{format_rubles(total_banker_commission)} ₽\n" \
                    f"Чистая выплата (Возврат ставки + Прибыль): **{format_rubles(bet_amount + final_banker_profit)} ₽**"

    # 3. Завершение игры в БД и удаление из кэша
    await upsert_game(
        game_id=game_id, creator_id=creator_id, game_type='banker', bet_amount=bet_amount,
        target_score=banker_roll, finished=1, winner_id=creator_id,
        rolls=game['rolls'], joiners=joiners_list
    )
    if game_id in games:
        del games[game_id]
    
    # Уведомление в чат 
    try:
        await bot.send_message(creator_id, results_text)
    except Exception:
        pass 

@dp.callback_query(F.data.startswith("banker_cancel_"))
async def cb_banker_cancel(callback: CallbackQuery):
    """Отмена игры "Банкир" (только Банкиром)."""
    game_id = int(callback.data.split('_')[-1])
    user_id = callback.from_user.id
    
    # ИСПРАВЛЕНО: Получаем актуальные данные из БД
    game = await get_game(game_id)
    if not game or game['finished'] != 0 or game['game_type'] != 'banker' or game['target_score'] != 0:
        return await callback.answer("Игра не найдена, уже завершена или начался бросок.", show_alert=True)

    if game['creator_id'] != user_id:
        return await callback.answer("Только Банкир может отменить игру.", show_alert=True)
    
    # Возвращаем ставки Банкиру и всем присоединившимся (ИСПРАВЛЕНО: await change_balance)
    await change_balance(game['creator_id'], game['bet_amount'])
    for joiner in game.get('joiners', []):
        await change_balance(joiner['user_id'], joiner['bet_amount'])
        
    # Завершаем игру (winner_id=0 для отмененных)
    await upsert_game(game_id, game['creator_id'], 'banker', game['bet_amount'], 0, 1, winner_id=0)
    
    if game_id in games:
        del games[game_id]
        
    await callback.message.edit_text(f"🚫 **Игра 'Банкир' №{game_id} отменена!**\nСредства возвращены игрокам.")
    await callback.answer()

# ==================================
#      ЛОГИКА БАЛАНСА И ПЕРЕВОДОВ (FSM)
# ==================================

@dp.message(F.text == "💼 Баланс")
async def msg_balance(m: types.Message):
    register_user(m.from_user)
    uid = m.from_user.id
    bal_text = await format_balance_text(uid)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💎 Пополнить (TON)", callback_data="deposit_menu")],
            [InlineKeyboardButton(text="🔄 Перевод", callback_data="transfer_menu")],
            [InlineKeyboardButton(text="💸 Вывод TON", callback_data="withdraw_menu")],
        ]
    )
    await m.answer(bal_text, reply_markup=kb)

@dp.callback_query(F.data == "transfer_menu")
async def cb_transfer_menu(callback: CallbackQuery, state: FSMContext):
    """Начало перевода (FSM)."""
    await state.set_state(Transfer.waiting_for_recipient)
    await callback.message.answer(
        "🔄 **Перевод ₽**\n"
        "Введите ID или @username получателя.\n"
        "Важно: получатель должен хотя бы раз написать боту."
    )
    await callback.answer()

@dp.message(Transfer.waiting_for_recipient)
async def handle_transfer_recipient(message: types.Message, state: FSMContext):
    """Обработка ID/Username получателя."""
    text = (message.text or "").strip()
    target_id: int | None = None
    
    if text.startswith("@"):
        target_id = resolve_user_by_username(text)
    elif text.isdigit():
        target_id = int(text)
    else:
        target_id = resolve_user_by_username(text)

    if not target_id or get_balance(target_id) == START_BALANCE_COINS: 
        # Дополнительная проверка, что пользователь есть в кэше/балансах
        return await message.answer(
            "Не удалось найти пользователя.\n"
            "Убедитесь, что он уже писал боту, и введите его ID или @username."
        )
    if target_id == message.from_user.id:
        return await message.answer("Нельзя переводить самому себе.")

    await state.update_data(target_id=target_id)
    await state.set_state(Transfer.waiting_for_amount)
    
    bal = get_balance(message.from_user.id)
    return await message.answer(
        f"Получатель ID: `{target_id}`. Ваш баланс: {format_rubles(bal)} ₽\n"
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

        # Выполнение перевода (ИСПРАВЛЕНО: await change_balance)
        await change_balance(uid, -amount)
        await change_balance(target_id, amount)

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
        f"1 TON ≈ {rate:.2f} ₽.\n\n"
        f"Введите сумму ₽ для вывода (целое число):"
    )
    await callback.answer()

@dp.message(Withdraw.waiting_for_amount)
async def handle_withdraw_amount(message: types.Message, state: FSMContext):
    """Обработка суммы вывода."""
    uid = message.from_user.id
    try:
        amount = int(message.text)
        bal = get_balance(uid)
        if amount <= 0:
            return await message.answer("Сумма должна быть > 0.")
        if amount > bal:
            return await message.answer(f"Недостаточно ₽. Ваш баланс: {format_rubles(bal)} ₽.")
        
        await state.update_data(amount=amount)
        await state.set_state(Withdraw.waiting_for_details)

        rate = await get_ton_rub_rate()
        ton_amount = amount / rate if rate > 0 else 0
        approx = f"{ton_amount:.4f} TON"
        
        return await message.answer(
            f"💸 Вывод в TON. Сумма: {format_rubles(amount)} ₽ (≈ {approx})\n\n"
            f"Напишите комментарий к выводу (например, TON-кошелёк, доп. информация):"
        )

    except ValueError:
        return await message.answer("Введите сумму числом.")

@dp.message(Withdraw.waiting_for_details)
async def handle_withdraw_details(message: types.Message, state: FSMContext):
    """Обработка реквизитов и отправка заявки."""
    # (ИСПРАВЛЕНО: Изменена логика: баланс не уменьшается сразу, уменьшать должен админ)
    
    uid = message.from_user.id
    details = message.text
    data = await state.get_data()
    amount = data["amount"]
    
    user = message.from_user
    username = user.username
    mention = f"@{username}" if username else f"id {uid}"
    link = f"https://t.me/{username}" if username else f"tg://user?id={uid}"

    rate = await get_ton_rub_rate()
    ton_amount = amount / rate if rate > 0 else 0
    ton_text = f"{ton_amount:.4f} TON"

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
    
    # finished_games содержит все игры за 30 дней, как в БД.
    finished_games, _ = await get_users_profit_and_games_30_days()
    now = datetime.now(timezone.utc)
    user_stats = {} 
    
    for g in finished_games:
        finished_at = g["finished_at"] 
        # Дополнительная проверка на 30 дней (хотя db.py должен был отфильтровать)
        if (now - finished_at) > timedelta(days=30):
            continue
            
        # Профит рассчитывается для создателя
        creator_id = g.get("creator_id")
        if creator_id:
            stats = user_stats.setdefault(creator_id, {"profit": 0, "games": 0})
            profit = calculate_profit(creator_id, g) 
            stats["profit"] += profit
            stats["games"] += 1

        # Профит рассчитывается для оппонента (если есть)
        opponent_id = g.get("opponent_id")
        if opponent_id:
            stats = user_stats.setdefault(opponent_id, {"profit": 0, "games": 0})
            profit = calculate_profit(opponent_id, g) 
            stats["profit"] += profit
            stats["games"] += 1


    top_list = sorted(user_stats.items(), key=lambda x: (x[1]['profit'], -x[1]['games']), reverse=True)
    
    # Формирование текста
    text = "👑 **Рейтинг Игроков в Кости (30 дней)** 👑\n\n"
    
    if not top_list:
        text += "Статистика по игре 'Кости' за последние 30 дней отсутствует."
    else:
        for i, (uid, player) in enumerate(top_list[:10]):
            rank = i + 1
            emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"{rank}.")
            profit_str = f"+{player['profit']}" if player['profit'] > 0 else f"{player['profit']}"
            username = user_usernames.get(uid) or f"ID{uid}"
            
            text += f"{emoji} **@{username}** — **{format_rubles(profit_str)} ₽** ({player['games']} игр)\n"
            
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к рейтингам", callback_data="rating")],
        ]
    )
    
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "rating_banker")
async def cb_rating_banker(callback: CallbackQuery):
    """Показывает топ-10 банкиров по прибыли за 30 дней."""
    top_bankers = await get_banker_rating_30_days() 
    
    text = "👑 **Рейтинг Банкиров (30 дней)** 👑\n\n"
    
    if not top_bankers:
        text += "Статистика по игре 'Банкир' за последние 30 дней отсутствует."
    else:
        for i, banker in enumerate(top_bankers):
            rank = i + 1
            emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"{rank}.")
            profit_str = f"+{banker['profit']}" if banker['profit'] > 0 else f"{banker['profit']}"
            
            # ИСПРАВЛЕНО: предполагаем, что db.py вернул username
            username = banker.get('username') or user_usernames.get(banker['creator_id']) or f"ID{banker['creator_id']}"
            
            text += f"{emoji} **@{username}** — **{format_rubles(profit_str)} ₽**\n"
            
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
    # ИСПРАВЛЕНО: Заменил на более корректное имя, но функция должна быть реализована в db.py
    banker_games_count = await get_user_raffle_bets_count(uid) # TODO: Заменить на get_user_banker_games_count

    text = (
        f"👤 Ваш Профиль:\n\n"
        f"🆔 ID Пользователя: <code>{uid}</code>\n"
        f"🗓 Дата регистрации: {reg_date_str}\n"
        f"🎲 Всего игр в Кости: {dice_games_count}\n"
        f"🎩 Всего игр в Банкир: {banker_games_count}"
    )

    await m.answer(text, parse_mode="HTML")

@dp.callback_query(F.data == "deposit_menu")
async def cb_deposit_menu(callback: CallbackQuery):
    uid = callback.from_user.id
    rate = await get_ton_rub_rate()
    half_ton = int(rate * 0.5)
    one_ton = int(rate * 1)

    ton_url = f"ton://transfer/{TON_WALLET_ADDRESS}?text=ID{uid}"

    text = (
        "💎 **Пополнение через TON**\n\n"
        f"1 TON ≈ {rate:.2f} ₽.\n"
        f"0.5 TON ≈ {format_rubles(half_ton)} ₽.\n"
        f"1 TON ≈ {format_rubles(one_ton)} ₽.\n\n"
        "Как пополнить:\n"
        "1️⃣ Откройте TON-кошелёк (Tonkeeper/@wallet).\n"
        f"2️⃣ Отправьте TON на адрес: <code>{TON_WALLET_ADDRESS}</code>\n"
        f"3️⃣ В комментарии к переводу укажите: <code>ID{uid}</code> (обязательно!).\n"
        "4️⃣ Бот автоматически зачислит ₽ по этому ID и отправит уведомление.\n\n"
        "Важно: 1 ₽ = 1 рубль (внутренняя валюта бота)."
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💎 Открыть кошелёк", url=ton_url)],
        ]
    )

    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

# === Админ-команды ===
@dp.message(Command("addbalance"))
async def cmd_addbalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id): return await m.answer("⛔ Нет прав.")
    parts = m.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /addbalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    await change_balance(uid, amount) # ИСПРАВЛЕНО: await change_balance
    await m.answer(f"✅ Баланс {uid} увеличен на {format_rubles(amount)} ₽. Теперь: {format_rubles(get_balance(uid))} ₽")

@dp.message(Command("removebalance"))
async def cmd_removebalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id): return await m.answer("⛔ Нет прав.")
    parts = m.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /removebalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    await change_balance(uid, -amount) # ИСПРАВЛЕНО: await change_balance
    await m.answer(f"✅ Баланс {uid} уменьшен на {format_rubles(amount)} ₽. Теперь: {format_rubles(get_balance(uid))} ₽")

@dp.message(Command("setbalance"))
async def cmd_setbalance(m: types.Message):
    register_user(m.from_user)
    if not is_admin(m.from_user.id): return await m.answer("⛔ Нет прав.")
    parts = m.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await m.answer("Использование: /setbalance user_id amount")

    uid = int(parts[1])
    amount = int(parts[2])
    await set_balance(uid, amount) # ИСПРАВЛЕНО: await set_balance
    await m.answer(f"✅ Баланс {uid} установлен на {format_rubles(amount)} ₽")

@dp.message(Command("adminprofit"))
async def cmd_adminprofit(m: types.Message):
    register_user(m.from_user)
    if m.from_user.id != MAIN_ADMIN_ID: return await m.answer("⛔ Только основной админ.")
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
    if not TON_WALLET_ADDRESS: return

    url = f"https://tonapi.io/v2/blockchain/accounts/{TON_WALLET_ADDRESS}/transactions?limit=50"

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    data = await resp.json()

            tx_list = data.get("transactions") or data.get("data") or []

            for tx in tx_list:
                tx_hash = tx.get("hash") or tx.get("transaction_id") or ""
                if not tx_hash or tx_hash in processed_ton_tx: continue

                comment = ""
                in_msg = tx.get("in_msg") or tx.get("in_message") or {}
                if isinstance(in_msg, dict):
                    comment = in_msg.get("message") or ""
                    msg_data = in_msg.get("msg_data") or {}
                    if isinstance(msg_data, dict):
                        comment = msg_data.get("text") or comment

                if not comment:
                    processed_ton_tx.add(tx_hash)
                    continue

                m = re.search(r"ID(\d{5,15})", str(comment))
                if not m:
                    processed_ton_tx.add(tx_hash)
                    continue

                user_id = int(m.group(1))

                value_nanoton = 0
                if isinstance(in_msg, dict):
                    v = in_msg.get("value")
                    if isinstance(v, str) and v.isdigit(): value_nanoton = int(v)
                    elif isinstance(v, int): value_nanoton = v

                if value_nanoton <= 0:
                    processed_ton_tx.add(tx_hash)
                    continue

                ton_amount = value_nanoton / 1e9
                rate = await get_ton_rub_rate()
                coins = int(ton_amount * rate)

                if coins <= 0:
                    processed_ton_tx.add(tx_hash)
                    continue

                await change_balance(user_id, coins) # ИСПРАВЛЕНО: await change_balance
                processed_ton_tx.add(tx_hash)

                await add_ton_deposit(tx_hash, user_id, ton_amount, coins, comment)

                try:
                    await bot.send_message(
                        user_id,
                        f"✅ **Пополнение через TON успешно!**\n\n"
                        f"Получено: {ton_amount:.4f} TON\n"
                        f"Курс: 1 TON ≈ {rate:.2f} ₽\n"
                        f"Зачислено: {format_rubles(coins)} ₽\n"
                        f"Текущий баланс: {format_rubles(get_balance(user_id))} ₽."
                    )
                except Exception:
                    pass

                try:
                    await bot.send_message(
                        MAIN_ADMIN_ID,
                        f"💎 Новое пополнение через TON\n"
                        f"User ID: {user_id}\n"
                        f"Комментарий: {comment}\n"
                        f"Сумма: {ton_amount:.4f} TON ≈ {format_rubles(coins)} ₽"
                    )
                except Exception:
                    pass

        except Exception as e:
            # print("Ошибка в ton_deposit_worker:", e) # Лучше использовать logging
            pass

        await asyncio.sleep(20)

# === История/Статистика ===
@dp.callback_query(F.data.startswith("my_games"))
async def cb_my_games(callback: CallbackQuery):
    uid = callback.from_user.id
    page = int(callback.data.split(":", 1)[1])

    # Копирование логики статистики из вашего старого кода, но с использованием обновленной get_user_games
    now = datetime.now(timezone.utc)
    # finished - список игр пользователя из БД
    finished = await get_user_games(uid)

    stats = {"month": {"games": 0, "profit": 0}, "week": {"games": 0, "profit": 0}, "day": {"games": 0, "profit": 0}}
    
    history = []

    for g in finished:
        if not g.get("finished_at"): continue
        
        finished_at = g["finished_at"] 
        delta = now - finished_at
        p = calculate_profit(uid, g) 

        if delta <= timedelta(days=30):
            stats["month"]["games"] += 1
            stats["month"]["profit"] += p
        if delta <= timedelta(days=7):
            stats["week"]["games"] += 1
            stats["week"]["profit"] += p
        if delta <= timedelta(days=1):
            stats["day"]["games"] += 1
            stats["day"]["profit"] += p
            
        # Логика для истории 
        if g.get('game_type') == 'dice':
            if uid == g["creator_id"]:
                rolls = g.get("rolls")
                my = rolls[0] if rolls and len(rolls) > 0 else "?"
                opp = rolls[1] if rolls and len(rolls) > 1 else "?"
            else:
                rolls = g.get("rolls")
                my = rolls[1] if rolls and len(rolls) > 1 else "?"
                opp = rolls[0] if rolls and len(rolls) > 0 else "?"

            profit = calculate_profit(uid, g)
            if profit > 0:
                emoji, text_res = "🟩", "Победа"
            elif profit < 0:
                emoji, text_res = "🟥", "Проигрыш"
            else:
                emoji, text_res = "⚪", "Ничья"

            history.append({
                "bet": g["bet_amount"],
                "emoji": emoji,
                "text": text_res,
                "my": my,
                "opp": opp
            })
        elif g.get('game_type') == 'banker':
            profit = calculate_profit(uid, g)
            
            if uid == g["creator_id"]:
                # Банкир
                if profit > 0:
                    emoji, text_res = "👑🟩", f"Банкир: Профит ({format_rubles(profit)})"
                elif profit < 0:
                    emoji, text_res = "👑🟥", f"Банкир: Убыток ({format_rubles(profit)})"
                else:
                    emoji, text_res = "👑⚪", "Банкир: Ноль"
                my, opp = g.get('target_score', '?'), 'Игроки'
            else:
                # Присоединившийся
                joiner_info = next((j for j in g.get('joiners', []) if j['user_id'] == uid), None)
                if profit > 0:
                    emoji, text_res = "🤝🟩", "Игрок: Победа"
                elif profit < 0:
                    emoji, text_res = "🤝🟥", "Игрок: Проигрыш"
                else:
                    emoji, text_res = "🤝⚪", "Игрок: Ноль"
                my, opp = joiner_info.get('roll', '?') if joiner_info else '?', g.get('target_score', '?')

            history.append({
                "bet": g["bet_amount"],
                "emoji": emoji,
                "text": text_res,
                "my": my,
                "opp": opp
            })
            
    # Форматирование профита
    def ps(v): return ("+" if v > 0 else "") + format_rubles(v)

    stats_text = (
        f"📋 Ваша статистика:\n\n"
        f"📊 Общие игры за месяц: {stats['month']['games']}\n"
        f"└ 💸 Профит: {ps(stats['month']['profit'])} ₽\n\n"
        f"📊 За неделю: {stats['week']['games']}\n"
        f"└ 💸 Профит: {ps(stats['week']['profit'])} ₽\n\n"
        f"📊 За сутки: {stats['day']['games']}\n"
        f"└ 💸 Профит: {ps(stats['day']['profit'])} ₽\n\n"
        f"📖 **История последних игр (бросок:противник)**"
    )

    
    # Копирование логики клавиатуры истории из вашего старого кода
    rows = []
    HISTORY_PAGE_SIZE = 10
    
    total = len(history)
    if total == 0:
        rows.append([InlineKeyboardButton(text="История пуста", callback_data="ignore")])
    else:
        pages = (total + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE
        # Убедимся, что page не выходит за границы
        page = max(0, min(page, pages - 1)) 

        start = page * HISTORY_PAGE_SIZE
        end = start + HISTORY_PAGE_SIZE

        for h in history[start:end]:
            text_line = f"{format_rubles(h['bet'])} ₽ | {h['emoji']} {h['text']} | {h['my']}:{h['opp']}"
            rows.append([InlineKeyboardButton(text=text_line, callback_data="ignore")])

        if pages > 1:
            rows.append([
                InlineKeyboardButton(text="<<", callback_data="my_games:0"),
                InlineKeyboardButton(text="<", callback_data=f"my_games:{max(0, page - 1)}"),
                InlineKeyboardButton(text=f"{page+1}/{pages}", callback_data="ignore"),
                InlineKeyboardButton(text=">", callback_data=f"my_games:{min(pages - 1, page + 1)}"),
                InlineKeyboardButton(text=">>", callback_data=f"my_games:{pages - 1}"),
            ])

    rows.append([InlineKeyboardButton(text="🎮 Игры", callback_data="menu_games")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)


    await callback.message.edit_text(stats_text, reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "menu_games")
async def cb_menu_games(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кости", callback_data="mode_dice")],
            [InlineKeyboardButton(text="🎩 Банкир", callback_data="mode_banker")],
        ]
    )
    await callback.message.edit_text("Выберите режим игры:", reply_markup=kb)
    await callback.answer()

# === HELP (помощь) ===
# ... (Остальной код помощи без изменений)

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
    # ИСПРАВЛЕНО: используем edit_message_text, чтобы не засорять чат
    await callback.message.edit_text("🐼 Выберите раздел помощи:", reply_markup=kb) 
    await callback.answer()

@dp.callback_query(F.data == "help_dice")
async def cb_help_dice(callback: CallbackQuery):
    text = (
        "🎲 Помощь: Кости (1 на 1)\n\n"
        "1. Игроки ставят в банк сумму первоначальной ставки.\n"
        "2. Игроки бросают кости, тот, кто выбросил больше - забирает весь банк (минус 1% комиссии). "
        "Результат генерируется на стороне Телеграм.\n"
        f"3. Ставку можно отменить **только в течение первой минуты** после создания."
    )
    await callback.message.edit_text(text) # ИСПРАВЛЕНО: edit_text
    await callback.answer()

@dp.callback_query(F.data == "help_banker")
async def cb_help_banker(callback: CallbackQuery):
    text = (
        "🎩 Помощь: Банкир (1 на N)\n\n"
        "1. **Банкир** создает игру, вносит ставку и бросает кости (цель: выбросить больше, чем игроки).\n"
        f"2. До {BANKER_MAX_JOINERS} **игроков** могут присоединиться, внеся ту же ставку.\n"
        "3. Игроки бросают кости, и каждый сравнивает свой результат с результатом Банкира.\n"
        "4. **Выигрыш Игрока:** Если Игрок выбросил больше, он получает **2x свою ставку** (минус 1% комиссии).\n"
        "5. **Выигрыш Банкира:** Если Банкир выбросил больше, он забирает ставку Игрока.\n"
        "6. **Банкир** может отменить игру до того, как бросит кости."
    )
    await callback.message.edit_text(text) # ИСПРАВЛЕНО: edit_text
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
    await callback.message.edit_text(text) # ИСПРАВЛЕНО: edit_text
    await callback.answer()

# ========================
#      ЗАПУСК БОТА
# ========================

async def main():
    print("Бот запущен (TON + Кости + Банкир + FSM, PostgreSQL).")
    
    # инициализация БД и загрузка данных
    try:
        # pool импортируется в db.py, поэтому он должен быть доступен
        from db import pool 
        await init_db(user_balances, user_usernames, processed_ton_tx)
        
        # Обновляем next_game_id для активных игр в кэше
        global next_game_id
        if pool:
            async with pool.acquire() as conn:
                max_id = await conn.fetchval("SELECT MAX(id) FROM games")
                next_game_id = (max_id or 0) + 1
            
    except Exception as e:
        print(f"Критическая ошибка при инициализации БД: {e}")
        return

    # Запуск воркера для проверки транзакций
    asyncio.create_task(ton_deposit_worker()) 
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
