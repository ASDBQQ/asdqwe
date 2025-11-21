import asyncio
import random
import re
from datetime import datetime, timedelta, timezone

import aiohttp
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
)

from db import (
    init_db,
    upsert_user,
    upsert_game,
    get_user_games,
    get_all_finished_games,
    upsert_raffle_round,
    add_raffle_bet,
    add_ton_deposit,
    add_transfer,
    get_user_registered_at,
    get_user_dice_games_count,
    get_user_raffle_bets_count,
    get_users_profit_and_games_30_days,
    get_user_bets_in_raffle,
)

# ========================
#      НАСТРОЙКИ
# ========================

BOT_TOKEN = "8589113961:AAH8bF8umtdtYhkhmBB5oW8NoMBMxI4bLxk"

# TON кошелёк для пополнений
TON_WALLET_ADDRESS = "UQCzzlkNLsCGqHTUj1zkD_3CVBMoXw-9Od3dRKGgHaBxysYe"

# 1 рубль = 1 монета (внутренняя валюта бота — теперь рубли/монеты)
TONAPI_RATES_URL = "https://tonapi.io/v2/rates?tokens=ton&currencies=rub"
TON_RUB_CACHE_TTL = 60  # секунд кэша курса

START_BALANCE_COINS = 0  # стартовый баланс (в рублях/монетах)

HISTORY_LIMIT = 30
HISTORY_PAGE_SIZE = 10
GAME_CANCEL_TTL_SECONDS = 60 
DICE_BET_MIN_CANCEL_AGE = timedelta(minutes=1) # 1 минута для отмены ставки

# розыгрыш (банкир)
RAFFLE_TIMER_SECONDS = 40       
RAFFLE_MIN_BET = 10             
DICE_MIN_BET = 10               
RAFFLE_MAX_BETS_PER_ROUND = 10 # Макс. ставок в раунде
RAFFLE_QUICK_BETS = [10, 100, 1000]

MAIN_ADMIN_ID = 7106398341
ADMIN_IDS = {MAIN_ADMIN_ID, 783924834}  # админы

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========================
#      ДАННЫЕ В ПАМЯТИ
# ========================

user_balances: dict[int, int] = {}         # user_id -> balance (рубли)
user_usernames: dict[int, str] = {}        # user_id -> username (для переводов и ссылок)

games: dict[int, dict] = {}                # game_id -> game dict (активные и недавно сыгранные)
pending_bet_input: dict[int, bool] = {}    # user_id -> ждём ставку для костей
next_game_id = 1

# вывод (заявки)
pending_withdraw_step: dict[int, str] = {}  # user_id -> "amount" / "details"
temp_withdraw: dict[int, dict] = {}         # user_id -> {amount: int}

# переводы между пользователями
pending_transfer_step: dict[int, str] = {}  # user_id -> "target" / "amount_transfer"
temp_transfer: dict[int, dict] = {}         # user_id -> {"target_id": int}

# розыгрыш (банкир)
raffle_round: dict | None = None    # текущий розыгрыш
raffle_task: asyncio.Task | None = None
next_raffle_id: int = 1
pending_raffle_bet_input: dict[int, bool] = {}  # ввод произвольной суммы для розыгрыша

# пополнение через TON: храним обработанные транзакции, чтобы не дублировать начисления
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
    """Возвращает баланс в рублях/монетах."""
    if uid not in user_balances:
        user_balances[uid] = START_BALANCE_COINS
    return user_balances[uid]


def _schedule_upsert_user(uid: int, registered_at: datetime | None = None):
    """Фоновое сохранение пользователя в БД (баланс + username + registered_at)."""
    username = user_usernames.get(uid)
    balance = user_balances.get(uid, 0)
    try:
        # Передаём registered_at в upsert_user в db.py
        asyncio.create_task(upsert_user(uid, username, balance, registered_at))
    except RuntimeError:
        # если event loop ещё не запущен (редкий случай)
        pass


def change_balance(uid: int, delta: int):
    get_balance(uid)
    user_balances[uid] += delta
    _schedule_upsert_user(uid)


def set_balance(uid: int, value: int):
    user_balances[uid] = value
    _schedule_upsert_user(uid)


def format_rubles(n: int) -> str:
    """Замена format_coins на format_rubles и замена текста 'монет' на '₽'/'RUB'."""
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
        # структура по доке: {"rates": {"TON": {"prices": {"RUB": 123.45}}}}
        rate = float(data["rates"]["TON"]["prices"]["RUB"])
        _ton_rate_cache["value"] = rate
        _ton_rate_cache["updated"] = now
        return rate
    except Exception:
        # если не удалось взять курс — возвращаем последний кэш или дефолт
        return float(cached_value or 100.0)


async def format_balance_text(uid: int) -> str:
    bal = get_balance(uid)
    rate = await get_ton_rub_rate()
    ton_equiv = bal / rate if rate > 0 else 0
    # Убран текст 'монет'
    return (
        f"💼 Ваш баланс: {ton_equiv:.4f} TON\n"
        f"≈ {format_rubles(bal)} ₽\n"
        f"Текущий курс: 1 TON ≈ {rate:.2f} ₽"
    )


def bottom_menu():
    # Добавлена кнопка "👤 Профиль"
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(text="🕹 Игры"),
                types.KeyboardButton(text="💼 Баланс"),
            ],
            [
                types.KeyboardButton(text="🎁 Розыгрыш"),
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
        # Новый пользователь
        user_balances[uid] = START_BALANCE_COINS
        _schedule_upsert_user(uid, datetime.now(timezone.utc)) # Сохраняем дату регистрации
    
    if user.username:
        user_usernames[uid] = user.username
        _schedule_upsert_user(uid) # Обновляем username и баланс


# ========================
#      СПИСОК ИГР (КОСТИ)
# ========================

def build_games_keyboard(uid: int) -> InlineKeyboardMarkup:
    rows = []

    rows.append([
        InlineKeyboardButton(text="✅Создать игру", callback_data="create_game"),
        InlineKeyboardButton(text="🔄Обновить", callback_data="refresh_games"),
    ])

    active = [g for g in games.values() if g["opponent_id"] is None]
    active.sort(key=lambda x: x["id"], reverse=True)

    for g in active:
        # Добавлено "(Вы)" для игр, созданных пользователем. Убран текст 'монет'.
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
        InlineKeyboardButton(text="🐼 Помощь", callback_data="help"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_games_text() -> str:
    return "Создайте игру или выберите уже имеющуюся:"


async def send_games_list(chat_id: int, uid: int):
    await bot.send_message(
        chat_id,
        build_games_text(),
        reply_markup=build_games_keyboard(uid)
    )


# ========================
#      ИСТОРИЯ / СТАТИСТИКА
# ========================

def calculate_profit(uid: int, g: dict) -> int:
    bet = g["bet"]
    if g["winner"] == "draw":
        return 0
    creator = uid == g["creator_id"]
    if g["winner"] == "creator" and creator:
        return bet
    if g["winner"] == "opponent" and not creator:
        return bet
    return -bet


async def build_user_stats_and_history(uid: int):
    now = datetime.now(timezone.utc)
    finished = await get_user_games(uid)

    stats = {
        "month": {"games": 0, "profit": 0},
        "week": {"games": 0, "profit": 0},
        "day": {"games": 0, "profit": 0},
    }

    for g in finished:
        if not g.get("finished_at"):
            continue
        finished_at = datetime.fromisoformat(g["finished_at"])
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

    def ps(v): return ("+" if v > 0 else "") + format_rubles(v)

    # Убран текст 'монет'
    stats_text = (
        f"🎲 Кости за месяц: {stats['month']['games']}\n"
        f"└ 💸 Профит: {ps(stats['month']['profit'])} ₽\n\n"
        f"🎲 За неделю: {stats['week']['games']}\n"
        f"└ 💸 Профит: {ps(stats['week']['profit'])} ₽\n\n"
        f"🎲 За сутки: {stats['day']['games']}\n"
        f"└ 💸 Профит: {ps(stats['day']['profit'])} ₽"
    )

    history = []
    for g in finished[:HISTORY_LIMIT]:
        if uid == g["creator_id"]:
            my = g["creator_roll"]
            opp = g["opponent_roll"]
        else:
            my = g["opponent_roll"]
            opp = g["creator_roll"]

        profit = calculate_profit(uid, g)
        if profit > 0:
            emoji, text = "🟩", "Победа"
        elif profit < 0:
            emoji, text = "🟥", "Проигрыш"
        else:
            emoji, text = "⚪", "Ничья"

        history.append({
            "bet": g["bet"],
            "emoji": emoji,
            "text": text,
            "my": my,
            "opp": opp
        })

    return stats_text, history


def build_history_keyboard(history: list[dict], page: int) -> InlineKeyboardMarkup:
    rows = []

    total = len(history)
    if total == 0:
        rows.append([InlineKeyboardButton(text="История пуста", callback_data="ignore")])
        rows.append([InlineKeyboardButton(text="🎮 Игры", callback_data="menu_games")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    pages = (total + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE
    page = max(0, min(page, pages - 1))

    start = page * HISTORY_PAGE_SIZE
    end = start + HISTORY_PAGE_SIZE

    for h in history[start:end]:
        # Убран текст 'монет'
        text = f"{format_rubles(h['bet'])} ₽ | {h['emoji']} {h['text']} | {h['my']}:{h['opp']}"
        rows.append([InlineKeyboardButton(text=text, callback_data="ignore")])

    if pages > 1:
        rows.append([
            InlineKeyboardButton(text="<<", callback_data="my_games:0"),
            InlineKeyboardButton(text="<", callback_data=f"my_games:{max(0, page - 1)}"),
            InlineKeyboardButton(text=f"{page+1}/{pages}", callback_data="ignore"),
            InlineKeyboardButton(text=">", callback_data=f"my_games:{min(pages - 1, page + 1)}"),
            InlineKeyboardButton(text=">>", callback_data=f"my_games:{pages - 1}"),
        ])

    rows.append([InlineKeyboardButton(text="🎮 Игры", callback_data="menu_games")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ========================
#      РЕЙТИНГ
# ========================

async def build_rating_text(requesting_uid: int) -> str:
    # Новая логика для рейтинга за 30 дней и новый формат
    now = datetime.now(timezone.utc)
    finished_games, all_uids = await get_users_profit_and_games_30_days()

    # 1. Считаем профит и игры для всех пользователей за 30 дней
    user_stats = {} # uid -> {'profit': int, 'games': int}
    
    for g in finished_games:
        # В PG и bot.py используем isoformat, поэтому fromisoformat
        finished_at = datetime.fromisoformat(g["finished_at"]) 
        if (now - finished_at) > timedelta(days=30):
            continue
            
        for uid in (g["creator_id"], g["opponent_id"]):
            if uid is None:
                continue
            
            stats = user_stats.setdefault(uid, {"profit": 0, "games": 0})
            stats["profit"] += calculate_profit(uid, g)
            stats["games"] += 1

    # 2. Сортируем ТОП
    # Сортировка: сначала по профиту (DESC), затем по количеству игр (ASC, чтобы при одинаковом профите выше был тот, кто сыграл меньше)
    top_list = sorted(user_stats.items(), key=lambda x: (x[1]['profit'], -x[1]['games']), reverse=True)
    
    # 3. Формируем ТОП-3
    top_lines = []
    place_emoji = ["🥇", "🥈", "🥉"] 
    
    for i, (uid, stats) in enumerate(top_list[:3]):
        profit = format_rubles(stats['profit'])
        games_count = format_rubles(stats['games'])
        # Использование @username, если доступен, иначе ID
        username = user_usernames.get(uid) or f"ID{uid}"
        top_lines.append(f"{place_emoji[i]} {username} - {profit} ₽ за {games_count} игр")

    if not top_lines:
        return "🏆 Рейтинг пока пуст — ещё нет завершённых игр за 30 дней."

    # 4. Находим место текущего пользователя
    user_place = None
    total_players = len(top_list)
    user_profit = user_stats.get(requesting_uid, {"profit": 0, "games": 0})

    for i, (uid, stats) in enumerate(top_list):
        if uid == requesting_uid:
            user_place = i + 1
            break
            
    # Формируем текст
    lines = ["🏆 ТОП 3 игроков в кости:\n"]
    lines.extend(top_lines)
    lines.append("\n")
    
    # Если данные о пользователе есть
    if user_place:
        profit = format_rubles(user_profit['profit'])
        games_count = format_rubles(user_profit['games'])
        sign = "+" if user_profit['profit'] >= 0 else ""
        lines.append(
            f"Ваше место в рейтинге: {user_place} из {total_players} ({sign}{profit} ₽ за {games_count} игр)"
        )
    else:
        # Если пользователя нет в рейтинге за 30 дней, но он есть в базе
        games_count_total = await get_user_dice_games_count(requesting_uid)
        if games_count_total > 0:
            lines.append(
                "Ваше место в рейтинге: Нет данных за последние 30 дней."
            )
        else:
             lines.append(
                "Ваше место в рейтинге: Нет данных (нет завершённых игр)."
            )

    lines.append("\nДанные приведены за последние 30 дней.")
    
    return "\n".join(lines)

# ========================
#      ИГРА КОСТИ (1% КОМИССИЯ)
# ========================

async def telegram_roll(uid: int) -> int:
    msg = await bot.send_dice(uid, emoji="🎲")
    await asyncio.sleep(3)
    return msg.dice.value


async def play_game(gid: int):
    g = games.get(gid)
    if not g:
        return

    c = g["creator_id"]
    o = g["opponent_id"]
    bet = g["bet"]

    cr = await telegram_roll(c)
    orr = await telegram_roll(o)

    g["creator_roll"] = cr
    g["opponent_roll"] = orr
    g["finished"] = True
    g["finished_at"] = datetime.now(timezone.utc)

    bank = bet * 2

    if cr > orr:
        winner = "creator"
        commission = bank // 100
        prize = bank - commission
        change_balance(c, prize)
        change_balance(MAIN_ADMIN_ID, commission)
    elif orr > cr:
        winner = "opponent"
        commission = bank // 100
        prize = bank - commission
        change_balance(o, prize)
        change_balance(MAIN_ADMIN_ID, commission)
    else:
        winner = "draw"
        change_balance(c, bet)
        change_balance(o, bet)
        commission = 0

    g["winner"] = winner

    # сохраняем результат игры в БД
    await upsert_game(g)

    for user in (c, o):
        is_creator = (user == c)
        your = cr if is_creator else orr
        their = orr if is_creator else cr

        if winner == "draw":
            result_text = "🤝 Ничья!"
            # Убран текст 'монет'
            bank_text = f"💰 Банк: {format_rubles(bank)} ₽ (вернули ставки)"
        else:
            if (winner == "creator" and is_creator) or (winner == "opponent" and not is_creator):
                result_text = "🥳 Поздравляем с победой!"
            else:
                result_text = "😔 К сожалению, вы проиграли!"
            # Убран текст 'монет'
            bank_text = (
                f"💰 Банк: {format_rubles(bank)} ₽\n"
                f"💸 Комиссия: {format_rubles(commission)} ₽ (1%)"
            )

        # Убран текст 'монет'
        txt = (
            f"🏁 Кости #{gid}\n"
            f"{bank_text}\n\n"
            f"🫵 Ваш результат: {your}\n"
            f"🧑‍🤝‍🧑 Результат соперника: {their}\n\n"
            f"{result_text}\n"
            f"💼 Баланс: {format_rubles(get_balance(user))} ₽" # форматируем баланс
        )

        await bot.send_message(user, txt)


# ========================
#      РОЗЫГРЫШ (БАНКИР)
# ========================

def build_raffle_text(uid: int) -> str:
    """
    Текст для режима "Банкир" (розыгрыш) c описанием текущего раунда.
    """
    global raffle_round

    r = raffle_round
    # Если раунда ещё нет или в нём нет ставок — приглашаем создать первую
    if not r or r.get("finished") or not r.get("bets"):
        return (
            "🎩 Игра «Банкир»\n\n"
            "Сделайте первую ставку, чтобы запустить новый розыгрыш.\n\n"
            f"Минимальная ставка: {RAFFLE_MIN_BET} ₽.\n"
            f"До {RAFFLE_MAX_BETS_PER_ROUND} ставок на игрока за раунд.\n"
            f"После появления минимум 2 участников стартует таймер на {RAFFLE_TIMER_SECONDS} секунд,\n"
            "после чего случайный победитель забирает весь банк (минус 1% комиссии)."
        )

    entry_amount = r.get("entry_amount") or 0
    total_bets = len(r.get("bets", []))
    participants = r.get("participants", set())
    user_bets = r.get("user_bets", {}).get(uid, 0)
    bank = r.get("total_bank", 0)

    timer_line = ""
    draw_at = r.get("draw_at")
    if draw_at:
        # Сколько секунд осталось до конца раунда
        seconds_left = int((draw_at - datetime.now(timezone.utc)).total_seconds())
        if seconds_left < 0:
            seconds_left = 0
        timer_line = f"\n⏳ До окончания раунда примерно {seconds_left} сек."
    else:
        need = max(0, 2 - len(participants))
        if need > 0:
            timer_line = f"\nОжидаем ещё {need} участника(ов) для запуска таймера."

    return (
        "🎩 Игра «Банкир» — текущий раунд\n\n"
        f"Фиксированная ставка: {format_rubles(entry_amount)} ₽\n"
        f"Общий банк: {format_rubles(bank)} ₽\n"
        f"Всего ставок: {total_bets}\n"
        f"Участников: {len(participants)}\n"
        f"Ваших ставок: {user_bets}"
        f"{timer_line}"
    )


def build_raffle_menu_keyboard(uid: int) -> InlineKeyboardMarkup:
    """Клавиатура для режима "Банкир"."""
    # Быстрые ставки
    quick_buttons = [
        InlineKeyboardButton(
            text=f"{format_rubles(amount)} ₽",
            callback_data=f"raffle_quick:{amount}",
        )
        for amount in RAFFLE_QUICK_BETS
        if amount >= RAFFLE_MIN_BET
    ]
    rows: list[list[InlineKeyboardButton]] = []
    if quick_buttons:
        rows.append(quick_buttons)

    # Ввод произвольной суммы
    rows.append(
        [InlineKeyboardButton(text="✏ Ввести сумму", callback_data="raffle_enter_amount")]
    )

    # Обновить / назад
    rows.append(
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="raffle_refresh"),
            InlineKeyboardButton(text="⬅ Назад", callback_data="menu_games"),
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_raffle_menu(chat_id: int, uid: int):
    """Показываем пользователю текущее состояние игры "Банкир"."""
    await bot.send_message(
        chat_id,
        build_raffle_text(uid),
        reply_markup=build_raffle_menu_keyboard(uid),
    )


def _ensure_raffle_round() -> dict:
    """Создаём новый раунд, если его ещё нет или он завершён."""
    global raffle_round, next_raffle_id

    if raffle_round is None or raffle_round.get("finished"):
        raffle_round = {
            "id": next_raffle_id,
            "created_at": datetime.now(timezone.utc),
            "finished_at": None,
            "entry_amount": None,  # фиксированная ставка раунда
            "total_bank": 0,
            "bets": [],  # список user_id (по одному за каждую ставку)
            "participants": set(),  # множество user_id
            "user_bets": {},  # user_id -> кол-во ставок
            "winner_id": None,
            "finished": False,
            "draw_at": None,  # время окончания таймера
        }
        next_raffle_id += 1

    return raffle_round


async def _process_raffle_bet(uid: int, chat_id: int, amount: int) -> str:
    """Общая логика принятия ставки в "Банкире"."""
    global raffle_task

    if amount < RAFFLE_MIN_BET:
        return f"Минимальная ставка в Банкире: {RAFFLE_MIN_BET} ₽."

    bal = get_balance(uid)
    if amount > bal:
        return "Недостаточно ₽ на балансе для этой ставки."

    r = _ensure_raffle_round()

    # Если это первая ставка в раунде — фиксируем размер ставки
    if r["entry_amount"] is None:
        r["entry_amount"] = amount
    elif amount != r["entry_amount"]:
        return (
            "В этом розыгрыше фиксированная ставка "
            f"{format_rubles(r['entry_amount'])} ₽.\n"
            "Вы можете участвовать только с этой суммой."
        )

    # Лимит ставок на игрока
    user_bets = r["user_bets"].get(uid, 0)
    if user_bets >= RAFFLE_MAX_BETS_PER_ROUND:
        return (
            "Вы уже сделали максимальное количество ставок в этом раунде "
            f"({RAFFLE_MAX_BETS_PER_ROUND})."
        )

    # Списываем средства
    change_balance(uid, -amount)

    # Обновляем состояние раунда
    r["total_bank"] += amount
    r["bets"].append(uid)
    r["participants"].add(uid)
    r["user_bets"][uid] = user_bets + 1

    # Пишем в БД историю ставок (для профиля)
    await add_raffle_bet(r["id"], uid, amount)

    # Запускаем таймер, когда участников стало >= 2 и таймер ещё не запущен
    if len(r["participants"]) >= 2 and r.get("draw_at") is None:
        r["draw_at"] = datetime.now(timezone.utc) + timedelta(seconds=RAFFLE_TIMER_SECONDS)
        raffle_task = asyncio.create_task(raffle_draw_worker(r["id"]))

    # Текст ответа игроку
    timer_line = ""
    if r.get("draw_at"):
        seconds_left = int((r["draw_at"] - datetime.now(timezone.utc)).total_seconds())
        if seconds_left < 0:
            seconds_left = 0
        timer_line = f"\n⏳ До окончания раунда примерно {seconds_left} сек."
    else:
        need = max(0, 2 - len(r["participants"]))
        if need > 0:
            timer_line = f"\nОжидаем ещё {need} участника(ов) для запуска таймера."

    return (
        "✅ Ставка в игре «Банкир» принята!\n\n"
        f"Сумма ставки: {format_rubles(amount)} ₽\n"
        f"Фиксированная ставка раунда: {format_rubles(r['entry_amount'])} ₽\n"
        f"Всего ставок в раунде: {len(r['bets'])}\n"
        f"Ваших ставок: {r['user_bets'][uid]}\n"
        f"Общий банк: {format_rubles(r['total_bank'])} ₽"
        f"{timer_line}"
    )


async def raffle_draw_worker(raffle_id: int):
    """Ожидаем завершения таймера и проводим розыгрыш."""
    global raffle_round, raffle_task

    await asyncio.sleep(RAFFLE_TIMER_SECONDS)

    r = raffle_round
    if not r or r.get("finished") or r.get("id") != raffle_id:
        return

    await perform_raffle_draw()
    raffle_task = None


async def perform_raffle_draw():
    """Подводим итоги раунда "Банкир"."""
    global raffle_round

    r = raffle_round
    if not r or r.get("finished") or not r.get("bets"):
        return

    participants = r.get("participants", set())
    if len(participants) < 2:
        # Без двух участников розыгрыш не имеет смысла — возвращаем ставки
        entry_amount = r.get("entry_amount") or 0
        if entry_amount > 0:
            for uid in r.get("bets", []):
                change_balance(uid, entry_amount)
                try:
                    await bot.send_message(
                        uid,
                        "Розыгрыш «Банкир» отменён: недостаточно участников. "
                        "Ставка возвращена на баланс.",
                    )
                except Exception:
                    pass

        r["finished"] = True
        r["finished_at"] = datetime.now(timezone.utc)
        r["winner_id"] = None

        await upsert_raffle_round(
            {
                "created_at": r.get("created_at"),
                "finished_at": r.get("finished_at"),
                "winner_id": None,
                "total_bank": 0,
            }
        )
        return

    # Определяем победителя (каждая ставка даёт один "билет")
    bank = r.get("total_bank", 0)
    winner_uid = random.choice(r["bets"])

    commission = bank // 100  # 1% комиссии
    prize = bank - commission

    change_balance(winner_uid, prize)
    change_balance(MAIN_ADMIN_ID, commission)

    r["finished"] = True
    r["finished_at"] = datetime.now(timezone.utc)
    r["winner_id"] = winner_uid

    await upsert_raffle_round(
        {
            "created_at": r.get("created_at"),
            "finished_at": r.get("finished_at"),
            "winner_id": winner_uid,
            "total_bank": bank,
        }
    )

    winner_username = user_usernames.get(winner_uid)
    if winner_username:
        winner_name = f"@{winner_username}"
    else:
        winner_name = f"ID{winner_uid}"

    common_part = (
        "🎩 Розыгрыш «Банкир» завершён!\n\n"
        f"💰 Общий банк: {format_rubles(bank)} ₽\n"
        f"💸 Комиссия: {format_rubles(commission)} ₽ (1%)\n"
        f"🏆 Победитель: {winner_name}\n"
    )

    for uid in participants:
        if uid == winner_uid:
            personal = (
                f"\n🥳 Поздравляем! Вы выиграли {format_rubles(prize)} ₽ (после комиссии)."
            )
        else:
            personal = "\n😔 В этот раз не повезло. Попробуйте ещё!"

        balance_line = f"\n\n💼 Ваш баланс: {format_rubles(get_balance(uid))} ₽"

        try:
            await bot.send_message(uid, common_part + personal + balance_line)
        except Exception:
            pass

    # Новый раунд создастся автоматически при следующей ставке
# ========================
#      СТАРТ, МЕНЮ
# ========================

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    register_user(m.from_user)
    get_balance(m.from_user.id)
    # Убран текст 'монеты'
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
    # Отдельная кнопка: только заглушка, без меню Банкира
    register_user(m.from_user)
    await m.answer("Розыгрыши скоро появятся.")


@dp.message(F.text == "👤 Профиль")
async def msg_profile(m: types.Message):
    # НОВАЯ ФУНКЦИЯ: Профиль
    register_user(m.from_user)
    uid = m.from_user.id
    
    reg_date_dt = await get_user_registered_at(uid)
    reg_date_str = reg_date_dt.strftime("%d.%m.%Y %H:%M:%S") if reg_date_dt else "Неизвестно"
    
    dice_games_count = await get_user_dice_games_count(uid)
    raffle_rounds_count = await get_user_raffle_bets_count(uid)

    text = (
        f"👤 Ваш Профиль:\n\n"
        f"🆔 ID Пользователя: <code>{uid}</code>\n"
        f"🗓 Дата регистрации: {reg_date_str}\n\n"
        f"🎲 Всего игр в Кости: {dice_games_count}\n"
        f"🎩 Всего игр в Банкир: {raffle_rounds_count}"
    )

    await m.answer(text, parse_mode="HTML")


@dp.callback_query(F.data == "mode_dice")
async def cb_mode_dice(callback: CallbackQuery):
    await send_games_list(callback.message.chat.id, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "mode_banker")
async def cb_mode_banker(callback: CallbackQuery):
    await send_raffle_menu(callback.message.chat.id, callback.from_user.id)
    await callback.answer()


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


@dp.message(F.text == "🌐 Поддержка")
async def msg_support(m: types.Message):
    register_user(m.from_user)
    await m.answer("Поддержка: @Btcbqq")


# ========================
#      АДМИН-КОМАНДЫ
# ========================

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
    change_balance(uid, amount)
    # Убран текст 'монет'
    await m.answer(f"✅ Баланс {uid} увеличен на {format_rubles(amount)} ₽. Теперь: {format_rubles(get_balance(uid))} ₽")


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
    change_balance(uid, -amount)
    # Убран текст 'монет'
    await m.answer(f"✅ Баланс {uid} уменьшен на {format_rubles(amount)} ₽. Теперь: {format_rubles(get_balance(uid))} ₽")


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
    set_balance(uid, amount)
    # Убран текст 'монет'
    await m.answer(f"✅ Баланс {uid} установлен на {format_rubles(amount)} ₽")


@dp.message(Command("adminprofit"))
async def cmd_adminprofit(m: types.Message):
    register_user(m.from_user)
    if m.from_user.id != MAIN_ADMIN_ID:
        return await m.answer("⛔ Только основной админ.")
    bal = get_balance(MAIN_ADMIN_ID)
    rate = await get_ton_rub_rate()
    ton_equiv = bal / rate if rate > 0 else 0
    # Убран текст 'монет'
    await m.answer(
        f"💸 Баланс админа (накопленная комиссия и игры): {format_rubles(bal)} ₽.\n"
        f"≈ {ton_equiv:.4f} TON по текущему курсу ({rate:.2f} ₽ за 1 TON).\n"
        f"Эти ₽ можно вывести, обменяв TON на рубли."
    )


# ========================
#      ПОПОЛНЕНИЕ ЧЕРЕЗ TON
# ========================

@dp.callback_query(F.data == "deposit_menu")
async def cb_deposit_menu(callback: CallbackQuery):
    uid = callback.from_user.id
    rate = await get_ton_rub_rate()
    half_ton = int(rate * 0.5)
    one_ton = int(rate * 1)

    ton_url = f"ton://transfer/{TON_WALLET_ADDRESS}?text=ID{uid}"

    text = (
        "💎 Пополнение через TON\n\n"
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


async def ton_deposit_worker():
    """Периодически опрашивает tonapi по адресу кошелька и ищет новые входящие переводы."""
    if not TON_WALLET_ADDRESS:
        print("TON_WALLET_ADDRESS не задан, ton_deposit_worker не запускается.")
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

                # пробуем вытащить комментарий (text) из разных полей
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

                # ищем ID<user_id>
                m = re.search(r"ID(\d{5,15})", str(comment))
                if not m:
                    processed_ton_tx.add(tx_hash)
                    continue

                user_id = int(m.group(1))

                # сумма перевода в nanotons, поле value может быть строкой
                value_nanoton = 0
                if isinstance(in_msg, dict):
                    v = in_msg.get("value")
                    if isinstance(v, str) and v.isdigit():
                        value_nanoton = int(v)
                    elif isinstance(v, int):
                        value_nanoton = v

                if value_nanoton <= 0:
                    processed_ton_tx.add(tx_hash)
                    continue

                ton_amount = value_nanoton / 1e9
                rate = await get_ton_rub_rate()
                coins = int(ton_amount * rate) # coins - это теперь рубли

                if coins <= 0:
                    processed_ton_tx.add(tx_hash)
                    continue

                change_balance(user_id, coins)
                processed_ton_tx.add(tx_hash)

                await add_ton_deposit(tx_hash, user_id, ton_amount, coins, comment)

                try:
                    # Убран текст 'монет'
                    await bot.send_message(
                        user_id,
                        f"✅ Пополнение через TON успешно!\n\n"
                        f"Получено: {ton_amount:.4f} TON\n"
                        f"Курс: 1 TON ≈ {rate:.2f} ₽\n"
                        f"Зачислено: {format_rubles(coins)} ₽\n"
                        f"Текущий баланс: {format_rubles(get_balance(user_id))} ₽."
                    )
                except Exception:
                    pass

                try:
                    # Убран текст 'монет'
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
            print("Ошибка в ton_deposit_worker:", e)

        await asyncio.sleep(20)


# ========================
#      ВЫВОД (ТОН)
# ========================

@dp.callback_query(F.data == "withdraw_menu")
async def cb_withdraw_menu(callback: CallbackQuery):
    uid = callback.from_user.id
    bal = get_balance(uid)
    if bal <= 0:
        await callback.answer("Баланс нулевой.", show_alert=True)
        return
    pending_withdraw_step[uid] = "amount"
    temp_withdraw[uid] = {}

    rate = await get_ton_rub_rate()
    ton_equiv = bal / rate if rate > 0 else 0

    await callback.message.answer(
        f"💸 Вывод средств в TON\n"
        f"Ваш баланс: {format_rubles(bal)} ₽ (≈ {ton_equiv:.4f} TON)\n"
        f"1 TON ≈ {rate:.2f} ₽.\n\n"
        f"Введите сумму ₽ для вывода (целое число):"
    )
    await callback.answer()


# ========================
#      ПЕРЕВОДЫ МЕЖДУ ПОЛЬЗОВАТЕЛЯМИ
# ========================

@dp.callback_query(F.data == "transfer_menu")
async def cb_transfer_menu(callback: CallbackQuery):
    uid = callback.from_user.id
    pending_transfer_step[uid] = "target"
    temp_transfer[uid] = {}
    # 'монет' -> '₽'
    await callback.message.answer(
        "🔄 Перевод ₽\n"
        "Введите ID или @username получателя.\n"
        "Важно: получатель должен хотя бы раз написать боту."
    )
    await callback.answer()


def resolve_user_by_username(username_str: str) -> int | None:
    uname = username_str.strip().lstrip("@").lower()
    for uid, uname_stored in user_usernames.items():
        if uname_stored and uname_stored.lower() == uname:
            return uid
    return None


# ========================
#      СОЗДАНИЕ ИГРЫ (КОСТИ)
# ========================

@dp.callback_query(F.data == "create_game")
async def cb_create_game(callback: CallbackQuery):
    uid = callback.from_user.id
    pending_bet_input[uid] = True
    # сбрасываем возможный ввод суммы для Банкира
    pending_raffle_bet_input.pop(uid, None)
    # 'монет' -> '₽'
    await callback.message.answer(
        f"Введите ставку (числом, в ₽). Минимум {DICE_MIN_BET} ₽:"
    )
    await callback.answer()


# ========================
#      РОЗЫГРЫШ: КНОПКИ
# ========================

@dp.callback_query(F.data == "raffle_make_bet")
async def cb_raffle_make_bet(callback: CallbackQuery):
    """
    Общая кнопка "Сделать ставку" — используем минимальную ставку раунда.
    Сейчас в меню нет отдельной кнопки с таким callback_data, но
    оставляем обработчик на будущее.
    """
    uid = callback.from_user.id
    chat_id = callback.message.chat.id

    msg_text = await _process_raffle_bet(uid, chat_id, RAFFLE_MIN_BET)
    await callback.message.answer(msg_text)
    await callback.answer()


@dp.callback_query(F.data.startswith("raffle_quick:"))
async def cb_raffle_quick(callback: CallbackQuery):
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


@dp.callback_query(F.data == "raffle_enter_amount")
async def cb_raffle_enter_amount(callback: CallbackQuery):
    uid = callback.from_user.id
    pending_raffle_bet_input[uid] = True
    # на всякий случай сбрасываем ввод ставки для костей
    pending_bet_input.pop(uid, None)

    await callback.message.answer(
        f"Введите сумму ₽ для участия в Банкире.\n"
        f"Минимальная ставка: {RAFFLE_MIN_BET} ₽."
    )
    await callback.answer()


@dp.callback_query(F.data == "raffle_refresh")
async def cb_raffle_refresh(callback: CallbackQuery):
    await send_raffle_menu(callback.message.chat.id, callback.from_user.id)
    await callback.answer("Обновлено!")


@dp.callback_query(F.data == "raffle_back")
async def cb_raffle_back(callback: CallbackQuery):
    # Возврат в главное меню игры Банкир
    await send_raffle_menu(callback.message.chat.id, callback.from_user.id)
    await callback.answer()
# ========================
#      ОБРАБОТКА ТЕКСТА
# ========================

@dp.message()
async def process_text(m: types.Message):
    register_user(m.from_user)
    uid = m.from_user.id
    text = (m.text or "").strip()

    # не перехватываем команды
    if text.startswith("/"):
        return

    # 1) ввод ставки для костей
    if pending_bet_input.get(uid):
        if not text.isdigit():
            return await m.answer("Введите корректную ставку (число):")
        bet = int(text)
        # 'монет' -> '₽'
        if bet < DICE_MIN_BET:
            return await m.answer(f"Минимальная ставка {DICE_MIN_BET} ₽.")
        if bet > get_balance(uid):
            return await m.answer("Недостаточно ₽ на балансе!")

        global next_game_id
        gid = next_game_id
        next_game_id += 1

        games[gid] = {
            "id": gid,
            "creator_id": uid,
            "opponent_id": None,
            "bet": bet,
            "creator_roll": None,
            "opponent_roll": None,
            "winner": None,
            "finished": False,
            "created_at": datetime.now(timezone.utc),
            "finished_at": None,
        }

        change_balance(uid, -bet)
        pending_bet_input.pop(uid)

        # сохраняем игру в БД
        await upsert_game(games[gid])

        await m.answer(f"✅ Игра №{gid} создана!")
        return await send_games_list(m.chat.id, uid)

    # 2) вывод — шаг суммы
    if pending_withdraw_step.get(uid) == "amount":
        if not text.isdigit():
            return await m.answer("Введите сумму числом:")
        amount = int(text)
        bal = get_balance(uid)
        if amount <= 0:
            return await m.answer("Сумма должна быть > 0.")
        if amount > bal:
            return await m.answer(f"Недостаточно ₽. Ваш баланс: {format_rubles(bal)} ₽.") # 'монет' -> '₽'
        temp_withdraw[uid]["amount"] = amount
        pending_withdraw_step[uid] = "details"

        rate = await get_ton_rub_rate()
        ton_amount = amount / rate if rate > 0 else 0
        approx = f"{ton_amount:.4f} TON"
        # 'монет' -> '₽'
        return await m.answer(
            f"💸 Вывод в TON\n"
            f"Сумма: {format_rubles(amount)} ₽ (≈ {approx})\n\n"
            f"Напишите комментарий к выводу (например, удобное время, TON-кошелёк, доп. информация):"
        )

    # 3) вывод — шаг реквизитов
    if pending_withdraw_step.get(uid) == "details":
        details = text
        amount = temp_withdraw[uid]["amount"]
        user = m.from_user
        username = user.username
        if username:
            mention = f"@{username}"
            link = f"https://t.me/{username}"
        else:
            mention = f"id {uid}"
            link = f"tg://user?id={uid}"

        rate = await get_ton_rub_rate()
        ton_amount = amount / rate if rate > 0 else 0
        ton_text = f"{ton_amount:.4f} TON"

        # 'монет' -> '₽'
        msg_admin = (
            f"💸 НОВАЯ ЗАЯВКА НА ВЫВОД (TON)\n\n"
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

        await m.answer(
            "✅ Заявка на вывод отправлена администратору.\n"
            "После обработки вам отправят TON на указанные реквизиты."
        )

        pending_withdraw_step.pop(uid, None)
        temp_withdraw.pop(uid, None)
        return

    # 4) перевод — выбор получателя
    if pending_transfer_step.get(uid) == "target":
        target_id: int | None = None
        if text.startswith("@"):
            target_id = resolve_user_by_username(text)
        elif text.isdigit():
            target_id = int(text)
        else:
            target_id = resolve_user_by_username(text)

        if not target_id:
            return await m.answer(
                "Не удалось найти пользователя.\n"
                "Убедитесь, что он уже писал боту, и введите его ID или @username."
            )
        if target_id == uid:
            return await m.answer("Нельзя переводить самому себе.")

        temp_transfer[uid]["target_id"] = target_id
        pending_transfer_step[uid] = "amount_transfer"
        # 'монет' -> '₽'
        return await m.answer(
            "Введите сумму ₽ для перевода (минимум 1):"
        )

    # 5) перевод — сумма
    if pending_transfer_step.get(uid) == "amount_transfer":
        if not text.isdigit():
            return await m.answer("Введите сумму числом:")
        amount = int(text)
        if amount <= 0:
            return await m.answer("Сумма должна быть > 0.")
        bal = get_balance(uid)
        # 'монет' -> '₽'
        if amount > bal:
            return await m.answer(f"Недостаточно ₽. Ваш баланс: {format_rubles(bal)} ₽.")

        target_id = temp_transfer[uid].get("target_id")
        if not target_id:
            pending_transfer_step.pop(uid, None)
            temp_transfer.pop(uid, None)
            return await m.answer("Ошибка: не найден получатель, попробуйте ещё раз.")

        change_balance(uid, -amount)
        change_balance(target_id, amount)

        await add_transfer(uid, target_id, amount)

        # 'монет' -> '₽'
        await m.answer(
            f"✅ Перевод выполнен.\n"
            f"Вы отправили {format_rubles(amount)} ₽ пользователю ID {target_id}.\n"
            f"Ваш новый баланс: {format_rubles(get_balance(uid))} ₽."
        )
        try:
            # 'монет' -> '₽'
            await bot.send_message(
                target_id,
                f"🔄 Вам перевели {format_rubles(amount)} ₽ от пользователя ID {uid}.\n"
                f"Ваш новый баланс: {format_rubles(get_balance(target_id))} ₽."
            )
        except Exception:
            pass

        pending_transfer_step.pop(uid, None)
        temp_transfer.pop(uid, None)
        return

    # 6) ввод суммы ставки для розыгрыша
    if pending_raffle_bet_input.get(uid):
        if not text.isdigit():
            return await m.answer("Введите сумму числом (₽) для участия в Банкире:")
        amount = int(text)
        pending_raffle_bet_input.pop(uid, None)
        msg_text = await _process_raffle_bet(uid, m.chat.id, amount)
        return await m.answer(msg_text)


    await m.answer("Используйте меню или /start.")


# ========================
#      ОКНО ЧУЖОЙ ИГРЫ (КОСТИ)
# ========================

@dp.callback_query(F.data.startswith("game_open:"))
async def cb_game_open(callback: CallbackQuery):
    gid = int(callback.data.split(":", 1)[1])
    g = games.get(gid)

    if not g:
        return await callback.answer("Игра не найдена.", show_alert=True)
    if g["opponent_id"] is not None:
        return await callback.answer("Кто-то уже вступил!", show_alert=True)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✔ Вступить", callback_data=f"join_confirm:{gid}")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="menu_games")],
        ]
    )

    await callback.message.answer(
        f"🎲 Игра №{gid}\n"
        f"💰 Ставка: {format_rubles(g['bet'])} ₽\n\n" # 'монет' -> '₽'
        f"Хотите вступить?",
        reply_markup=kb
    )
    await callback.answer()


# ========================
#      ОКНО СВОЕЙ ИГРЫ (КОСТИ)
# ========================

@dp.callback_query(F.data.startswith("game_my:"))
async def cb_game_my(callback: CallbackQuery):
    uid = callback.from_user.id
    gid = int(callback.data.split(":", 1)[1])

    g = games.get(gid)
    if not g:
        return await callback.answer("Игра не найдена.", show_alert=True)
    if g["creator_id"] != uid:
        return await callback.answer("Это не ваша игра.", show_alert=True)
    if g["opponent_id"] is not None:
        return await callback.answer("Уже есть соперник.", show_alert=True)

    # НОВАЯ ЛОГИКА: Кнопка "Отменить" доступна только в первую минуту
    time_passed = datetime.now(timezone.utc) - g["created_at"]
    cancel_button = []
    if time_passed < DICE_BET_MIN_CANCEL_AGE: # Меньше 1 минуты
        cancel_button = [InlineKeyboardButton(text="❌ Отменить ставку", callback_data=f"cancel_game:{gid}")]

    rows = [
        cancel_button,
        [InlineKeyboardButton(text="⬅ Назад", callback_data="menu_games")],
    ]
    
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    # 'монет' -> '₽'
    await callback.message.answer(
        f"🎲 Ваша игра №{gid}\n"
        f"💰 Ставка: {format_rubles(g['bet'])} ₽\n\n"
        f"Ожидание соперника...",
        reply_markup=kb
    )
    await callback.answer()


# ========================
#      ОТМЕНА СТАВКИ (КОСТИ)
# ========================

@dp.callback_query(F.data.startswith("cancel_game:"))
async def cb_cancel_game(callback: CallbackQuery):
    uid = callback.from_user.id
    gid = int(callback.data.split(":", 1)[1])

    g = games.get(gid)
    if not g:
        return await callback.answer("Игра не найдена.", show_alert=True)
    if g["creator_id"] != uid:
        return await callback.answer("Это не ваша игра.", show_alert=True)
    if g["opponent_id"] is not None:
        return await callback.answer("Уже есть соперник.", show_alert=True)

    # Проверка времени (Отмена возможна только в течение 1 минуты)
    created_at = g["created_at"]
    if (datetime.now(timezone.utc) - created_at) > DICE_BET_MIN_CANCEL_AGE:
        return await callback.answer(
            f"Ставку можно отменить только в течение первой минуты после создания.", 
            show_alert=True
        )

    bet = g["bet"]
    change_balance(uid, bet)
    del games[gid]
    # При использовании PostgreSQL и SERIAL PRIMARY KEY, удаление из локального кэша достаточно

    # 'монет' -> '₽'
    await callback.message.answer(
        f"❌ Ставка №{gid} отменена. {format_rubles(bet)} ₽ возвращены на баланс."
    )
    await send_games_list(callback.message.chat.id, uid)
    await callback.answer()


# ========================
#      ПОДТВЕРЖДЕНИЕ ВСТУПЛЕНИЯ (КОСТИ)
# ========================

@dp.callback_query(F.data.startswith("join_confirm:"))
async def cb_join_confirm(callback: CallbackQuery):
    uid = callback.from_user.id
    gid = int(callback.data.split(":", 1)[1])

    g = games.get(gid)
    if not g:
        return await callback.answer("Игра не найдена.", show_alert=True)
    if g["opponent_id"] is not None:
        return await callback.answer("Кто-то уже вступил!", show_alert=True)

    bet = g["bet"]
    # 'монет' -> '₽'
    if get_balance(uid) < bet:
        return await callback.answer("Недостаточно ₽.", show_alert=True)

    g["opponent_id"] = uid
    change_balance(uid, -bet)

    # обновляем игру в БД (добавился соперник)
    await upsert_game(g)

    await callback.message.answer(f"✅ Вы присоединились к игре №{gid}!")
    await callback.answer()

    await play_game(gid)


# ========================
#      МОИ ИГРЫ (СТАТИСТИКА)
# ========================

@dp.callback_query(F.data.startswith("my_games"))
async def cb_my_games(callback: CallbackQuery):
    uid = callback.from_user.id
    page = int(callback.data.split(":", 1)[1])

    stats, history = await build_user_stats_and_history(uid)
    kb = build_history_keyboard(history, page)

    await callback.message.answer(stats, reply_markup=kb)
    await callback.answer()


# ========================
#      ОБНОВИТЬ СПИСОК ИГР
# ========================

@dp.callback_query(F.data == "refresh_games")
async def cb_refresh_games(callback: CallbackQuery):
    uid = callback.from_user.id
    try:
        await callback.message.edit_text(
            build_games_text(),
            reply_markup=build_games_keyboard(uid)
        )
    except Exception:
        await callback.message.answer(
            build_games_text(),
            reply_markup=build_games_keyboard(uid)
        )
    await callback.answer("Обновлено!")


# ========================
#      РЕЙТИНГ
# ========================

@dp.callback_query(F.data == "rating")
async def cb_rating(callback: CallbackQuery):
    # FIX: Убеждаемся, что build_rating_text получает uid, как и требуется
    text = await build_rating_text(callback.from_user.id) 
    await callback.message.answer(text)
    await callback.answer()


# ========================
#      ПОМОЩЬ (РАЗДЕЛЕНА НА ДВЕ ЧАСТИ)
# ========================

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
    # Новый текст (п. 4)
    text = (
        "🎲 Помощь: Кости (1 на 1)\n\n"
        "1. Игроки ставят в банк сумму первоначальной ставки. Максимальное число игроков - 2.\n"
        "2. После того как игроки найдены, запускается розыгрыш.\n"
        "3. Игроки бросают кости, тот, кто выбросил больше - забирает весь банк. "
        "Результат генерируется на стороне Телеграм.\n"
        "4. Ставку можно отменить **только в течение первой минуты** после создания."
    )
    await callback.message.answer(text)
    await callback.answer()

@dp.callback_query(F.data == "help_banker")
async def cb_help_banker(callback: CallbackQuery):
    # Новый текст (п. 5)
    text = (
        "🎩 Помощь: Банкир (Розыгрыш)\n\n"
        "1. Участники кладут в банк сумму равную самой первой ставке. Можно сделать не больше 10 ставок за игру.\n"
        "2. Чем больше вы положили в банк, тем выше ваш шанс на победу.\n"
        "3. После того как набралось 2 участника, запускается таймер до окончания игры.\n"
        "4. По истечению таймера начинается розыгрыш, система выбирает случайного победителя "
        "из всех кто скинул в банк. Победитель забирает весь банк.\n"
        "5. Ставку можно отменить через 10 минут."
    )
    await callback.message.answer(text)
    await callback.answer()

@dp.callback_query(F.data == "help_balance")
async def cb_help_balance(callback: CallbackQuery):
    # Добавлен раздел помощи для баланса/вывода
    text = (
        "💸 Помощь: Баланс и Вывод\n\n"
        "1. Пополнение: Отправьте TON на указанный адрес, обязательно указав в комментарии "
        "свой ID (формат IDXXXXXX). Бот автоматически зачислит ₽ по текущему курсу.\n"
        "2. Вывод: Вывод осуществляется в TON по курсу. Заявка отправляется администратору.\n"
        "3. Переводы: Доступны между игроками в разделе 'Баланс'.\n"
        "4. Комиссия: С каждой игры (Кости, Банкир) удерживается 1% комиссии."
    )
    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "menu_games")
async def cb_menu_games(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Кости", callback_data="mode_dice")],
            [InlineKeyboardButton(text="🎩 Банкир", callback_data="mode_banker")],
        ]
    )
    await callback.message.answer("Выберите режим игры:", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data == "ignore")
async def cb_ignore(callback: CallbackQuery):
    await callback.answer()


# ========================
#      ЗАПУСК БОТА
# ========================

async def main():
    print("Бот запущен (TON + Кости + Банкир + переводы, PostgreSQL).")
    # инициализация БД и загрузка данных
    await init_db(user_balances, user_usernames, processed_ton_tx)
    # Запуск воркера для проверки транзакций
    asyncio.create_task(ton_deposit_worker()) 
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

