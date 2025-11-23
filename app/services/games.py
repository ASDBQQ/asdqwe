# app/services/games.py
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from app.bot import bot
from app.config import (
    HISTORY_LIMIT,
    HISTORY_PAGE_SIZE,
    MAIN_ADMIN_ID,
)
from app.db.games import (
    get_user_games,
    get_users_profit_and_games_30_days,
    get_user_dice_games_count,
    upsert_game,
)
from app.services.balances import change_balance, get_balance, user_usernames
from app.utils.formatters import format_rubles

# Активные игры и служебные флаги
games: Dict[int, Dict[str, Any]] = {}
pending_bet_input: Dict[int, bool] = {}
next_game_id: int = 1


# =====================================================
#                     МЕНЮ ИГР
# =====================================================

def build_games_keyboard(uid: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    # верхний ряд — создать / обновить
    rows.append(
        [
            InlineKeyboardButton(text="✅ Создать игру", callback_data="create_game"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_games"),
        ]
    )

    # активные игры (без соперника)
    active = [g for g in games.values() if g.get("opponent_id") is None]
    active.sort(key=lambda x: x["id"], reverse=True)

    for g in active:
        txt = f"🎲 Игра №{g['id']} | {format_rubles(g['bet'])} ₽"
        if g["creator_id"] == uid:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{txt} (Вы)", callback_data=f"game_my:{g['id']}"
                    )
                ]
            )
        else:
            rows.append(
                [InlineKeyboardButton(text=txt, callback_data=f"game_open:{g['id']}")]
            )

    # мои игры / рейтинг
    rows.append(
        [
            InlineKeyboardButton(text="📋 Мои игры", callback_data="my_games:0"),
            InlineKeyboardButton(text="🏆 Рейтинг", callback_data="rating"),
        ]
    )

    # ВАЖНО: помощь ТОЛЬКО по костям
    rows.append(
        [
            InlineKeyboardButton(text="🎮 Игры", callback_data="menu_games"),
            InlineKeyboardButton(text="🐼 Помощь", callback_data="help_dice"),
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_games_text() -> str:
    return "Создайте игру или выберите уже имеющуюся:"


async def send_games_list(chat_id: int, uid: int):
    await bot.send_message(
        chat_id,
        build_games_text(),
        reply_markup=build_games_keyboard(uid),
    )


# =====================================================
#               ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =====================================================

def calculate_profit(uid: int, g: Dict[str, Any]) -> int:
    """
    Сколько пользователь заработал/проиграл в конкретной игре.
    Положительное число = профит, отрицательное = убыток.
    """
    bet = g["bet"]
    winner = g.get("winner")

    if winner == "draw":
        return 0

    creator = uid == g["creator_id"]

    if winner == "creator" and creator:
        return bet
    if winner == "opponent" and not creator:
        return bet
    if winner == "creator" and not creator:
        return -bet
    if winner == "opponent" and creator:
        return -bet

    return 0


async def build_user_stats_and_history(
    uid: int,
) -> tuple[str, List[Dict[str, Any]]]:
    """
    Статистика и история игр пользователя.
    История берётся из БД → get_user_games.
    """
    finished = await get_user_games(uid)
    finished = finished[:HISTORY_LIMIT]

    stats = {
        "month": {"games": 0, "profit": 0},
        "week": {"games": 0, "profit": 0},
        "day": {"games": 0, "profit": 0},
    }

    now = datetime.now(timezone.utc)

    for g in finished:
        finished_at = g.get("finished_at")
        if not finished_at:
            continue

        if isinstance(finished_at, str):
            finished_at = datetime.fromisoformat(finished_at)
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=timezone.utc)

        diff = now - finished_at
        profit = calculate_profit(uid, g)

        if diff <= timedelta(days=30):
            stats["month"]["games"] += 1
            stats["month"]["profit"] += profit
        if diff <= timedelta(days=7):
            stats["week"]["games"] += 1
            stats["week"]["profit"] += profit
        if diff <= timedelta(days=1):
            stats["day"]["games"] += 1
            stats["day"]["profit"] += profit

    def ps(v: int) -> str:
        return ("+" if v > 0 else "") + format_rubles(v)

    stats_text = (
        f"🎲 Кости за месяц: {stats['month']['games']}\n"
        f"└ 💸 Профит: {ps(stats['month']['profit'])} ₽\n\n"
        f"🎲 За неделю: {stats['week']['games']}\n"
        f"└ 💸 Профит: {ps(stats['week']['profit'])} ₽\n\n"
        f"🎲 За сутки: {stats['day']['games']}\n"
        f"└ 💸 Профит: {ps(stats['day']['profit'])} ₽"
    )

    # История
    history: List[Dict[str, Any]] = []
    for g in finished[:HISTORY_LIMIT]:
        creator = g["creator_id"] == uid
        opp_id = g["opponent_id"] if creator else g["creator_id"]
        opp_name = user_usernames.get(opp_id, f"ID{opp_id}")
        bet = g["bet"]
        profit = calculate_profit(uid, g)

        if profit > 0:
            emoji, text = "✅", f"Победа над {opp_name} (+{format_rubles(profit)} ₽)"
        elif profit < 0:
            emoji, text = "❌", f"Поражение от {opp_name} ({format_rubles(profit)} ₽)"
        else:
            emoji, text = "🤝", f"Ничья с {opp_name}"

        my = g["creator_roll"] if creator else g["opponent_roll"]
        opp = g["opponent_roll"] if creator else g["creator_roll"]

        history.append(
            {"bet": bet, "emoji": emoji, "text": text, "my": my, "opp": opp}
        )

    return stats_text, history


def build_history_keyboard(
    history: List[Dict[str, Any]], page: int
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

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
        text = (
            f"{format_rubles(h['bet'])} ₽ | "
            f"{h['emoji']} | "
            f"Вы: {h['my']} | "
            f"Соперник: {h['opp']}"
        )
        rows.append([InlineKeyboardButton(text=text, callback_data="ignore")])

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"my_games:{page - 1}")
        )
    if page < pages - 1:
        nav_row.append(
            InlineKeyboardButton(text="➡️ Вперёд", callback_data=f"my_games:{page + 1}")
        )
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="🎮 Игры", callback_data="menu_games")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# =====================================================
#                     РЕЙТИНГ
# =====================================================

async def build_rating_text(requesting_uid: int) -> str:
    """
    Строим рейтинг, учитывая, что get_users_profit_and_games_30_days()
    возвращает КОРТЕЖ: (finished_games, all_uids)
    """
    now = datetime.now(timezone.utc)
    finished_games, all_uids = await get_users_profit_and_games_30_days()

    user_stats: Dict[int, Dict[str, int]] = {}

    # собираем профит и количество игр по каждому пользователю
    for g in finished_games:
        finished_at = g.get("finished_at")
        if isinstance(finished_at, str):
            finished_at = datetime.fromisoformat(finished_at)

        if not finished_at:
            continue
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=timezone.utc)

        if (now - finished_at) > timedelta(days=30):
            continue

        for uid in (g["creator_id"], g["opponent_id"]):
            if uid is None:
                continue
            stats = user_stats.setdefault(uid, {"profit": 0, "games": 0})
            stats["profit"] += calculate_profit(uid, g)
            stats["games"] += 1

    if not user_stats:
        return "🏆 Рейтинг пока пуст — за последние 30 дней не было завершённых игр."

    # сортировка по профиту, при равном профите — по количеству игр
    sorted_stats = sorted(
        user_stats.items(),
        key=lambda x: (x[1]["profit"], -x[1]["games"]),
        reverse=True,
    )

    top_list = sorted_stats[:3]
    medals = ["🥇", "🥈", "🥉"]
    top_lines: List[str] = []

    for i, (uid, s) in enumerate(top_list):
        username = user_usernames.get(uid) or f"ID{uid}"
        profit = s["profit"]
        games_count = s["games"]
        sign = "+" if profit > 0 else ""
        top_lines.append(
            f"{medals[i]} {username} — {sign}{format_rubles(profit)} ₽ за {games_count} игр"
        )

    total_players = len(sorted_stats)
    user_place = None
    user_profit = user_stats.get(requesting_uid, {"profit": 0, "games": 0})

    for i, (uid, _) in enumerate(sorted_stats):
        if uid == requesting_uid:
            user_place = i + 1
            break

    lines: List[str] = ["🏆 ТОП 3 игроков в кости:\n"]
    lines.extend(top_lines)
    lines.append("\n")

    if user_place:
        profit_str = format_rubles(user_profit["profit"])
        games_count = user_profit["games"]
        sign = "+" if user_profit["profit"] >= 0 else ""
        lines.append(
            f"Ваше место в рейтинге: {user_place} из {total_players} "
            f"({sign}{profit_str} ₽ за {games_count} игр)"
        )
    else:
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


# =====================================================
#                 ЛОГИКА ИГРЫ В КОСТИ
# =====================================================

async def telegram_roll(uid: int) -> int:
    """
    Бросок кубика через Telegram.
    (Функция оставлена для совместимости, вдруг где-то используется)
    """
    msg = await bot.send_dice(uid, emoji="🎲")
    await asyncio.sleep(3)  # ждём анимацию
    return msg.dice.value


async def play_game(gid: int):
    """
    Логика игры в кости:
    - бросок кубика каждому
    - при ничьей — переброс
    - результат показывается ПОСЛЕ окончания анимации
    """
    g = games.get(gid)
    if not g:
        return

    c = g["creator_id"]
    o = g["opponent_id"]
    bet = g["bet"]

    # 🎲 Перебрасываем, пока не будет победитель
    while True:
        creator_roll_msg = await bot.send_dice(c, emoji="🎲")
        opponent_roll_msg = await bot.send_dice(o, emoji="🎲")

        # ждём завершения анимации (2.5–3 секунды)
        await asyncio.sleep(3)

        cr = creator_roll_msg.dice.value
        orr = opponent_roll_msg.dice.value

        if cr != orr:
            break  # победитель найден, выходим из цикла (иначе переброс)

    g["creator_roll"] = cr
    g["opponent_roll"] = orr
    g["finished"] = True
    g["finished_at"] = datetime.now(timezone.utc)

    bank = bet * 2
    commission = bank // 100
    prize = bank - commission

    if cr > orr:
        winner = "creator"
        change_balance(c, prize)
    else:
        winner = "opponent"
        change_balance(o, prize)

    change_balance(MAIN_ADMIN_ID, commission)
    g["winner"] = winner

    # сохраняем в БД
    await upsert_game(g)

    # отправляем результат обоим игрокам
    for user in (c, o):
        is_creator = user == c
        your = cr if is_creator else orr
        their = orr if is_creator else cr

        result_text = (
            "🥳 Поздравляем с победой!"
            if (winner == "creator" and is_creator)
            or (winner == "opponent" and not is_creator)
            else "😔 К сожалению, вы проиграли."
        )

        bank_text = (
            f"💰 Банк: {format_rubles(bank)} ₽\n"
            f"💸 Комиссия: {format_rubles(commission)} ₽ (1%)"
        )

        txt = (
            f"🏁 Кости #{gid}\n"
            f"{bank_text}\n\n"
            f"🫵 Ваш результат: {your}\n"
            f"🎲 Результат соперника: {their}\n\n"
            f"{result_text}\n"
            f"💼 Баланс: {format_rubles(get_balance(user))} ₽"
        )

        await bot.send_message(user, txt)









