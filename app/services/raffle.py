# app/services/raffle.py
import asyncio
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Set, List

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from app.bot import bot
from app.config import (
    RAFFLE_MIN_BET,
    RAFFLE_MAX_BETS_PER_ROUND,
    RAFFLE_TIMER_SECONDS,
    RAFFLE_CANCEL_WINDOW_SECONDS,
    MAIN_ADMIN_ID,
)
from app.db.raffle import upsert_raffle_round, add_raffle_bet, get_raffle_rounds_and_bets_30_days
from app.services.balances import change_balance, get_balance, user_usernames
from app.utils.formatters import format_rubles


# Текущее состояние розыгрыша
raffle_round: Dict[str, Any] | None = None
raffle_task: asyncio.Task | None = None
next_raffle_id: int = 1

# ожидание ввода суммы для Банкира (используется в handlers/text.py)
pending_raffle_bet_input: Dict[int, bool] = {}


def _ensure_raffle_round() -> Dict[str, Any]:
    """
    Гарантирует наличие текущего раунда.
    Если раунд завершён или отсутствует — создаёт новый.
    """
    global raffle_round, next_raffle_id

    if raffle_round is None or raffle_round.get("finished"):
        raffle_round = {
            "id": next_raffle_id,
            "created_at": datetime.now(timezone.utc),
            "finished_at": None,

            # фиксированная ставка за 1 «долю» (share)
            "entry_amount": None,            # int | None

            # банк и ставки
            "total_bank": 0,                 # общая сумма в банке
            "tickets": [],                   # список user_id, по 1 на каждую ставку
            "participants": set(),           # set(user_id)
            "user_bets": {},                 # user_id -> количество ставок (долей)
            "user_last_bet_at": {},          # user_id -> datetime последней ставки

            # итог
            "winner_id": None,
            "finished": False,
            "draw_at": None,                 # datetime, когда должен быть розыгрыш
        }
        next_raffle_id += 1

    return raffle_round


def build_raffle_text(uid: int) -> str:
    """
    Текст состояния игры «Банкир» для пользователя uid.
    Как на твоём примере: участники, банк, твой вклад, шанс и таймер.
    """
    r = raffle_round

    if not r or r.get("finished") or not r.get("tickets"):
        return (
            "🏁 Розыгрыш начнётся когда будет как минимум два участника.\n\n"
            "🧑‍🦳 Станьте первым, кто сделает ставку.\n\n"
            f"Минимальная первая ставка: {RAFFLE_MIN_BET} ₽.\n"
            f"Можно сделать до {RAFFLE_MAX_BETS_PER_ROUND} ставок за раунд.\n\n"
            "Чем больше вы положили в банк, тем выше шанс на победу.\n"
            "После появления 2 участников запускается таймер на 60 секунд.\n"
            "По его истечении случайный участник забирает весь банк (минус 1% комиссии)."
        )

    entry_amount: int = r["entry_amount"]
    total_bank: int = r["total_bank"]
    participants: Set[int] = r["participants"]
    user_bets: Dict[int, int] = r["user_bets"]

    user_shares = user_bets.get(uid, 0)
    user_amount = user_shares * entry_amount

    # шансы в процентах
    if total_bank > 0 and user_amount > 0:
        user_chance = round((user_amount / total_bank) * 100)
    else:
        user_chance = 0

    # шанс победителя ≈ максимальная доля
    if total_bank > 0 and participants:
        biggest_uid = max(
            participants,
            key=lambda u: user_bets.get(u, 0) * entry_amount,
        )
        biggest_amount = user_bets.get(biggest_uid, 0) * entry_amount
        winner_chance = round((biggest_amount / total_bank) * 100)
    else:
        winner_chance = 0

    timer_line = ""
    draw_at = r.get("draw_at")
    if draw_at:
        seconds_left = int((draw_at - datetime.now(timezone.utc)).total_seconds())
        if seconds_left < 0:
            seconds_left = 0
        timer_line = f"\n⏳ До окончания раунда: {seconds_left} сек."
    else:
        need = max(0, 2 - len(participants))
        if need > 0:
            timer_line = f"\nОжидаем ещё {need} участника(ов) для запуска таймера."

    text_lines = [
        "🎩 Игра «Банкир» — текущий раунд\n",
        f"👥 Участников: {len(participants)}",
        f"💰 Банк: {format_rubles(total_bank)} ₽",
        f"💵 Фиксированная ставка за 1 долю: {format_rubles(entry_amount)} ₽",
        timer_line,
        "",
    ]

    if user_shares > 0:
        text_lines += [
            f"🪙 Вы положили: {format_rubles(user_amount)} ₽ ({user_shares}/{RAFFLE_MAX_BETS_PER_ROUND})",
            f"🎲 Ваш шанс: {user_chance}%",
        ]
    else:
        text_lines.append("🧑‍🦳 Вы ещё не делали ставки в этом раунде.")

    return "\n".join(text_lines)


def build_raffle_menu_keyboard(uid: int) -> InlineKeyboardMarkup:
    """
    Клавиатура как в твоём примере:
    - Сделать ставку
    - Обновить
    - Игры / Помощь
    + динамические быстрые суммы, если уже известна entry_amount
    """
    r = raffle_round

    rows: List[List[InlineKeyboardButton]] = []

    # Быстрые суммы, если уже есть фиксированная ставка
    if r and not r.get("finished") and r.get("entry_amount"):
        entry_amount: int = r["entry_amount"]
        # 1, 3, 7 долей — как 25 / 75 / 175 RUB на твоём скрине
        quick_amounts = [
            entry_amount * 1,
            entry_amount * 3,
            entry_amount * 7,
        ]
        quick_buttons = [
            InlineKeyboardButton(
                text=f"{format_rubles(a)} ₽",
                callback_data=f"raffle_quick:{a}",
            )
            for a in quick_amounts
        ]
        rows.append(quick_buttons)

    # Кнопка «Сделать ставку»
    rows.append(
        [InlineKeyboardButton(text="💰 Сделать ставку", callback_data="raffle_make_bet")]
    )

    # Кнопка отмены ставок в текущем раунде
    rows.append(
        [InlineKeyboardButton(text="♻ Отменить мои ставки", callback_data="raffle_cancel")]
    )

    # Обновить
    rows.append(
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="raffle_refresh")]
    )

    # Игры / Помощь
    rows.append(
        [
            InlineKeyboardButton(text="🎮 Игры", callback_data="menu_games"),
            InlineKeyboardButton(text="🐼 Помощь", callback_data="help_banker"),
        ]
    )

    # Отдельный рейтинг Банкира
    rows.append(
        [InlineKeyboardButton(text="🏆 Рейтинг Банкира", callback_data="raffle_rating")]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_raffle_menu(chat_id: int, uid: int):
    await bot.send_message(
        chat_id,
        build_raffle_text(uid),
        reply_markup=build_raffle_menu_keyboard(uid),
    )


async def _process_raffle_bet(uid: int, chat_id: int, amount: int) -> str:
    """
    Обработка ставки пользователя:
    - первая ставка задаёт entry_amount
    - далее сумма должна быть кратна entry_amount
    - максимум RAFFLE_MAX_BETS_PER_ROUND долей на игрока
    """
    global raffle_task

    if amount < RAFFLE_MIN_BET:
        return f"Минимальная сумма первой ставки: {format_rubles(RAFFLE_MIN_BET)} ₽."

    # Проверяем баланс
    bal = get_balance(uid)
    if amount > bal:
        return (
            f"Недостаточно средств. Ваш баланс: {format_rubles(bal)} ₽, "
            f"ставка: {format_rubles(amount)} ₽."
        )

    r = _ensure_raffle_round()

    if r["entry_amount"] is None:
        # первая ставка в раунде задаёт entry_amount и ровно 1 долю
        entry_amount = amount
        shares_to_add = 1
        r["entry_amount"] = entry_amount
    else:
        entry_amount: int = r["entry_amount"]
        if amount % entry_amount != 0:
            return (
                "Сумма должна быть кратной фиксированной ставке за 1 долю — "
                f"{format_rubles(entry_amount)} ₽.\n"
                "Например, 1, 3 или 7 долей."
            )
        shares_to_add = amount // entry_amount
        if shares_to_add <= 0:
            return "Сумма слишком мала."

    # проверка лимита долей на игрока
    current_shares = r["user_bets"].get(uid, 0)
    if current_shares + shares_to_add > RAFFLE_MAX_BETS_PER_ROUND:
        return (
            f"Нельзя сделать более {RAFFLE_MAX_BETS_PER_ROUND} ставок в одном раунде.\n"
            f"Сейчас у вас уже {current_shares}."
        )

    # списываем деньги
    change_balance(uid, -amount)

    # обновляем состояние раунда
    r["total_bank"] += amount
    r["participants"].add(uid)
    r["user_bets"][uid] = current_shares + shares_to_add
    r["user_last_bet_at"][uid] = datetime.now(timezone.utc)

    # добавляем «билеты» в список
    for _ in range(shares_to_add):
        r["tickets"].append(uid)

    # пишем в БД поштучные суммы (как есть)
    await add_raffle_bet(r["id"], uid, amount)

    # запускаем таймер, если это второй участник
    if len(r["participants"]) >= 2 and r.get("draw_at") is None:
        r["draw_at"] = datetime.now(timezone.utc) + timedelta(
            seconds=RAFFLE_TIMER_SECONDS
        )
        raffle_task = asyncio.create_task(raffle_draw_worker(r["id"]))

    # текст ответа пользователю
    entry_amount = r["entry_amount"]
    total_bets = len(r["tickets"])
    user_shares = r["user_bets"][uid]
    total_bank = r["total_bank"]

    # шанс пользователя
    user_amount = user_shares * entry_amount
    if total_bank > 0:
        user_chance = round((user_amount / total_bank) * 100)
    else:
        user_chance = 0

    timer_line = ""
    draw_at = r.get("draw_at")
    if draw_at:
        seconds_left = int((draw_at - datetime.now(timezone.utc)).total_seconds())
        if seconds_left < 0:
            seconds_left = 0
        timer_line = f"\n⏳ До окончания: ~{seconds_left} сек."
    else:
        need = max(0, 2 - len(r["participants"]))
        timer_line = f"\nОжидаем ещё {need} участника(ов) для запуска таймера."

    return (
        "✅ Ставка в игре «Банкир» принята!\n\n"
        f"👥 Участников: {len(r['participants'])}\n"
        f"💰 Банк: {format_rubles(total_bank)} ₽\n"
        f"🪙 Вы положили: {format_rubles(user_amount)} ₽ ({user_shares}/{RAFFLE_MAX_BETS_PER_ROUND})\n"
        f"🎲 Ваш шанс: {user_chance}%"
        f"{timer_line}"
    )


async def raffle_draw_worker(raffle_id: int):
    """
    Фоновая задача: ждёт RAFFLE_TIMER_SECONDS и запускает розыгрыш.
    """
    global raffle_round, raffle_task
    await asyncio.sleep(RAFFLE_TIMER_SECONDS)

    r = raffle_round
    if not r or r.get("finished") or r.get("id") != raffle_id:
        return

    await perform_raffle_draw()
    raffle_task = None


async def perform_raffle_draw():
    """
    Сам розыгрыш:
    - если участников < 2 — возврат ставок
    - иначе случайный победитель по билетам (tickets)
    """
    global raffle_round
    r = raffle_round
    if not r or r.get("finished"):
        return

    participants: Set[int] = r["participants"]
    tickets: List[int] = r["tickets"]
    entry_amount: int | None = r["entry_amount"]
    total_bank: int = r["total_bank"]

    if not tickets or not entry_amount:
        # Нечего разыгрывать
        r["finished"] = True
        r["finished_at"] = datetime.now(timezone.utc)
        await upsert_raffle_round(
            {
                "created_at": r["created_at"],
                "finished_at": r["finished_at"],
                "winner_id": None,
                "total_bank": 0,
            }
        )
        return

    # если участников меньше 2 — отменяем раунд и возвращаем всем деньги
    if len(participants) < 2:
        for uid, shares in r["user_bets"].items():
            refund_amount = shares * entry_amount
            if refund_amount > 0:
                change_balance(uid, refund_amount)
                try:
                    await bot.send_message(
                        uid,
                        "⚠ Розыгрыш «Банкир» отменён: недостаточно участников.\n"
                        f"Вам возвращено {format_rubles(refund_amount)} ₽.",
                    )
                except Exception:
                    pass

        r["finished"] = True
        r["finished_at"] = datetime.now(timezone.utc)
        r["winner_id"] = None

        await upsert_raffle_round(
            {
                "created_at": r["created_at"],
                "finished_at": r["finished_at"],
                "winner_id": None,
                "total_bank": 0,
            }
        )
        return

    # случайный победитель по «билетам»
    winner_uid = random.choice(tickets)
    commission = total_bank // 100
    prize = total_bank - commission

    # рассчитываем статистику по ставкам
    user_bets: Dict[int, int] = r["user_bets"]
    per_user_amount: Dict[int, int] = {
        uid: shares * entry_amount for uid, shares in user_bets.items()
    }

    # прибыль/убыток по пользователям (используется для рейтинга)
    # winner: prize - свой вклад
    # остальные: - свой вклад
    profit_by_user: Dict[int, int] = {}
    for uid, put_amount in per_user_amount.items():
        if uid == winner_uid:
            profit_by_user[uid] = prize - put_amount
        else:
            profit_by_user[uid] = -put_amount

    # выплаты
    change_balance(winner_uid, prize)
    change_balance(MAIN_ADMIN_ID, commission)

    r["finished"] = True
    r["finished_at"] = datetime.now(timezone.utc)
    r["winner_id"] = winner_uid

    await upsert_raffle_round(
        {
            "created_at": r["created_at"],
            "finished_at": r["finished_at"],
            "winner_id": winner_uid,
            "total_bank": total_bank,
        }
    )

    # сообщения участникам
    for uid in participants:
        put_amount = per_user_amount.get(uid, 0)
        shares = user_bets.get(uid, 0)

        if total_bank > 0 and put_amount > 0:
            user_chance = round((put_amount / total_bank) * 100)
        else:
            user_chance = 0

        if total_bank > 0:
            winner_chance = round(
                (per_user_amount.get(winner_uid, 0) / total_bank) * 100
            )
        else:
            winner_chance = 0

        if uid == winner_uid:
            result_text = (
                "🥳 Поздравляем! Вы выиграли розыгрыш Банкира!\n"
                f"🏆 Ваш выигрыш: {format_rubles(prize)} ₽."
            )
        else:
            result_text = "😔 К сожалению, вы проиграли в этом раунде."

        msg = (
            "🏁 Розыгрыш Банкира завершён!\n\n"
            f"👥 Участников: {len(participants)}\n"
            f"💰 Банк составил: {format_rubles(total_bank)} ₽\n"
            f"🎲 Шанс победителя: {winner_chance}%\n\n"
            f"🪙 Вы положили: {format_rubles(put_amount)} ₽ ({shares}/{RAFFLE_MAX_BETS_PER_ROUND})\n"
            f"🎯 Ваш шанс: {user_chance}%\n\n"
            f"{result_text}\n\n"
            f"💼 Баланс: {format_rubles(get_balance(uid))} ₽"
        )

        try:
            await bot.send_message(uid, msg)
        except Exception:
            pass


async def cancel_user_bets(uid: int) -> str:
    """
    Отмена ставок пользователя в текущем раунде (если прошло не более 10 минут
    с его последней ставки).
    """
    r = raffle_round
    if not r or r.get("finished") or not r.get("tickets"):
        return "Сейчас нет активного розыгрыша с вашими ставками."

    user_bets: Dict[int, int] = r["user_bets"]
    last_bet_at: Dict[int, datetime] = r["user_last_bet_at"]

    if uid not in user_bets or user_bets[uid] <= 0:
        return "У вас нет активных ставок в текущем раунде."

    last_time = last_bet_at.get(uid)
    if not last_time:
        return "Не удалось определить время ставки. Отмена невозможна."

    delta = datetime.now(timezone.utc) - last_time
    if delta.total_seconds() > RAFFLE_CANCEL_WINDOW_SECONDS:
        return "Ставку можно отменить только в течение 10 минут после последней ставки."

    shares = user_bets[uid]
    entry_amount: int = r["entry_amount"]
    refund_amount = shares * entry_amount

    # возвращаем деньги
    change_balance(uid, refund_amount)

    # убираем билеты пользователя
    r["tickets"] = [u for u in r["tickets"] if u != uid]
    r["total_bank"] -= refund_amount
    if r["total_bank"] < 0:
        r["total_bank"] = 0

    # убираем пользователя из структуры
    del user_bets[uid]
    last_bet_at.pop(uid, None)
    if uid in r["participants"]:
        r["participants"].remove(uid)

    return (
        f"♻ Ваши ставки в текущем раунде отменены.\n"
        f"Вам возвращено {format_rubles(refund_amount)} ₽."
    )


async def build_raffle_rating_text(requesting_uid: int) -> str:
    """
    Отдельный рейтинг Банкира за последние 30 дней.
    Считаем прибыль по каждому игроку.
    """
    rounds, bets = await get_raffle_rounds_and_bets_30_days()
    if not rounds:
        return "🏆 Рейтинг Банкира пока пуст — за последние 30 дней не было завершённых раундов."

    # организуем ставки по раундам
    bets_by_round: Dict[int, List[Dict[str, Any]]] = {}
    for b in bets:
        rid = b["raffle_id"]
        bets_by_round.setdefault(rid, []).append(b)

    user_stats: Dict[int, Dict[str, int]] = {}

    for r in rounds:
        rid = r["id"]
        total_bank: int = r["total_bank"] or 0
        winner_id: int | None = r["winner_id"]

        if total_bank <= 0 or not winner_id:
            continue

        per_round_bets: Dict[int, int] = {}
        for b in bets_by_round.get(rid, []):
            uid = b["user_id"]
            amount = b["amount"]
            per_round_bets[uid] = per_round_bets.get(uid, 0) + amount

        if not per_round_bets:
            continue

        commission = total_bank // 100
        prize = total_bank - commission

        for uid, amount in per_round_bets.items():
            stats = user_stats.setdefault(uid, {"profit": 0, "rounds": 0})
            if uid == winner_id:
                stats["profit"] += prize - amount
            else:
                stats["profit"] -= amount
            stats["rounds"] += 1

    if not user_stats:
        return "🏆 Рейтинг Банкира пока пуст — нет данных по выигрышам."

    top_list = sorted(
        user_stats.items(),
        key=lambda x: (x[1]["profit"], -x[1]["rounds"]),
        reverse=True,
    )

    lines = ["🏆 ТОП-3 игроков Банкира за последние 30 дней:\n"]
    place_emoji = ["🥇", "🥈", "🥉"]

    for i, (uid, stats) in enumerate(top_list[:3]):
        profit = stats["profit"]
        rounds_count = stats["rounds"]
        profit_str = ("+" if profit > 0 else "") + format_rubles(profit)
        username = user_usernames.get(uid) or f"ID{uid}"
        lines.append(
            f"{place_emoji[i]} {username} — {profit_str} ₽ за {rounds_count} раунд(ов)"
        )

    # информация о запрашивающем
    user_place = None
    total_players = len(top_list)
    user_profit = user_stats.get(requesting_uid, {"profit": 0, "rounds": 0})

    for i, (uid, stats) in enumerate(top_list):
        if uid == requesting_uid:
            user_place = i + 1
            break

    lines.append("")

    if user_place:
        profit = user_profit["profit"]
        rounds_count = user_profit["rounds"]
        profit_str = ("+" if profit >= 0 else "") + format_rubles(profit)
        lines.append(
            f"Ваше место в рейтинге Банкира: {user_place} из {total_players} "
            f"({profit_str} ₽ за {rounds_count} раунд(ов))."
        )
    else:
        lines.append(
            "Ваше место в рейтинге Банкира: нет данных (вы ещё не участвовали или не выигрывали)."
        )

    return "\n".join(lines)



