# app/services/balances.py

import asyncio
from typing import Dict, Any

from app.db.users import upsert_user

# Баланс пользователей (кэш в памяти, синхронизируется с БД)
user_balances: Dict[int, int] = {}

# username по user_id (для переводов и отображения)
user_usernames: Dict[int, str] = {}

# ----- Пополнения -----
pending_topup: Dict[int, Any] = {}

# ----- Вывод -----
pending_withdraw: Dict[int, Any] = {}
temp_withdraw: Dict[int, Any] = {}

# ----- Переводы -----
# шаг перевода: None / "await_username" / "await_amount"
pending_transfer_step: Dict[int, str] = {}
# временно сохраняем id получателя
pending_transfer_target: Dict[int, int] = {}
# временно сохраняем данные перевода
temp_transfer: Dict[int, Any] = {}


# 🟦 USER MANAGEMENT --------------------------------------------------------


def register_user(user) -> None:
    """Регистрируем пользователя:
    - сохраняем username в кэш
    - создаём/обновляем запись в users в БД
    """
    uid = user.id

    # Сохраняем username в кэше
    if user.username:
        user_usernames[uid] = user.username

    # Если пользователя ещё нет в кэше балансов — создаём с нулём
    if uid not in user_balances:
        user_balances[uid] = 0

    # Асинхронно создаём/обновляем запись в БД
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # нет запущенного цикла (теоретически не должно быть в боте)
        return

    loop.create_task(
        upsert_user(
            uid=uid,
            username=user.username,
            balance=user_balances[uid],
        )
    )


# 🟦 BALANCE ----------------------------------------------------------------


def get_balance(uid: int) -> int:
    """Получаем баланс из кэша (он синхронизируется с БД при изменениях)."""
    return user_balances.get(uid, 0)


def _sync_user_to_db(uid: int) -> None:
    """Планируем обновление баланса/username в БД в фоне."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return

    username = user_usernames.get(uid)
    balance = user_balances.get(uid, 0)

    loop.create_task(
        upsert_user(
            uid=uid,
            username=username,
            balance=balance,
        )
    )


def change_balance(uid: int, amount: int) -> None:
    """Изменить баланс на +amount или -amount и сохранить в БД."""
    current = user_balances.get(uid, 0)
    new_balance = current + amount
    user_balances[uid] = new_balance

    _sync_user_to_db(uid)


def set_balance(uid: int, amount: int) -> None:
    """Админская функция — установить баланс напрямую и сохранить в БД."""
    user_balances[uid] = amount
    _sync_user_to_db(uid)


