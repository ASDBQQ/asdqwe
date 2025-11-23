# app/db/users.py
from datetime import datetime, timezone
from typing import Optional

from .pool import pool


async def upsert_user(
    uid: int,
    username: str | None,
    balance: int,
    registered_at: Optional[datetime] = None,
):
    """Создать/обновить пользователя в БД."""
    if not pool:
        return
    async with pool.acquire() as db:
        await db.execute(
            """
            INSERT INTO users (user_id, username, balance, registered_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(user_id) DO UPDATE SET
                username=EXCLUDED.username,
                balance=EXCLUDED.balance
        """,
            uid,
            username,
            balance,
            registered_at.isoformat()
            if registered_at
            else datetime.now(timezone.utc).isoformat(),
        )


async def get_user_registered_at(uid: int) -> Optional[datetime]:
    """Получить дату регистрации пользователя."""
    if not pool:
        return None
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT registered_at FROM users WHERE user_id = $1",
            uid,
        )
        if row and row["registered_at"]:
            try:
                return datetime.fromisoformat(row["registered_at"])
            except ValueError:
                return None
        return None


