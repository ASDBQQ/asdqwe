# app/services/transfers.py

from datetime import datetime, timezone
from typing import Optional, Dict, Any

from app.db.pool import pool


async def add_transfer(sender_id: int, receiver_id: int, amount: int):
    """
    Добавляет запись о переводе между пользователями.
    """
    if not pool:
        return

    async with pool.acquire() as db:
        await db.execute(
            """
            INSERT INTO transfers (sender_id, receiver_id, amount, created_at)
            VALUES ($1, $2, $3, $4)
            """,
            sender_id,
            receiver_id,
            amount,
            datetime.now(timezone.utc).isoformat(),
        )


async def get_user_transfers(uid: int) -> list[Dict[str, Any]]:
    """
    Получить историю переводов пользователя.
    """
    if not pool:
        return []

    async with pool.acquire() as db:
        rows = await db.fetch(
            """
            SELECT * FROM transfers
            WHERE sender_id = $1 OR receiver_id = $1
            ORDER BY created_at DESC
            """,
            uid,
        )
        return [dict(r) for r in rows]
