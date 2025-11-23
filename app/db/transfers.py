# app/db/transfers.py
from datetime import datetime, timezone
from typing import Dict, Any, List

from app.db.pool import pool


async def add_transfer(sender_id: int, receiver_id: int, amount: int) -> None:
    """
    Сохраняет перевод в таблицу transfers.
    Вызывается из handlers/text.py
    """
    if not pool:
        return

    async with pool.acquire() as db:
        await db.execute(
            """
            INSERT INTO transfers (from_id, to_id, amount, at)
            VALUES ($1, $2, $3, $4)
        """,
            sender_id,
            receiver_id,
            amount,
            datetime.now(timezone.utc).isoformat(),
        )


async def get_user_transfers(uid: int) -> List[Dict[str, Any]]:
    """
    Получить список переводов пользователя (как отправитель или получатель).
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



