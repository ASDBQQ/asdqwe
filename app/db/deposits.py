# app/db/deposits.py
from datetime import datetime, timezone

from .pool import pool


async def add_ton_deposit(
    tx_hash: str,
    user_id: int,
    ton_amount: float,
    coins: int,
    comment: str,
):
    """Сохранить факт пополнения через TON."""
    if not pool:
        return
    async with pool.acquire() as db:
        await db.execute(
            """
            INSERT INTO ton_deposits (tx_hash, user_id, ton_amount, coins, comment, at)
            VALUES ($1, $2, $3, $4, $5, $6)
        """,
            tx_hash,
            user_id,
            ton_amount,
            coins,
            comment,
            datetime.now(timezone.utc).isoformat(),
        )
