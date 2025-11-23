# app/db/raffle.py
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple

from .pool import pool


async def upsert_raffle_round(r: Dict[str, Any]):
    """Сохранить результат раунда 'Банкир'."""
    if not pool:
        return
    async with pool.acquire() as db:
        await db.execute(
            """
            INSERT INTO raffle_rounds (id, created_at, finished_at, winner_id, total_bank)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT(id) DO UPDATE SET
                created_at=EXCLUDED.created_at,
                finished_at=EXCLUDED.finished_at,
                winner_id=EXCLUDED.winner_id,
                total_bank=EXCLUDED.total_bank
        """,
            r.get("id"),
            r["created_at"].isoformat() if r.get("created_at") else None,
            r["finished_at"].isoformat() if r.get("finished_at") else None,
            r.get("winner_id"),
            r.get("total_bank", 0),
        )


async def add_raffle_bet(raffle_id: int, user_id: int, amount: int):
    """Добавить ставку пользователя в конкретный раунд."""
    if not pool:
        return
    async with pool.acquire() as db:
        await db.execute(
            """
            INSERT INTO raffle_bets (raffle_id, user_id, amount)
            VALUES ($1, $2, $3)
        """,
            raffle_id,
            user_id,
            amount,
        )


async def get_user_raffle_bets_count(uid: int) -> int:
    """Количество раундов Банкира, где участвовал пользователь."""
    if not pool:
        return 0
    async with pool.acquire() as db:
        count = await db.fetchval(
            "SELECT COUNT(DISTINCT raffle_id) FROM raffle_bets WHERE user_id = $1",
            uid,
        )
        return count if count is not None else 0


async def get_user_bets_in_raffle(raffle_id: int, user_id: int) -> int:
    """Количество ставок пользователя в конкретном раунде Банкира."""
    if not pool:
        return 0
    async with pool.acquire() as db:
        count = await db.fetchval(
            "SELECT COUNT(*) FROM raffle_bets WHERE raffle_id = $1 AND user_id = $2",
            raffle_id,
            user_id,
        )
        return count if count is not None else 0


async def get_raffle_rounds_and_bets_30_days() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Для рейтинга Банкира:
    - возвращает список раундов за последние 30 дней
    - и список всех ставок по этим раундам
    """
    if not pool:
        return [], []

    now = datetime.now(timezone.utc)
    delta_30 = now - timedelta(days=30)

    async with pool.acquire() as db:
        rounds_records = await db.fetch(
            """
            SELECT id, created_at, finished_at, winner_id, total_bank
            FROM raffle_rounds
            WHERE finished_at IS NOT NULL AND finished_at >= $1
        """,
            delta_30.isoformat(),
        )

        round_ids = [r["id"] for r in rounds_records]
        if not round_ids:
            return [], []

        bets_records = await db.fetch(
            """
            SELECT raffle_id, user_id, amount
            FROM raffle_bets
            WHERE raffle_id = ANY($1::int[])
        """,
            round_ids,
        )

    rounds = [dict(r) for r in rounds_records]
    bets = [dict(b) for b in bets_records]
    return rounds, bets


