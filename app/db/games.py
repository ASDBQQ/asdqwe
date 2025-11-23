# app/db/games.py

from typing import Dict, Any, List
from datetime import datetime
from app.db.pool import pool


# -------------------------------------------
# СОХРАНЕНИЕ/ОБНОВЛЕНИЕ ИГР
# -------------------------------------------
async def upsert_game(g: Dict[str, Any]):
    async with pool.acquire() as db:
        await db.execute(
            """
            INSERT INTO games (
                id, creator_id, opponent_id, bet,
                creator_roll, opponent_roll, winner,
                finished, created_at, finished_at
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
            )
            ON CONFLICT (id) DO UPDATE SET
                creator_id = EXCLUDED.creator_id,
                opponent_id = EXCLUDED.opponent_id,
                bet = EXCLUDED.bet,
                creator_roll = EXCLUDED.creator_roll,
                opponent_roll = EXCLUDED.opponent_roll,
                winner = EXCLUDED.winner,
                finished = EXCLUDED.finished,
                created_at = EXCLUDED.created_at,
                finished_at = EXCLUDED.finished_at
            """,
            g["id"], g["creator_id"], g["opponent_id"], g["bet"],
            g["creator_roll"], g["opponent_roll"], g["winner"],
            g["finished"], g["created_at"], g["finished_at"]
        )


# -------------------------------------------
# ИСТОРИЯ ИГР ПОЛЬЗОВАТЕЛЯ
# -------------------------------------------
async def get_user_games(uid: int) -> List[Dict[str, Any]]:
    async with pool.acquire() as db:
        rows = await db.fetch(
            """
            SELECT *
            FROM games
            WHERE creator_id = $1 OR opponent_id = $1
            ORDER BY id DESC
            """,
            uid,
        )
        return [dict(r) for r in rows]


# -------------------------------------------
# КОЛ-ВО ИГР ДЛЯ ПРОФИЛЯ
# -------------------------------------------
async def get_user_dice_games_count(uid: int) -> int:
    async with pool.acquire() as db:
        row = await db.fetchrow(
            """
            SELECT COUNT(*) AS c
            FROM games
            WHERE finished = TRUE AND (creator_id = $1 OR opponent_id = $1)
            """,
            uid,
        )
        return row["c"] if row else 0


# -------------------------------------------
# РЕЙТИНГ (прибыль за 30 дней)
# -------------------------------------------
async def get_users_profit_and_games_30_days() -> List[Dict[str, Any]]:
    async with pool.acquire() as db:
        rows = await db.fetch(
            """
            SELECT
                u.user_id,
                COALESCE(SUM(g.profit), 0) AS profit,
                COUNT(g.id) AS games
            FROM users u
            LEFT JOIN games g ON g.winner = u.user_id 
                AND g.finished = TRUE
                AND g.finished_at >= NOW() - INTERVAL '30 days'
            GROUP BY u.user_id
            ORDER BY profit DESC
            """
        )
        return [dict(r) for r in rows]


# -------------------------------------------
# ВЫГРУЗКА ВСЕХ ЗАВЕРШЕННЫХ ИГР (для статистики / возможно будущего)
# -------------------------------------------
async def get_all_finished_games():
    async with pool.acquire() as db:
        rows = await db.fetch(
            """
            SELECT *
            FROM games
            WHERE finished = TRUE
            ORDER BY id DESC
            """
        )
        return [dict(r) for r in rows]

