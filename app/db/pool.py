# app/db/pool.py
import os
from datetime import datetime, timezone
from typing import Dict

import asyncpg


# Глобальный пул подключений к PostgreSQL
pool: asyncpg.Pool | None = None


async def init_db(
    user_balances: Dict[int, int],
    user_usernames: Dict[int, str],
    processed_ton_tx: set[str],
):
    """Инициализация пула подключений и создание таблиц + загрузка кэша."""
    global pool

    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        raise Exception(
            "Переменная окружения DATABASE_URL не найдена. "
            "Подключение к PostgreSQL невозможно."
        )

    pool = await asyncpg.create_pool(DATABASE_URL)

    async with pool.acquire() as db:
        # 1. Таблица users
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                balance INTEGER,
                registered_at TEXT
            )
        """
        )

        # 2. Таблица games
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                creator_id BIGINT,
                opponent_id BIGINT,
                bet INTEGER,
                creator_roll INTEGER,
                opponent_roll INTEGER,
                winner TEXT,
                finished INTEGER,
                created_at TEXT,
                finished_at TEXT
            )
        """
        )

        # 3. Таблица raffle_rounds
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS raffle_rounds (
                id SERIAL PRIMARY KEY,
                created_at TEXT,
                finished_at TEXT,
                winner_id BIGINT,
                total_bank INTEGER
            )
        """
        )

        # 4. Таблица raffle_bets
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS raffle_bets (
                id SERIAL PRIMARY KEY,
                raffle_id INTEGER,
                user_id BIGINT,
                amount INTEGER
            )
        """
        )

        # 5. Таблица ton_deposits
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS ton_deposits (
                tx_hash TEXT PRIMARY KEY,
                user_id BIGINT,
                ton_amount REAL,
                coins INTEGER,
                comment TEXT,
                at TEXT
            )
        """
        )

        # 6. Таблица transfers
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS transfers (
                id SERIAL PRIMARY KEY,
                from_id BIGINT,
                to_id BIGINT,
                amount INTEGER,
                at TEXT
            )
        """
        )

        # 7. Загрузка пользователей в память
        records = await db.fetch("SELECT user_id, username, balance FROM users")
        for record in records:
            uid = record["user_id"]
            username = record["username"]
            balance = record["balance"]
            user_balances[uid] = balance
            user_usernames[uid] = username

        # 8. Загрузка обработанных TON-транзакций
        records = await db.fetch("SELECT tx_hash FROM ton_deposits")
        for record in records:
            processed_ton_tx.add(record["tx_hash"])


