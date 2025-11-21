import os
import asyncpg
from datetime import datetime, timedelta, timezone # <-- ИСПРАВЛЕНИЕ: timedelta теперь импортирован!
from typing import Dict, Any, List, Optional

# Глобальный пул подключений к PostgreSQL
pool: asyncpg.Pool | None = None

async def init_db(user_balances: Dict[int, int], user_usernames: Dict[int, str], processed_ton_tx: set):
    """Инициализация пула подключений и создание таблиц."""
    global pool
    
    # Чтение переменной, которую предоставляет Railway
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        # Это должно быть установлено на Railway. Если нет - бот не запустится.
        raise Exception("Переменная окружения DATABASE_URL не найдена. Подключение к PostgreSQL невозможно.")

    # Создание пула подключений
    pool = await asyncpg.create_pool(DATABASE_URL)

    async with pool.acquire() as db:
        # 1. Таблица users (BIGINT для user_id, TEXT для registered_at)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                balance INTEGER,
                registered_at TEXT
            )
        """)
        
        # 2. Таблица games
        await db.execute("""
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
        """)

        # 3. Таблица raffle_rounds
        await db.execute("""
            CREATE TABLE IF NOT EXISTS raffle_rounds (
                id SERIAL PRIMARY KEY,
                created_at TEXT,
                finished_at TEXT,
                winner_id BIGINT,
                total_bank INTEGER
            )
        """)

        # 4. Таблица raffle_bets
        await db.execute("""
            CREATE TABLE IF NOT EXISTS raffle_bets (
                id SERIAL PRIMARY KEY,
                raffle_id INTEGER,
                user_id BIGINT,
                amount INTEGER
            )
        """)

        # 5. Таблица ton_deposits
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ton_deposits (
                tx_hash TEXT PRIMARY KEY,
                user_id BIGINT,
                ton_amount REAL,
                coins INTEGER,
                comment TEXT,
                at TEXT
            )
        """)

        # 6. Таблица transfers
        await db.execute("""
            CREATE TABLE IF NOT EXISTS transfers (
                id SERIAL PRIMARY KEY,
                from_id BIGINT,
                to_id BIGINT,
                amount INTEGER,
                at TEXT
            )
        """)

        # 7. Загрузка данных
        records = await db.fetch("SELECT user_id, username, balance FROM users")
        for record in records:
            uid, username, balance = record['user_id'], record['username'], record['balance']
            user_balances[uid] = balance
            user_usernames[uid] = username

        # загрузка обработанных транзакций TON
        records = await db.fetch("SELECT tx_hash FROM ton_deposits")
        for record in records:
            processed_ton_tx.add(record['tx_hash'])


# ----------------------------------------------------
#  Обновлённые функции CRUD (PostgreSQL синтаксис)
# ----------------------------------------------------

async def upsert_user(uid, username, balance, registered_at: Optional[datetime] = None):
    if not pool: return
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO users (user_id, username, balance, registered_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(user_id) DO UPDATE SET
                username=EXCLUDED.username,
                balance=EXCLUDED.balance
        """, 
            uid,
            username,
            balance,
            registered_at.isoformat() if registered_at else datetime.now(timezone.utc).isoformat()
        )

async def get_user_registered_at(uid: int) -> Optional[datetime]:
    if not pool: return
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT registered_at FROM users WHERE user_id = $1",
            uid
        )
        if row and row['registered_at']:
            try:
                return datetime.fromisoformat(row['registered_at'])
            except ValueError:
                return None
        return None

async def upsert_game(g: Dict[str, Any]):
    if not pool: return
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO games (
                creator_id, opponent_id, bet,
                creator_roll, opponent_roll, winner,
                finished, created_at, finished_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT(id) DO UPDATE SET
                creator_id=EXCLUDED.creator_id,
                opponent_id=EXCLUDED.opponent_id,
                bet=EXCLUDED.bet,
                creator_roll=EXCLUDED.creator_roll,
                opponent_roll=EXCLUDED.opponent_roll,
                winner=EXCLUDED.winner,
                finished=EXCLUDED.finished,
                created_at=EXCLUDED.created_at,
                finished_at=EXCLUDED.finished_at
        """, 
            g["creator_id"],
            g["opponent_id"],
            g["bet"],
            g.get("creator_roll"),
            g.get("opponent_roll"),
            g.get("winner"),
            int(g.get("finished", False)),
            g["created_at"].isoformat() if g.get("created_at") else None,
            g["finished_at"].isoformat() if g.get("finished_at") else None,
        )

async def get_user_games(uid: int) -> List[Dict[str, Any]]:
    if not pool: return []
    async with pool.acquire() as db:
        records = await db.fetch("""
            SELECT * FROM games
            WHERE (creator_id = $1 OR opponent_id = $1) AND finished = 1
            ORDER BY finished_at DESC
        """, uid)
        return [dict(r) for r in records]

async def get_all_finished_games() -> List[Dict[str, Any]]:
    if not pool: return []
    async with pool.acquire() as db:
        records = await db.fetch("SELECT * FROM games WHERE finished = 1")
        return [dict(r) for r in records]

async def get_user_dice_games_count(uid: int, finished_only: bool = True) -> int:
    if not pool: return 0
    async with pool.acquire() as db:
        query = """
            SELECT COUNT(*) FROM games
            WHERE (creator_id = $1 OR opponent_id = $1)
        """
        params = [uid]
        if finished_only:
            query += " AND finished = 1"
        
        # Обратите внимание на синтаксис asyncpg для fetchval
        count = await db.fetchval(query, *params)
        return count if count is not None else 0

async def get_user_raffle_bets_count(uid: int) -> int:
    if not pool: return 0
    async with pool.acquire() as db:
        count = await db.fetchval(
            "SELECT COUNT(DISTINCT raffle_id) FROM raffle_bets WHERE user_id = $1",
            uid
        )
        return count if count is not None else 0

async def upsert_raffle_round(r: Dict[str, Any]):
    if not pool: return
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO raffle_rounds (created_at, finished_at, winner_id, total_bank)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(id) DO UPDATE SET
                created_at=EXCLUDED.created_at,
                finished_at=EXCLUDED.finished_at,
                winner_id=EXCLUDED.winner_id,
                total_bank=EXCLUDED.total_bank
        """, 
            r["created_at"].isoformat() if r.get("created_at") else None,
            r["finished_at"].isoformat() if r.get("finished_at") else None,
            r.get("winner_id"),
            r.get("total_bank", 0),
        )

async def add_raffle_bet(raffle_id: int, user_id: int, amount: int):
    if not pool: return
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO raffle_bets (raffle_id, user_id, amount)
            VALUES ($1, $2, $3)
        """, raffle_id, user_id, amount)

async def add_ton_deposit(tx_hash: str, user_id: int, ton_amount: float, coins: int, comment: str):
    if not pool: return
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO ton_deposits (tx_hash, user_id, ton_amount, coins, comment, at)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, 
            tx_hash,
            user_id,
            ton_amount,
            coins,
            comment,
            datetime.now(timezone.utc).isoformat()
        )

async def add_transfer(from_id: int, to_id: int, amount: int):
    if not pool: return
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO transfers (from_id, to_id, amount, at)
            VALUES ($1, $2, $3, $4)
        """, 
            from_id,
            to_id,
            amount,
            datetime.now(timezone.utc).isoformat()
        )

async def get_user_bets_in_raffle(raffle_id: int, user_id: int) -> int:
    if not pool: return 0
    async with pool.acquire() as db:
        count = await db.fetchval(
            "SELECT COUNT(*) FROM raffle_bets WHERE raffle_id = $1 AND user_id = $2",
            raffle_id, user_id
        )
        return count if count is not None else 0

async def get_users_profit_and_games_30_days() -> tuple[List[Dict[str, Any]], List[int]]:
    if not pool: return [], []
    now = datetime.now(timezone.utc)
    # timedelta теперь доступна
    delta_30_days = now - timedelta(days=30) 
    
    async with pool.acquire() as db:
        # Получаем все игры за последние 30 дней
        finished_games_records = await db.fetch(
            "SELECT * FROM games WHERE finished = 1 AND finished_at >= $1",
            delta_30_days.isoformat()
        )
        finished_games = [dict(r) for r in finished_games_records]

        # Получаем всех пользователей для имен и ID
        all_uids_records = await db.fetch("SELECT user_id FROM users")
        all_uids = [row['user_id'] for row in all_uids_records]

    return finished_games, all_uids

