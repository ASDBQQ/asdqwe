import aiosqlite
from datetime import datetime, timezone
from typing import Dict, Set, List, Any

UTC = timezone.utc

DB_PATH = "database.db"


async def init_db(
    user_balances: Dict[int, int],
    user_usernames: Dict[int, str],
    processed_ton_tx: Set[str],
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(
            """
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                balance INTEGER NOT NULL DEFAULT 0,
                reg_date TEXT
            );

            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY,
                creator_id INTEGER,
                opponent_id INTEGER,
                bet INTEGER,
                creator_roll INTEGER,
                opponent_roll INTEGER,
                winner TEXT,
                finished INTEGER NOT NULL DEFAULT 0,
                created_at TEXT,
                finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS raffle_rounds (
                id INTEGER PRIMARY KEY,
                created_at TEXT,
                finished_at TEXT,
                winner_id INTEGER,
                total_bank INTEGER
            );

            CREATE TABLE IF NOT EXISTS raffle_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id INTEGER,
                user_id INTEGER,
                amount INTEGER
            );

            CREATE TABLE IF NOT EXISTS ton_deposits (
                tx_hash TEXT PRIMARY KEY,
                user_id INTEGER,
                ton_amount REAL,
                coins_amount INTEGER,
                comment TEXT,
                timestamp TEXT
            );

            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_user INTEGER,
                to_user INTEGER,
                amount INTEGER,
                timestamp TEXT
            );
            """
        )
        await db.commit()

        async with db.execute("SELECT id, username, balance FROM users") as cur:
            rows = await cur.fetchall()
            for uid, uname, bal in rows:
                user_balances[int(uid)] = int(bal)
                if uname:
                    user_usernames[int(uid)] = uname

        async with db.execute("SELECT tx_hash FROM ton_deposits") as cur:
            rows = await cur.fetchall()
            for (tx_hash,) in rows:
                processed_ton_tx.add(tx_hash)


async def upsert_user(user_id: int, username: str | None, balance: int):
    reg_date = datetime.now(UTC).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (id, username, balance, reg_date)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                username = excluded.username,
                balance = excluded.balance
            """,
            (user_id, username, balance, reg_date),
        )
        await db.commit()


async def upsert_game(game: Dict[str, Any]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO games (
                id, creator_id, opponent_id, bet,
                creator_roll, opponent_roll, winner,
                finished, created_at, finished_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                creator_id = excluded.creator_id,
                opponent_id = excluded.opponent_id,
                bet = excluded.bet,
                creator_roll = excluded.creator_roll,
                opponent_roll = excluded.opponent_roll,
                winner = excluded.winner,
                finished = excluded.finished,
                created_at = excluded.created_at,
                finished_at = excluded.finished_at
            """,
            (
                game.get("id"),
                game.get("creator_id"),
                game.get("opponent_id"),
                game.get("bet"),
                game.get("creator_roll"),
                game.get("opponent_roll"),
                game.get("winner"),
                1 if game.get("finished") else 0,
                game.get("created_at").isoformat() if game.get("created_at") else None,
                game.get("finished_at").isoformat() if game.get("finished_at") else None,
            ),
        )
        await db.commit()


async def get_user_games(uid: int) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT *
            FROM games
            WHERE finished = 1
              AND (creator_id = ? OR opponent_id = ?)
            ORDER BY datetime(finished_at) DESC
            """,
            (uid, uid),
        ) as cur:
            rows = await cur.fetchall()
            return [dict(row) for row in rows]


async def get_all_finished_games() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT * FROM games WHERE finished = 1"""
        ) as cur:
            return [dict(row) for row in await cur.fetchall()]


async def upsert_raffle_round(raffle_round: Dict[str, Any]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO raffle_rounds (id, created_at, finished_at, winner_id, total_bank)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                created_at = excluded.created_at,
                finished_at = excluded.finished_at,
                winner_id = excluded.winner_id,
                total_bank = excluded.total_bank
            """,
            (
                raffle_round.get("id"),
                raffle_round.get("created_at").isoformat()
                if raffle_round.get("created_at") else None,
                raffle_round.get("finished_at").isoformat()
                if raffle_round.get("finished_at") else None,
                raffle_round.get("winner_id"),
                raffle_round.get("total_bank"),
            ),
        )
        await db.commit()
