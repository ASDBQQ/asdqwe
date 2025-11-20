import aiosqlite
from datetime import datetime, timezone

DB_PATH = "bot.db"


async def init_db(user_balances, user_usernames, processed_ton_tx):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                balance INTEGER,
                registered_at TEXT  -- Добавлено поле для даты регистрации
            )
        """)
        # ... (остальные CREATE TABLE остаются без изменений) ...
        await db.execute("""
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY,
                creator_id INTEGER,
                opponent_id INTEGER,
                bet INTEGER,
                creator_roll INTEGER,
                opponent_roll INTEGER,
                winner TEXT,
                finished INTEGER,
                created_at TEXT,
                finished_at TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS raffle_rounds (
                id INTEGER PRIMARY KEY,
                created_at TEXT,
                finished_at TEXT,
                winner_id INTEGER,
                total_bank INTEGER
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS raffle_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raffle_id INTEGER,
                user_id INTEGER,
                amount INTEGER
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS ton_deposits (
                tx_hash TEXT PRIMARY KEY,
                user_id INTEGER,
                ton_amount REAL,
                coins INTEGER,
                comment TEXT,
                at TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_id INTEGER,
                to_id INTEGER,
                amount INTEGER,
                at TEXT
            )
        """)

        await db.commit()

        # загрузка пользователей
        async with db.execute("SELECT user_id, username, balance FROM users") as cur:
            for uid, username, balance in await cur.fetchall():
                user_balances[uid] = balance
                user_usernames[uid] = username

        # загрузка обработанных транзакций TON
        async with db.execute("SELECT tx_hash FROM ton_deposits") as cur:
            for (tx_hash,) in await cur.fetchall():
                processed_ton_tx.add(tx_hash)


# ... (upsert_user обновлён для работы с registered_at) ...
async def upsert_user(uid, username, balance, registered_at=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, balance, registered_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                balance=excluded.balance
        """, (
            uid,
            username,
            balance,
            registered_at.isoformat() if registered_at else datetime.now(timezone.utc).isoformat() # Вставляем дату при первой записи
        ))
        await db.commit()


# 🔥 НОВАЯ ФУНКЦИЯ: для получения даты регистрации
async def get_user_registered_at(uid: int) -> datetime | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT registered_at FROM users WHERE user_id = ?",
            (uid,)
        ) as cur:
            row = await cur.fetchone()
            if row and row[0]:
                try:
                    return datetime.fromisoformat(row[0])
                except ValueError:
                    return None
            return None

# ... (остальные функции остаются без изменений, кроме той, что в конце) ...
async def upsert_game(g):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO games (
                id, creator_id, opponent_id, bet,
                creator_roll, opponent_roll, winner,
                finished, created_at, finished_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                creator_id=excluded.creator_id,
                opponent_id=excluded.opponent_id,
                bet=excluded.bet,
                creator_roll=excluded.creator_roll,
                opponent_roll=excluded.opponent_roll,
                winner=excluded.winner,
                finished=excluded.finished,
                created_at=excluded.created_at,
                finished_at=excluded.finished_at
        """, (
            g["id"],
            g["creator_id"],
            g["opponent_id"],
            g["bet"],
            g.get("creator_roll"),
            g.get("opponent_roll"),
            g.get("winner"),
            int(g.get("finished", False)),
            g["created_at"].isoformat() if g.get("created_at") else None,
            g["finished_at"].isoformat() if g.get("finished_at") else None,
        ))
        await db.commit()


async def get_user_games(uid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT * FROM games
            WHERE (creator_id = ? OR opponent_id = ?) AND finished = 1
            ORDER BY finished_at DESC
        """, (uid, uid)) as cur:
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in await cur.fetchall()]


async def get_all_finished_games():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM games WHERE finished = 1") as cur:
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in await cur.fetchall()]

# 🔥 НОВАЯ ФУНКЦИЯ: для подсчёта игр в кости (пользователь участвовал)
async def get_user_dice_games_count(uid: int, finished_only: bool = True) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        query = """
            SELECT COUNT(*) FROM games
            WHERE (creator_id = ? OR opponent_id = ?)
        """
        params = [uid, uid]
        if finished_only:
            query += " AND finished = 1"
        
        async with db.execute(query, params) as cur:
            count = (await cur.fetchone())[0]
            return count

# 🔥 НОВАЯ ФУНКЦИЯ: для подсчёта ставок в банкире (пользователь участвовал)
async def get_user_raffle_bets_count(uid: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(DISTINCT raffle_id) FROM raffle_bets WHERE user_id = ?",
            (uid,)
        ) as cur:
            count = (await cur.fetchone())[0]
            return count

async def upsert_raffle_round(r):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO raffle_rounds (id, created_at, finished_at, winner_id, total_bank)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                created_at=excluded.created_at,
                finished_at=excluded.finished_at,
                winner_id=excluded.winner_id,
                total_bank=excluded.total_bank
        """, (
            r["id"],
            r["created_at"].isoformat() if r.get("created_at") else None,
            r["finished_at"].isoformat() if r.get("finished_at") else None,
            r.get("winner_id"),
            r.get("total_bank", 0),
        ))
        await db.commit()


async def add_raffle_bet(raffle_id, user_id, amount):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO raffle_bets (raffle_id, user_id, amount)
            VALUES (?, ?, ?)
        """, (raffle_id, user_id, amount))
        await db.commit()


async def add_ton_deposit(tx_hash, user_id, ton_amount, coins, comment):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO ton_deposits (tx_hash, user_id, ton_amount, coins, comment, at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            tx_hash,
            user_id,
            ton_amount,
            coins,
            comment,
            datetime.now(timezone.utc).isoformat()
        ))
        await db.commit()


async def add_transfer(from_id, to_id, amount):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO transfers (from_id, to_id, amount, at)
            VALUES (?, ?, ?, ?)
        """, (
            from_id,
            to_id,
            amount,
            datetime.now(timezone.utc).isoformat()
        ))
        await db.commit()


# 🔥 ИЗМЕНЕНИЕ: Добавлен необязательный параметр max_bets_per_raffle
async def get_user_bets_in_raffle(raffle_id, user_id) -> int:
    """Возвращает количество ставок, сделанных пользователем в текущем раунде розыгрыша."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM raffle_bets WHERE raffle_id = ? AND user_id = ?",
            (raffle_id, user_id)
        ) as cur:
            return (await cur.fetchone())[0]

# 🔥 НОВАЯ ФУНКЦИЯ: для получения всех пользователей с их профитом за 30 дней (для рейтинга)
async def get_users_profit_and_games_30_days():
    now = datetime.now(timezone.utc)
    delta_30_days = now - timedelta(days=30)
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Получаем все игры за последние 30 дней
        async with db.execute(
            "SELECT * FROM games WHERE finished = 1 AND finished_at >= ?",
            (delta_30_days.isoformat(),)
        ) as cur:
            cols = [c[0] for c in cur.description]
            finished_games = [dict(zip(cols, row)) for row in await cur.fetchall()]

        # Получаем всех пользователей для имен и ID
        async with db.execute("SELECT user_id FROM users") as cur:
            all_uids = [row[0] for row in await cur.fetchall()]

    return finished_games, all_uids
