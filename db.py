import aiosqlite
from datetime import datetime, timezone

DB_PATH = "bot.db"


async def init_db(user_balances, user_usernames, processed_ton_tx):
    async with aiosqlite.connect(DB_PATH) as db:
        # --- USERS ---
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                balance INTEGER,
                reg_date TEXT
            )
        """)

        # Проверяем, есть ли колонка reg_date (на случай старой БД без неё)
        try:
            async with db.execute("PRAGMA table_info(users)") as cur:
                cols = await cur.fetchall()
            col_names = {c[1] for c in cols}
            if "reg_date" not in col_names:
                # Добавляем колонку, если её не было
                await db.execute("ALTER TABLE users ADD COLUMN reg_date TEXT")
        except Exception:
            # Если что-то пошло не так — просто продолжаем, бот всё равно будет работать
            pass

        # --- GAMES (кости) ---
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

        # --- RAFFLE ROUNDS (банкир / розыгрыши) ---
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

        # --- TON DEPOSITS ---
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

        # --- TRANSFERS ---
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

        # загрузка пользователей (reg_date сейчас не нужен в памяти)
        async with db.execute("SELECT user_id, username, balance FROM users") as cur:
            for uid, username, balance in await cur.fetchall():
                user_balances[uid] = balance
                user_usernames[uid] = username

        # загрузка обработанных транзакций TON
        async with db.execute("SELECT tx_hash FROM ton_deposits") as cur:
            for (tx_hash,) in await cur.fetchall():
                processed_ton_tx.add(tx_hash)


async def upsert_user(uid, username, balance):
    """
    Обновляем/создаём пользователя.
    - При первом появлении пользователя записываем дату регистрации reg_date.
    - При последующих обновлениях reg_date не меняется.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        now = datetime.now(timezone.utc).isoformat()

        # 1) Пытаемся вставить пользователя, если его ещё нет
        await db.execute("""
            INSERT OR IGNORE INTO users (user_id, username, balance, reg_date)
            VALUES (?, ?, ?, ?)
        """, (uid, username, balance, now))

        # 2) В любом случае обновляем username и balance
        await db.execute("""
            UPDATE users
            SET username = ?, balance = ?
            WHERE user_id = ?
        """, (username, balance, uid))

        await db.commit()


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

