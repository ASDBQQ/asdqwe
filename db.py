import os
import asyncpg
import json
from datetime import datetime, timedelta, timezone
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
        
        # 2. Таблица games (ДОБАВЛЕНО: game_type)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                creator_id BIGINT,
                game_type TEXT NOT NULL DEFAULT 'dice', -- ДОБАВЛЕНО: тип игры
                bet_amount INTEGER,
                target_score INTEGER,
                finished INTEGER,
                winner_id BIGINT,
                finished_at TEXT,
                rolls JSONB,
                joiners JSONB  -- Для "Банкира" и других игр с присоединением
            )
        """)
        
        # 3. Таблица raffle_rounds
        await db.execute("""
            CREATE TABLE IF NOT EXISTS raffle_rounds (
                id SERIAL PRIMARY KEY,
                finished INTEGER,
                winner_id BIGINT,
                winning_ticket INTEGER,
                started_at TEXT,
                finished_at TEXT
            )
        """)

        # 4. Таблица raffle_bets
        await db.execute("""
            CREATE TABLE IF NOT EXISTS raffle_bets (
                id SERIAL PRIMARY KEY,
                raffle_id INTEGER REFERENCES raffle_rounds(id),
                user_id BIGINT,
                at TEXT
            )
        """)

        # 5. Таблица ton_deposits
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ton_deposits (
                tx_hash TEXT PRIMARY KEY,
                user_id BIGINT,
                amount_ton REAL,
                amount_rub INTEGER,
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

        # Начальная загрузка данных в память
        if user_balances and user_usernames:
            # Загрузка пользователей
            user_data = []
            for user_id, balance in user_balances.items():
                username = user_usernames.get(user_id, 'Неизвестный')
                registered_at = datetime.now(timezone.utc).isoformat()
                user_data.append((user_id, username, balance, registered_at))
            
            # Добавление пользователей в БД, игнорируя существующие
            await db.executemany("""
                INSERT INTO users (user_id, username, balance, registered_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO NOTHING
            """, user_data)
        
        # Загрузка обработанных TON транзакций
        processed_tx_records = await db.fetch("SELECT tx_hash FROM ton_deposits")
        for record in processed_tx_records:
            processed_ton_tx.add(record['tx_hash'])

# --- ОСНОВНЫЕ ФУНКЦИИ БАЗЫ ДАННЫХ ---

async def upsert_user(user_id: int, username: Optional[str], balance_delta: int) -> Dict[str, Any]:
    """
    Обновляет баланс пользователя. Создает пользователя, если он не существует.
    Возвращает обновленные данные пользователя.
    """
    if not pool:
        return {'user_id': user_id, 'username': username, 'balance': 0}
    
    async with pool.acquire() as db:
        # Проверка существования и создание, если не существует
        registered_at = datetime.now(timezone.utc).isoformat()
        username_to_use = username or 'Неизвестный'

        await db.execute("""
            INSERT INTO users (user_id, username, balance, registered_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE SET 
                username = COALESCE($2, users.username),
                balance = users.balance + $3
        """, user_id, username_to_use, balance_delta, registered_at)

        # Получение обновленных данных
        user_record = await db.fetchrow("SELECT user_id, username, balance FROM users WHERE user_id = $1", user_id)
        
        if user_record:
            return dict(user_record)
        
        # В случае ошибки
        return {'user_id': user_id, 'username': username, 'balance': 0}


async def upsert_game(
    game_id: Optional[int],
    creator_id: int,
    game_type: str, # <-- НОВЫЙ ПАРАМЕТР
    bet_amount: int,
    target_score: int,
    finished: int,
    winner_id: Optional[int] = None,
    rolls: Optional[List[int]] = None,
    joiners: Optional[List[Dict[str, Any]]] = None, # Для Банкира
) -> int:
    """Создает или обновляет игру."""
    if not pool:
        return 0

    rolls_json = json.dumps(rolls) if rolls else '[]'
    joiners_json = json.dumps(joiners) if joiners else '[]'
    
    async with pool.acquire() as db:
        if game_id is None:
            # Создание новой игры
            return await db.fetchval(
                """
                INSERT INTO games (creator_id, game_type, bet_amount, target_score, finished, rolls, joiners)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
                """,
                creator_id,
                game_type, # <-- ИСПОЛЬЗУЕМ game_type
                bet_amount,
                target_score,
                finished,
                rolls_json,
                joiners_json,
            )
        else:
            # Обновление существующей игры
            finished_at = datetime.now(timezone.utc).isoformat() if finished == 1 else None
            await db.execute(
                """
                UPDATE games 
                SET 
                    bet_amount = $1, 
                    target_score = $2, 
                    finished = $3, 
                    winner_id = $4,
                    finished_at = COALESCE($5, finished_at),
                    rolls = $6,
                    joiners = $7
                WHERE id = $8
                """,
                bet_amount,
                target_score,
                finished,
                winner_id,
                finished_at,
                rolls_json,
                joiners_json,
                game_id,
            )
            return game_id


async def get_game(game_id: int) -> Optional[Dict[str, Any]]:
    """Получает данные игры по ID."""
    if not pool: return None
    async with pool.acquire() as db:
        game_record = await db.fetchrow("SELECT * FROM games WHERE id = $1", game_id)
        if game_record:
            game_data = dict(game_record)
            # Десериализация JSONB полей
            game_data['rolls'] = json.loads(game_data['rolls']) if game_data['rolls'] else []
            game_data['joiners'] = json.loads(game_data['joiners']) if game_data['joiners'] else []
            return game_data
        return None

# --- ФУНКЦИИ РЕЙТИНГА ---

async def get_banker_rating_30_days() -> List[Dict[str, Any]]:
    """Получает топ-10 пользователей по чистой прибыли в играх "Банкир" за последние 30 дней."""
    if not pool: return []
    now = datetime.now(timezone.utc)
    delta_30_days = now - timedelta(days=30) 

    async with pool.acquire() as db:
        # SQL-запрос для расчета чистой прибыли Банкира (creator_id)
        # 1. Считаем, сколько Банкир выиграл у joiners: (SUM(bet_amount) у проигравших joiners)
        # 2. Считаем, сколько Банкир проиграл joiners: (-SUM(bet_amount) у выигравших joiners)
        # 3. Прибавляем/вычитаем ставку, которую он сам поставил (game['bet_amount'] уже учтен в боте).
        
        # NOTE: Логика чистой прибыли (profit): 
        # - Если joiner проиграл, Банкир получает +bet_amount.
        # - Если joiner выиграл, Банкир теряет -bet_amount.
        
        # Расчет прибыли Банкира:
        # Прибыль = SUM(ставка_присоединившегося * (1, если проиграл / -1, если выиграл))
        
        # Получаем статистику по creator_id (Банкиру)
        creator_stats_records = await db.fetch("""
            SELECT 
                creator_id AS user_id, 
                SUM(
                    -- Используем JSONB_ARRAY_ELEMENTS для развертывания массива joiners
                    joiner_data->>'bet_amount'::int * CASE 
                        -- Если joiner['won'] == true, Банкир проиграл (-1)
                        WHEN (joiner_data->>'won')::boolean = true THEN -1 
                        -- Если joiner['won'] == false, Банкир выиграл (+1)
                        ELSE 1 
                    END
                ) AS profit
            FROM games, jsonb_array_elements(joiners) AS joiner_data
            WHERE game_type = 'banker' AND finished = 1 AND finished_at >= $1
            GROUP BY creator_id
            ORDER BY profit DESC
            LIMIT 10
        """, delta_30_days.isoformat())
        
        stats = [dict(r) for r in creator_stats_records]
        
        # Получаем никнеймы
        user_ids = [s['user_id'] for s in stats]
        usernames_records = await db.fetch("""
            SELECT user_id, username FROM users WHERE user_id = ANY($1)
        """, user_ids)
        usernames = {r['user_id']: r['username'] for r in usernames_records}
        
        # Форматируем результат
        formatted_stats = []
        for s in stats:
            s['username'] = usernames.get(s['user_id'], 'Неизвестный')
            formatted_stats.append(s)
            
        return formatted_stats

# --- СУЩЕСТВУЮЩИЕ ФУНКЦИИ (ОСТАВЛЕНЫ ДЛЯ КОМПЛЕКТНОСТИ) ---

async def get_user_games(user_id: int) -> List[Dict[str, Any]]:
    # ... (Оставлено без изменений)
    if not pool: return []
    async with pool.acquire() as db:
        records = await db.fetch("SELECT * FROM games WHERE creator_id = $1 ORDER BY id DESC LIMIT 5", user_id)
        return [dict(r) for r in records]

async def get_all_finished_games() -> List[Dict[str, Any]]:
    # ... (Оставлено без изменений)
    if not pool: return []
    async with pool.acquire() as db:
        records = await db.fetch("SELECT * FROM games WHERE finished = 1 ORDER BY id DESC LIMIT 10")
        return [dict(r) for r in records]

async def upsert_raffle_round(raffle_id: Optional[int], finished: int, winner_id: Optional[int] = None, winning_ticket: Optional[int] = None) -> int:
    # ... (Оставлено без изменений)
    if not pool: return 0
    async with pool.acquire() as db:
        if raffle_id is None:
            # Создание нового розыгрыша
            return await db.fetchval(
                "INSERT INTO raffle_rounds (finished, started_at) VALUES ($1, $2) RETURNING id",
                finished, datetime.now(timezone.utc).isoformat()
            )
        else:
            # Обновление существующего
            finished_at = datetime.now(timezone.utc).isoformat() if finished == 1 else None
            await db.execute(
                """
                UPDATE raffle_rounds 
                SET 
                    finished = $1, 
                    winner_id = $2, 
                    winning_ticket = $3,
                    finished_at = COALESCE($4, finished_at)
                WHERE id = $5
                """,
                finished, winner_id, winning_ticket, finished_at, raffle_id
            )
            return raffle_id

async def add_raffle_bet(raffle_id: int, user_id: int):
    # ... (Оставлено без изменений)
    if not pool: return
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO raffle_bets (raffle_id, user_id, at) VALUES ($1, $2, $3)",
            raffle_id, user_id, datetime.now(timezone.utc).isoformat()
        )

async def add_ton_deposit(tx_hash: str, user_id: int, amount_ton: float, amount_rub: int):
    # ... (Оставлено без изменений)
    if not pool: return
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO ton_deposits (tx_hash, user_id, amount_ton, amount_rub, at) VALUES ($1, $2, $3, $4, $5)",
            tx_hash, user_id, amount_ton, amount_rub, datetime.now(timezone.utc).isoformat()
        )

async def add_transfer(from_id: int, to_id: int, amount: int):
    # ... (Оставлено без изменений)
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
    # ... (Оставлено без изменений)
    if not pool: return 0
    async with pool.acquire() as db:
        count = await db.fetchval(
            "SELECT COUNT(*) FROM raffle_bets WHERE raffle_id = $1 AND user_id = $2",
            raffle_id, user_id
        )
        return count if count is not None else 0

async def get_users_profit_and_games_30_days() -> tuple[List[Dict[str, Any]], List[int]]:
    # ... (Оставлено без изменений)
    if not pool: return [], []
    now = datetime.now(timezone.utc)
    delta_30_days = now - timedelta(days=30) 
    
    async with pool.acquire() as db:
        # Получаем все игры за последние 30 дней
        finished_games_records = await db.fetch(
            "SELECT * FROM games WHERE finished = 1 AND finished_at >= $1",
            delta_30_days.isoformat()
        )

        game_data = [dict(r) for r in finished_games_records]
        
        # Получаем список всех user_id
        all_user_ids = set()
        for game in game_data:
            all_user_ids.add(game['creator_id'])
            if game['winner_id']:
                all_user_ids.add(game['winner_id'])

        # Получаем никнеймы
        usernames_records = await db.fetch("""
            SELECT user_id, username FROM users WHERE user_id = ANY($1)
        """, list(all_user_ids))
        usernames = {r['user_id']: r['username'] for r in usernames_records}

        # Расчет прибыли для рейтинга "Кости" (старый рейтинг)
        profit_by_user: Dict[int, int] = {}
        games_by_user: Dict[int, int] = {}
        
        for game in game_data:
            creator_id = game['creator_id']
            winner_id = game['winner_id']
            bet = game['bet_amount']

            games_by_user[creator_id] = games_by_user.get(creator_id, 0) + 1
            
            # Для рейтинга "Кости" считаем только игры 'dice'
            if game.get('game_type', 'dice') != 'dice':
                continue

            if winner_id == creator_id:
                # Победитель получает ставку + чистый выигрыш (ставка - 1% комиссии)
                profit = bet - int(bet * 0.01)
                profit_by_user[creator_id] = profit_by_user.get(creator_id, 0) + profit
            else:
                # Проигравший теряет ставку (она уже списана)
                profit_by_user[creator_id] = profit_by_user.get(creator_id, 0) - bet

        # Форматирование и сортировка для рейтинга "Кости"
        dice_rating = []
        for user_id, profit in profit_by_user.items():
            if games_by_user.get(user_id, 0) > 0:
                dice_rating.append({
                    'user_id': user_id,
                    'username': usernames.get(user_id, 'Неизвестный'),
                    'profit': profit,
                    'games_count': games_by_user.get(user_id, 0)
                })

        dice_rating.sort(key=lambda x: x['profit'], reverse=True)
        
        # Получение ID последних 5 игр
        last_5_games = [g['id'] for g in game_data[:5]]

        return dice_rating[:10], last_5_games


async def get_user_registered_at(user_id: int) -> Optional[str]:
    # ... (Оставлено без изменений)
    if not pool: return None
    async with pool.acquire() as db:
        return await db.fetchval("SELECT registered_at FROM users WHERE user_id = $1", user_id)

async def get_user_dice_games_count(user_id: int) -> int:
    # ... (Оставлено без изменений)
    if not pool: return 0
    async with pool.acquire() as db:
        count = await db.fetchval("SELECT COUNT(*) FROM games WHERE creator_id = $1 AND game_type = 'dice'", user_id)
        return count if count is not None else 0

async def get_user_raffle_bets_count(user_id: int) -> int:
    # ... (Оставлено без изменений)
    if not pool: return 0
    async with pool.acquire() as db:
        count = await db.fetchval("SELECT COUNT(*) FROM raffle_bets WHERE user_id = $1", user_id)
        return count if count is not None else 0
