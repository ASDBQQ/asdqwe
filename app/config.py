# app/config.py
import os
from datetime import timedelta

# ⚠️ URL базы данных PostgreSQL
# Railway обычно создаёт переменную "DATABASE_URL" или "PGDATABASE_URL".
# Если у тебя именно PGDATABASE_URL — замени на os.getenv("PGDATABASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("PGDATABASE_URL")


# --- Telegram BOT ---
BOT_TOKEN = "8329084308:AAGsDacIge3lu_O5PyPjKGepwHp4i_jBnWA"

# --- TON ---
TON_WALLET_ADDRESS = "UQCzzlkNLsCGqHTUj1zkD_3CVBMoXw-9Od3dRKGgHaBxysYe"
TONAPI_RATES_URL = "https://tonapi.io/v2/rates?tokens=ton&currencies=rub"
TON_RUB_CACHE_TTL = 60  # секунд

# --- Баланс ---
START_BALANCE_COINS = 0

# --- История игр ---
HISTORY_LIMIT = 30
HISTORY_PAGE_SIZE = 10

# --- Кости ---
GAME_CANCEL_TTL_SECONDS = 60
DICE_MIN_BET = 10
DICE_BET_MIN_CANCEL_AGE = timedelta(minutes=1)

# --- Банкир ---
RAFFLE_TIMER_SECONDS = 60
RAFFLE_MIN_BET = 10
RAFFLE_MAX_BETS_PER_ROUND = 10
RAFFLE_CANCEL_WINDOW_SECONDS = 600  # 10 минут

# --- Админы ---
MAIN_ADMIN_ID = 7106398341
ADMIN_IDS = {MAIN_ADMIN_ID, 783924834}
