# app/main.py
import asyncio

from app.bot import bot, dp
from app.db.pool import init_db
from app.services.balances import user_balances, user_usernames
from app.services.ton import processed_ton_tx


# ⚠️ Хендлеры просто импортируем — при импорте они сами регистрируются
import app.handlers.start
import app.handlers.games_menu
import app.handlers.balance
import app.handlers.admin
import app.handlers.profile
import app.handlers.text


async def main():
    print("📦 Initializing database...")
    
    # Правильный вызов инициализации БД
    await init_db(
        user_balances=user_balances,
        user_usernames=user_usernames,
        processed_ton_tx=processed_ton_tx,
    )

    print("🚀 Бот запущен!")
    # Запускаем пуллинг
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())






