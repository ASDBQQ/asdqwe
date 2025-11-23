# app/main.py
import asyncio

from app.bot import bot, dp
from app.services.balances import user_balances, user_usernames
from app.services.ton import processed_ton_tx
from app.db.pool import init_db

# Хендлеры просто импортируются, они сами регистрируются внутри dp
import app.handlers.start
import app.handlers.games_menu
import app.handlers.balance
import app.handlers.admin
import app.handlers.profile
import app.handlers.text


async def main():
    # ❗ ВОТ ТАК ДОЛЖНО БЫТЬ
    await init_db(user_balances, user_usernames, processed_ton_tx)

    print("🚀 Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


