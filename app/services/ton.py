# app/services/ton.py
import asyncio
import re
from datetime import datetime, timezone
from typing import Dict, Set

import aiohttp

from app.config import (
    TON_WALLET_ADDRESS,
    TONAPI_RATES_URL,
    TON_RUB_CACHE_TTL,
    MAIN_ADMIN_ID,
)
from app.db.deposits import add_ton_deposit
from app.services.balances import change_balance, get_balance
from app.utils.formatters import format_rubles
from app.bot import bot


# Кэш курса TON→RUB
_ton_rate_cache: Dict[str, float | datetime] = {
    "value": 0.0,
    "updated": datetime.fromtimestamp(0, tz=timezone.utc),
}

# Список обработанных транзакций
processed_ton_tx: Set[str] = set()


async def get_ton_rub_rate() -> float:
    """Возвращает кэшированный курс TON → RUB."""
    now = datetime.now(timezone.utc)
    cached_value = _ton_rate_cache["value"]
    updated: datetime = _ton_rate_cache["updated"]  # type: ignore

    if cached_value and (now - updated).total_seconds() < TON_RUB_CACHE_TTL:
        return float(cached_value)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(TONAPI_RATES_URL, timeout=10) as resp:
                data = await resp.json()

        rate = float(data["rates"]["TON"]["prices"]["RUB"])
        _ton_rate_cache["value"] = rate
        _ton_rate_cache["updated"] = now
        return rate

    except Exception:
        return float(cached_value or 100.0)


async def ton_deposit_worker():
    """Постоянно опрашивает tonapi и смотрит входящие транзакции на адрес."""
    if not TON_WALLET_ADDRESS:
        print("TON_WALLET_ADDRESS не указан — пополнения отключены.")
        return

    url = (
        f"https://tonapi.io/v2/blockchain/accounts/"
        f"{TON_WALLET_ADDRESS}/transactions?limit=50"
    )

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    data = await resp.json()

            tx_list = data.get("transactions") or data.get("data") or []

            for tx in tx_list:
                tx_hash = tx.get("hash") or tx.get("transaction_id") or ""
                if not tx_hash or tx_hash in processed_ton_tx:
                    continue

                # Комментарий транзакции
                comment = ""
                in_msg = tx.get("in_msg") or tx.get("in_message") or {}
                if isinstance(in_msg, dict):
                    comment = in_msg.get("message") or ""
                    msg_data = in_msg.get("msg_data") or {}
                    if isinstance(msg_data, dict):
                        comment = msg_data.get("text") or comment

                # Должен содержать IDxxxxxxx
                m = re.search(r"ID(\d{5,15})", str(comment))
                if not m:
                    processed_ton_tx.add(tx_hash)
                    continue

                user_id = int(m.group(1))

                # Сумма (nanoton)
                value_nanoton = 0
                if isinstance(in_msg, dict):
                    v = in_msg.get("value")
                    if isinstance(v, str) and v.isdigit():
                        value_nanoton = int(v)
                    elif isinstance(v, int):
                        value_nanoton = v

                if value_nanoton <= 0:
                    processed_ton_tx.add(tx_hash)
                    continue

                ton_amount = value_nanoton / 1e9
                rate = await get_ton_rub_rate()
                coins = int(ton_amount * rate)

                if coins <= 0:
                    processed_ton_tx.add(tx_hash)
                    continue

                # Зачисление ₽
                change_balance(user_id, coins)
                processed_ton_tx.add(tx_hash)

                # Запись в БД
                await add_ton_deposit(tx_hash, user_id, ton_amount, coins, comment)

                # Уведомления
                try:
                    await bot.send_message(
                        user_id,
                        "💎 <b>Пополнение через TON успешно!</b>\n\n"
                        f"Получено: {ton_amount:.4f} TON\n"
                        f"Курс: 1 TON ≈ {rate:.2f} ₽\n"
                        f"Зачислено: {format_rubles(coins)} ₽\n"
                        f"Текущий баланс: {format_rubles(get_balance(user_id))} ₽",
                    )
                except:
                    pass

                try:
                    await bot.send_message(
                        MAIN_ADMIN_ID,
                        "💎 <b>Новое пополнение TON</b>\n"
                        f"User ID: {user_id}\n"
                        f"Комментарий: {comment}\n"
                        f"TON: {ton_amount:.4f}\n"
                        f"₽: {format_rubles(coins)}",
                    )
                except:
                    pass

        except Exception as e:
            print("Ошибка в ton_deposit_worker:", e)

        await asyncio.sleep(20)
