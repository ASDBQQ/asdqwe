# app/services/state_reset.py

"""
Сбрасывает временные состояния пользователя:
- перевод (target / amount)
- вывод средств
- ставки игр
"""

from app.services.balances import (
    pending_transfer_step,
    temp_transfer,
)

from app.handlers.balance import (
    pending_withdraw_step,
    temp_withdraw,
)

from app.services.games import (
    pending_bet_input,
)

from app.services.raffle import (
    pending_raffle_bet_input,
)


def reset_user_state(uid: int):
    """Полная очистка временных состояний пользователя."""

    # --- переводы ---
    pending_transfer_step.pop(uid, None)
    temp_transfer.pop(uid, None)

    # --- вывод ---
    pending_withdraw_step.pop(uid, None)
    temp_withdraw.pop(uid, None)

    # --- игры (ставка) ---
    pending_bet_input.pop(uid, None)

    # --- банкир ---
    pending_raffle_bet_input.pop(uid, None)

