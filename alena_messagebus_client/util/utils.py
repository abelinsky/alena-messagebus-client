"""
Утилиты для работы с шиной.
"""
import json
import logging
from typing import Callable


def create_echo_function(name: str) -> Callable[[str], None]:
    """Стандартный механизм логирования на платформе.

    Args:
        name (str): Имя процесса.

    Returns:
        func: Функция для логирования.
    """
    log = logging.getLogger(name)

    def echo(message: str) -> None:
        try:
            msg = json.loads(message)
            msg_type = msg.get("type", "")
            if msg_type == "registration":
                msg["data"]["token"] = None
                message = json.dumps(msg)
        except Exception as exc:
            log.info("Error: %s", repr(exc), exc_info=True)

        log.info("MESSAGEBUS: %s", repr(message))

    return echo
