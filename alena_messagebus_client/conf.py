"""Функции для обработки конфирураций."""


import json

from .client import MessageBusClient


def client_from_config(
    subconf: str = "core", file_path: str = "/etc/alena/messagebus.conf"
) -> MessageBusClient:
    """Загружает настройки из файла.

    Файл с настройками представляет собой json.

    Пример:
    {
      "core": {
        "route": "/core",
        "port": "8181"
      }
      "gui": {
        "route": "/gui",
        "port": "8811"
      }
    }

    Args:
        subconf:    выбираемая конфигурация, по умолчанию "core"
        file_path:  путь к файлу с настройкамию

    Returns:
        MessageBusClient инстанс на основе настроек.
    """
    with open(file_path) as f:
        conf = json.load(f)

    return MessageBusClient(**conf[subconf])
