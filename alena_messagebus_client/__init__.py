"""Alena MessageBus Client.

Клиент платформы виртуального ассистента.

ПОзволяет зарегистрировать обработчики конкретных типов сообщений, поступающих из платформы виртуального ассистента, а также отправлять сообщения другим клиентам (сервисам), подключенным к платформе.
"""

from .client.client import MessageBusClient
from .message import Message
from .send_message import send
from .conf import client_from_config

__all__ = [MessageBusClient, Message, send, client_from_config]
