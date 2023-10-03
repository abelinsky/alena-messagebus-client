""" Функция отправки сообщения в платформу.

python -c "from alena_messagebus_client.send_message import send; \
send('speak', {'utterance': 'hello'})"

"""
from websocket import create_connection
from .client import MessageBusClient
from .message import Message


def send(message: str, payload: dict = None, config=None) -> None:
    """Отправляет одно сообщение в платформу через веб-сокет.

    Args:
        message (str): Отправляемое сообщение.
        payload (dict, optional): Полезная нагрузка (данные). По умолчанию None.
        config (_type_, optional): _description_. По умолчанию None.
    """
    payload = payload or {}
    config = config or {}

    url = MessageBusClient.build_url(
        config.get("host"), config.get("port"), config.get("route"), config.get("ssl")
    )

    ws = create_connection(url)
    packet = Message(message, payload).serialize()
    ws.send(packet)
    ws.close()
