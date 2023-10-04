"""
Клиент (расширенное соединение websocket), использующий стандартный формат
сообщения и автоматически выполняющий сериализацию/десериализацию сообщений.
"""
from collections import namedtuple
import json
import logging
import time
import traceback
from threading import Event, Thread
from uuid import uuid4

from pyee import ExecutorEventEmitter
from websocket import (
    WebSocketApp,
    WebSocketConnectionClosedException,
    WebSocketException,
)

from alena_messagebus_client.message import Message, CollectionMessage
from alena_messagebus_client.util import create_echo_function

from .collector import MessageCollector
from .waiter import MessageWaiter

LOG = logging.getLogger(__name__)

MessageBusClientConf = namedtuple(
    "MessageBusClientConf", ["host", "port", "route", "ssl"]
)


class MessageBusClient:
    """Клиент платформы виртуального ассистента.

    Подключается к платформенной шине и интегрируется с системой.
    Работает подобно `pyee` `EventEmitter`, но добавялет сервисы для разработчика.
    """

    def __init__(
        self, host="0.0.0.0", port=8181, route="/core", ssl=False, emitter=None
    ):
        self.config = MessageBusClientConf(host, port, route, ssl)
        self.emitter = emitter or ExecutorEventEmitter()
        self.client = self.create_client()
        self.retry = 5
        self.connected_event = Event()
        self.started_running = False
        self.wrapped_funcs = {}

    @staticmethod
    def build_url(host, port, route, ssl):
        """Формирует url для веб-сокета."""
        return "{scheme}://{host}:{port}{route}".format(
            scheme="wss" if ssl else "ws",
            host=host,
            port=str(port),
            route=route,
        )

    def create_client(self):
        """Создает клиента веб-сокета."""
        url = MessageBusClient.build_url(
            ssl=self.config.ssl,
            host=self.config.host,
            port=self.config.port,
            route=self.config.route,
        )
        return WebSocketApp(
            url,
            on_open=self.on_open,
            on_close=self.on_close,
            on_error=self.on_error,
            on_message=self.on_message,
        )

    def on_open(self, *args):
        """Обрабатывает событие "open" от сокета."""
        LOG.info("Connected")
        self.connected_event.set()
        self.emitter.emit("open")
        self.retry = 5

    def on_close(self, *args):
        """Обрабатывает событие "close" от вебсокета."""
        self.emitter.emit("close")

    def on_error(self, *args):
        """При ошибке пробует подключиться заново."""
        if len(args) == 1:
            error = args[0]
        else:
            error = args[1]
        if isinstance(error, WebSocketConnectionClosedException):
            LOG.warning("Could not send message because connection has closed")
        elif isinstance(error, ConnectionRefusedError):
            LOG.warning("Connection Refused. Is Messagebus Service running?")
        else:
            LOG.exception("=== %s ===", repr(error))

        try:
            self.emitter.emit("error", error)
            if self.client.keep_running:
                self.client.close()
        except Exception as e:
            LOG.error(f"Exception closing websocket at {self.client.url}: {e}")

        LOG.warning(
            "Message Bus Client " "will reconnect in %.1f seconds.", self.retry
        )
        time.sleep(self.retry)
        self.retry = min(self.retry * 2, 60)
        try:
            self.emitter.emit("reconnecting")
            self.client = self.create_client()
            self.run_forever()
        except WebSocketException:
            pass

    def on_message(self, *args):
        """Обрабатывает входящее сообщение.

        Args:
            message (str): сериализованное сообщение платформы.
        """
        if len(args) == 1:
            message = args[0]
        else:
            message = args[1]
        parsed_message = Message.deserialize(message)
        self.emitter.emit("message", message)
        self.emitter.emit(parsed_message.message_type, parsed_message)

    def emit(self, message: Message) -> None:
        """Отправляет сообщение в шину.

        Отправляет сообщение в локальный процесс с помощью event emitter и в
        вебсокет для других процессов (сервисов).

        Args:
            message (Message): Сообщение.
        """
        if not self.connected_event.wait(10):
            if not self.started_running:
                raise ValueError(
                    "You must execute run_forever() " "before emitting messages"
                )
            self.connected_event.wait()

        try:
            if hasattr(message, "serialize"):
                self.client.send(message.serialize())
            else:
                self.client.send(json.dumps(message.__dict__))
        except WebSocketConnectionClosedException:
            LOG.warning(
                "Could not send %s message because connection "
                "has been closed",
                message.message_type,
            )

    def collect_responses(
        self,
        message,
        min_timeout=0.2,
        max_timeout=3.0,
        direct_return_func=lambda msg: False,
    ):
        """Собирает ответы от несколькиз обработчиков.

        Args:
            message (Message): Сообщение.
            min_timeout (int/float): Минимальное время ожидания ответа.
            max_timeout (int/float): Максимальное время ожидания ответа.
            direct_return_func (callable): Optional function for allowing an
                early return (not all registered handlers need to respond)

            Returns:
                (list) собранные ответы.
        """
        collector = MessageCollector(
            self, message, min_timeout, max_timeout, direct_return_func
        )
        return collector.collect()

    def on_collect(self, event_name, func, timeout=2):
        """Создает обработчик.

        Args:
            event_name (str): Тип сообщения для прослушкию
            func (callable): Метод для обработки сообщения.
            timeout (int/float): Таймаут для обработчика.
        """

        def wrapper(msg):
            collect_id = msg.context["__collect_id__"]
            handler_id = str(uuid4())
            # Immediately respond that something is working on the issue
            acknowledge = msg.reply(
                msg.message_type + ".handling",
                data={
                    "query": collect_id,
                    "handler": handler_id,
                    "timeout": timeout,
                },
            )
            self.emit(acknowledge)
            func(CollectionMessage.from_message(msg, handler_id, collect_id))

        self.wrapped_funcs[func] = wrapper
        self.on(event_name, wrapper)

    def wait_for_message(self, message_type, timeout=3.0):
        """Ожидает сообщение конкретного типа.

        Arguments:
            message_type (str): Тип ожидаемого сообщения.
            timeout: время ожидания, сек.

        Returns:
            Полученное сообщение или None при истечении времени ожидания.
        """

        return MessageWaiter(self, message_type).wait(timeout)

    def wait_for_response(self, message, reply_type=None, timeout=3.0):
        """Отправляет сообщение м ждет ответюа

        Arguments:
            message (Message): Отправляемое сообщение.
            reply_type (str): Ожидаемый тип ответного сообщения.
                              Defaults to "<message.message_type>.response".
            timeout: Время ожидания, сек.

        Returns:
            Полученное сообщение или None при истечении времени ожидания.
        """
        message_type = reply_type or message.message_type + ".response"
        waiter = MessageWaiter(self, message_type)  # Setup response handler
        # Send message and wait for it's response
        self.emit(message)
        return waiter.wait(timeout)

    def on(self, event_name, func):
        """Регистрирует колбэк с `event emitter`.

        Args:
            event_name (str): Тип сообщения.
            func (callable): callback.
        """
        self.emitter.on(event_name, func)

    def once(self, event_name, func):
        """Регистрирует колбэк с event emitter для разового вызова.

        Args:
            event_name (str): Тип сообщения.
            func (callable): callback
        """
        self.emitter.once(event_name, func)

    def remove(self, event_name, func):
        """Удаляет зарегистрированное сообщение.

        Args:
            event_name (str): Тип сообщения.
            func (callable): callback
        """
        if func in self.wrapped_funcs:
            self._remove_wrapped(event_name, func)
        else:
            self._remove_normal(event_name, func)

    def _remove_wrapped(self, event_name, external_func):
        wrapper = self.wrapped_funcs.pop(external_func)
        self._remove_normal(event_name, wrapper)

    def _remove_normal(self, event_name, func):
        try:
            if event_name not in self.emitter._events:
                LOG.debug("Not able to find '%s'", event_name)
            self.emitter.remove_listener(event_name, func)
        except ValueError:
            LOG.warning("Failed to remove event %s: %s", event_name, str(func))
            for line in traceback.format_stack():
                LOG.warning(line.strip())

            if event_name not in self.emitter._events:
                LOG.debug("Not able to find '%s'", event_name)
            LOG.warning("Existing events: %s", repr(self.emitter._events))
            for evt in self.emitter._events:
                LOG.warning("   %s", repr(evt))
                LOG.warning("       %s", repr(self.emitter._events[evt]))
            if event_name in self.emitter._events:
                LOG.debug("Removing found '%s'", event_name)
            else:
                LOG.debug("Not able to find '%s'", event_name)
            LOG.warning("----- End dump -----")

    def remove_all_listeners(self, event_name):
        """Удаляет всех прослушивателей event_name.

        Arguments:
            event_name: Событие, для которого удаляется прослушивание.
        """
        if event_name is None:
            raise ValueError
        self.emitter.remove_all_listeners(event_name)

    def run_forever(self):
        """Стартует обработку сокета."""
        self.started_running = True
        self.client.run_forever()

    def close(self):
        """Закрывает соединение с сокетом."""
        self.client.close()
        self.connected_event.clear()

    def run_in_thread(self):
        """Запускается и выполняется в демоне."""
        t = Thread(target=self.run_forever)
        t.daemon = True
        t.start()
        return t


def echo():
    message_bus_client = MessageBusClient()

    def repeat_utterance(message):
        message.message_type = "speak"
        message_bus_client.emit(message)

    message_bus_client.on("message", create_echo_function(None))
    message_bus_client.on("recognizer_loop:utterance", repeat_utterance)
    message_bus_client.run_forever()


if __name__ == "__main__":
    echo()
