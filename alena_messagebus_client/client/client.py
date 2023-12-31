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
    Работает подобно `pyee` `EventEmitter`, но добавляет сервисы
    для разработчика.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8181,
        route: str = "/core",
        ssl: bool = False,
        emitter: ExecutorEventEmitter | None = None,
    ):
        self.config = MessageBusClientConf(host, port, route, ssl)
        self.emitter = emitter or ExecutorEventEmitter()
        self.client = self.create_client()
        self.retry = 5
        self.connected_event = Event()
        self.started_running = False
        self.wrapped_funcs = {}

    @staticmethod
    def build_url(host: str, port: int, route: str, ssl: bool) -> str:
        """Формирует url для веб-сокета."""
        return "{scheme}://{host}:{port}{route}".format(
            scheme="wss" if ssl else "ws",
            host=host,
            port=str(port),
            route=route,
        )

    def create_client(self) -> WebSocketApp:
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

    def on_open(self, *args) -> None:
        """Обрабатывает событие "open" от сокета."""
        LOG.info("Подключен")
        self.connected_event.set()
        self.emitter.emit("open")
        self.retry = 5

    def on_close(self, *args) -> None:
        """Обрабатывает событие "close" от вебсокета."""
        self.emitter.emit("close")

    def on_error(self, *args) -> None:
        """При ошибке пробует подключиться заново."""
        if len(args) == 1:
            error = args[0]
        else:
            error = args[1]
        if isinstance(error, WebSocketConnectionClosedException):
            LOG.warning(
                "Невозможно отправить сообщение, т.к. соединение закрыто"
            )
        elif isinstance(error, ConnectionRefusedError):
            LOG.warning("Соединение отклонено. Платформенная шина запущена?")
        else:
            LOG.exception("=== %s ===", repr(error))

        try:
            self.emitter.emit("error", error)
            if self.client.keep_running:
                self.client.close()
        except Exception as e:
            LOG.error(
                f"Exception при закрытии сокета на {self.client.url}: {e}"
            )

        LOG.warning(
            "Попытка соединения с шиной будет повторена через "
            f"{self.retry:.1f} секунд.",
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
                    "You must execute run_forever() before emitting messages"
                )
            self.connected_event.wait()

        try:
            if hasattr(message, "serialize"):
                self.client.send(message.serialize())
            else:
                self.client.send(
                    json.dumps(message.__dict__, ensure_ascii=False)
                )
        except WebSocketConnectionClosedException:
            LOG.warning(
                f"Не удалось отправить {message.message_type} сообщение, "
                "потому что соединение закрыто"
            )

    def collect_responses(
        self,
        message: Message,
        min_timeout: float = 0.2,
        max_timeout: float = 3.0,
        direct_return_func: callable = lambda msg: False,
    ):
        """Собирает ответы от нескольких обработчиков.

        Args:
            message (Message): Сообщение.
            min_timeout (int/float): Минимальное время ожидания ответа.
            max_timeout (int/float): Максимальное время ожидания ответа.
            direct_return_func (callable): колбэк

            Returns:
                (list) собранные ответы.
        """
        collector = MessageCollector(
            self, message, min_timeout, max_timeout, direct_return_func
        )
        return collector.collect()

    def on_collect(self, event_name: str, func: callable, timeout: float = 2):
        """Создает обработчик.

        Args:
            event_name (str): Тип сообщения для прослушкию
            func (callable): Метод для обработки сообщения.
            timeout (int/float): Таймаут для обработчика.
        """

        def wrapper(msg: Message):
            collect_id = msg.context["__collect_id__"]
            handler_id = str(uuid4())
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

    def wait_for_message(self, message_type: str, timeout: float = 3.0):
        """Ожидает сообщение конкретного типа.

        Arguments:
            message_type (str): Тип ожидаемого сообщения.
            timeout: время ожидания, сек.

        Returns:
            Полученное сообщение или None при истечении времени ожидания.
        """

        return MessageWaiter(self, message_type).wait(timeout)

    def wait_for_response(
        self,
        message: Message,
        reply_type: str | None = None,
        timeout: float = 3.0,
    ):
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
        waiter = MessageWaiter(self, message_type)
        self.emit(message)
        return waiter.wait(timeout)

    def on(self, event_name: str, func: callable):
        """Регистрирует колбэк с `event emitter`.

        Args:
            event_name (str): Тип сообщения.
            func (callable): callback.
        """
        self.emitter.on(event_name, func)

    def once(self, event_name: str, func: callable):
        """Регистрирует колбэк с event emitter для разового вызова.

        Args:
            event_name (str): Тип сообщения.
            func (callable): callback
        """
        self.emitter.once(event_name, func)

    def remove(self, event_name: str, func: callable):
        """Удаляет зарегистрированное сообщение.

        Args:
            event_name (str): Тип сообщения.
            func (callable): callback
        """
        if func in self.wrapped_funcs:
            self._remove_wrapped(event_name, func)
        else:
            self._remove_normal(event_name, func)

    def _remove_wrapped(self, event_name: str, external_func: callable):
        wrapper = self.wrapped_funcs.pop(external_func)
        self._remove_normal(event_name, wrapper)

    def _remove_normal(self, event_name: str, func: callable):
        try:
            if event_name not in self.emitter._events:
                LOG.debug("Not able to find '%s'", event_name)
            self.emitter.remove_listener(event_name, func)
        except ValueError:
            LOG.warning(
                "Не удалось удалить " f"событие {event_name}: {str(func)}"
            )
            for line in traceback.format_stack():
                LOG.warning(line.strip())

            if event_name not in self.emitter._events:
                LOG.debug("Не удалось найти '%s'", event_name)
            LOG.warning("Существующие события: %s", repr(self.emitter._events))
            for evt in self.emitter._events:
                LOG.warning("   %s", repr(evt))
                LOG.warning("       %s", repr(self.emitter._events[evt]))
            if event_name in self.emitter._events:
                LOG.debug("При удалении '%s'", event_name)
            else:
                LOG.debug("Не удалось найти '%s'", event_name)
            LOG.warning("----- Завершение дампа -----")

    def remove_all_listeners(self, event_name: str):
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

    def repeat_utterance(message: Message):
        message.message_type = "speak"
        message_bus_client.emit(message)

    message_bus_client.on("message", create_echo_function(None))
    message_bus_client.on("recognizer_loop:utterance", repeat_utterance)
    message_bus_client.run_forever()


if __name__ == "__main__":
    echo()
