from queue import Queue
from threading import Lock, Event
from uuid import uuid4
import time


class MessageCollector:
    """Инструмент для сбора множественных ответов.

    Класс инкапсулирует логику сбора сообщений от
    нескольких обработчиков, возвращающих список всех ответов.

    Args:
        message_bus: Шина, от которой ожидаются сообщения.
        message (Message): Сообщения.
        min_timeout (int/float): Минимальное время ожидания ответа.
        max_timeout (int/float): Максимальное время ожидания ответа.
        direct_return_func (callable): Опциональная функция для раннего ответа.
    """

    def __init__(
        self, message_bus, message, min_timeout, max_timeout, direct_return_func=None
    ):
        self.lock = Lock()
        self.message_bus = message_bus
        self.min_timeout = min_timeout
        self.max_timeout = max_timeout
        self.direct_return_func = direct_return_func or (lambda msg: False)
        self.collect_id = str(uuid4())
        self.handlers = {}
        self.responses = {}
        self.all_collected = Event()
        self.message = message
        self.message.context["__collect_id__"] = self.collect_id
        self._start_time = 0

        self.on_response_callback = None
        self.queue = Queue()

    def __iter__(self):
        return self

    def __next__(self):
        msg = Queue.get()
        if msg is not None:
            return msg
        else:
            raise StopIteration

    def on_response(self, callback_func):
        self.on_response_callback = callback_func

    def _register_handler(self, msg):
        handler_id = msg.data["handler"]
        timeout = msg.data["timeout"]
        with self.lock:
            if msg.data["query"] == self.collect_id and handler_id not in self.handlers:
                previous_timeout = self.handlers.get(handler_id, 0)
                self.handlers[handler_id] = previous_timeout + timeout

    def _receive_response(self, msg):
        """Обработчик ответа.

        Args:
            msg: Сообщение.
        """
        with self.lock:
            if msg.data["query"] == self.collect_id:
                self.queue.put(msg)
                self.responses[msg.data["handler"]] = msg
                self.handlers[msg.data["handler"]] = 0  # Reset timeout
                # If all registered handlers have responded with an answer
                # or a VERY good answer has been found indicate end of wait.
                all_collected = len(self.responses) == len(self.handlers)
                if all_collected or self.direct_return_func(msg):
                    self.queue.put(None)
                    self.all_collected.set()

        if self.on_response_callback:
            self.on_response_callback(msg)

    def _setup_collection_handlers(self):
        base_message_type = self.message.message_type
        self.bus.on(base_message_type + ".handling", self._register_handler)
        self.bus.on(base_message_type + ".response", self._receive_response)

    def _teardown_collection_handlers(self):
        base_message_type = self.message.message_type
        self.bus.remove(base_message_type + ".handling", self._register_handler)
        self.bus.remove(base_message_type + ".response", self._receive_response)

    def start(self):
        self._setup_collection_handlers()
        self.bus.emit(self.message)
        self.start_time = time.monotonic()

        time.sleep(self.min_timeout)

    def collect(self):
        self.start()
        if len(self.handlers) == 0:
            # No handlers has registered to answer the query
            result = []
        else:
            result = self._wait_for_registered_handlers()

        self.shutdown()
        return result

    def wait(self):
        self._wait_for_registered_handlers()

    def _wait_for_registered_handlers(self):
        with self.lock:
            all_collected = len(self.responses) == len(self.handlers)
            if not all_collected:
                self.all_collected.clear()

        time_waited = self.min_timeout
        remaining_timeout = max(self.handlers.values()) - time_waited
        while remaining_timeout > 0.0 and time_waited < self.max_timeout:
            if self.all_collected.wait(timeout=0.1):
                break

            time_waited += 0.1
            remaining_timeout = max(self.handlers.values()) - time_waited

        self.queue.put(None)
        return [self.responses[key] for key in self.responses]

    def shutdown(self):
        self._teardown_collection_handlers()
        if self.on_response_callback:
            self.on_response_callback = None
