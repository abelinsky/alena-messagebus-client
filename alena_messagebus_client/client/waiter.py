from threading import Event


class MessageWaiter:
    """Механизм ожидания одного сообщения.

    Arguments:
        message_bus: Шина, от которой ожидаются сообщения.
        message_type: Тип ожидаемого сообщения.
    """

    def __init__(self, message_bus, message_type):
        self.message_bus = message_bus
        self.msg_type = message_type
        self.received_msg = None
        self.response_event = Event()
        self.message_bus.once(message_type, self._handler)

    def _handler(self, message):
        """Обработчик полученного сообщения"""
        self.received_msg = message
        self.response_event.set()

    def wait(self, timeout=3.0):
        """Ожидает сообщение.

        Args:
            timeout (int or float): время ожидания, сек

        Returns:
            Message or None
        """
        self.response_event.wait(timeout)
        if not self.response_event.is_set():
            # Очистка
            try:
                self.message_bus.remove(self.msg_type, self._handler)
            except (ValueError, KeyError):
                # ValueError occurs on pyee 5.0.1 removing handlers
                # registered with once.
                # KeyError may theoretically occur if the event occurs as
                # the handler is removed
                pass
        return self.received_msg
