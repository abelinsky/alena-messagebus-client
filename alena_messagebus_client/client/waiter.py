from threading import Event


class MessageWaiter:
    """Механизм ожидания одного сообщения.

    Arguments:
        message_bus: Шина, от которой ожидаются сообщения.
        message_type: Тип ожидаемого сообщения.
    """

    def __init__(self, message_bus, message_type: str):
        self.message_bus = message_bus
        self.message_type = message_type
        self.received_msg = None
        self.response_event = Event()
        self.message_bus.once(message_type, self._handler)

    def _handler(self, message):
        """Обработчик полученного сообщения"""
        self.received_msg = message
        self.response_event.set()

    def wait(self, timeout: float = 3.0):
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
                self.message_bus.remove(self.message_type, self._handler)
            except (ValueError, KeyError):
                pass
        return self.received_msg
