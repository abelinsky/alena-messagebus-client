"""Сообщение платформы виртуального ассистента.

Объект `Сообщение` является базовой конструкцией платформенной шины.
Он содержит методы для отслеживания контекста сообщения,
а также методы для сериализации / десериализации сообщения при его передаче.

"""
import inspect
import json
from copy import deepcopy
from typing import Optional


class Message:
    """Содержит данные, пересылаемые в платформенной шине между сервисами.

    Attributes:
      message_type (str): тип данных, пересылаемых в сообщении.
      payload (dict): полезная информация, пересылаемая в сообщении.
      context (dict): данные, не входящие в полезную нагрузку, например,
      информация об отправителе, получателе, предметной области и др.

    """

    def __init__(
        self, message_type: str, payload: dict = None, context: dict = None
    ) -> None:
        self.message_type = message_type
        self.payload = payload or {}
        self.context = context or {}

    def serialize(self) -> str:
        """Сериализует сообщение.

        Используется для отправки  через веб-сокет. Использует
        json для формирования строки-сообщения с типом, данными и контекстом.

        Returns:
            str: сообщение в формате json.
        """
        return json.dumps(
            {
                "type": self.message_type,
                "payload": self.payload,
                "context": self.context,
            }
        )

    @staticmethod
    def deserialize(value: str):
        """Формирует объект `Message` из строки.

        Предназначен для создания объекта из строки, полученной из веб-сокета.

        Args:
            value(str): json-строка.

        Returns:
            Message: объект `Message`
        """
        obj = json.loads(value)
        return Message(
            obj.get("type") or "",
            obj.get("payload") or {},
            obj.get("context") or {},
        )

    def forward(self, message_type: str, payload: dict | None = None):
        """Создает новый объект с аналогичным контекстом.

        Args:
            message_type (str): тип нового сообщений
            payload (dict): данные нового сообщения

        Returns:
            Message: Новый объект `Message`
        """
        payload = payload or {}
        return Message(message_type, payload, context=self.context)

    def reply(
        self, message_type: str, payload: dict = None, context: dict = None
    ):
        """Формирует ответное сообщение.

        Args:
            message_type (str): тип сообщения.
            payload (dict): полезная нагрузка сообщения.
            context (dict): контекст нового сообщения.

        Returns:
            Message: Объект `Message` - ответ на сообщение.
        """

        payload = deepcopy(payload) or {}
        context = context or {}

        new_context = deepcopy(self.context)
        for key in context:
            new_context[key] = context[key]
        if "destination" in payload:
            new_context["destination"] = payload["destination"]
        if "source" in new_context and "destination" in new_context:
            s = new_context["destination"]
            new_context["destination"] = new_context["source"]
            new_context["source"] = s
        return Message(message_type, payload, context=new_context)

    def response(
        self, payload: dict | None = None, context: dict | None = None
    ):
        return self.reply(self.message_type + ".response", payload, context)

    def publish(self, message_type: str, payload: dict, context=None):
        context = context or {}
        new_context = self.context.copy()
        for key in context:
            new_context[key] = context[key]

        if "target" in new_context:
            del new_context["target"]

        return Message(message_type, payload, context=new_context)


def dig_for_message(max_records: int = 10) -> Optional[Message]:
    """
    Рассматривает стек сообщений. В текущем стеке ищет сообщение.

    Args:
        max_records (int): максимальное число записей для поиска

    Returns:
        Message, если найдена в аргументах, иначе None
    """
    stack = inspect.stack()[1:]  # на первом месте будет сама эта функция
    stack = stack if len(stack) <= max_records else stack[:max_records]
    for record in stack:
        args = inspect.getargvalues(record.frame)
        if args.args:
            for arg in args.args:
                if isinstance(args.locals[arg], Message):
                    return args.locals[arg]
    return None


class CollectionMessage(Message):
    """Расширение класса сообщение с обработчиком сборщиков."""

    def __init__(
        self, message_type, handler_id, query_id, payload=None, context=None
    ):
        super().__init__(message_type, payload, context)
        self.handler_id = handler_id
        self.query_id = query_id

    @classmethod
    def from_message(cls, message, handler_id, query_id):
        """Формирует CollectionMessage из Message.

        Args:
            message (Message): исходное сообщение
            handler_id (str): handler_id получателя
            query_id (str): идентификатор сессии

        Returns:
            CollectionMessage на основе исходного сообщения
        """
        return cls(
            message.message_type,
            handler_id,
            query_id,
            message.payload,
            message.context,
        )

    def success(
        self, payload: dict | None = None, context: dict | None = None
    ) -> Message:
        """Создает сообщение с индикацией успешной операции.

        Args:
            payload (dict): данные
            context (dict): контекст

        Returns:
            Message
        """
        payload = payload or {}
        payload["query"] = self.query_id
        payload["handler"] = self.handler_id
        payload["succeeded"] = True
        response_message = self.reply(
            self.message_type + ".response", payload, context or self.context
        )
        return response_message

    def failure(self):
        """Создает сообщение с индикацией неуспешной операции.

        Args:
            payload (dict): данные
            context (dict): контекст

        Returns:
            Message
        """
        payload = {}
        payload["query"] = self.query_id
        payload["handler"] = self.handler_id
        payload["succeeded"] = False
        response_message = self.reply(
            self.message_type + ".response", payload, self.context
        )
        return response_message

    def extend(self, timeout):
        """Расширяет текущий таймаут.

        Args:
            timeout (int/float): расширение таймаута

        Returns:
            расширенное сообщение
        """
        payload = {}
        payload["query"] = self.query_id
        payload["handler"] = self.handler_id
        payload["timeout"] = timeout
        response_message = self.reply(
            self.message_type + ".handling", payload, self.context
        )
        return response_message
