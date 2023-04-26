import concurrent.futures
import logging
import threading
from collections.abc import Iterable
from types import TracebackType
from typing import Type

from pydantic import ValidationError
from websockets.exceptions import ConnectionClosedOK
from websockets.sync.connection import Connection

from pi_utils.web.channels.messages import Buffer
from pi_utils.web.channels.models import ChannelMessage


_LOGGER = logging.getLogger("pi_utils.web.channel")


class Subscriber(Iterable[ChannelMessage]):
    """Iterable interface for consuming from multiple websocket connections
    simultaneously.
    """

    def __init__(self, *connections: Connection, maxsize: int | None = None) -> None:
        self._connections = connections
        self._buffer = Buffer(maxsize=maxsize)
        self.exception: BaseException = None
        self._consumer = threading.Thread(target=self._consume)
        self._consumer.start()

    def close(self) -> None:
        """Close all connections and close the buffer."""
        try:
            self._close()
        finally:
            self._consumer.join()

    def _close(self) -> None:
        """Close all connections and close the buffer."""
        try:
            for connection in self._connections:
                connection.close()
        finally:
            self._buffer.close()

    def _consume(self) -> None:
        """Consume messages from all connections concurrently."""
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                consumers = [
                    executor.submit(self._consume_from, connection)
                    for connection in self._connections
                ]
                for consumer in consumers:
                    consumer.add_done_callback(self._connection_lost)
                concurrent.futures.wait(
                    consumers, return_when=concurrent.futures.FIRST_COMPLETED
                )
        finally:
            self._close()

    def _consume_from(self, connection: Connection) -> None:
        """Consume from a single connection and buffer the messages."""
        with connection:
            try:
                for message in connection:
                    try:
                        self._buffer.put(ChannelMessage.parse_raw(message))
                    except ValidationError as e:
                        _LOGGER.warning(
                            "Message validation failed, discarding message",
                            extra={"message": message, "errors": e.json()},
                        )
                    except EOFError:
                        _LOGGER.debug("EOF received on buffer, closing connection")
                        return
            except ConnectionClosedOK:
                _LOGGER.debug("Connection closed by client")
                return

    def _connection_lost(self, fut: concurrent.futures.Future) -> None:
        """Callback after a connection is lost. If the connection raised a
        `ConnectionClosedError`, it is set as the exception on the subscriber.
        """
        exc = fut.exception()
        if exc is not None and self.exception is None:
            self.exception = exc

    def __iter__(self) -> Iterable[ChannelMessage]:
        while True:
            try:
                yield self._buffer.get()
            except EOFError as e:
                if self.exception is not None:
                    raise self.exception from e
                break

    def __enter__(self) -> "Subscriber":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        traceback: TracebackType,
    ) -> None:
        self.close()
