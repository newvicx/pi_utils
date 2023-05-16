import threading
from collections import deque
from typing import Deque

from pi_utils.web.channels.models import ChannelMessage


MAX_BUFFER = 200


class Buffer:
    """Message interface for a subscriber. Modeled after
    `websockets.sync.messages.Assembler`
    """

    def __init__(self, maxsize: int | None = None) -> None:
        self.maxsize = maxsize or MAX_BUFFER
        # Serialize reads and writes -- except for reads via synchronization
        # primitives provided by the threading and queue modules.
        self.mutex = threading.Lock()

        # We create a latch with two events to ensure proper interleaving of
        # writing and reading messages.
        # put() sets this event to tell get() that a message can be fetched.
        self.message_received = threading.Event()
        # This allows us to control how many messages can be buffered and
        # notifies a thread waiting to put() that it can enqueue its message
        self.message_fetched = threading.Condition()

        # This flag prevents concurrent calls to get() by user code.
        self.get_in_progress = False

        # Defining a queue size prevents infinite buffering and allows
        # backpressure control to work down to the TCP level.
        self.message_queue: Deque[ChannelMessage] = deque()

        # This flag marks the end of the stream.
        self.closed = False

    def get(self, timeout: float | None = None) -> ChannelMessage:
        """Read the next message.

        Args:
            timeout: If a timeout is provided and elapses before a complete
                message is received, raises `TimeoutError`.
        Raises:
            EOFError: If at least one of the streams has ended.
            RuntimeError: If two threads run `get` concurrently.
        """
        with self.mutex:
            try:
                message = self.message_queue.popleft()
            except IndexError:
                # No messages, lets wait
                pass
            else:
                # We may need to notify waiting threads that they can put a message
                with self.message_fetched:
                    self.message_fetched.notify(1)
                self.message_received.clear()
                return message

            if self.closed:
                raise EOFError("stream ended")

            if self.get_in_progress:
                raise RuntimeError("get is already running")

            self.get_in_progress = True

        # If the message_received event isn't set yet, release the lock to
        # allow put() to run and eventually set it.
        # Locking with get_in_progress ensures only one thread can get here.
        completed = self.message_received.wait(timeout)

        with self.mutex:
            self.get_in_progress = False

            # Waiting for a complete message timed out.
            if not completed:
                raise TimeoutError(f"timed out in {timeout:.1f}s")

            # get() was unblocked by close() rather than put().
            if self.closed:
                raise EOFError("stream ended")

            assert self.message_received.is_set()
            self.message_received.clear()

            # This is not expected to raise an error
            message = self.message_queue.popleft()

            return message

    def put(self, message: ChannelMessage) -> None:
        """Add a message to the buffer.

        This blocks if the buffer is full and allows backpressure control in
        upstream systems to operate correctly but the block can be interrupted
        by closing the buffer.

        Raises:
            EOFError: If the stream has ended.
        """
        with self.mutex:
            if self.closed:
                raise EOFError("stream ended")

            if len(self.message_queue) < self.maxsize:
                # Notify get we are buffering a message.
                self.message_received.set()
                return self.message_queue.append(message)

        # The buffer is full...
        # Release the lock to allow get() to run and eventually notify us to
        # enqueue a message
        with self.message_fetched:
            self.message_fetched.wait()

        with self.mutex:
            if self.closed:
                # put() was unblocked by close() rather than get()
                raise EOFError("stream ended")

            # Otherwise it was unlocked by get() and we should be able to enqueue
            # a message
            assert len(self.message_queue) < self.maxsize
            # Notify get we are buffering a message.
            self.message_received.set()
            self.message_queue.append(message)

    def close(self) -> None:
        """End the stream of messages.

        Callling `close` concurrently with `get` or `put` is safe. They will
        raise `EOFError`.
        """
        with self.mutex:
            if self.closed:
                return

            self.closed = True

            # Unblock get.
            if self.get_in_progress:
                self.message_received.set()

            with self.message_fetched:
                self.message_fetched.notify_all()
