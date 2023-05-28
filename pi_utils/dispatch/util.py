import threading
from contextvars import ContextVar

from pi_utils.dispatch.resource import Resource


RESOURCE_CONTEXT: ContextVar[Resource] = ContextVar("_dispatch_resource_context", default=None)
SHUTDOWN_CONTEXT: ContextVar[threading.Event] = ContextVar("_dispatch_shutdown_context", default=None)

def get_resource() -> Resource:
    """Retrieve the current resource in context.
    
    Called by a task started with dispatch.
    """
    resource = RESOURCE_CONTEXT.get()
    if resource is None:
        raise RuntimeError(
            f"'{get_resource.__name__}' cannot be called outside scope of task run."
        )
    return resource


def wait_for_shutdown(timeout: float | None = None) -> None:
    """Wait until the shutdown event is set on the dispatch or until a timeout
    occurs.
    
    Behaves the same as the `.wait()` method on an event. It can only be called
    by a task run started with dispatch.
    """
    event = SHUTDOWN_CONTEXT.get()
    if event is None:
        raise RuntimeError(
            f"'{wait_for_shutdown.__name__}' cannot be called outside scope of task run."
        )
    event.wait(timeout=timeout)


def shutting_down() -> bool:
    """Check if the dispatch is shutting down.
    
    Behaves the same as the `.is_set()` method on an event. It can only be called
    by a task run started with dispatch.
    """
    event = SHUTDOWN_CONTEXT.get()
    if event is None:
        raise RuntimeError(
            f"'{shutting_down.__name__}' cannot be called outside scope of task run."
        )
    return event.is_set()