import atexit
import concurrent.futures
import functools
import itertools
import logging
import math
import time
import threading
from collections.abc import Iterable, Sequence
from contextvars import ContextVar
from typing import Callable, Dict, Set

from pi_utils.web.resource import Resource


_resource_context = ContextVar("_resource_context")
_shutdown_context = ContextVar("_shutdown_context")

_LOGGER = logging.getLogger("pi_utils.web.dispatch")

class Dispatch:
    def __init__(
        self,
        resource: Resource,
        interval: float,
        tasks: Sequence[Callable[[Resource, threading.Event], None]] | None = None,
        recorded: bool = False,
    ) -> None:
        self.resource = resource
        self.interval = interval
        self._recorded = recorded

        self._dispatch: Dict[int, Callable[[Resource, threading.Event], None]] = {}
        self._ids: Iterable[int] = itertools.count()
        
        tasks = tasks or []
        for task in tasks:
            self.add_task(task)

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=math.inf)
        self._running_futs: Set[concurrent.futures.Future] = set()
        self._running_ids: Set[int] = set()
        self._shutdown_event = threading.Event()
        self._update_event = threading.Event()
        self._lock = threading.Lock()
        self._update_thread = threading.Thread(target=self._run_updates)
        self._dispatch_thread = threading.Thread(target=self._run_dispatch)
    
    def add_task(self, task: Callable[[Resource, threading.Event], None]) -> None:
        with self._lock:
            _id = next(self._ids)
            self._dispatch[_id] = task

    def start(self) -> None:
        pass

    def _run_updates(self) -> None:
        while True:
            run_next_at = time.monotonic() + self.interval
            try:
                self.resource.update(self._recorded)
            except Exception:
                _LOGGER.warning("Failed to update resource", exc_info=True)
            else:
                self._update_event.set()
            
            wait_for = max(0, run_next_at - time.monotonic())
            self._shutdown_event.wait(wait_for)
            if self._shutdown_event.is_set():
                break
    
    def _run_dispatch(self) -> None:
        while True:
            self._update_event.wait()
            self._update_event.clear()
            if self._shutdown_event.is_set():
                break
            with self._lock:
                runnable = set(self._dispatch.keys()).difference(self._running_ids)
                for _id in runnable:
                    task = self._dispatch[_id]
                    fut = self._executor.submit(self._run_task, task=task)
                    fut.add_done_callback(functools.partial(self._on_complete, _id))
                    self._running_ids.add(_id)
                    self._running_futs.add(fut)

    def _on_complete(self, _id: int, fut: concurrent.futures.Future) -> None:
        with self._lock:
            assert _id in self._running_ids
            assert fut in self._running_futs
            self._running_ids.remove(_id)
            self._running_futs.remove(fut)

            e = fut.exception()
            if e is not None:
                _LOGGER.warning("Task run failed", exc_info=e)
    
    def _run_task(self, task: Callable[[], None]) -> None:
        resource_token = _resource_context.set(self.resource)
        shutdown_token = _shutdown_context.set(self._shutdown_event)
        try:
            task()
        finally:
            _resource_context.reset(resource_token)
            _shutdown_context.reset(shutdown_token)