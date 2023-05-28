import atexit
import concurrent.futures
import functools
import itertools
import logging
import math
import threading
import time
from typing import Callable, Dict, List, Set

from pi_utils.dispatch.resource import Resource
from pi_utils.dispatch.util import RESOURCE_CONTEXT, SHUTDOWN_CONTEXT


_LOGGER = logging.getLogger("pi_utils.dispatch")

class Dispatch:
    """Updates a resource at set intervals and manages task runs.
    
    Tasks are run in separate thread. Only one instance of a task will ever be
    running and task runs do not need to finish before the next update interval.

    Args:
        resource: The resource to update.
        interval: The update interval (in seconds).
        recorded: If `True`, get recorded data on each update. Otherwise, get
            interpolated data.
    """
    def __init__(
        self,
        resource: Resource,
        interval: float,
        recorded: bool = False,
    ) -> None:
        if interval <= 0:
            raise ValueError("Update interval must be greater than 0.")

        self.resource = resource
        self.interval = interval
        self._recorded = recorded

        self._ids = itertools.count()
        self._tasks: Dict[int, Callable[[], None]] = {}
        self._dispatched: Set[int] = set()
        self._futs: Set[concurrent.futures.Future] = set()

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=math.inf)
        self._event_lock = threading.Lock()
        self._task_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._update_event = threading.Event()

        self._started = False
        self._stopped = False

        self._dispatch_thread = threading.Thread(target=self._run_dispatch)
        self._update_thread = threading.Thread(target=self._run_updates)

        atexit.register(self.stop)
    
    def add_task(self, task: Callable[[], None]) -> int:
        """Add a task to dispatch.
        
        This can be called anytime after the dispatch worker has started. Tasks
        added will be dispatched on the next update.
        """
        with self._task_lock:
            id_ = next(self._ids)
            self._tasks[id_] = task
            return id_

    def remove_task(self, id_: int) -> None:
        """Remove all instances of a task from dispatch.
        
        This can be called anytime after the dispatch worker has started. This
        does not stop any current running tasks.
        """
        with self._task_lock:
            self._tasks.pop(id_, None)

    def find_task_id(self, find_task: Callable[[], None], partials: bool = False) -> List[int | None]:
        """Find all ids for a given task.
        
        By default, tasks wrapped by `functools.partial` are not matched. Set,
        `partials=True` to match to the underlying task wrapped by a partial
        object.
        """
        with self._task_lock:
            ids = []
            for id_, task in self._tasks.items():
                if find_task is task:
                    ids.append(id_)
                elif partials and isinstance(task, functools.partial) and task.func is find_task:
                    ids.append(id_)
            return ids

    def start(self) -> None:
        """Start the dispatch worker threads."""
        with self._event_lock:
            if not self._started and not self._stopped:
                self._dispatch_thread.start()
                self._update_thread.start()
                self._started = True
            elif self._stopped:
                raise RuntimeError("Dispatch cannot be started after stopping.")

    def stop(self) -> None:
        """Stop the dispatch worker threads and shutdown the task executor."""
        with self._event_lock:
            if self._started and not self._stopped:
                self._shutdown_event.set()
                self._update_event.set()
                self._update_thread.join()
                self._dispatch_thread.join()
                futs = list(self._futs)
                concurrent.futures.wait(futs)
                self._executor.shutdown()
                self._started = False
                self._stopped = True
    
    def _on_task_complete(self, fut: concurrent.futures.Future) -> None:
        """Callback when task runs complete."""
        with self._task_lock:
            id_ = fut.id_
            assert id_ in self._dispatched
            assert fut in self._futs
            self._dispatched.remove(id_)
            self._futs.remove(fut)
        
        exc = fut.exception()
        if exc is not None:
            _LOGGER.warning(
                "Task run failed: %s",
                self._tasks[id_].__name__,
                exc_info=exc
            )
    
    def _run_updates(self) -> None:
        """Continuously update the resource data and notify the the dispatch
        thread that an update completed.
        """
        while True:
            run_next_at = time.monotonic() + self.interval
            try:
                self.resource.update_current(self._recorded)
            except Exception:
                _LOGGER.warning("Failed to update resource", exc_info=True)
            else:
                # Dispatcher will not run if updates continue to fail
                self._update_event.set()
            
            wait_for = max(0, run_next_at - time.monotonic())
            self._shutdown_event.wait(wait_for)
            if self._shutdown_event.is_set():
                break

    def _run_dispatch(self) -> None:
        """Wait for resource update then dispatch all tasks which are not
        currently running."""
        while True:
            self._update_event.wait()
            self._update_event.clear()
            if self._shutdown_event.is_set():
                break

            with self._task_lock:
                runnable = set(self._tasks.keys()).difference(self._dispatched)
                for id_ in runnable:
                    task = self._tasks[id_]
                    fut = self._executor.submit(self._run_task, task=task)
                    setattr(fut, "id_", id_)
                    fut.add_done_callback(self._on_task_complete)
                    self._dispatched.add(id_)
                    self._futs.add(fut)
    
    def _run_task(self, task: Callable[[], None]) -> None:
        """Set the context for the task run and run the task."""
        rsrc_tkn = RESOURCE_CONTEXT.set(self.resource)
        shtdn_tkn = SHUTDOWN_CONTEXT.set(self._shutdown_event)
        try:
            task()
        finally:
            RESOURCE_CONTEXT.reset(rsrc_tkn)
            SHUTDOWN_CONTEXT.reset(shtdn_tkn)