import atexit
import concurrent.futures
import itertools
import threading
from collections.abc import Iterable, Sequence
from typing import Callable, Dict, Set

from pi_utils.web.resource import Resource


class Dispatch:
    def __init__(
        self,
        resource: Resource,
        interval: float,
        tasks: Sequence[Callable[[Resource, threading.Event], None]] | None = None,
    ) -> None:
        self.resource = resource
        self.interval = interval

        self._dispatch: Dict[int, Callable[[Resource, threading.Event], None]] = {}
        self._ids: Iterable[int] = itertools.count()
        
        tasks = tasks or []
        for task in tasks:
            self.add_task(task)

        self._executor = concurrent.futures.ThreadPoolExecutor()
        self._running: Set[concurrent.futures.Future] = set()
        self._shutdown_event = threading.Event()
        self._update_event = threading.Event()
    
    def add_task(self, task: Callable[[Resource, threading.Event], None]) -> None:
        _id = next(self._ids)
        self._dispatch[_id] = task


