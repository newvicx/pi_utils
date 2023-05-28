import functools
import io
import itertools
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, TextIO

from pi_utils.types import JSONPrimitive
from pi_utils.util.files import write_csv_buffer
from pi_utils.web.client import PIWebClient, get_default_web_client
from pi_utils.web.ops.find import find_tags
from pi_utils.web.ops.interpolated import get_interpolated, get_interpolated_at_time
from pi_utils.web.ops.recorded import get_recorded, get_recorded_at_time


class Resource:
    """A container for mapping common names to PI tags for assets.
    
    A resource is analogous to an element with attributes in asset framework.
    The API wraps web client calls to retrieve data associated to the resource.

    Resource objects should not be created directly, the `new()` method should
    be used instead.

    If a web client is not initialized for the runtime, one will be initialized
    from the environment.
    """

    def __init__(
        self,
        resource_id: str,
        mapping: Dict[str, str],
        data_items: List[str],
        tags: List[str],
        web_ids: List[str],
        dataserver: str | None,
        timezone: str | None,
        retention: int = 15,
    ) -> None:
        self.resource_id = resource_id
        self.mapping = mapping
        self.data_items = data_items
        self.tags = tags
        self.web_ids = web_ids
        self.dataserver = dataserver
        self.timezone = timezone
        self.retention = retention

        self._data: Dict[str, Deque[datetime | JSONPrimitive]] = {
            data_item: deque(maxlen=retention) for data_item in data_items
        }
        self._data.update({"timestamp": deque(maxlen=retention)})
        self._update_lock = threading.Lock()

    @property
    def client(self) -> PIWebClient:
        return self.get_client()

    @staticmethod
    def get_client() -> PIWebClient:
        return get_default_web_client()

    @classmethod
    def new(
        cls,
        resource_id: str,
        mapping: Dict[str, str | None],
        dataserver: str | None = None,
        timezone: str | None = None,
        retention: int = 15
    ) -> "Resource":
        """Create a new resource instance.

        Discovers all WebId for all tags in the mapping and handles ordering of
        data items and tags for operations.

        Args:
            resource_id: The name of the resource.
            mapping: The mapping of {data item: tag} for all data items in the
                resource.
            dataserver: The name of the data archive server. The WebId of the
                archive server will be searched.
            timezone: The timezone to convert the returned data into. Defaults
                to the local system timezone.
            retention: The maximum number of data updates which can be stored on
                the resource.
        """
        client = Resource.get_client()

        index = sorted([(k, v) for k, v in mapping.items() if v is not None])
        index.extend(sorted([(k, v) for k, v in mapping.items() if v is None]))

        mapped, unmapped = find_tags(
            client=client,
            tags=[v for _, v in index if v is not None],
            dataserver=dataserver,
        )

        data_items = []
        tags = []
        web_ids = []

        def map_data_items():
            for i, v in enumerate(index):
                if v[1].upper() == tag:
                    break
            data_items.append(index.pop(i)[0])

        for tag, web_id in mapped:
            tags.append(tag)
            web_ids.append(web_id)
            map_data_items()

        for tag in unmapped:
            tags.append(tag)
            map_data_items()

        # The remaining values in index should be data items where the
        # tag was `None`
        while index:
            data_items.append(index.pop(0)[0])

        return cls(
            resource_id=resource_id,
            mapping=mapping,
            data_items=data_items,
            tags=tags,
            web_ids=web_ids,
            dataserver=dataserver,
            timezone=timezone,
            retention=retention,
        )

    def set_meta(self, meta: Dict[str, Any]) -> None:
        """Set any meta information on the resource as attributes.
        
        This method ensures that no current attributes are overwritten.
        """
        for k, v in meta.items():
            if hasattr(self, k):
                raise AttributeError(f"'{k}' already used.")
            setattr(self, k, v)

    def new_child(self, *items: str) -> "Resource":
        """Returns a new child resource containing only the specified data items.

        Raises:
            KeyError: An item does not exist in the mapping.
        """
        mapping = {item: self.mapping[item] for item in items}

        resource = self.new(
            resource_id=self.resource_id,
            mapping=mapping,
            dataserver=self.dataserver,
            timezone=self.timezone,
            retention=self.retention
        )
        meta_keys = set(self.__dict__.keys()).difference(set(resource.__dict__.keys()))
        meta = {k: self.__dict__[k] for k in meta_keys}
        resource.set_meta(meta=meta)
        return resource

    def new_joined(self, prefix: str, mapping: Dict[str, str | None]) -> "Resource":
        """Return a new resource containing both the old mapping and the joined
        mapping.
        
        Each data item in the joined mapping is prefixed with '{prefix}_', the
        data items on this resource are unchanged.
        """
        mapping = {f"{prefix}_{item}": v for item, v in mapping.items()}
        mapping.update(self.mapping)

        resource = self.new(
            resource_id=self.resource_id,
            mapping=mapping,
            dataserver=self.dataserver,
            timezone=self.timezone,
            retention=self.retention
        )
        meta_keys = set(self.__dict__.keys()).difference(set(resource.__dict__.keys()))
        meta = {k: self.__dict__[k] for k in meta_keys}
        resource.set_meta(meta=meta)
        return resource

    def get_current(self, recorded: bool = False) -> Dict[str, datetime | JSONPrimitive]:
        """Retrieve the current value for each tag in the resource. Current time
        is rounded down to the second.
        """
        now = datetime.now().replace(microsecond=0)
        if recorded:
            getter = get_recorded_at_time
        else:
            getter = get_interpolated_at_time

        timestamp, row = getter(
            client=self.client, web_ids=self.web_ids, time=now, timezone=self.timezone
        )

        current = {"timestamp": timestamp}
        for data_item, value in itertools.zip_longest(self.data_items, row):
            current[data_item] = value

        return current

    def get_last(
        self,
        seconds: float | None = 0,
        minutes: float | None = 0,
        hours: float | None = 0,
        days: float | None = 0,
        recorded: bool = False,
        interval: int | None = None,
        scan_rate: int | None = None,
        max_workers: int = 1,
    ) -> TextIO:
        """Retrieve data for all data items in a time period relative to the
        current time. If no period is specified, defaults to the last 15 minutes.
        """
        if not seconds and not minutes and not hours and not days:
            minutes = 15

        now = datetime.now().replace(microsecond=0)
        start_time = now - timedelta(
            days=days, hours=hours, minutes=minutes, seconds=seconds
        )

        return self.get_range(
            start_time=start_time,
            end_time=now,
            recorded=recorded,
            interval=interval,
            scan_rate=scan_rate,
            max_workers=max_workers,
        )

    def get_range(
        self,
        start_time: datetime,
        end_time: datetime,
        recorded: bool = False,
        interval: int | None = None,
        scan_rate: int | None = None,
        max_workers: int = 1,
    ) -> TextIO:
        """Retrieve data for all data items in a time range."""
        if recorded:
            getter = functools.partial(get_recorded, scan_rate=scan_rate)
        else:
            getter = functools.partial(get_interpolated, interval=interval)

        stream = getter(
            client=self.client,
            web_ids=self.web_ids,
            start_time=start_time,
            end_time=end_time,
            timezone=self.timezone,
            max_workers=max_workers,
        )
        buffer = io.StringIO(newline="")
        pad = len(self.data_items) - len(self.web_ids)
        header = self.data_items.copy()
        header.insert(0, "timestamp")
        write_csv_buffer(buffer=buffer, stream=stream, header=header, pad=pad)

        return buffer

    def update_current(self, recorded: bool = False) -> None:
        """Update the current value in the resource and add to the history."""
        # Lock the update to a single thread so data cannot potentially be
        # inserted out of order
        with self._update_lock:
            current = self.get_current(recorded=recorded)
            for k, v in current.items():
                self._data[k].append(v)

    def __getitem__(self, __k: str) -> Deque[datetime | JSONPrimitive]:
        return self._data[__k]
