import functools
import io
import itertools
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, TextIO

from pi_utils.types import JSONPrimitive
from pi_utils.util.files import write_csv_buffer
from pi_utils.web.client import PIWebClient
from pi_utils.web.ops.find import find_tags
from pi_utils.web.ops.interpolated import get_interpolated, get_interpolated_at_time
from pi_utils.web.ops.recorded import get_recorded, get_recorded_at_time


class Resource:
    """A container for mapping common names to PI tags for assets."""

    def __init__(
        self,
        client: PIWebClient,
        mapping: Dict[str, str],
        data_items: List[str],
        tags: List[str],
        web_ids: List[str],
        dataserver: str | None,
        timezone: str | None,
        retention: int = 15,
    ) -> None:
        self.client = client
        self.mapping = mapping
        self.data_items = data_items
        self.tags = tags
        self.web_ids = web_ids
        self.dataserver = dataserver
        self.timezone = timezone

        self._data: Dict[str, Deque[datetime | JSONPrimitive]] = {
            data_item: deque(maxlen=retention) for data_item in data_items
        }
        self._data.update({"timestamp": deque(maxlen=retention)})

    @classmethod
    def new(
        cls,
        client: PIWebClient,
        mapping: Dict[str, str],
        dataserver: str | None = None,
        timezone: str | None = None,
        retention: int = 15,
    ) -> "Resource":
        """Create a new resource instance.

        Discovers all WebId for all tags in the mapping and handles ordering of
        data items and tags for operations.

        Args:
            client: The PIWebClient used to execute queries.
            mapping: The mapping of {data item: tag} for all data items in the
                resource.
            dataserver: The name of the data archive server. The WebId of the
                archive server will be searched.
            timezone: The timezone to convert the returned data into. Defaults
                to the local system timezone.
            retention: The maximum number of data updates which can be stored on
                the resource.
        """
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
            client=client,
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

    def child(self, *items: str) -> "Resource":
        """Returns a child resource containing only the specified data items.

        Raises:
            KeyError: An item does not exist in the mapping.
        """
        mapping = {item: self.mapping[item] for item in items}

        return self.new(
            client=self.client,
            mapping=mapping,
            dataserver=self.dataserver,
            timezone=self.timezone,
        )

    def join(self, prefix: str, mapping: Dict[str, str]) -> "Resource":
        """Join the mapping of the current resource with a different mapping.
        Each data item in the joined mapping is prefixed with '{prefix}_'.
        """
        mapping = {f"{prefix}_{item}": v for item, v in mapping.items()}
        mapping.update(self.mapping)
        return self.new(
            client=self.client,
            mapping=mapping,
            dataserver=self.dataserver,
            timezone=self.timezone
        )

    def current(self, recorded: bool = False) -> Dict[str, datetime | JSONPrimitive]:
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

    def last(
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

        return self.range(
            start_time=start_time,
            end_time=now,
            recorded=recorded,
            interval=interval,
            scan_rate=scan_rate,
            max_workers=max_workers,
        )

    def range(
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

    def update(self, recorded: bool = False) -> None:
        """Update the current value in the resource and add to the history."""
        current = self.current(recorded=recorded)
        for k, v in current.items():
            self._data[k].append(v)

    def __getitem__(self, __k: str) -> Deque[datetime | JSONPrimitive]:
        return self._data[__k]
