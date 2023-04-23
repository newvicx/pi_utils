import math
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import dateutil.parser
import pendulum
from dateparser.date import DateDataParser
from pendulum.datetime import DateTime

from pi_utils.types import TimeseriesRow



# Order of parsers is important here. If absolute-time is first, some relative
# formats that we want to support (*-3h) will be interpreted by the absolute-time
# parser and not parsed correctly
DDP = DateDataParser(languages=["en"], settings={"PARSERS": ["relative-time", "absolute-time",]})

LOCAL_TZ = pendulum.now().timezone_name


def isoparse(timestamp: str) -> DateTime:
    """Parse iso8601 string to datetime. The object is timezone aware in UTC."""
    return pendulum.instance(dateutil.parser.isoparse(timestamp))


def fuzzy_parse(timestamp: str) -> DateTime:
    """Fuzzy timestamp parsing. Supports relative dates."""
    parsed = DDP.get_date_data(timestamp)["date_obj"]
    if not parsed:
        raise dateutil.parser.ParserError("Invalid timestamp format %s", timestamp)
    return pendulum.instance(parsed)


def in_timezone(timestamp: str | datetime | DateTime, timezone: str) -> DateTime:
    """Parse iso8601 timestamp or DateTime object to DateTime in specified timezone.
    The object is timezone aware in UTC.
    """
    match timestamp:
        case str():
            return isoparse(timestamp).in_timezone(timezone).replace(tzinfo=None)
        case datetime():
            return pendulum.instance(timestamp).in_timezone(timezone).replace(tzinfo=None)
        case DateTime():
            return timestamp.in_timezone(timezone).replace(tzinfo=None)
        case _:
            raise TypeError(f"Expected str | datetime | DateTime, got {type(timestamp)}")


def split_range_on_interval(
    start_time: datetime,
    end_time: datetime,
    interval: timedelta,
    request_chunk_size: int = 5000
) -> Tuple[List[datetime], List[datetime]]:
    """Split a time range into smaller ranges based on a time interval."""
    td: timedelta = end_time - start_time
    request_time_range = td.total_seconds()
    items_requested = math.ceil(
        request_time_range/interval.total_seconds()
    )
    
    if items_requested <= request_chunk_size:
        return [start_time], [end_time]
    
    dt = timedelta(seconds=math.floor(interval.total_seconds()*request_chunk_size))
    return split_range(start_time, end_time, dt)


def split_range_on_frequency(
    start_time: datetime,
    end_time: datetime,
    request_chunk_size: int = 5000,
    scan_rate: float = 5
) -> Tuple[List[datetime], List[datetime]]:
    """Split a time range into smaller ranges based on the relative update
    frequency of the data.
    """
    td: timedelta = end_time - start_time
    request_time_range = td.total_seconds()
    items_requested = math.ceil(request_time_range/scan_rate)
    
    if items_requested <= request_chunk_size:
        return [start_time], [end_time]
    
    dt = timedelta(seconds=math.floor(request_chunk_size*scan_rate))
    return split_range(start_time, end_time, dt)


def split_range(
    start_time: datetime,
    end_time: datetime,
    dt: timedelta
) -> Tuple[List[datetime], List[datetime]]:
    """Split a time range into smaller ranges."""
    start_times = []
    end_times = []
    
    while start_time < end_time:
        start_times.append(start_time)
        next_timestamp = start_time + dt
        
        if next_timestamp >= end_time:
            start_time = end_time
        
        else:
            start_time = next_timestamp
        end_times.append(start_time)
    
    return start_times, end_times


def get_timestamp_index(data: List[Dict[str, Any] | None]) -> List[str]:
    """Create a single, sorted timestamp index from a chunk of timeseries data
    potentially containing duplicate timestamps.
    
    Duplicate timestamps are removed.
    """
    index = set()
    for datum in data:
        index.update(datum["timestamp"])
    return sorted(index)


def iter_timeseries_rows(
    index: List[str | None],
    data: List[Dict[str, List[Any]] | None],
    timezone: str | None = None
) -> Iterable[TimeseriesRow]:
    """Iterate a collection of timeseries data row by row and produce rows
    which have data aligned on a common timestamp.

    This will also handle timezone conversion. If a timezone is specified, it
    is assumed we are converting from UTC to the timezone. The returned timestamps
    are always timezone unaware.
    
    Note: The data must be in monotonically increasing order for this to work
    correctly.
    """
    for timestamp in index:
        row = []
        for datum in data:
            try:
                if datum["timestamp"][0] == timestamp:
                    row.append(datum["value"].pop(0))
                    datum["timestamp"].pop(0)
                else:
                    # Most recent data point is later than current timestamp
                    row.append(None)
            except IndexError:
                # No more data for that column
                row.append(None)
        if timezone is not None:
            timestamp = in_timezone(timestamp, timezone)
        if isinstance(timestamp, str):
            timestamp = isoparse(timestamp)
        yield timestamp, row