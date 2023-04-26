import concurrent.futures
from collections.abc import Iterable
from datetime import datetime, timedelta
from functools import partial
from typing import List

from pi_utils.types import TimeseriesRow
from pi_utils.util.time import (
    get_timestamp_index,
    iter_timeseries_rows,
    split_range_on_interval,
    LOCAL_TZ
)
from pi_utils.web.client import PIWebClient
from pi_utils.web.util import (
    format_streams_content,
    handle_request,
    handle_response
)



def get_interpolated(
    client: PIWebClient,
    web_ids: List[str],
    start_time: datetime,
    end_time: datetime | None = None,
    interval: timedelta | int | None = None,
    request_chunk_size: int | None = None,
    timezone: str | None = None
) -> Iterable[TimeseriesRow]:
    """Stream timestamp aligned, interpolated data for a sequence of PI tags.

    Args:
        client: The PIWebClient used to retrieve the data.
        web_ids: The web_ids to stream data for.
        start_time: The start time of the batch. This will be the timestamp
            in the first row of data.
        end_time: The end time of the batch. This will be the timestamp in the
            last row. Defaults to now.
        interval: The time interval (in seconds) between successive rows. Defaults
            to 60.
        request_chunk_size: The maximum number of rows to be returned from a
            single HTTP request. This splits up the time range into successive
            pieces. Defaults to 5000.
        timezone: The timezone to convert the returned data into. Defaults to
            the local system timezone.

    Yields:
        row: A `TimeseriesRow`.

    Raises:
        ValueError: If `start_time` >= `end_time`.
        TypeError: If `interval` is an invalid type. 
        ClientError: Error in `aiohttp.ClientSession`.
    """
    end_time = end_time or datetime.now()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")
    
    interval = interval or 60
    interval = timedelta(seconds=interval) if isinstance(interval, int) else interval
    if not isinstance(interval, timedelta):
        raise TypeError(f"Interval must be timedelta or int. Got {type(interval)}")

    timezone = timezone or LOCAL_TZ
    request_chunk_size = request_chunk_size or 5000
    
    start_times, end_times = split_range_on_interval(
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        request_chunk_size=request_chunk_size
    )
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for start_time, end_time in zip(start_times, end_times):
            futs = [
                executor.submit(
                    handle_request,
                    partial(
                        client.streams.get_interpolated,
                        web_id,
                        startTime=start_time,
                        endTime=end_time,
                        timeZone=timezone,
                        interval=f"{interval.total_seconds()} seconds",
                        selectedFields="Items.Timestamp;Items.Value;Items.Good"
                    ),
                    raise_for_status=False
                ) for web_id in web_ids
            ]
            concurrent.futures.wait(futs)
            
            results = [
                handle_response(
                    fut.result(),
                    raise_for_status=False,
                    raise_for_content_error=False
                ) for fut in futs
            ]
            
            data = [format_streams_content(result) for result in results]
            index = get_timestamp_index(data)

            for row in iter_timeseries_rows(index=index, data=data, timezone=timezone):
                yield row