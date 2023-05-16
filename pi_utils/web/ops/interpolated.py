import concurrent.futures
import functools
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import List

from pi_utils.types import TimeseriesRow
from pi_utils.util.time import (
    get_timestamp_index,
    iter_timeseries_rows,
    split_range_on_interval,
    LOCAL_TZ,
)
from pi_utils.web.client import PIWebClient
from pi_utils.web.util import format_streams_content, handle_request, handle_response


def get_interpolated(
    client: PIWebClient,
    web_ids: List[str],
    start_time: datetime,
    end_time: datetime | None = None,
    interval: timedelta | int | None = None,
    request_chunk_size: int | None = None,
    timezone: str | None = None,
    max_workers: int = 6
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
        max_workers: The maximum number of concurrent threads to make requests.

    Yields:
        row: A `TimeseriesRow`.

    Raises:
        ValueError: If `start_time` >= `end_time`.
        TypeError: If `interval` is an invalid type.
        RequestException: There was an ambiguous exception that occurred while
            handling the request.
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
        request_chunk_size=request_chunk_size,
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for start_time, end_time in zip(start_times, end_times):
            futs = [
                executor.submit(
                    handle_request,
                    functools.partial(
                        client.streams.get_interpolated,
                        web_id,
                        startTime=start_time,
                        endTime=end_time,
                        timeZone=timezone,
                        interval=f"{interval.total_seconds()} seconds",
                        selectedFields="Items.Timestamp;Items.Value;Items.Good",
                    ),
                    raise_for_status=False,
                )
                for web_id in web_ids
            ]
            concurrent.futures.wait(futs)

            results = [
                handle_response(
                    fut.result(), raise_for_status=False, raise_for_content_error=False
                )
                for fut in futs
            ]

            data = [format_streams_content(result) for result in results]
            index = get_timestamp_index(data)

            for row in iter_timeseries_rows(index=index, data=data, timezone=timezone):
                yield row


def get_interpolated_at_time(
    client: PIWebClient, web_ids: List[str], time: datetime, timezone: str | None = None
) -> TimeseriesRow:
    """Returns the interpolated value for sequence of PI tags at a specific time.

    Args:
        client: The PIWebClient used to retrieve the data.
        web_ids: The web_ids to stream data for.
        time: The time to get the value at.
        timezone: The timezone to convert the returned data into. Defaults to
            the local system timezone.

    Raises:
        RequestException: There was an ambiguous exception that occurred while
            handling the request.
    """
    timezone = timezone or LOCAL_TZ

    results = [
        handle_response(
            handle_request(
                functools.partial(
                    client.streams.get_interpolated_at_times,
                    web_id,
                    time=time.isoformat(),
                    timeZone=timezone,
                    selectedFields="Items.Timestamp;Items.Value;Items.Good",
                ),
                raise_for_status=False,
            ),
            raise_for_status=False,
            raise_for_content_error=False,
        )
        for web_id in web_ids
    ]

    row = []
    for result in results:
        if not result:
            row.append(None)
            continue
        items = result.get("Items", [])
        if items:
            assert len(items) == 1
            result = items[0]
            if result["Good"]:
                value = result["Value"]
                if isinstance(value, dict):
                    row.append(value["Name"])
                else:
                    row.append(value)
            else:
                row.append(None)
        else:
            row.append(None)
    return time, row