import concurrent.futures
import functools
from collections.abc import Iterable
from datetime import datetime
from typing import List

from pi_utils.types import TimeseriesRow
from pi_utils.util.time import (
    get_timestamp_index,
    iter_timeseries_rows,
    split_range_on_frequency,
    LOCAL_TZ,
)
from pi_utils.web.client import PIWebClient
from pi_utils.web.util import format_streams_content, handle_request, handle_response


def get_recorded(
    client: PIWebClient,
    web_ids: List[str],
    start_time: datetime,
    end_time: datetime | None = None,
    request_chunk_size: int | None = None,
    scan_rate: float | None = None,
    timezone: str | None = None,
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
        RequestException: There was an ambiguous exception that occurred while
            handling the request.
    """
    end_time = end_time or datetime.now()
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")

    timezone = timezone or LOCAL_TZ
    request_chunk_size = request_chunk_size or 5000
    scan_rate = scan_rate or 5

    start_times, end_times = split_range_on_frequency(
        start_time=start_time,
        end_time=end_time,
        request_chunk_size=request_chunk_size,
        scan_rate=scan_rate,
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for start_time, end_time in zip(start_times, end_times):
            futs = [
                executor.submit(
                    handle_request,
                    functools.partial(
                        client.streams.get_recorded,
                        web_id,
                        startTime=start_time.isoformat(),
                        endTime=end_time.isoformat(),
                        timeZone=timezone,
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

            start_row = get_recorded_at_time(
                client=client, web_ids=web_ids, time=start_time, timezone=timezone
            )
            yield start_row

            # With recorded data we cannot guarentee a value will exist at the start
            # and end times so we always get the recorded at time value for each tag.
            # But, data at that time may exist for some tags so we need to check the
            # timestamps coming out of the iterator and only yield the ones not equal
            # to the start/end time since this would lead to duplicate data.
            for timestamp, row in iter_timeseries_rows(
                index=index, data=data, timezone=timezone
            ):
                if timestamp == start_time:
                    continue
                elif timestamp == end_time:
                    continue
                yield timestamp, row

        # The next start time is always the last end time, so the only time we
        # need to get the last row is when there are no more time chunks to work
        # through
        else:
            end_row = get_recorded_at_time(
                client=client, web_ids=web_ids, time=end_time, timezone=timezone
            )
            yield end_row


def get_recorded_at_time(
    client: PIWebClient, web_ids: List[str], time: datetime, timezone: str | None = None
) -> TimeseriesRow:
    """Returns the recorded value for sequence of PI tags at a specific time.

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
                    client.streams.get_recorded_at_time,
                    web_id,
                    time=time.isoformat(),
                    timeZone=timezone,
                    selectedFields="Timestamp;Value;Good",
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
        if result["Good"]:
            value = result["Value"]
            if isinstance(value, dict):
                row.append(value["Name"])
            else:
                row.append(value)
        else:
            row.append(None)
    return time, row
