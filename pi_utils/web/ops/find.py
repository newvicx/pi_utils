import functools
import logging
from collections.abc import Sequence
from typing import List, Tuple

from pi_utils.web.client import PIWebClient
from pi_utils.web.exceptions import APIResponseError
from pi_utils.web.util import handle_request, handle_response


_LOGGER = logging.getLogger("pi_utils.web")


def find_dataserver(client: PIWebClient, dataserver: str | None = None) -> str:
    """Get the dataserver WebId.

    If no dataserver is given, the first one in the list will be returned.

    Raises:
        HTTPError: An HTTP error occurred.
        APIResponseError: Unable to find dataserver WebID.
        RequestException: There was an ambiguous exception that occurred while
            handling the request.
    """
    response = handle_request(
        functools.partial(
            client.dataservers.list, selectedFields="Items.Name;Items.WebId"
        )
    )
    data = handle_response(response)
    items = data.get("Items")
    if not items or not isinstance(items, list):
        raise APIResponseError("Unable to find dataserver WebID", response=response)
    if dataserver:
        for item in items:
            if item["WebId"] == dataserver:
                return item["WebId"]
        else:
            raise APIResponseError("Unable to find dataserver WebID", response=response)
    else:
        return items[0]["WebId"]


def find_tags(
    client: PIWebClient, tags: Sequence[str], dataserver: str | None = None
) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Get the WebId for a sequence of pi tags.

    If a tag is not found or the query returns multiple results, the query for
    for that tag will fail. Therefore you cannot use wild card searches
    for this method (unless the wild card search returns 1 tag).

    If the data archive server is not provided, this will attempt to discover
    the archive server.

    Args:
        client: The `PIWebClient` to execute requests.
        tags: The sequence of tags to search for WebId's.
        dataserver: The name of the data archive server. The WebId of the archive
            server will be searched.

    Raises:
        HTTPError: An HTTP error occurred trying to find the dataserver WebID.
        APIResponseError: Unable to find dataserver WebID.
        RequestException: There was an ambiguous exception that occurred while
            handling the request.
    """
    tags = [tag.upper() for tag in tags]

    dataserver_web_id = find_dataserver(client, dataserver=dataserver)

    results = [
        handle_response(
            handle_request(
                functools.partial(
                    client.dataservers.get_points,
                    dataserver_web_id,
                    nameFilter=tag,
                    selectedFields="Items.Name;Items.WebId",
                ),
                raise_for_status=False,
            ),
            raise_for_status=False,
            raise_for_content_error=False,
        )
        for tag in tags
    ]

    found = 0
    mapped: List[Tuple[str, str]] = []
    unmapped: List[str] = []
    for tag, result in zip(tags, results):
        if not result:
            _LOGGER.warning("'%s' search failed", tag)
            unmapped.append(tag)
            continue
        items = result.get("Items")
        if not isinstance(items, list):
            _LOGGER.warning(
                "'%s' search returned unhandled data type for 'Items'. Expected list, got %s",
                tag,
                type(items),
            )
            unmapped.append(tag)
        elif not items:
            _LOGGER.warning("'%s' search returned no results", tag)
            unmapped.append(tag)
        elif len(items) > 1:
            _LOGGER.warning("'%s' search returned more than 1 result", tag)
            unmapped.append(tag)
        else:
            assert items[0]["Name"].upper() == tag
            mapped.append((tag, items[0]["WebId"]))
            found += 1

    _LOGGER.info("Found %i of %i PI tags", found, len(tags))
    return mapped, unmapped
