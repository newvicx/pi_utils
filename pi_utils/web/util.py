import logging
from http import HTTPStatus
from typing import Callable, Dict, List

import orjson
from requests import HTTPError, Response
from uplink import Consumer

from pi_utils.types import JSONContent, JSONPrimitive
from pi_utils.util.websockets import Connection
from pi_utils.web.exceptions import APIResponseError


_LOGGER = logging.getLogger("pi_utils.web")


def format_streams_content(
    content: Dict[str, List[Dict[str, JSONPrimitive]]]
) -> Dict[str, List[JSONPrimitive]]:
    """Extract timestamp and value for each item in a multi-value stream response."""
    formatted = {"timestamp": [], "value": []}
    items = content.get("Items", []) if content is not None else []

    for item in items:
        timestamp = item["Timestamp"]
        good = item["Good"]
        if not good:
            value = None
        else:
            # If a stream item returned an error, the value will be `None` anyway
            # and we're not particularly interested in the errors
            # https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/topics/error-handling.html
            value = item["Value"]
            if isinstance(value, dict):
                value = value["Name"]
        formatted["timestamp"].append(timestamp)
        formatted["value"].append(value)

    return formatted


def handle_request(
    request: Callable[[], Response], raise_for_status: bool = True
) -> JSONContent | None:
    """Primary request handling for all HTTP requests to the PI Web API. This
    will load all the content from the response into memory.

    Args:
        request: A callable that returns a response.
        raise_for_status: If `True` unsuccessful status codes will raise
            `HTTPError`. If `False`, will return `None`.
    """
    response = request()
    with response:
        try:
            response.raise_for_status()
        except HTTPError as e:
            if raise_for_status:
                raise
            _LOGGER.warning(str(e))
            return
        else:
            # Download the content
            response.content
            return response


def handle_response(
    response: Response | None,
    raise_for_status: bool = True,
    raise_for_content_error: bool = True,
) -> JSONContent | None:
    """Primary response handling for all data HTTP requests to the PI Web API.

    Args:
        response: The response object returned from the request.
        raise_for_status: If `True` and the 'WebException' property is present
            and the new status code is not successful, raise an `HTTPError`.
        raise_for_content_error: If `True` and the 'Errors' property is not
            empty in a successful response raise `APIResponseError`.
    """
    if response is None:
        return

    data: JSONContent = orjson.loads(response.content)

    # Check for WebException and potentially raise an HTTPError
    # https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/error-handling.html
    web_exc: dict = data.get("WebException")
    if web_exc:
        status_code = web_exc.get("StatusCode")
        response.status_code = status_code
        response.reason = HTTPStatus(status_code).phrase
        try:
            response.raise_for_status()
        except HTTPError as e:
            if raise_for_status:
                raise
            _LOGGER.warning(str(e))
            return

    # Check for Errors property
    # Some controllers can have an errors property on a successful response
    # if invalid/not enough parameters were passed.
    errors = data.get("Errors")
    if errors:
        err_msg = f"API response returned {len(errors)} errors for url: {response.request.url}"
        if raise_for_content_error:
            raise APIResponseError(err_msg, errors=errors, response=response)
        _LOGGER.warning(err_msg, extra={"errors": errors})
        return

    return data


def add_to_client(consumer: Consumer, connection: Connection) -> Connection:
    """Adds the websocket connection to the client session so it closes when
    the session closes.
    """
    try:
        consumer.client.session.websockets.add(connection)
    except AttributeError:
        # Client does not keep a reference to the websockets or the the consumer
        # does not have a direct reference to the session
        pass
    else:
        setattr(
            connection, "close_callback", consumer.client.session.websockets.discard
        )

    return connection
