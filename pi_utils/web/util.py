import base64
import hashlib
import logging
import secrets
from http import HTTPStatus
from typing import Dict, List

import orjson
from requests import HTTPError, Response
from requests.exceptions import InvalidHeader
from uplink import Consumer
from websockets.datastructures import Headers
from websockets.headers import parse_extension, parse_subprotocol
from websockets.protocol import Protocol, CLIENT
from websockets.sync.connection import Connection

from pi_utils.types import JSONContent, JSONPrimitive
from pi_utils.web.context import _s_w_extensions_context, _s_w_protocols_context
from pi_utils.web.exceptions import (
    APIResponseError,
    InvalidHandshake,
    InvalidUpgrade,
    NegotiationError
)



_LOGGER = logging.getLogger("pi_utils.web")
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


_orig__close_socket = Connection.close_socket

def _patched__close_socket(self) -> None:
    _orig__close_socket(self)
    close_callback = getattr(self, "close_callback", None)
    if close_callback is not None and callable(close_callback):
        close_callback(self)


Connection.close_socket = _patched__close_socket


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
            # If a stream item returned an error, the value will be `None`` anyway
            # and we're not particularly interested in the errors
            # https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/topics/error-handling.html
            value = item["Value"]
            if isinstance(value, dict):
                value = value["Name"]
        formatted["timestamp"].append(timestamp)
        formatted["value"].append(value)
    
    return formatted


def handle_response(
    response: Response,
    raise_for_status: bool = True,
    raise_for_content_error: bool = True
) -> JSONContent | None:
    """Primary response handling for all HTTP requests to the PI Web API.
    
    Args:
        response: The response object returned from the request.
        raise_for_status: If `True` unsuccessful status codes will raise
            `HTTPError`. If `False`, will return `None`.
        raise_for_content_error: If `True` and the 'Errors' property is not
            empty in a successful response a  
    """
    with response:
        try:
            response.raise_for_status()
        except HTTPError as e:
            if raise_for_status:
                raise
            _LOGGER.warning(str(e))
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
            err_msg = f"API response return {len(errors)} errors for url: {response.request.url}"
            if raise_for_content_error:
                raise APIResponseError(err_msg, errors=errors, response=response)
            _LOGGER.warning(err_msg, extra={"errors": errors})
            return
        
        return data


def accept_key(key: str) -> str:
    """Compute the value of the Sec-WebSocket-Accept header.

    Args:
        key: value of the Sec-WebSocket-Key header.
    """
    sha1 = hashlib.sha1((key + GUID).encode()).digest()
    return base64.b64encode(sha1).decode()


def generate_key() -> str:
    """Generate a random key for the Sec-WebSocket-Key header."""
    key = secrets.token_bytes(16)
    return base64.b64encode(key).decode()

    
def check_handshake(response: Response) -> Response:
    """Check the status code of the handshake response. If not 101 raises HTTPError."""
    if response.status_code != 101:
        # Check for client/server error
        response.raise_for_status()
        # Raise our own HTTPError
        if isinstance(response.reason, bytes):
            try:
                reason = response.reason.decode("utf-8")
            except UnicodeDecodeError:
                reason = response.reason.decode("iso-8859-1")
        else:
            reason = response.reason
        
        http_error_msg = (
            f"{response.status_code} Not Switching Error: {reason} for url: {response.url}"
        )
        raise HTTPError(http_error_msg, response=response)
    
    upgrade = response.headers.get("Upgrade")
    if upgrade is None or upgrade.lower() != "websocket":
        raise InvalidUpgrade(f"Upgrade: {upgrade}.", response=response)

    s_w_accept = response.headers.get("Sec-Websocket-Accept")
    if s_w_accept is None or s_w_accept != accept_key(response.request.headers.get("Sec-Websocket-Key")):
        raise InvalidHeader("Sec-Websocket-Accept", response=response)
    
    process_extentions(response)
    process_subprotocols(response)

    return response


def process_extentions(response: Response) -> None:
    """Handle the Sec-WebSocket-Extensions HTTP response header.
    
    Check that each extension is supported, as well as its parameters.
    """
    headers = Headers(**response.headers)
    extensions = headers.get_all("Sec-WebSocket-Extensions")
    if extensions:
        available_extensions = _s_w_extensions_context.get()
        if available_extensions is None:
            raise InvalidHandshake("No extensions supported.", response=response)
        
        parsed_extensions = sum(
            [parse_extension(header_value) for header_value in extensions], []
        )
        
        for name, response_params in parsed_extensions:
            for extension_factory in available_extensions:
                # Skip non-matching extensions based on their name.
                if extension_factory.name != name:
                    continue

                # Skip non-matching extensions based on their params.
                try:
                    extension_factory.process_response_params(
                        response_params, []
                    )
                except NegotiationError:
                    continue

                # Break out of the loop once we have a match.
                break

            # If we didn't break from the loop, no extension in our list
            # matched what the server sent. Fail the connection.
            else:
                raise NegotiationError(
                    f"Unsupported extension: "
                    f"name = {name}, params = {response_params}.",
                    response=response
                )


def process_subprotocols(response: Response) -> None:
    """Handle the Sec-WebSocket-Protocol HTTP response header.
    
    If provided, check that it contains exactly one supported subprotocol.
    """
    headers = Headers(**response.headers)
    subprotocols = headers.get_all("Sec-WebSocket-Protocol")

    if subprotocols:
        available_subprotocols = _s_w_protocols_context.get()
        if available_subprotocols is None:
            raise InvalidHandshake("No subprotocols supported.", response=response)

        parsed_subprotocols = sum(
            [parse_subprotocol(header_value) for header_value in subprotocols], []
        )

        if len(parsed_subprotocols) > 1:
            subprotocols_display = ", ".join(parsed_subprotocols)
            raise InvalidHandshake(
                f"Multiple subprotocols: {subprotocols_display}.", response=response
            )

        subprotocol = parsed_subprotocols[0]

        if subprotocol not in available_subprotocols:
            raise NegotiationError(
                f"Unsupported subprotocol: {subprotocol}.", response=response
            )


def wrap_socket(response: Response) -> Connection:
    """Wrap the socket used for the handshake in a websocket protocol connection."""
    try:
        socket = response.raw.connection.sock
    except AttributeError as e:
        raise RuntimeError("Unable to retrieve socket from response object.") from e
    else:
        # We need to remove the reference to the socket from the connection.
        # Once we do, we can safely close the response object and release the
        # connection back to the pool. If the connection is to be used on a
        # subsequent request, urllib's connection pool will test the
        # connection when it pulls it from the pool and determine the connection
        # has dropped and another socket will be created. This way we can safely
        # transfer the socket to a different protocol wrapper (websockets in
        # this case).
        response.raw.connection.sock = None
        response.raw.close()
    
    protocol = Protocol(side=CLIENT)
    return Connection(socket=socket, protocol=protocol)


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
        setattr(connection, "close_callback", consumer.client.session.websockets.discard)

    return connection