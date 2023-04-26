import base64
import hashlib
import secrets
from collections.abc import Sequence
from contextvars import ContextVar

from requests import HTTPError, Response
from requests.exceptions import InvalidHeader
from websockets.datastructures import Headers
from websockets.extensions import ClientExtensionFactory
from websockets.headers import parse_extension, parse_subprotocol
from websockets.protocol import Protocol, CLIENT
from websockets.sync.connection import Connection
from websockets.typing import Subprotocol

from pi_utils.web.exceptions import (
    InvalidHandshake,
    InvalidUpgrade,
    NegotiationError
)



GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
EXTENSIONS_CONTEXT: ContextVar[
    Sequence[ClientExtensionFactory]
] = ContextVar("_s_w_extensions_context", default=None)
SUBPROTOCOLS_CONTEXT: ContextVar[
    Sequence[Subprotocol]
] = ContextVar("_s_w_protocols_context", default=None)


_orig__close_socket = Connection.close_socket

def _patched__close_socket(self) -> None:
    _orig__close_socket(self)
    close_callback = getattr(self, "close_callback", None)
    if close_callback is not None and callable(close_callback):
        close_callback(self)


Connection.close_socket = _patched__close_socket


def accept_key(key: str) -> str:
    """Compute the value of the Sec-WebSocket-Accept header.

    Args:
        key: value of the Sec-WebSocket-Key header.
    """
    # Copied from websockets package
    sha1 = hashlib.sha1((key + GUID).encode()).digest()
    return base64.b64encode(sha1).decode()


def generate_key() -> str:
    """Generate a random key for the Sec-WebSocket-Key header."""
    # Copied from websockets package
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
    # Copied and modified from websockets package
    headers = Headers(**response.headers)
    extensions = headers.get_all("Sec-WebSocket-Extensions")
    if extensions:
        available_extensions = EXTENSIONS_CONTEXT.get()
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
    # Copied and modified from websockets package
    headers = Headers(**response.headers)
    subprotocols = headers.get_all("Sec-WebSocket-Protocol")

    if subprotocols:
        available_subprotocols = SUBPROTOCOLS_CONTEXT.get()
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