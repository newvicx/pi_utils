import itertools
import logging
import math
from collections.abc import Iterable, Sequence
from typing import Tuple

from websockets.extensions import ClientExtensionFactory
from websockets.extensions.permessage_deflate import enable_client_permessage_deflate
from websockets.headers import build_extension, build_subprotocol, validate_subprotocols
from websockets.typing import Subprotocol

from pi_utils.util.websockets import (
    generate_key,
    EXTENSIONS_CONTEXT,
    SUBPROTOCOLS_CONTEXT,
)
from pi_utils.web.channels.subscriber import Subscriber
from pi_utils.web.client import PIWebClient
from pi_utils.web.exceptions import SubscriptionError
from pi_utils.web.ops.find import find_tags


_LOGGER = logging.getLogger("pi_utils.web")
MAX_HEADER_BYTES = 4096  # 4KB


def batch_web_ids(web_ids: Sequence[str], n: int) -> Iterable[Tuple[str]]:
    # https://docs.python.org/3/library/itertools.html#itertools-recipes
    it = iter(web_ids)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def subscribe(
    client: PIWebClient,
    tags: Sequence[str],
    dataserver: str | None = None,
    extensions: Sequence[ClientExtensionFactory] | None = None,
    subprotocols: Sequence[Subprotocol] | None = None,
    compression: str = "deflate",
) -> Subscriber:
    """Subscribe to a sequence of PI tags for near real-time data streaming.

    Wraps N websocket connections and returns a `Subscriber` for iterating
    data returned from the PI Web API. The number of websocket connections is
    a function of the number of tags and how long the WebId's are for the tags,
    it is not deterministic.

    Args:
        client: The `PIWebClient` to execute requests and create websocket
            connections.
        tags: The sequence of tags to search for WebId's.
        dataserver: The name of the data archive server. The WebId of the archive
            server will be searched.
        extensions: List of supported extensions, in order in which they
            should be negotiated and run.
        subprotocols: List of supported subprotocols, in order of decreasing
            preference.
        compression: The "permessage-deflate" extension is enabled by default.
            Set to `None` to disable it.

    Raises:
        HTTPError: An HTTP error occurred.
        APIResponseError: Unable to find dataserver WebID.
        InvalidUpgrade: The server did not respond with a valid 'Upgrade' header.
        InvalidHeader: The server did not respond with a valid 'Sec-Websocket-Accept'
            header.
        InvalidHandshake: Extensions or subprotocols proposed by server are not
            supported by client.
        NegotiationError: Client-server negotiation on extensions or subprotocols
            failed.
        RequestException: There was an ambiguous exception that occurred while
            handling the request.
    """
    if subprotocols is not None:
        validate_subprotocols(subprotocols)
    if compression == "deflate":
        extensions = enable_client_permessage_deflate(extensions)
    elif compression is not None:
        raise ValueError(f"unsupported compression: {compression}")

    s_w_extensions = None
    if extensions is not None:
        s_w_extensions = build_extension(
            [
                (extension_factory.name, extension_factory.get_request_params())
                for extension_factory in extensions
            ]
        )
    s_w_protocols = None
    if subprotocols is not None:
        s_w_protocols = build_subprotocol(subprotocols)

    mapped, unmapped = find_tags(client=client, tags=tags, dataserver=dataserver)
    if unmapped:
        raise SubscriptionError(
            f"Could not find {len(unmapped)} tags.", unmapped=unmapped
        )
    web_ids = [web_id for _, web_id in mapped]

    # Determine how many connections we need to create based on the size of all
    # WebId's so we don't exceed the header size limit on servers. This is not
    # perfect, we set a relatively conservative limit of 4KB to add another
    # connection. This gives a minumum of 4KB (on most servers) for other headers
    # such as cookies, authorization, extensions, etc. Also, we assume the length
    # of each WebId is more or less the same (which is not guarenteed to be true)
    # and we make no attempt to adjust our limit based on the current size of
    # the other headers.
    num_connections = len("&webId=".join(web_ids).encode()) // MAX_HEADER_BYTES + 1

    extensions_token = EXTENSIONS_CONTEXT.set(extensions)
    subprotocols_token = SUBPROTOCOLS_CONTEXT.set(subprotocols)
    try:
        connections = [
            client.channels.subscribe(
                webId=batch,
                sec_websocket_key=generate_key(),
                sec_websocket_extensions=s_w_extensions,
                sec_websocket_subprotocol=s_w_protocols,
            )
            for batch in batch_web_ids(
                web_ids, math.ceil(len(web_ids) / num_connections)
            )
        ]
    finally:
        EXTENSIONS_CONTEXT.reset(extensions_token)
        SUBPROTOCOLS_CONTEXT.reset(subprotocols_token)

    _LOGGER.debug("Created %i connections for %i tags", num_connections, len(mapped))
    return Subscriber(*connections)
