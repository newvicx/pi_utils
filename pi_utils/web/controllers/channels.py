from uplink import Consumer, Header, Query, get, headers, response_handler

from pi_utils.util.websockets import Connection, check_handshake, wrap_socket
from pi_utils.web.util import add_to_client


@response_handler(add_to_client, requires_consumer=True)
@response_handler(wrap_socket)
@response_handler(check_handshake)
@headers({"Upgrade": "websocket", "Connection": "Upgrade"})
class Channels(Consumer):
    """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/channels.html"""

    @get("/piwebapi/streamsets/channel")
    def subscribe(
        self,
        webId: Query,
        *,
        sec_websocket_key: Header("Sec-Websocket-Key"),
        sec_websocket_version: Header("Sec-Websocket-Version") = 13,
        sec_websocket_extensions: Header("Sec-Websocket-Extensions") = None,
        sec_websocket_subprotocol: Header("Sec-Websocket-Protocol") = None,
        includeInitialValues: Query = None,
        heartbeatRate: Query = None,
        webIdType: Query = None,
    ) -> Connection:
        """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/streamset/actions/getchanneladhoc.html"""
