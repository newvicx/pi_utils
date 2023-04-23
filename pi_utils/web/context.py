from collections.abc import Sequence
from contextvars import ContextVar

from websockets.extensions import ClientExtensionFactory
from websockets.typing import Subprotocol



_s_w_extensions_context: ContextVar[
    Sequence[ClientExtensionFactory]
] = ContextVar("_s_w_extensions_context", default=None)

_s_w_protocols_context: ContextVar[
    Sequence[Subprotocol]
] = ContextVar("_s_w_protocols_context", default=None)