from requests.exceptions import RequestException



class APIResponseError(RequestException):
    """Raised when the HTTP request was successful but the response was invalid."""
    def __init__(self, *args, **kwargs):
        self.errors = kwargs.pop("errors", None)
        super().__init__(*args, **kwargs)


class InvalidUpgrade(RequestException):
    """Raised if server sends an invalid response on an upgrade request."""


class InvalidHandshake(RequestException):
    """Raised if websocket extensions or subprotocols proposed by server are not
    supported by the client.
    """

class NegotiationError(InvalidHandshake):
    """Raised if client-server negotiation on extensions or subprotocols fails."""


class SubscriptionError(ValueError):
    """Raised if a WebId cannot be found for a PI tag while trying to subscribe
    to a channel.
    """
    def __init__(self, *args, **kwargs) -> None:
        self.unmapped = kwargs.pop("unmapped", None)
        super().__init__(*args, **kwargs)