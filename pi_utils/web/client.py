import threading
from types import TracebackType
from typing import Any, Dict, Set, Type

import requests
from requests.cookies import RequestsCookieJar
from requests.models import CaseInsensitiveDict
from uplink import Consumer
from websockets.sync.connection import Connection

from pi_utils.util.kerberos import HTTPKerberosAuth
from pi_utils.web.controllers import Channels, DataServers, Streams


_WEB_CLIENT: "PIWebClient" = None
_web_client_lock = threading.Lock()


class PIWebClient:
    """Interface for a PI Web API client."""

    consumers: Dict[str, Consumer] = {}
    _lock = threading.Lock()

    def __init__(
        self,
        host: str,
        port: int | None = None,
        tls: bool = True,
        session: requests.Session | None = None,
        verify: str | bool = True,
        headers: Dict[str, str] | None = None,
        cookies: RequestsCookieJar | Dict[str, str] | None = None,
        kerberos: bool = True,
        **kerberos_args: Any,
    ) -> None:
        scheme = "https" if tls else "http"
        port = f":{port}" if port else ""
        self.base_url = f"{scheme}://{host}{port}"

        if not session:
            session = requests.Session()
            session.verify = verify
            if headers:
                headers = CaseInsensitiveDict(**headers)
                session.headers = headers
            if cookies:
                if not isinstance(cookies, RequestsCookieJar):
                    jar = RequestsCookieJar()
                    jar.update(cookies)
                else:
                    jar = cookies
                session.cookies = jar
            if kerberos:
                session.auth = HTTPKerberosAuth(**kerberos_args)

        session.stream = True
        setattr(session, "websockets", set())
        self.session = session

    @property
    def channels(self) -> Channels:
        """Return a `Channels` consumer."""
        return self._get_consumer_instance(Channels)

    @property
    def dataservers(self) -> DataServers:
        """Returns a `DataServers` consumer."""
        return self._get_consumer_instance(DataServers)

    @property
    def streams(self) -> Streams:
        """Returns a `Streams` consumer."""
        return self._get_consumer_instance(Streams)

    def close(self) -> None:
        """Close the underlying session."""
        websockets: Set[Connection] = getattr(self.session, "websockets", set())
        while True:
            try:
                websocket = websockets.pop()
            except KeyError:
                break
            else:
                websocket.close()
        self.session.close()

    def _get_consumer_instance(self, consumer: Type[Consumer]) -> Consumer:
        """Get an instance of the consumer for a controller.

        This caches the consumer instance in the class for reuse.
        """
        with self._lock:
            name = consumer.__name__
            if name in self.consumers:
                return self.consumers[name]
            instance = consumer(base_url=self.base_url, client=self.session)
            self.consumers[name] = instance
            setattr(instance, "client", self)
            return instance

    def __enter__(self) -> "PIWebClient":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        traceback: TracebackType,
    ) -> None:
        self.close()


def get_web_client() -> PIWebClient:
    """Get an initialized web client instance.
    
    If `initialize_web_client` has not been called, this will raise `RuntimeError`.
    """
    with _web_client_lock:
        global _WEB_CLIENT
        if _WEB_CLIENT is not None:
            assert isinstance(_WEB_CLIENT, PIWebClient)
            return _WEB_CLIENT
        raise RuntimeError("Web client not initialized.")


def initialize_web_client(*args: Any, **kwargs: Any) -> PIWebClient:
    """Build a `PiWebClient`. This returns a singleton instance of the client."""
    with _web_client_lock:
        global _WEB_CLIENT
        if _WEB_CLIENT is not None:
            assert isinstance(_WEB_CLIENT, PIWebClient)
            return _WEB_CLIENT
        client = PIWebClient(*args, **kwargs)
        _WEB_CLIENT = client
        return client