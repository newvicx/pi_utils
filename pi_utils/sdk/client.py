import logging
import threading
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Type

import clr

from pi_utils.sdk.types import SDK, SDKConnection, SDKSubBatch, SDKUnitBatch



_LOGGER = logging.getLogger("pi_utils.sdk")
_SDK_CLIENT: "SDKClient" = None
_sdk_client_lock = threading.Lock()


class SDKClient:
    """Interface for connecting to PI through the PI SDK.
    
    Args:
        server: The PI server to connect to.
        path: The path to the SDK assembly (.dll). Defaults to
            'C:/Program Files/PIPC/pisdk/PublicAssemblies/OSIsoft.PISDK.dll' which
            is the standard path for client installs.
        max_connections: Limits the total number of the concurrent connections to
            the server through the SDK.
    """
    _sdk: Type[SDK] = None
    _sub_batch: Type[SDKSubBatch] = None
    _unit_batch: Type[SDKUnitBatch] = None

    def __new__(cls: Type["SDKClient"], *args: Any, **kwargs: Any) -> "SDKClient":
        if cls._sdk is None:
            path = kwargs.get('path') or "C:/Program Files/PIPC/pisdk/PublicAssemblies/OSIsoft.PISDK.dll"
            path = Path(path)

            if not path.exists():
                raise FileNotFoundError(str(path))
            
            clr.AddReference(str(path))
            
            from PISDK import PISDK, PISubBatch, PIUnitBatch
            
            cls._sdk = PISDK
            cls._sub_batch = PISubBatch
            cls._unit_batch = PIUnitBatch
        
        return super(SDKClient, cls).__new__(cls)
    
    def __init__(
        self,
        *,
        server: str,
        path: str | Path | None = None,
        max_connections: int = 4
    ) -> None:
        self._server_name = server
        self._path = path

        self._limiter: threading.Semaphore = threading.Semaphore(max_connections)

    @property
    def unit_batch(self) -> Type[SDKUnitBatch]:
        return self._unit_batch

    @property
    def sub_batch(self) -> Type[SDKSubBatch]:
        return self._sub_batch

    @contextmanager
    def get_connection(self) -> Iterator[SDKConnection]:
        """Obtain an SDK connection. This always opens a new connection."""
        with self._limiter:
            connection = self._sdk().Servers[self._server_name]
            try:
                connection.Open()
            except:
                err = ConnectionError("Unable to connect to PI SDK.")
                _, _, tb = sys.exc_info()
                err.__traceback__ = tb
                raise err
            try:
                yield connection
            finally:
                try:
                    if connection.Connected:
                        connection.Close()
                except:
                    _LOGGER.warning("Failed to close connection", exc_info=True)
                    pass
                del connection


def get_sdk_client(
    server: str,
    path: str | Path | None = None,
    max_connections: int = 4
) -> SDKClient:
    """Build a `SDKClient`. This returns a singleton instance of the client."""
    with _sdk_client_lock:
        global _SDK_CLIENT
        if _SDK_CLIENT is not None:
            assert isinstance(_SDK_CLIENT, SDKClient)
            return _SDK_CLIENT
        client = SDKClient(
            server=server,
            path=path,
            max_connections=max_connections
        )
        _SDK_CLIENT = client
        return client