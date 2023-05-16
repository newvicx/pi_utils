from pathlib import Path
from typing import Dict

from pydantic import BaseSettings, FilePath, Field, SecretStr, validator

from pi_utils.util.formatting import format_docstring
from pi_utils.util.kerberos import MutualAuthentication


class SDKSettings(BaseSettings):
    server: str = Field(
        default=None,
        description=format_docstring(
            """The PI server name to connect to through
        the SDK. Defaults to `None`. This is required if performing any batch
        queries."""
        ),
    )
    path: FilePath = Field(
        default=Path("C:/Program Files/PIPC/pisdk/PublicAssemblies/OSIsoft.PISDK.dll"),
        description=format_docstring(
            """The absolute path to the directory
        containing the SDK assembly files. Defaults to
        'C:/Program Files/PIPC/pisdk/PublicAssemblies/OSIsoft.PISDK.dll'."""
        ),
    )
    max_connections: int = Field(
        default=4,
        description=format_docstring(
            """The maximum number of connections that
        can be opened concurrently using the SDK. Defaults to 4."""
        ),
        gt=0,
    )

    class Config:
        env_file = ".env"
        env_prefix = "pi_utils_sdk_"
        extra = "ignore"


class WebSettings(BaseSettings):
    host: str = Field(
        default=None,
        description=format_docstring(
            """The host to connect to the web API.
        Defaults to `None`. This is required for any web API queries."""
        ),
    )
    port: int = Field(
        default=None,
        description=format_docstring(
            """The port to connect to the web API.
        Defaults to `None`, if TLS is `True` will use 443 else 80."""
        ),
    )
    tls: bool = Field(
        default=False,
        description=format_docstring(
            """Connect to the web API over TLS. Defaults
        to `False`."""
        ),
    )
    dataserver: str = Field(
        default=None,
        description=format_docstring(
            """The PI server name to connect to through
        the web API. This should be the data(archive) server or collective.
        Defaults to `None`."""
        ),
    )
    verify: bool | FilePath = Field(
        default=True,
        description=format_docstring(
            """Either a boolean, in which case it
        controls whether we verify the servers TLS certificate, or a string,
        in which case it must be a path to a CA bundle to use. Defaults to
        `True`."""
        ),
    )
    headers: Dict[str, str] = Field(
        default=None,
        description=format_docstring(
            """Dictionary of HTTP Headers to send with
        each request. Defaults to `None`."""
        ),
    )
    cookies: Dict[str, str] = Field(
        default=None,
        description=format_docstring(
            """Dictionary of cookies to send with each
        request. Defaults to `None`."""
        ),
    )
    kerberos: bool = Field(
        default=True,
        description=format_docstring(
            """If `True`, kerberos authentication will
        used on requests. Defaults to `True`."""
        ),
    )
    mutual_authentication: MutualAuthentication = Field(
        default=MutualAuthentication,
        decription=format_docstring(
            """Integer value defining the mutual
        authentication requirements (1: Required, 2: Optional, 3: Disabled).
        Defaults to 1 (Required)."""
        ),
    )
    delegate: bool = Field(
        default=False,
        description=format_docstring(
            """Indicates that the user's credentials
        are to be delegated to the server. Defaults to `False`."""
        ),
    )
    service: str = Field(
        default="HTTP",
        description=format_docstring(
            """The service principle protocol to use
        (such as HTTP or HTTPS) for kerberos authentication. Defaults to 'HTTP'."""
        ),
    )
    principal: SecretStr = Field(
        default=None,
        description=format_docstring(
            """An explicit principal to use for
        authentication. By default the user running the application is used.
        Defaults to `None`."""
        ),
    )
    hostname_override: str = Field(
        default=None,
        description=format_docstring(
            """If communicating with a host whose DNS
        name doesn't match its kerberos hostname (eg, behind a content switch
        or load balancer), the hostname used for the Kerberos GSS exchange can
        be overridden. Defaults to `None`."""
        ),
    )
    send_cbt: bool = Field(
        default=True,
        description=format_docstring(
            """If `True`, automatically attempts to
        bind the authentication token with the channel binding data when
        connecting over a TLS connection. Defaults to `True`."""
        ),
    )

    @validator("verify")
    def maybe_disable_verification_warning(cls, verify: bool | Path) -> bool | Path:
        # Only disable if verify is explicitely False
        if verify is False:
            import urllib3
            import urllib3.exceptions

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return verify

    @property
    def kerberos_settings(self) -> Dict[str, str | bool | int | None]:
        """Returns optional kerberos authentication settings."""
        return {
            "mutual_authentication": self.mutual_authentication,
            "delegate": self.delegate,
            "service": self.service,
            "principal": self.principal
            if self.principal is None
            else self.principal.get_secret_value(),
            "hostname_override": self.hostname_override,
            "send_cbt": self.send_cbt,
        }

    class Config:
        env_file = ".env"
        env_prefix = "pi_utils_web_"
        extra = "ignore"
