import base64
import logging
import re
import warnings
from typing import Any
from urllib.parse import urlparse

import spnego
import spnego.channel_bindings
import spnego.exceptions
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.exceptions import UnsupportedAlgorithm
from requests.auth import AuthBase
from requests.exceptions import RequestException
from requests.models import Request, Response
from requests.structures import CaseInsensitiveDict
from requests.cookies import cookiejar_from_dict
from requests.packages.urllib3 import HTTPResponse


"""Re-implementation of requests-kerberos with type hints, modified string
formatting and less verbose logging.
"""

_LOGGER = logging.getLogger("kerberos")
REQUIRED = 1
OPTIONAL = 2
DISABLED = 3


class NoCertificateRetrievedWarning(Warning):
    pass


class UnknownSignatureAlgorithmOID(Warning):
    pass


class MutualAuthenticationError(RequestException):
    """Mutual Authentication Error"""


class KerberosExchangeError(RequestException):
    """Kerberos Exchange Failed Error"""


class SanitizedResponse(Response):
    """A server's response to an HTTP request.

    This differs from `requests.models.Response` in that it's headers and
    content have been sanitized. This is only used for HTTP Error messages
    which do not support mutual authentication when mutual authentication is
    required.
    """

    def __init__(self, response: Response):
        super(SanitizedResponse, self).__init__()
        self.status_code = response.status_code
        self.encoding = response.encoding
        self.raw = response.raw
        self.reason = response.reason
        self.url = response.url
        self.request = response.request
        self.connection = response.connection
        self._content_consumed = True

        self._content = ""
        self.cookies = cookiejar_from_dict({})
        self.headers = CaseInsensitiveDict()
        self.headers["content-length"] = "0"
        for header in ("date", "server"):
            if header in response.headers:
                self.headers[header] = response.headers[header]


def format_auth_header(auth_header: str | None) -> str:
    if auth_header is not None and len(auth_header) > 75:
        return auth_header[:48] + "..." + auth_header[-24:]


def negotiate_value(response: Response) -> str | None:
    """Extracts the gssapi authentication token from the appropriate header"""
    if hasattr(negotiate_value, "regex"):
        regex = negotiate_value.regex
    else:
        # There's no need to re-compile this EVERY time it is called. Compile
        # it once and you won't have the performance hit of the compilation.
        regex = re.compile(r"Negotiate\s*([^,]*)", re.I)
        negotiate_value.regex = regex

    if response.status_code == 407:
        authreq = response.headers.get("proxy-authenticate", None)
    else:
        authreq = response.headers.get("www-authenticate", None)

    if authreq:
        match_obj = regex.search(authreq)
        if match_obj:
            return base64.b64decode(match_obj.group(1))


def get_certificate_hash(certificate_der: bytes):
    # https://tools.ietf.org/html/rfc5929#section-4.1
    cert = x509.load_der_x509_certificate(certificate_der, default_backend())

    try:
        hash_algorithm = cert.signature_hash_algorithm
    except UnsupportedAlgorithm as e:
        warnings.warn(
            "Failed to get signature algorithm from certificate, "
            f"unable to pass channel bindings: {str(e)}",
            UnknownSignatureAlgorithmOID,
        )
        return

    # if the cert signature algorithm is either md5 or sha1 then use sha256
    # otherwise use the signature algorithm
    if hash_algorithm.name in ["md5", "sha1"]:
        digest = hashes.Hash(hashes.SHA256(), default_backend())
    else:
        digest = hashes.Hash(hash_algorithm, default_backend())

    digest.update(certificate_der)
    certificate_hash = digest.finalize()

    return certificate_hash


def get_channel_bindings_application_data(response: Response):
    """https://tools.ietf.org/html/rfc5929 4. The 'tls-server-end-point' Channel
    Binding Type.

    Gets the application_data value for the 'tls-server-end-point' CBT Type.
    This is ultimately the SHA256 hash of the certificate of the HTTPS endpoint
    appended onto tls-server-end-point. This value is then passed along to the
    kerberos library to bind to the auth response. If the socket is not an SSL
    socket or the raw HTTP object is not a urllib3 HTTPResponse then `None` will
    be returned and the Kerberos auth will use GSS_C_NO_CHANNEL_BINDINGS.
    """

    application_data = None
    raw_response = response.raw

    if isinstance(raw_response, HTTPResponse):
        try:
            socket = raw_response.connection.sock
        except AttributeError:
            warnings.warn(
                "Failed to get raw socket for CBT; has urllib3 impl changed",
                NoCertificateRetrievedWarning,
            )
        else:
            try:
                server_certificate = socket.getpeercert(True)
            except AttributeError:
                pass
            else:
                certificate_hash = get_certificate_hash(server_certificate)
                application_data = b"tls-server-end-point:" + certificate_hash
    else:
        warnings.warn(
            "Requests is running with a non urllib3 backend, cannot retrieve "
            "server certificate for CBT",
            NoCertificateRetrievedWarning,
        )

    return application_data


class HTTPKerberosAuth(AuthBase):
    """Attaches HTTP GSSAPI/Kerberos Authentication to the given Request.
    object."""

    def __init__(
        self,
        mutual_authentication: int = REQUIRED,
        service: str = "HTTP",
        delegate: bool = False,
        force_preemptive: bool = False,
        principal: str | None = None,
        hostname_override: str | None = None,
        sanitize_mutual_error_response: bool = True,
        send_cbt: bool = True,
    ):
        self._context = {}
        self.mutual_authentication = mutual_authentication
        self.delegate = delegate
        self.pos = None
        self.service = service
        self.force_preemptive = force_preemptive
        self.principal = principal
        self.hostname_override = hostname_override
        self.sanitize_mutual_error_response = sanitize_mutual_error_response
        self.auth_done = False

        # Set the CBT values populated after the first response
        self.send_cbt = send_cbt
        self._cbts = {}

    def generate_request_header(
        self, response: Response, host: str, is_preemptive: bool = False
    ) -> str:
        """Generates the GSSAPI authentication token with kerberos.

        If any GSSAPI step fails, raise `KerberosExchangeError` with failure detail.
        """

        # Flags used by kerberos module.
        gssflags = spnego.ContextReq.sequence_detect
        if self.delegate:
            gssflags |= spnego.ContextReq.delegate
        if self.mutual_authentication != DISABLED:
            gssflags |= spnego.ContextReq.mutual_auth

        try:
            kerb_stage = "ctx init"
            # contexts still need to be stored by host, but hostname_override
            # allows use of an arbitrary hostname for the kerberos exchange
            # (eg, in cases of aliased hosts, internal vs external, CNAMEs
            # w/ name-based HTTP hosting)
            kerb_host = (
                self.hostname_override if self.hostname_override is not None else host
            )

            self._context[host] = ctx = spnego.client(
                username=self.principal,
                hostname=kerb_host,
                service=self.service,
                channel_bindings=self._cbts.get(host, None),
                context_req=gssflags,
                protocol="kerberos",
            )

            # if we have a previous response from the server, use it to continue
            # the auth process, otherwise use an empty value
            negotiate_resp_value = None if is_preemptive else negotiate_value(response)

            kerb_stage = "ctx step"
            gss_response = ctx.step(in_token=negotiate_resp_value)

            return f"Negotiate {base64.b64encode(gss_response).decode()}"

        except spnego.exceptions.SpnegoError as e:
            _LOGGER.exception("%s failed", kerb_stage)
            raise KerberosExchangeError(
                f"{kerb_stage} failed: {str(e)}", response=response
            ) from e

    def authenticate_user(self, response: Response, **kwargs: Any):
        """Handles user authentication with gssapi/kerberos."""

        host = urlparse(response.url).hostname
        if response.status_code == 407:
            if (
                "proxies" in kwargs
                and urlparse(response.url).scheme in kwargs["proxies"]
            ):
                host = urlparse(
                    kwargs["proxies"][urlparse(response.url).scheme]
                ).hostname

        try:
            auth_header = self.generate_request_header(response, host)
        except KerberosExchangeError:
            # GSS Failure, return existing response
            return response

        if response.status_code == 407:
            _LOGGER.debug(
                "Proxy-Authorization header: %s", format_auth_header(auth_header)
            )
            response.request.headers["Proxy-Authorization"] = auth_header
        else:
            _LOGGER.debug("Authorization header: %s", format_auth_header(auth_header))
            response.request.headers["Authorization"] = auth_header

        # Consume the content so we can reuse the connection for the next
        # request.
        response.content
        response.raw.release_conn()

        _r = response.connection.send(response.request, **kwargs)
        _r.history.append(response)

        _LOGGER.debug("%r", _r)
        return _r

    def handle_auth_error(self, response: Response, **kwargs: Any) -> Response:
        """Handles 401's and 407's, attempts to use gssapi/kerberos authentication"""

        _LOGGER.debug("Handling %i", response.status_code)
        if negotiate_value(response) is not None:
            return self.authenticate_user(response, **kwargs)
        else:
            _LOGGER.debug("Kerberos is not supported, returning %r", response)
            return response

    def handle_other(self, response: Response) -> Response:
        """Handles all responses with the exception of 401s and 407s.

        This is necessary so that we can authenticate responses if requested.
        """
        _LOGGER.debug("Handling %i", response.status_code)
        if self.mutual_authentication in (REQUIRED, OPTIONAL) and not self.auth_done:
            is_http_error = response.status_code >= 400
            if negotiate_value(response) is not None:
                _LOGGER.debug("Authenticating the server")
                if not self.authenticate_server(response):
                    # Mutual authentication failure when mutual auth is wanted,
                    # raise an exception so the user doesn't use an untrusted
                    # response.
                    _LOGGER.error("Mutual authentication failed")
                    raise MutualAuthenticationError(
                        f"Unable to authenticate {repr(response)}"
                    )

                # Authentication successful
                self.auth_done = True
                _LOGGER.debug("Mutual authentication succeeded, returning %r", response)
                return response

            elif is_http_error or self.mutual_authentication == OPTIONAL:
                if not response.ok:
                    _LOGGER.error(
                        "Mutual authentication unavailable on %i response",
                        response.status_code,
                    )

                if (
                    self.mutual_authentication == REQUIRED
                    and self.sanitize_mutual_error_response
                ):
                    return SanitizedResponse(response)
                else:
                    return response
            else:
                # Unable to attempt mutual authentication when mutual auth is
                # required, raise an exception so the user doesn't use an
                # untrusted response.
                _LOGGER.error("Mutual authentication failed")
                raise MutualAuthenticationError(
                    f"Unable to authenticate {repr(response)}"
                )
        else:
            _LOGGER.debug("Skipping mutual authentication, returning %r", response)
            return response

    def authenticate_server(self, response: Response) -> bool:
        """Uses GSSAPI to authenticate the server.

        Returns `True` on success, `False` on failure.
        """

        response_token = negotiate_value(response)
        _LOGGER.debug(
            "Authenticating server response: %s",
            base64.b64encode(response_token).decode() if response_token else "",
        )

        host = urlparse(response.url).hostname

        try:
            self._context[host].step(in_token=response_token)
        except spnego.exceptions.SpnegoError:
            _LOGGER.exception("Context step failed")
            return False
        return True

    def handle_response(self, response: Response, **kwargs: Any) -> Response:
        """Takes the given response and tries kerberos-auth, as needed."""
        num_401s = kwargs.pop("num_401s", 0)
        num_407s = kwargs.pop("num_407s", 0)

        # Check if we have already tried to get the CBT data value
        if self.send_cbt:
            host = urlparse(response.url).hostname
            # If we haven't tried, try getting it now
            if host not in self._cbts:
                cbt_application_data = get_channel_bindings_application_data(response)
                if cbt_application_data:
                    self._cbts[host] = spnego.channel_bindings.GssChannelBindings(
                        application_data=cbt_application_data,
                    )
                else:
                    # Store None so we don't waste time next time
                    self._cbts[host] = None

        if self.pos is not None:
            # Rewind the file position indicator of the body to where
            # it was to resend the request.
            response.request.body.seek(self.pos)

        if response.status_code == 401 and num_401s < 2:
            # 401 Unauthorized. Handle it, and if it still comes back as 401,
            # that means authentication failed.
            _r = self.handle_auth_error(response, **kwargs)
            num_401s += 1
            _LOGGER.debug("Seen %i 401 responses", num_401s)
            return self.handle_response(_r, num_401s=num_401s, **kwargs)
        elif response.status_code == 401 and num_401s >= 2:
            # Still receiving 401 responses after attempting to handle them.
            # Authentication has failed. Return the 401 response.
            return response
        elif response.status_code == 407 and num_407s < 2:
            # 407 Unauthorized. Handle it, and if it still comes back as 407,
            # that means authentication failed.
            _r = self.handle_auth_error(response, **kwargs)
            num_407s += 1
            _LOGGER.debug("Seen %i 407 responses", num_401s)
            return self.handle_response(_r, num_407s=num_407s, **kwargs)
        elif response.status_code == 407 and num_407s >= 2:
            # Still receiving 407 responses after attempting to handle them.
            # Authentication has failed. Return the 407 response.
            return response
        else:
            return self.handle_other(response)

    def deregister(self, response: Response) -> None:
        """Deregisters the response handler"""
        response.request.deregister_hook("response", self.handle_response)

    def __call__(self, request: Request):
        if self.force_preemptive and not self.auth_done:
            # add Authorization header before we receive a 401
            # by the 401 handler
            host = urlparse(request.url).hostname
            auth_header = self.generate_request_header(None, host, is_preemptive=True)
            _LOGGER.debug(
                "Sending preemptive authorization header: %s",
                format_auth_header(auth_header),
            )
            request.headers["Authorization"] = auth_header

        request.register_hook("response", self.handle_response)
        try:
            self.pos = request.body.tell()
        except AttributeError:
            # In the case of HTTPKerberosAuth being reused and the body
            # of the previous request was a file-like object, pos has
            # the file position of the previous body. Ensure it's set to
            # None.
            self.pos = None
        return request
