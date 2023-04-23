import csv
import functools
import itertools
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pydantic import SecretStr
from rich.json import JSON
from rich.progress import track
from rich.prompt import Prompt
from typer import Option, Typer

from pi_utils.cli.root import app
from pi_utils.settings import SDKSettings, WebSettings
from pi_utils.sdk.client import get_sdk_client
from pi_utils.sdk.ops import batch_search
from pi_utils.util.cli import exit_with_error, exit_with_success
from pi_utils.util.files import load_csv_col, write_csv
from pi_utils.web.client import get_web_client
from pi_utils.web.ops import find_tags, get_interpolated, get_recorded



help_message = """
    Commands for querying batch data.
"""

batch_app = Typer(name="batch", help=help_message)
app.add_typer(batch_app)

_LOGGER = logging.getLogger("pi_utils.cli")


@batch_app.command()
def search(
    unit_id: Optional[str] = Option(
        default="*",
        help=(
            "The unit id mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)"
        )
    ),
    start_time: Optional[str] = Option(
        default="*-30d",
        help="The start time of the search. Supports relative times."
    ),
    end_time: Optional[str] = Option(
        default="*",
        help="The end time of the search. Supports relative times."
    ),
    batch_id: Optional[str] = Option(
        default="*",
        help=(
            "The batch id mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)."
        )
    ),
    product: Optional[str] = Option(
        default="*",
        help=(
            "The product mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)."
        )
    ),
    procedure: Optional[str] = Option(
        default="*",
        help=(
            "The procedure mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)."
        )
    ),
    sub_batch: Optional[str] = Option(
        default="*",
        help=(
            "The sub batch mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)."
        )
    ),
    exclude_sub_batches: bool = Option(
        default=False
    ),
    output_file: Optional[Path] = Option(
        default=None,
        help=(
            "Output file for queried data. Must be a '.csv' filepath. If not "
            "defined, the query results will print to the console."
        ),
        dir_okay=False
    ),
    server: Optional[str] = Option(
        default=None,
        help=(
            "The PI server to query against. Can be set through the "
            "PI_UTILS_SDK_SERVER environment variable."
        )
    ),
    path: Optional[Path] = Option(
        default=None,
        help=(
            "The filepath to the to the SDK assembly file. Can be set through "
            "the PI_UTILS_SDK_PATH environment variable."
        ),
        exists=True,
        dir_okay=False
    )
) -> None:
    """Query batch info through PISDK."""
    runtime_settings = {
        "server": server,
        "path": path
    }
    runtime_settings = {k:v for k, v in runtime_settings.items() if v is not None}
    settings = SDKSettings(**runtime_settings)
    
    if not settings.server:
        exit_with_error(
            "Server is not defined. Use the '--server' option or set the "
            "PI_UTILS_SDK_SERVER env var."
        )

    client = get_sdk_client(
        server=settings.server,
        path=settings.path,
        max_connections=1
    )

    if output_file is not None:
        if output_file.suffix.lower() != ".csv":
            exit_with_error("Invalid path for output file. Must be a '.csv' file.")
        os.makedirs(output_file.parent, exist_ok=True)
    
    batch_info = batch_search(
        client=client,
        unit_id=unit_id,
        start_time=start_time,
        end_time=end_time,
        batch_id=batch_id,
        product=product,
        procedure=procedure,
        sub_batch=sub_batch,
        exclude_sub_batches=exclude_sub_batches
    )

    if not output_file:
        app.console.print(JSON(batch_info.json()))
        exit_with_success("Done")
    
    buffer = batch_info.as_csv()
    with open(output_file, "w", newline="") as fh:
        while True:
            data = buffer.read(4096)
            if not data:
                break
            fh.write(data)

    exit_with_success("Done")


@batch_app.command()
def data(
    unit_id: str = Option(
        default=...,
        help=(
            "The unit id mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)"
        )
    ),
    output_dir: Optional[Path] = Option(
        default=None,
        help=(
            "Path to output directory for batch data. If the path does not exist "
            "it will be created."
        ),
        file_okay=False
    ),
    tag: Optional[List[str]] = Option(
        default=None,
        help=(
            "The tag(s) to export data for. Multiple tags can be specified with "
            "multiple instances of this option."
        )
    ),
    tag_file: Optional[Path] = Option(
        default=None,
        help=(
            "The path to a '.csv' file containing all tags to export data for. "
            "Reads from the 'tag' column."
        ),
        exists=True,
        dir_okay=False,
    ),
    compressed: bool = Option(
        default=False,
        help="Retrieve compressed data."
    ),
    interval: Optional[int] = Option(
        default=None,
        help=(
            "The time interval (in seconds) between successive rows. Applies "
            "to interpolated data only."
        )
    ),
    scan_rate: Optional[int] = Option(
        default=None,
        help=(
            "The scan rate of the PI server (in seconds). This along with "
            "'--request-chunk-size' helps slice the time range. This does not "
            "need to be exact but it should not be orders of magnitude off. "
            "It's better to air on the low side for this value. Applies to "
            "recorded data only."
        )
    ),
    request_chunk_size: Optional[int] = Option(
        default=None,
        help=(
            "The maximum number of rows to be returned from a single HTTP "
            "request. This splits up the time range into successive pieces."
        )
    ),
    start_time: Optional[str] = Option(
        default="*-30d",
        help="The start time of the search. Supports relative times."
    ),
    end_time: Optional[str] = Option(
        default="*",
        help="The end time of the search. Supports relative times."
    ),
    batch_id: Optional[str] = Option(
        default="*",
        help=(
            "The batch id mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)."
        )
    ),
    product: Optional[str] = Option(
        default="*",
        help=(
            "The product mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)."
        )
    ),
    procedure: Optional[str] = Option(
        default="*",
        help=(
            "The procedure mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)."
        )
    ),
    sub_batch: Optional[str] = Option(
        default="*",
        help=(
            "The sub batch mask for the search. Supports wildcards (*) and "
            "multiple values (separated by comma)."
        )
    ),
    exclude_sub_batches: bool = Option(
        default=True,
        help="Exclude sub batches from output. Only unit batches will be exported."
    ),
    server: Optional[str] = Option(
        default=None,
        help=(
            "The PI server to query against. Can be set through the "
            "PI_UTILS_SDK_SERVER environment variable."
        )
    ),
    path: Optional[Path] = Option(
        default=None,
        help=(
            "The filepath to the to the SDK assembly file. Can be set through "
            "the PI_UTILS_SDK_PATH environment variable."
        ),
        exists=True,
        dir_okay=False
    ),
    host: Optional[str] = Option(
        default=None,
        help=(
            "The hostname to connect to the web API. Can be set through the "
            "PI_UTILS_WEB_HOST environment variable."
        )
    ),
    port: Optional[int] = Option(
        default=None,
        help=(
            "The port to connect to the web API. Can be set through the "
            "PI_UTILS_WEB_PORT environment variable."
        )
    ),
    tls: bool = Option(
        default=True,
        help="Connect to the web API over TLS."
    ),
    dataserver: Optional[str] = Option(
        default=None,
        help=(
            "The PI server name to connect to through the web API. Can be set "
            "through the PI_UTILS_WEB_DATASERVER environment variable."
        )
    ),
    login: bool = Option(
        default=False,
        help="Prompt for kerberos principal."
    ),
    mutual_authentication: bool = Option(
        default=True,
        help="Require mutual authentication from server in kerberos auth."
    ),
    service: Optional[str] = Option(
        default=None,
        help=(
            "The service principle protocol to use (such as HTTP or HTTPS) for "
            "kerberos authentication. Can be set through the PI_UTILS_WEB_SERVICE "
            "environment variable."
        )
    ),
    delegate: bool = Option(
        default=False,
        help=(
            "Indicates that the user's credentials are to be delegated to the server. "
            "Can be set through the PI_UTILS_WEB_DELEGATE environment variable."
        )
    ),
    hostname_override: Optional[str] = Option(
        default=None,
        help=(
            "If communicating with a host whose DNS name doesn't match its "
            "kerberos hostname (eg, behind a content switch or load balancer), "
            "the hostname used for the Kerberos GSS exchange can be overridden. "
            "Defaults to `None`."
        )
    ),
    send_cbt: bool = Option(
        default=True,
        help=(
            "Automatically attempt to bind the authentication token with the "
            "channel binding data when connecting over a TLS connection."
        )
    )
) -> None:
    """Export interpolated or compressed batch data for any number of PI tags
    based on a batch search query.
    """
    output_dir = output_dir or Path("./output")
    os.makedirs(output_dir, exist_ok=True)

    if not tag and not tag_file:
        exit_with_error("No PI tags defined. Use the '--tag' or '--tag-file' option.")
    if tag and tag_file:
        _LOGGER.info("Ignoring '--tag' option, '--tag-file' was provided")
    if tag_file is not None:
        if tag_file.suffix.lower() != ".csv":
            exit_with_error("Invalid file format for '--tag-file'. Must be a '.csv' file.")
        try:
            tag = load_csv_col(tag_file, "tag")
        except RuntimeError as e:
            exit_with_error(str(e))
    
    runtime_settings = {
        "server": server,
        "path": path,
        "host": host,
        "port": port,
        "tls": tls,
        "dataserver": dataserver,
        "service": service,
        "delegate": delegate,
        "hostname_override": hostname_override,
        "send_cbt": send_cbt,
        "mutual_authentication": 1 if mutual_authentication else 3
    }
    if login:
        principal = SecretStr(Prompt.ask("Enter Principal", password=True))
        runtime_settings.update({"principal": principal.get_secret_value()})

    runtime_settings = {k:v for k, v in runtime_settings.items() if v is not None}
    sdk_settings = SDKSettings(**runtime_settings)
    web_settings = WebSettings(**runtime_settings)
    
    if not sdk_settings.server:
        exit_with_error(
            "Server is not defined. Use the '--server' option or set the "
            "PI_UTILS_SDK_SERVER env var."
        )
    if not web_settings.base_url:
        exit_with_error(
            "Base URL is not defined. Use the '--base-url' option or set the "
            "PI_UTILS_WEB_BASE_URL env var."
        )

    sdk_client = get_sdk_client(
        server=sdk_settings.server,
        path=sdk_settings.path,
        max_connections=1
    )
    reader = csv.DictReader(
        batch_search(
            client=sdk_client,
            unit_id=unit_id,
            start_time=start_time,
            end_time=end_time,
            batch_id=batch_id,
            product=product,
            procedure=procedure,
            sub_batch=sub_batch,
            exclude_sub_batches=exclude_sub_batches
        ).as_csv(),
        delimiter=',',
        quotechar='|',
        quoting=csv.QUOTE_MINIMAL
    )
    times = [
        (
            datetime.fromisoformat(row["start_time"]),
            datetime.fromisoformat(row["end_time"] or datetime.now().isoformat())
        ) for row in reader
    ]

    with get_web_client(
        host=web_settings.host,
        port=web_settings.port,
        tls=web_settings.tls,
        verify=web_settings.verify,
        headers=web_settings.headers,
        cookies=web_settings.cookies,
        kerberos=web_settings.kerberos,
        **web_settings.kerberos_settings
    ) as web_client:
        mapped, unmapped = find_tags(client=web_client, tags=tag, dataserver=dataserver)
        web_ids = [web_id for _, web_id in mapped]
        # Header includes unmapped tags. The output data file is padded with
        # null values.
        header = list(itertools.chain(["timestamp"], [tag for tag, _ in mapped], unmapped))

        if compressed:
            buffer = functools.partial(
                get_recorded,
                client=web_client,
                web_ids=web_ids,
                request_chunk_size=request_chunk_size,
                scan_rate=scan_rate
            )
        else:
            buffer = functools.partial(
                get_interpolated,
                client=web_client,
                web_ids=web_ids,
                request_chunk_size=request_chunk_size,
                interval=interval
            )
        
        for start_time, end_time in track(times, description=f"Downloading {len(times)} batches..."):
            filepath = output_dir.joinpath(f"{int(start_time.timestamp()*1000)}.csv")
            write_csv(
                filepath=filepath,
                buffer=buffer(start_time=start_time, end_time=end_time),
                header=header,
                pad=len(unmapped)
            )
    
    exit_with_success(f"Files saved to '{str(output_dir.absolute())}'")