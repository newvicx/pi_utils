from typer import Exit, Option, Typer
from typer.rich_utils import _get_rich_console

from pi_utils.logging.setup import setup_console_logging
from pi_utils.__version__ import __version__



app = Typer(add_completion=False, no_args_is_help=True)


def version_callback(value: bool):
    if value:
        print(__version__)
        raise Exit()


@app.callback()
def main(
    version: bool = Option(
        None,
        "--version",
        "-v",
        # A callback is necessary for Typer to call this without looking for additional
        # commands and erroring when excluded
        callback=version_callback,
        help="Display the current version.",
        is_eager=True,
    ),
    debug: bool = Option(
        default=False,
        help="Enable debug logging."
    )
) -> None:
    app.console = _get_rich_console(stderr=False)
    setup_console_logging(debug)