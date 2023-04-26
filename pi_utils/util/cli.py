from rich.panel import Panel
from typer import Exit
from typer.rich_utils import (
    highlighter,
    ALIGN_ERRORS_PANEL,
    ERRORS_PANEL_TITLE,
    STYLE_ERRORS_PANEL_BORDER,
)


def exit_with_error(message, code=1):
    """Utility to print a stylized error message and exit with a non-zero code."""
    from pi_utils.cli.root import app

    app.console.print(
        Panel(
            highlighter(message),
            border_style=STYLE_ERRORS_PANEL_BORDER,
            title=ERRORS_PANEL_TITLE,
            title_align=ALIGN_ERRORS_PANEL,
        )
    )
    raise Exit(code)


def exit_with_success(message, **kwargs):
    """Utility to print a stylized success message and exit with a zero code."""
    from pi_utils.cli.root import app

    kwargs.setdefault("style", "green")
    app.console.print(message, style="green")
    raise Exit(0)
