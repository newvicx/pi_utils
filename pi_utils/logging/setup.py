import logging

from pi_utils.logging.handlers import ConsoleHandler



def setup_console_logging(debug: bool = False) -> None:
    """Configure console logging for the CLI."""
    styles = {
        "log.debug_level": "green",
        "log.info_level": "cyan",
        "log.warning_level": "yellow3",
        "log.error_level": "red3",
        "log.critical_level": "bright_red"
    }
    handler = ConsoleHandler(styles=styles, level=logging.INFO)
    logging.basicConfig(
        level=logging.INFO if not debug else logging.DEBUG,
        handlers=[handler]
    )