import logging
from typing import Dict

from rich.console import Console
from rich.highlighter import Highlighter
from rich.theme import Theme

from pi_utils.logging.highlighters import DefaultHighlighter


class ConsoleHandler(logging.StreamHandler):
    """The default console handler which highlights log levels."""

    def __init__(
        self,
        stream=None,
        highlighter: Highlighter = DefaultHighlighter,
        styles: Dict[str, str] = None,
        level: int | str = logging.NOTSET,
    ):
        super().__init__(stream=stream)

        highlighter = highlighter()
        theme = Theme(styles, inherit=False)

        self.level = level
        self.console = Console(highlighter=highlighter, theme=theme, file=self.stream)

    def emit(self, record: logging.LogRecord):
        try:
            message = self.format(record)
            self.console.print(message, soft_wrap=True)
        except RecursionError:
            # This was copied over from logging.StreamHandler().emit()
            # https://bugs.python.org/issue36272
            raise
        except Exception:
            self.handleError(record)
