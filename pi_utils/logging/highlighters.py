from rich.highlighter import RegexHighlighter



class LevelHighlighter(RegexHighlighter):
    """Apply style to log levels."""
    base_style = "level."
    highlights = [
        r"(?P<debug_level>DEBUG)",
        r"(?P<info_level>INFO)",
        r"(?P<warning_level>WARNING)",
        r"(?P<error_level>ERROR)",
        r"(?P<critical_level>CRITICAL)",
    ]


class DefaultHighlighter(RegexHighlighter):
    """Applies style from multiple highlighters."""
    base_style = "log."
    highlights = (
        LevelHighlighter.highlights
    )