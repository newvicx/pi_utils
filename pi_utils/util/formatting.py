def format_docstring(description: str) -> str:
    """Takes a docstring formatted string and converts it to a string with no
    line breaks or extra spaces.
    """
    return " ".join(segment.strip() for segment in description.splitlines())


def snake_to_camel(string: str) -> str:
    """Covert snake case (arg_a) to camel case (ArgA)."""
    return ''.join(word.capitalize() for word in string.split('_'))