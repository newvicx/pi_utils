import json
from typing import Any

import orjson



def json_loads(v: str | bytes) -> Any:
    """JSON decoder which uses orjson for bytes and builtin json for str."""
    match v:
        case str():
            return json.loads(v)
        case bytes():
            return orjson.loads(v)
        case _:
            raise TypeError(f"Expected str | bytes, got {type(v)}")


def json_dumps(obj: Any, **dumps_kwargs) -> str:
    """JSON encoder which uses orjson for serializing data."""
    try:
        return orjson.dumps(obj, **dumps_kwargs).decode()
    except TypeError:
        return json.dumps(obj, **dumps_kwargs)