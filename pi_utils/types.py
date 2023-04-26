from datetime import datetime
from typing import Any, Dict, List, Tuple, Type


JSONPrimitive = str | int | float | bool | None
JSONContent = JSONPrimitive | List["JSONContent"] | Dict[str, "JSONContent"]

TimeseriesRow = Tuple[datetime, List[Any]]
