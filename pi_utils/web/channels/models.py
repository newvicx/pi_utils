from datetime import datetime
from typing import Any, Dict, List

from pendulum.datetime import DateTime
from pydantic import BaseModel, Field, root_validator, validator

from pi_utils.util.formatting import snake_to_camel
from pi_utils.util.json import json_dumps, json_loads
from pi_utils.util.time import isoparse


class BaseChannelModel(BaseModel):
    """Base model for data returned from 'streamsets/channel' endpoint.

    Sample Message (Raw)
    ```json
    {
    "Links": {},
    "Items": [
        {
        "WebId": "F2DXEloryy_bV0GzilxLXH31pgjowAAAQUJDX1BJX09QU1xBSUM2ODEwNTkuUFY",
        "Name": "AI1.PV",
        "Path": "\\\\OSI_PI_OPS\\AI1.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2023-01-01T00:00:00Z",
            "Value": 50,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        },
        {
        "WebId": "F2DXEloryy_bV0GzilxLXH31pgcAQAAAQUJDX1BJX09QU1xGSVExNDAxMi5QVg",
        "Name": "AI2.PV",
        "Path": "\\\\OSI_PI_OPS\\AI2.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2023-01-01T00:00:00Z",
            "Value": 60,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        },
        {
        "WebId": "F2DXEloryy_bV0GzilxLXH31pgLAcAAAQUJDX1BJX09QU1xUSTE0MDEzLlBW",
        "Name": "AI3.PV",
        "Path": "\\\\OSI_PI_OPS\\AI3.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2023-01-01T00:00:00Z",
            "Value": 70,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        }
    ]
    }
    ```

    Target conversion to
    ```json
    [
        {
            'name': 'AI1.PV',
            'web_id': 'F2DXEloryy_bV0GzilxLXH31pgjowAAAQUJDX1BJX09QU1xBSUM2ODEwNTkuUFY',
            'items': [
                {'timestamp': '2022-01-01T00:00:00', 'value': 50}
            ]
        },
        {
            'name': 'AI2.PV',
            'web_id': 'F2DXEloryy_bV0GzilxLXH31pgcAQAAAQUJDX1BJX09QU1xGSVExNDAxMi5QVg',
            'items': [
                {'timestamp': '2022-01-01T00:00:00', 'value': 60}
            ]
        },
        {
            'name': 'AI3.PV',
            'web_id': 'F2DXEloryy_bV0GzilxLXH31pgLAcAAAQUJDX1BJX09QU1xUSTE0MDEzLlBW',
            'items': [
                {'timestamp': '2022-01-01T00:00:00', 'value': 70}
            ]
        }
    ]
    ```
    """

    class Config:
        alias_generator = snake_to_camel
        extra = "ignore"
        allow_arbitrary_types = True
        json_dumps = json_dumps
        json_loads = json_loads


class ChannelSubItem(BaseChannelModel):
    """Model for sub items containing timeseries data for a particular WebId."""

    timestamp: DateTime
    value: Any
    good: bool = Field(exclude=True)

    @validator("timestamp", pre=True)
    def _parse_timestamp(cls, v: str) -> DateTime:
        """Parse timestamp (str) to DateTime."""
        if not isinstance(v, str):
            raise TypeError("Expected type str.")
        try:
            return isoparse(v).replace(tzinfo=None)
        except Exception as e:
            raise ValueError("Cannot parse timestamp.") from e

    @root_validator
    def _format_content(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Replace value of not 'good' items with `None`."""
        value, good = v.get("value"), v.get("good")
        if not good:
            v["value"] = None
        elif value is not None:
            if isinstance(value, dict):
                v["value"] = value["Name"]

        return v

    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, ChannelSubItem)):
            raise TypeError(
                f"'>' not supported between instances of {type(self)} and {type(__o)}"
            )
        if isinstance(__o, ChannelSubItem):
            return self.timestamp > __o.timestamp
        else:
            return self.timestamp > __o

    def __ge__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, ChannelSubItem)):
            raise TypeError(
                f"'>=' not supported between instances of {type(self)} and {type(__o)}"
            )
        if isinstance(__o, ChannelSubItem):
            return self.timestamp >= __o.timestamp
        else:
            return self.timestamp >= __o

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, ChannelSubItem)):
            raise TypeError(
                f"'<' not supported between instances of {type(self)} and {type(__o)}"
            )
        if isinstance(__o, ChannelSubItem):
            return self.timestamp < __o.timestamp
        else:
            return self.timestamp < __o

    def __le__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, ChannelSubItem)):
            raise TypeError(
                f"'<=' not supported between instances of {type(self)} and {type(__o)}"
            )
        if isinstance(__o, ChannelSubItem):
            return self.timestamp <= __o.timestamp
        else:
            return self.timestamp <= __o


class ChannelItem(BaseChannelModel):
    """Model for single top level item pertaining to a WebId."""

    name: str
    web_id: str
    items: List[ChannelSubItem]

    @validator("items")
    def _sort_items(cls, v: List[ChannelSubItem]) -> List[ChannelSubItem]:
        # Timeseries values for a given WebId are not guarenteed to be in
        # chronological order so we sort the items on the timestamp to ensure
        # they are
        # https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/topics/channels.html
        return sorted(v)


class ChannelMessage(BaseChannelModel):
    """Model for streamset messages received from PI Web API."""

    items: List[ChannelItem]
