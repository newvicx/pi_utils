import csv
import datetime
import io
import itertools
import json
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, TextIO

from pi_utils.sdk.client import SDKClient
from pi_utils.sdk.types import SDKSubBatch, SDKUnitBatch, SubBatchInfo, UnitBatchInfo
from pi_utils.util.time import fuzzy_parse


# Prop enums define a label, getter, and priority
# - The label is present in all output formats
# - The getter is a callable that extracts the property from the COM object
# - The priority defines the column ordering when exporting to CSV. Identical
#   labels in different props must have the same priority
class UnitBatchProps(Enum):
    """Properties to extract from a unit batch."""

    ID = ("unique_id", lambda batch: batch.UniqueID, 8)
    START = (
        "start_time",
        lambda batch: fuzzy_parse(batch.StartTime.LocalDate.ToString())
        .replace(tzinfo=None)
        .isoformat(),
        1,
    )
    END = (
        "end_time",
        lambda batch: fuzzy_parse(batch.EndTime.LocalDate.ToString())
        .replace(tzinfo=None)
        .isoformat(),
        2,
    )
    UNIT = ("unit", lambda batch: batch.PIUnit.Name, 2)
    BATCH = ("batch", lambda batch: batch.BatchID, 3)
    PRODUCT = ("product", lambda batch: batch.Product, 6)
    PROCEDURE = ("procedure", lambda batch: batch.ProcedureName, 7)


class SubBatchProps(Enum):
    """Properties to extract from a sub batch."""

    ID = ("unique_id", lambda batch: batch.UniqueID, 8)
    START = (
        "start_time",
        lambda batch: fuzzy_parse(batch.StartTime.LocalDate.ToString())
        .replace(tzinfo=None)
        .isoformat(),
        1,
    )
    END = (
        "end_time",
        lambda batch: fuzzy_parse(batch.EndTime.LocalDate.ToString())
        .replace(tzinfo=None)
        .isoformat(),
        2,
    )
    NAME = ("name", lambda batch: batch.Name, 5)


ALL_LABELS = [
    label
    for _, label in sorted(
        set(
            itertools.chain.from_iterable(
                [
                    [(enum.value[2], enum.value[0]) for enum in prop]
                    for prop in (UnitBatchProps, SubBatchProps)
                ]
            )
        )
    )
]


@dataclass
class BatchInfo:
    """Data model returned with batch search result."""

    info: List[UnitBatchInfo]

    def as_csv(self) -> TextIO:
        """Convert batch info to CSV format.

        Sub batches are associated via the 'parent_id' property and sub batches
        will also inherit undefined properties from the unit batch such as 'unit',
        'batch' etc.

        Returns:
            TextIO: An in memory buffer that can be written to a file or re-read
                with a CSV reader.
        """
        unpacked = {label: [] for label in ALL_LABELS}
        unpacked.setdefault("parent_id", [])
        unpacked.setdefault("type", [])

        def unpack(
            batches: List[UnitBatchInfo] | List[SubBatchInfo],
            parent: Dict[str, str] = {},
        ) -> None:
            for batch in batches:
                for label, val in parent.items():
                    if label not in batch:
                        batch[label] = val

                for label in ALL_LABELS:
                    unpacked[label].append(batch.get(label))

                unpacked["parent_id"].append(parent.get("unique_id"))
                if parent:
                    unpacked["type"].append("sub")
                else:
                    unpacked["type"].append("unit")

                sub_batches = batch.get("sub_batches")
                if sub_batches:
                    unpack(sub_batches, parent=batch)

        unpack(self.info)

        buffer = io.StringIO(newline="")
        writer = csv.writer(
            buffer, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        headers = list(unpacked.keys())
        writer.writerow(headers)
        for row in zip(*[unpacked[header] for header in headers]):
            writer.writerow(row)

        buffer.seek(0)
        return buffer

    def json(self, *args: Any, **kwargs: Any) -> str:
        """Convert batch data to a JSON string. Accepts any valid arguments to
        `json.dumps`.
        """
        return json.dumps(self.info, *args, **kwargs)

    def __iter__(self) -> Iterable[UnitBatchInfo]:
        for batch in self.info:
            yield batch


def batch_search(
    client: SDKClient,
    unit_id: str = "*",
    start_time: str | datetime.datetime = "-30d",
    end_time: str | datetime.datetime = "*",
    batch_id: str = "*",
    product: str = "*",
    procedure: str = "*",
    sub_batch: str = "*",
    exclude_sub_batches: bool = False,
) -> BatchInfo:
    """Execute a PI batch search against the module DB.

    Args:
        client: The `SDKClient` to execute the search.
        unit_id: The unit id mask for the search. Supports wildcards (*) and
            multiple values (separated by comma)
        start_time: The start time of the search. Supports relative times. Defaults
            to last 30 days.
        end_time: The end time of the search. Supports relative times. Defaults
            to now.
        batch_id: The batch id mask for the search. Supports wildcards (*) and
            multiple values (separated by comma)
        product: The product mask for the search. Supports wildcards (*) and
            multiple values (separated by comma)
        procedure: The procedure mask for the search. Supports wildcards (*) and
            multiple values (separated by comma)
        sub_batch: The sub batch mask for the search. Supports wildcards (*) and
            multiple values (separated by comma)
        exclude_sub_batches: If `True`, sub batches will not be returned, only
            unit batches.

    Returns:
        List[UnitBatchInfo]

    Raises:
        ConnectionError: Unable to connect to PI server.
    """

    def parse_batches(
        batches: List[SDKUnitBatch | SDKSubBatch],
    ) -> Iterable[UnitBatchInfo | SubBatchInfo]:
        for batch in batches:
            props = {}
            if isinstance(batch, client.unit_batch):
                spec = UnitBatchProps
            else:
                spec = SubBatchProps

            for prop in spec:
                label, get = prop.value[0:2]
                try:
                    props[label] = get(batch)
                except AttributeError:
                    props[label] = None

            props.setdefault("sub_batches", [])
            if not exclude_sub_batches and batch.PISubBatches.Count > 0:
                props["sub_batches"] = list(
                    parse_batches(
                        [client.sub_batch(sb) for sb in batch.PISubBatches],
                    )
                )
            yield props

    start_time = (
        start_time.isoformat()
        if isinstance(start_time, datetime.datetime)
        else start_time.lower()
    )
    end_time = (
        end_time.isoformat()
        if isinstance(end_time, datetime.datetime)
        else end_time.lower()
    )

    units = [unit.strip() for unit in unit_id.split(",")]
    batch_ids = [batch.strip() for batch in batch_id.split(",")]
    products = [product_.strip() for product_ in product.split(",")]
    procedures = [procedure_.strip() for procedure_ in procedure.split(",")]
    sub_batches = [batch.strip() for batch in sub_batch.split(",")]

    with client.get_connection() as connection:
        db = connection.PIModuleDB
        batches: List[SDKUnitBatch] = list(
            itertools.chain.from_iterable(
                [
                    [
                        client.unit_batch(batch)
                        for batch in db.PIUnitBatchSearch(
                            start_time,
                            end_time,
                            unit,
                            batch,
                            product_,
                            procedure_,
                            sub_batch_,
                        )
                    ]
                    for unit, batch, product_, procedure_, sub_batch_
                    in itertools.product(
                        units, batch_ids, products, procedures, sub_batches
                    )
                ]
            )
        )

        return BatchInfo(info=list(parse_batches(batches=batches)))
