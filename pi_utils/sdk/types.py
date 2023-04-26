from collections.abc import Mapping
from typing import Dict, List, Protocol, Union


SubBatchInfo = Dict[str, str | List["SubBatchInfo"]]
UnitBatchInfo = Dict[str, str | List["SubBatchInfo"]]


class SDKTime(Protocol):
    @property
    def LocalDate(self) -> "SDKTime":
        ...

    def ToString(self) -> str:
        ...


class SDKSubBatches(Protocol):
    @property
    def Count(self) -> int:
        ...


class SDKUnit(Protocol):
    @property
    def Name(self) -> str:
        ...


class SDKSubBatch(Protocol):
    def __init__(self, batch: Union["SDKUnitBatch", "SDKSubBatch"]) -> None:
        ...

    @property
    def UniqueID(self) -> str:
        ...

    @property
    def Name(self) -> str:
        ...

    @property
    def StartTime(self) -> SDKTime:
        ...

    @property
    def EndTime(self) -> SDKTime:
        ...

    @property
    def PISubBatches(self) -> SDKSubBatches:
        ...


class SDKUnitBatches(Protocol):
    @property
    def Count(self) -> int:
        ...


class SDKUnitBatch(Protocol):
    @property
    def UniqueID(self) -> str:
        ...

    @property
    def BatchID(self) -> str:
        ...

    @property
    def Product(self) -> str:
        ...

    @property
    def ProcedureName(self) -> str:
        ...

    @property
    def StartTime(self) -> SDKTime:
        ...

    @property
    def EndTime(self) -> SDKTime:
        ...

    @property
    def PISubBatches(self) -> SDKSubBatches:
        ...

    @property
    def PIUnit(self) -> SDKUnit:
        ...


class SDKModuleDB(Protocol):
    def PIUnitBatchSearch(
        self,
        start_time: str,
        end_time: str,
        unit_id: str,
        batch_id: str,
        product: str,
        procedure: str,
        sub_batch: str,
    ) -> SDKUnitBatches:
        ...


class SDKConnection(Protocol):
    @property
    def Connected(self) -> bool:
        ...

    @property
    def PIModuleDB(self) -> SDKModuleDB:
        ...

    def Open(self) -> None:
        ...

    def Close(self) -> None:
        ...


class SDK(Protocol):
    @property
    def Servers(self) -> Mapping[str, SDKConnection]:
        ...
