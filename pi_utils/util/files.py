import csv
from collections.abc import Iterable
from pathlib import Path
from typing import List

from pi_utils.types import TimeseriesRow



def load_csv_col(filepath: Path, col: str) -> List[str]:
    """Load a column as a list from a CSV file."""
    try:
        with open(filepath, "r", encoding="utf-8-sig") as fh:
            return [
                row[col] for row in
                csv.DictReader(fh, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            ]
    except KeyError:
        pass    
    raise RuntimeError(f"'{col}' column not found.")


def write_csv(
    filepath: Path,
    buffer: Iterable[TimeseriesRow],
    header: List[str],
    pad: int = 0
) -> None:
    """Write timeseries data to a CSV file."""
    padding = [None] * pad
    with open(filepath, "w", newline="") as fh:
        writer = csv.writer(fh, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        for timestamp, data in buffer:
            data.extend(padding)
            writer.writerow((timestamp.isoformat(), *data))