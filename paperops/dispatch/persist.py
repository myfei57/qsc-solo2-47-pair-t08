"""Small crash-safe JSONL helpers shared by the durable dispatch files."""

import json
import os
from typing import Any

Row = dict[str, Any]


def append_row(path: str, row: Row) -> None:
    """Append one JSON row and force it to disk before returning."""
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, ensure_ascii=True))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_rows(path: str) -> list[Row]:
    """Read back every JSON row in append order."""
    if not os.path.exists(path):
        return []
    rows: list[Row] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows
