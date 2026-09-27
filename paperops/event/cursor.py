"""Durable progress markers, one file per named subscriber.

A cursor says "this subscriber has processed every event up to and including
this stream position". The move to a new position is written atomically and
only after the event's handlers have returned, so a redelivery after a crash
lands on the exact event whose effect was not yet acknowledged.
"""

import json
import os
import re
from typing import Any, Mapping

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class Cursor:
    """The stream position one subscriber has processed up to."""

    def __init__(self, name: str, offset: int = 0, path: str | None = None) -> None:
        self._name = name
        self._offset = int(offset)
        self._path = path

    @property
    def name(self) -> str:
        return self._name

    @property
    def offset(self) -> int:
        return self._offset

    def advance(self, offset: int) -> None:
        """Move the marker forward and make the move durable."""
        if offset <= self._offset:
            return
        self._offset = int(offset)
        self._persist()

    def to_row(self) -> dict[str, int | str]:
        return {"name": self._name, "offset": self._offset}

    @classmethod
    def from_row(cls, row: Mapping[str, Any], path: str | None = None) -> "Cursor":
        name = str(row["name"])
        validate_name(name)
        return cls(name, int(row.get("offset", 0)), path)

    def _persist(self) -> None:
        if self._path is None:
            return
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(self.to_row(), handle, sort_keys=True)
        os.replace(tmp, self._path)


def validate_name(name: str) -> None:
    """Reject names that would not map cleanly to one cursor file."""
    if not isinstance(name, str) or not _NAME.match(name) or name in {".", ".."}:
        raise ValueError(f"invalid subscriber name {name!r}")
    if os.sep in name or (os.altsep and os.altsep in name):
        raise ValueError(f"invalid subscriber name {name!r}")


class CursorStore:
    """Loads and keeps the cursor of every named subscriber."""

    DIR_NAME = "cursors"

    def __init__(self, directory: str | None = None) -> None:
        self._directory = directory
        self._cursors: dict[str, Cursor] = {}
        if directory is not None:
            os.makedirs(directory, exist_ok=True)
            self._load()

    def get(self, name: str) -> Cursor:
        """The cursor for a subscriber, starting at the stream head when new."""
        validate_name(name)
        if name not in self._cursors:
            path = os.path.join(self._directory, f"{name}.json") if self._directory else None
            self._cursors[name] = Cursor(name, 0, path)
        return self._cursors[name]

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._cursors))

    def offsets(self) -> dict[str, int]:
        return {name: self._cursors[name].offset for name in self.names()}

    def _load(self) -> None:
        for filename in sorted(os.listdir(self._directory)):
            if not filename.endswith(".json"):
                continue
            path = os.path.join(self._directory, filename)
            with open(path, "r", encoding="utf-8") as handle:
                cursor = Cursor.from_row(json.load(handle), path)
            self._cursors[cursor.name] = cursor
