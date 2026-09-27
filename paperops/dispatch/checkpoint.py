"""Durable per-subscriber positions.

Each subscriber remembers the highest stream position it has fully handled.
The position is forced to disk as soon as it advances, so after a restart the
dispatcher resumes reading strictly after it and re-runs only the unhandled
tail. Positions only move forward.
"""

import os

from .persist import append_row, read_rows


class PositionLog:
    """Maps a subscriber name to its last handled stream position."""

    LOG_NAME = "dispatch.positions.log"

    def __init__(self, directory: str | None = None) -> None:
        self._path = os.path.join(directory, self.LOG_NAME) if directory else None
        self._positions: dict[str, int] = {}
        if directory is not None:
            os.makedirs(directory, exist_ok=True)
            self._load()

    def position(self, subscriber: str) -> int:
        return self._positions.get(subscriber, 0)

    def advance(self, subscriber: str, position: int) -> int:
        """Move a subscriber forward; an older position is ignored."""
        current = self._positions.get(subscriber, 0)
        if position <= current:
            return current
        self._positions[subscriber] = position
        if self._path is not None:
            append_row(self._path, {"subscriber": subscriber, "position": position})
        return position

    def subscribers(self) -> tuple[str, ...]:
        return tuple(sorted(self._positions))

    def _load(self) -> None:
        for row in read_rows(self._path) if self._path else []:
            name = str(row["subscriber"])
            self._positions[name] = max(
                self._positions.get(name, 0), int(row["position"])
            )
