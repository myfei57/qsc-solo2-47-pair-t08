"""Append-only event stream that survives process restarts.

Every published device action is given a gap-free sequence number and forced
to disk before the publish returns. Subscribers later read the stream by
position, so an event that was on disk but never handled is not lost when the
process is restarted.
"""

import os
from typing import Any

from .message import Delivery
from .persist import append_row, read_rows


class DispatchLog:
    """The durable record of every published device action."""

    LOG_NAME = "dispatch.log"

    def __init__(self, directory: str | None = None) -> None:
        self._path = os.path.join(directory, self.LOG_NAME) if directory else None
        self._events: list[Delivery] = []
        self._dedup: dict[str, int] = {}
        if directory is not None:
            os.makedirs(directory, exist_ok=True)
            self._load()

    def publish(
        self,
        topic: str,
        payload: Any = None,
        dedup_key: str = "",
        event_id: str = "",
    ) -> Delivery:
        """Append one action; a repeated ``dedup_key`` yields the original row."""
        if dedup_key and dedup_key in self._dedup:
            return self._events[self._dedup[dedup_key] - 1]
        seq = len(self._events) + 1
        event = Delivery(
            seq=seq,
            event_id=event_id or f"evt:{seq:010d}",
            topic=topic,
            payload=payload,
            dedup_key=dedup_key,
        )
        self._events.append(event)
        if dedup_key:
            self._dedup[dedup_key] = seq
        if self._path is not None:
            append_row(self._path, event.to_row())
        return event

    def read(self, after: int, limit: int = 0) -> tuple[Delivery, ...]:
        """Entries strictly after position ``after`` in stream order."""
        after = max(0, after)
        tail = self._events[after:]
        return tuple(tail[:limit] if limit > 0 else tail)

    def at(self, seq: int) -> Delivery:
        return self._events[seq - 1]

    @property
    def sequence(self) -> int:
        """One past the last stored position."""
        return len(self._events)

    def _load(self) -> None:
        for row in read_rows(self._path) if self._path else []:
            event = Delivery.from_row(row)
            self._events.append(event)
            if event.dedup_key:
                self._dedup[event.dedup_key] = event.seq
