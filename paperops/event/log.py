"""Append-only stream of published events, durable when given a directory.

Each row carries a stable identifier. A subscriber only ever learns the
identifier from the log, so replaying the same row after a restart still
addresses the same event and can be recognised as a duplicate.
"""

import json
import os
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class Event:
    """One published control event."""

    seq: int
    topic: str
    payload: Any
    event_id: str = ""

    def to_row(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "topic": self.topic,
            "payload": self.payload,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "Event":
        return cls(
            seq=int(row["seq"]),
            topic=str(row["topic"]),
            payload=row.get("payload"),
            event_id=str(row.get("event_id", "")),
        )


class EventLog:
    """Keeps every published event in publish order and replays it on restart."""

    LOG_NAME = "events.log"

    def __init__(self, directory: str | None = None) -> None:
        self._events: list[Event] = []
        self._path = os.path.join(directory, self.LOG_NAME) if directory else None
        if directory is not None:
            os.makedirs(directory, exist_ok=True)
            self._load()

    def append(self, topic: str, payload: Any) -> Event:
        """Persist one event at the tail of the stream before returning it."""
        seq = len(self._events) + 1
        event = Event(
            seq=seq,
            topic=topic,
            payload=payload,
            event_id=f"evt-{seq:08d}",
        )
        self._events.append(event)
        self._write(event)
        return event

    def after(self, seq: int) -> tuple[Event, ...]:
        """Every event strictly past a stream position, in publish order."""
        return tuple(event for event in self._events if event.seq > seq)

    def events(self) -> tuple[Event, ...]:
        return tuple(self._events)

    def count(self, topic: str) -> int:
        return sum(1 for event in self._events if event.topic == topic)

    @property
    def sequence(self) -> int:
        return len(self._events)

    @property
    def directory(self) -> str | None:
        if self._path is None:
            return None
        return os.path.dirname(self._path)

    def _write(self, event: Event) -> None:
        if self._path is None:
            return
        with open(self._path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_row(), sort_keys=True, ensure_ascii=True))
            handle.write("\n")

    def _load(self) -> None:
        if self._path is None or not os.path.exists(self._path):
            return
        with open(self._path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    self._events.append(Event.from_row(json.loads(line)))
