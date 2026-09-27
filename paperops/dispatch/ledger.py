"""Durable record of the actions a subscriber has already counted.

Delivery is at-least-once: a crash between doing the work and moving the
checkpoint makes the dispatcher hand the same event over again. This ledger
lets a subscriber refuse to count a redelivery. A marker is appended and
synced only after the side effect succeeded, so the bookkeeping never gets
ahead of the work.
"""

import os

from .persist import append_row, read_rows


class IdempotencyLedger:
    """The set of ``(subscriber, event_id)`` pairs already processed once."""

    LOG_NAME = "dispatch.processed.log"

    def __init__(self, directory: str | None = None) -> None:
        self._path = os.path.join(directory, self.LOG_NAME) if directory else None
        self._seen: set[tuple[str, str]] = set()
        if directory is not None:
            os.makedirs(directory, exist_ok=True)
            self._load()

    def has(self, subscriber: str, event_id: str) -> bool:
        return (subscriber, event_id) in self._seen

    def mark(self, subscriber: str, event_id: str) -> None:
        """Durably record that one action has been counted."""
        key = (subscriber, event_id)
        if key in self._seen:
            return
        self._seen.add(key)
        if self._path is not None:
            append_row(self._path, {"subscriber": subscriber, "event_id": event_id})

    def _load(self) -> None:
        for row in read_rows(self._path) if self._path else []:
            self._seen.add((str(row["subscriber"]), str(row["event_id"])))
