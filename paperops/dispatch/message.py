"""One entry of the durable dispatch stream."""

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class Delivery:
    """One device event stored in arrival order.

    ``seq`` is the unique gap-free position in the stream and the unit a
    subscriber's checkpoint advances by. ``event_id`` identifies the same
    action across redeliveries, so a handler can refuse to count it twice.
    ``dedup_key`` is an optional producer-supplied identity: publishing an
    already-seen key returns the original entry instead of appending again.
    """

    seq: int
    event_id: str
    topic: str
    payload: Any
    dedup_key: str = ""

    def to_row(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "topic": self.topic,
            "dedup_key": self.dedup_key,
            "payload": self.payload,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "Delivery":
        return cls(
            seq=int(row["seq"]),
            event_id=str(row["event_id"]),
            topic=str(row["topic"]),
            payload=row.get("payload"),
            dedup_key=str(row.get("dedup_key", "")),
        )
