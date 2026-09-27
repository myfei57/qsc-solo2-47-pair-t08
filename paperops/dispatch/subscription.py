"""A named subscriber and the topics it wants."""

from collections.abc import Callable
from dataclasses import dataclass

from .message import Delivery

Handler = Callable[[Delivery], None]

WILDCARD = ""


@dataclass(frozen=True)
class Subscription:
    """A handler addressed by a stable name so its position survives restart."""

    name: str
    handler: Handler
    topic: str = WILDCARD

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("a subscriber needs a non-empty name")

    def accepts(self, topic: str) -> bool:
        return self.topic == WILDCARD or self.topic == topic
