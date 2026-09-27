"""Reliable fan-out over the durable dispatch stream.

This is the difference from shouting on the in-process bus:

* published actions land in :class:`DispatchLog` and are synced before the
  publish returns, so they survive a process restart;
* every named subscriber has its own position in
  :class:`PositionLog`; a pump reads strictly past that position, so after a
  restart the unhandled tail is delivered and finished;
* delivery is at-least-once. ``subscribe_idempotent`` records each handled
  event in :class:`IdempotencyLedger`, so the same event redelivered after a
  crash is skipped instead of counted twice.

The checkpoint moves only after the handler returns. A handler whose side
effect is itself durable should make the work and the ledger marker one
transaction to be exactly-once end to end; otherwise the marker is the
deduplication seam and a duplicate marker simply does nothing.
"""

from dataclasses import dataclass, field
from typing import Any

from ..event import EventBus
from .checkpoint import PositionLog
from .ledger import IdempotencyLedger
from .log import DispatchLog
from .message import Delivery
from .subscription import Handler, Subscription, WILDCARD


@dataclass
class PumpReport:
    """What one :meth:`Dispatcher.pump` did, per subscriber."""

    delivered: int = 0
    skipped: int = 0
    failed_subscriber: str = ""
    error: Exception | None = None
    per_subscriber: dict[str, dict[str, int]] = field(default_factory=dict)


class Dispatcher:
    """Publishes durably and drives each subscriber from its own position."""

    BATCH = 64

    def __init__(
        self,
        log: DispatchLog,
        positions: PositionLog,
        ledger: IdempotencyLedger | None = None,
    ) -> None:
        self._log = log
        self._positions = positions
        self._ledger = ledger
        self._subscriptions: dict[str, Subscription] = {}

    def subscribe(
        self, name: str, handler: Handler, topic: str = WILDCARD
    ) -> Subscription:
        """Register a subscriber; its stored position resumes where it left off."""
        if name in self._subscriptions:
            raise ValueError(f"subscriber {name} is already registered")
        subscription = Subscription(name=name, handler=handler, topic=topic)
        self._subscriptions[name] = subscription
        return subscription

    def subscribe_idempotent(
        self, name: str, handler: Handler, topic: str = WILDCARD
    ) -> Subscription:
        """Like :meth:`subscribe`, but a redelivered event is never counted twice."""
        if self._ledger is None:
            raise ValueError("idempotent subscribers need an idempotency ledger")

        def guarded(event: Delivery) -> None:
            if self._ledger.has(name, event.event_id):
                raise _Skipped(event.event_id)
            handler(event)
            self._ledger.mark(name, event.event_id)

        return self.subscribe(name, guarded, topic)

    def attach(self, bus: EventBus) -> None:
        """Mirror every future in-process bus event into the durable stream."""
        bus.subscribe(WILDCARD, self._from_bus)

    def _from_bus(self, event: Any) -> None:
        self.publish(getattr(event, "topic", ""), getattr(event, "payload", None))

    def publish(self, topic: str, payload: Any = None, dedup_key: str = "") -> Delivery:
        return self._log.publish(topic, payload, dedup_key=dedup_key)

    def pump(self, limit: int = 0) -> PumpReport:
        """Deliver every event each subscriber has not yet handled.

        Subscribers are pumped independently. A failing subscriber stops at the
        event it could not handle without advancing past it; other subscribers
        keep going and the failure is reported for the caller to retry.
        """
        report = PumpReport()
        remaining = limit
        for subscription in self._subscriptions.values():
            delivered = skipped = 0
            stop = False
            while not stop:
                position = self._positions.position(subscription.name)
                batch = self._log.read(position, self.BATCH)
                if not batch:
                    break
                for event in batch:
                    if not subscription.accepts(event.topic):
                        self._commit(subscription.name, event.seq)
                    else:
                        try:
                            subscription.handler(event)
                        except _Skipped:
                            skipped += 1
                        except Exception as exc:  # failed; retried on next pump
                            report.failed_subscriber = subscription.name
                            report.error = exc
                            stop = True
                            break
                        else:
                            delivered += 1
                        self._commit(subscription.name, event.seq)
                    if limit > 0:
                        remaining -= 1
                        if remaining <= 0:
                            stop = True
                            break
                if len(batch) < self.BATCH:
                    break
            report.per_subscriber[subscription.name] = {
                "delivered": delivered,
                "skipped": skipped,
            }
            report.delivered += delivered
            report.skipped += skipped
        return report

    def pending(self, name: str) -> int:
        """Stored events a subscriber still has to handle."""
        subscription = self._subscriptions.get(name)
        tail = self._log.read(self._positions.position(name))
        if subscription is None:
            return len(tail)
        return sum(1 for event in tail if subscription.accepts(event.topic))

    def position(self, name: str) -> int:
        return self._positions.position(name)

    @property
    def sequence(self) -> int:
        return self._log.sequence

    def _commit(self, subscriber: str, position: int) -> None:
        """Advance the checkpoint; split out so crash tests can interpose."""
        self._positions.advance(subscriber, position)


class _Skipped(Exception):
    """Internal signal that an idempotent handler dropped a redelivery."""

    def __init__(self, event_id: str) -> None:
        super().__init__(f"already processed {event_id}")
        self.event_id = event_id
