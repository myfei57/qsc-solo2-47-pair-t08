"""Publish and subscribe bus with durable, resumable delivery.

Every published event is appended to an on-disk log before any handler runs, so
an event whose subscriber is down is not lost. Each named subscriber keeps a
durable cursor on that stream:

* the cursor only advances once the event's handlers have returned, so a crash
  before that point leaves the event pending and a later ``resume`` finishes it;
* an event at or before the cursor is never handed to the subscriber again, so a
  redelivery cannot be processed twice by a subscriber that keeps its own
  progress;
* events are delivered in stream order and a failing handler blocks later
  events for that subscriber until it succeeds (``drain`` retries it).

An anonymous subscriber (no ``name``) stays live-only and is not resumed.
"""

import os
from typing import Any, Callable, NamedTuple

from .cursor import CursorStore, validate_name
from .log import Event, EventLog

Handler = Callable[[Event], None]


class _Subscriber(NamedTuple):
    topic: str
    handler: Handler


class EventBus:
    """Durable log of events and per-subscriber delivery positions."""

    def __init__(self, directory: str | None = None) -> None:
        self._log = EventLog(directory)
        cursor_dir = os.path.join(directory, CursorStore.DIR_NAME) if directory else None
        self._cursors = CursorStore(cursor_dir)
        self._named: dict[str, list[_Subscriber]] = {}
        self._anonymous: list[_Subscriber] = []
        self._inflight: set[tuple[str, int]] = set()

    def subscribe(self, topic: str, handler: Handler, *, name: str = "") -> None:
        """Register a handler; the empty topic string receives every event.

        A named subscriber keeps a durable cursor and is caught up by
        :meth:`resume` or :meth:`drain`. Register every topic for one subscriber
        before resuming, so the backlog is matched against all of them.
        """
        if not name:
            self._anonymous.append(_Subscriber(topic, handler))
            return
        validate_name(name)
        self._named.setdefault(name, []).append(_Subscriber(topic, handler))

    def publish(self, topic: str, payload: Any = None) -> Event:
        """Record one event durably, then offer it to every subscriber."""
        event = self._log.append(topic, payload)
        self._offer(event)
        return event

    def resume(self) -> int:
        """Work through every named subscriber's backlog, in name order.

        Called after wiring (including on every restart) to continue events
        that were never acknowledged. One failing subscriber does not stop the
        others; the first failure is reported after everyone was attempted.
        """
        delivered = 0
        first_error: BaseException | None = None
        for name in sorted(self._named):
            try:
                delivered += self.drain(name)
            except BaseException as exc:  # noqa: BLE001 - keep draining, report one
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error
        return delivered

    def drain(self, name: str) -> int:
        """Deliver everything one subscriber has not processed; retry after a failure."""
        if name not in self._named:
            raise ValueError(f"no subscriber named {name!r}")
        start = self._cursors.get(name).offset
        self._advance(name, self._log.sequence)
        return self._cursors.get(name).offset - start

    def pending(self, name: str) -> tuple[Event, ...]:
        """Events one subscriber has not processed yet, in stream order."""
        if name not in self._named:
            raise ValueError(f"no subscriber named {name!r}")
        return self._log.after(self._cursors.get(name).offset)

    def cursor(self, name: str) -> int:
        """The stream position a named subscriber has processed up to."""
        return self._cursors.get(name).offset

    def offsets(self) -> dict[str, int]:
        return self._cursors.offsets()

    def records(self) -> tuple[Event, ...]:
        return self._log.events()

    def count(self, topic: str) -> int:
        return self._log.count(topic)

    @property
    def sequence(self) -> int:
        return self._log.sequence

    # ------------------------------------------------------------- internals

    def _offer(self, event: Event) -> None:
        """Hand one fresh event to every subscriber; the event is never lost."""
        first_error: BaseException | None = None
        for name in sorted(self._named):
            try:
                self._advance(name, event.seq)
            except BaseException as exc:  # noqa: BLE001 - keep delivering, report one
                if first_error is None:
                    first_error = exc
        for subscriber in self._anonymous:
            if self._matches(subscriber.topic, event.topic):
                try:
                    subscriber.handler(event)
                except BaseException as exc:  # noqa: BLE001
                    if first_error is None:
                        first_error = exc
        if first_error is not None:
            raise first_error

    def _advance(self, name: str, upto: int) -> None:
        """Deliver one subscriber's pending events in order up to a position."""
        cursor = self._cursors.get(name)
        for event in self._log.after(cursor.offset):
            if event.seq > upto:
                break
            self._process(name, event)

    def _process(self, name: str, event: Event) -> None:
        """Move one subscriber past one event, invoking every matching handler."""
        cursor = self._cursors.get(name)
        if event.seq <= cursor.offset:
            return
        key = (name, event.seq)
        if key in self._inflight:
            return  # an outer delivery frame is already running this event
        self._inflight.add(key)
        try:
            for subscriber in self._named[name]:
                if self._matches(subscriber.topic, event.topic):
                    subscriber.handler(event)
            cursor.advance(event.seq)
        finally:
            self._inflight.discard(key)

    @staticmethod
    def _matches(subscribed: str, topic: str) -> bool:
        return subscribed == "" or subscribed == topic
