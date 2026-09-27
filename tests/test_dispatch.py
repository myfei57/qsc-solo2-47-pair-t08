"""Tests for durable, at-least-once dispatch with per-subscriber checkpoints."""

import os

import pytest

from paperops.dispatch import (
    DispatchLog,
    Dispatcher,
    IdempotencyLedger,
    PositionLog,
)


def build(tmp_path, ledger=True):
    directory = str(tmp_path / "dispatch")
    log = DispatchLog(directory)
    positions = PositionLog(directory)
    seen = IdempotencyLedger(directory) if ledger else None
    return Dispatcher(log, positions, seen), directory


# ---------------------------------------------------------------- durability


def test_published_events_survive_restart(tmp_path):
    dispatcher, directory = build(tmp_path)
    dispatcher.publish("press.load", {"load": 45})
    dispatcher.publish("steam.valve", {"open": True})

    restarted = Dispatcher(
        DispatchLog(directory), PositionLog(directory), IdempotencyLedger(directory)
    )
    assert restarted.sequence == 2
    [first, second] = restarted._log.read(0)
    assert first.topic == "press.load"
    assert second.topic == "steam.valve"
    assert first.event_id != second.event_id


def test_sequence_is_gap_free_and_stream_is_append_only(tmp_path):
    dispatcher, _ = build(tmp_path)
    seqs = [dispatcher.publish("t", i).seq for i in range(3)]
    assert seqs == [1, 2, 3]
    assert [event.seq for event in dispatcher._log.read(0)] == [1, 2, 3]
    assert [event.seq for event in dispatcher._log.read(1)] == [2, 3]


# ----------------------------------------------------- independent positions


def test_each_subscriber_remembers_its_own_position(tmp_path):
    dispatcher, _ = build(tmp_path)
    a, b = [], []
    dispatcher.subscribe("a", a.append)
    dispatcher.subscribe("b", b.append)
    dispatcher.publish("t", 1)
    dispatcher.publish("t", 2)

    report = dispatcher.pump()
    assert report.delivered == 4
    assert len(a) == 2 and len(b) == 2
    assert dispatcher.position("a") == 2 and dispatcher.position("b") == 2


def test_restart_resumes_only_the_unfinished_subscriber(tmp_path):
    dispatcher, directory = build(tmp_path)
    dispatcher.subscribe("a", lambda event: None)
    dispatcher.publish("t", 1)
    dispatcher.pump()  # a is fully caught up at 1

    # New events arrive; a fresh subscriber "b" is registered in a new process.
    dispatcher.publish("t", 2)
    dispatcher.publish("t", 3)
    handled_b = []
    restarted = Dispatcher(
        DispatchLog(directory), PositionLog(directory), IdempotencyLedger(directory)
    )
    restarted.subscribe("a", lambda event: None)
    restarted.subscribe("b", handled_b.append)
    restarted.pump()

    # a already handled seq 1 and keeps going from 2; b starts from zero and
    # catches up on every stored event.
    assert restarted.position("a") == 3
    assert [event.payload for event in handled_b] == [1, 2, 3]
    assert restarted.pending("b") == 0


def test_subscription_registration_is_not_needed_for_publish(tmp_path):
    # An event published with nobody online must still be delivered later.
    dispatcher, directory = build(tmp_path)
    dispatcher.publish("t", "saved-before-anyone-listened")
    handled = []
    restarted = Dispatcher(
        DispatchLog(directory), PositionLog(directory), IdempotencyLedger(directory)
    )
    restarted.subscribe("late", handled.append)
    restarted.pump()
    assert [event.payload for event in handled] == ["saved-before-anyone-listened"]


# ------------------------------------------------------------------- failure


def test_failing_handler_does_not_lose_the_event_and_others_continue(tmp_path):
    dispatcher, directory = build(tmp_path)
    good = []
    calls = {"flaky": 0}

    class Boom(Exception):
        pass

    def flaky(event):
        calls["flaky"] += 1
        raise Boom("device offline")

    dispatcher.subscribe("flaky", flaky)
    dispatcher.subscribe("good", good.append)
    dispatcher.publish("t", 1)
    dispatcher.publish("t", 2)

    report = dispatcher.pump()
    assert isinstance(report.error, Boom)
    assert report.failed_subscriber == "flaky"
    # flaky is parked at zero; good still drains its whole queue.
    assert dispatcher.position("flaky") == 0
    assert len(good) == 2
    assert dispatcher.pending("flaky") == 2

    # After a restart with the device back, flaky finishes the backlog.
    handled = []
    restarted = Dispatcher(
        DispatchLog(directory), PositionLog(directory), IdempotencyLedger(directory)
    )
    restarted.subscribe("flaky", handled.append)
    restarted.pump()
    assert [event.payload for event in handled] == [1, 2]
    assert restarted.position("flaky") == 2


# --------------------------------------------------------------- idempotency


def test_redelivery_after_crash_is_counted_once(tmp_path):
    dispatcher, directory = build(tmp_path, ledger=True)
    counts = {"n": 0}

    def side_effect(event):
        counts["n"] += 1  # the thing that must never be double counted

    dispatcher.subscribe_idempotent("counter", side_effect)
    dispatcher.publish("t", "only-once")
    dispatcher.pump()
    assert counts["n"] == 1

    # Simulate a crash after the side effect but before the checkpoint moved:
    # wipe the position, keep the processed ledger, then restart.
    PositionLogCrash.reset(directory, "counter")
    restarted = Dispatcher(
        DispatchLog(directory), PositionLog(directory), IdempotencyLedger(directory)
    )
    restarted.subscribe_idempotent("counter", side_effect)
    report = restarted.pump()

    assert counts["n"] == 1  # the redelivery was skipped, not re-counted
    assert report.skipped == 1
    assert restarted.position("counter") == 1


def test_ledger_marker_survives_restart(tmp_path):
    dispatcher, directory = build(tmp_path)
    handled = []
    dispatcher.subscribe_idempotent("c", handled.append)
    dispatcher.publish("t", 1)
    dispatcher.pump()

    restarted_ledger = IdempotencyLedger(directory)
    event_id = dispatcher._log.at(1).event_id
    assert restarted_ledger.has("c", event_id)


def test_ledger_keeps_subscribers_independent(tmp_path):
    dispatcher, _ = build(tmp_path)
    a, b = [], []
    dispatcher.subscribe_idempotent("a", a.append)
    dispatcher.subscribe_idempotent("b", b.append)
    dispatcher.publish("t", 1)
    dispatcher.pump()
    assert len(a) == 1 and len(b) == 1  # both count their own copy


def test_producer_dedup_key_collapses_retries(tmp_path):
    dispatcher, _ = build(tmp_path)
    first = dispatcher.publish("t", 1, dedup_key="command-7")
    retry = dispatcher.publish("t", 1, dedup_key="command-7")
    other = dispatcher.publish("t", 2, dedup_key="command-8")
    assert retry.seq == first.seq
    assert other.seq == first.seq + 1
    assert dispatcher.sequence == 2


# -------------------------------------------------------------------- topics


def test_topic_filter_only_sees_matching_events(tmp_path):
    dispatcher, _ = build(tmp_path, ledger=False)
    steam, everything = [], []
    dispatcher.subscribe("steam", steam.append, topic="steam.valve")
    dispatcher.subscribe("all", everything.append)
    dispatcher.publish("press.load", 1)
    dispatcher.publish("steam.valve", 2)
    dispatcher.pump()

    assert [e.payload for e in steam] == [2]
    assert [e.payload for e in everything] == [1, 2]
    # The filtered event still advances the steam position past it.
    assert dispatcher.position("steam") == 2


def test_attach_mirrors_live_bus_events(tmp_path):
    from paperops.event import EventBus

    dispatcher, _ = build(tmp_path)
    bus = EventBus()
    dispatcher.attach(bus)
    bus.publish("press.load", {"load": 9})
    assert dispatcher.sequence == 1
    assert dispatcher._log.at(1).topic == "press.load"


def test_duplicate_subscriber_name_rejected(tmp_path):
    dispatcher, _ = build(tmp_path)
    dispatcher.subscribe("x", lambda event: None)
    with pytest.raises(ValueError):
        dispatcher.subscribe("x", lambda event: None)


def test_unnamed_subscriber_rejected(tmp_path):
    dispatcher, _ = build(tmp_path)
    with pytest.raises(ValueError):
        dispatcher.subscribe("", lambda event: None)


def test_idempotent_subscriber_requires_ledger(tmp_path):
    directory = str(tmp_path / "dispatch")
    dispatcher = Dispatcher(DispatchLog(directory), PositionLog(directory), None)
    with pytest.raises(ValueError):
        dispatcher.subscribe_idempotent("x", lambda event: None)


class PositionLogCrash:
    """Test helper: roll back one subscriber's checkpoint while keeping the log."""

    @staticmethod
    def reset(directory: str, subscriber: str) -> None:
        path = os.path.join(directory, PositionLog.LOG_NAME)
        lines = []
        for line in open(path, encoding="utf-8"):
            if f'"{subscriber}"' not in line and subscriber not in line:
                lines.append(line)
        with open(path, "w", encoding="utf-8") as handle:
            handle.writelines(lines)
