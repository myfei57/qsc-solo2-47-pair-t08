"""Durable event delivery: resumable cursors and redelivery deduplication."""

import json
import os

import pytest

from paperops.event import EventBus

from support import build, ok, run_line


def _read_offset(data_dir, name):
    with open(os.path.join(data_dir, "cursors", f"{name}.json"), encoding="utf-8") as handle:
        return json.load(handle)["offset"]


def test_events_are_persisted_and_replayed(tmp_path):
    data_dir = str(tmp_path / "data")
    bus = EventBus(data_dir)
    first = bus.publish("press.load.changed", 45)
    bus.publish("steam.drained", 7)
    assert first.event_id == "evt-00000001"
    assert os.path.exists(os.path.join(data_dir, "events.log"))

    rebuilt = EventBus(data_dir)
    assert rebuilt.sequence == 2
    assert rebuilt.records()[0].event_id == first.event_id


def test_named_subscriber_picks_up_the_backlog_on_restart(tmp_path):
    data_dir = str(tmp_path / "data")
    bus = EventBus(data_dir)
    bus.publish("steam.valve.changed", True)
    bus.publish("steam.valve.changed", False)

    rebuilt = EventBus(data_dir)
    seen = []
    rebuilt.subscribe("steam.valve.changed", lambda event: seen.append(event.payload), name="dry")
    delivered = rebuilt.resume()
    assert seen == [True, False]
    assert delivered == 2
    assert rebuilt.cursor("dry") == 2
    assert _read_offset(data_dir, "dry") == 2


def test_cursor_advances_over_events_the_subscriber_does_not_want(tmp_path):
    bus = EventBus(str(tmp_path / "data"))
    seen = []
    bus.subscribe("a", lambda event: seen.append(event.payload), name="only-a")
    bus.publish("a", 1)
    bus.publish("b", 2)
    bus.publish("a", 3)
    assert seen == [1, 3]
    assert bus.cursor("only-a") == 3


def test_restart_resumes_at_the_cursor_without_redelivery(tmp_path):
    data_dir = str(tmp_path / "data")
    bus = EventBus(data_dir)
    bus.subscribe("t", lambda event: None, name="worker")
    bus.publish("t", 1)
    bus.publish("t", 2)

    rebuilt = EventBus(data_dir)
    seen = []
    rebuilt.subscribe("t", lambda event: seen.append(event.payload), name="worker")
    assert rebuilt.resume() == 0
    assert seen == []
    rebuilt.publish("t", 3)
    assert seen == [3]


def test_new_subscriber_receives_the_history_of_its_topics(tmp_path):
    bus = EventBus(str(tmp_path / "data"))
    bus.publish("a", 1)
    bus.publish("b", 2)
    late = []
    bus.subscribe("a", lambda event: late.append(event.payload), name="late")
    bus.resume()
    assert late == [1]
    assert bus.cursor("late") == 2


def test_multi_topic_subscriber_matches_the_whole_backlog(tmp_path):
    data_dir = str(tmp_path / "data")
    bus = EventBus(data_dir)
    bus.publish("x", 1)
    bus.publish("y", 2)
    bus.publish("z", 3)

    rebuilt = EventBus(data_dir)
    hits = []
    rebuilt.subscribe("x", lambda event: hits.append(event.topic), name="multi")
    rebuilt.subscribe("y", lambda event: hits.append(event.topic), name="multi")
    rebuilt.resume()
    assert hits == ["x", "y"]


def test_failing_handler_keeps_the_event_pending_and_blocks_later_ones(tmp_path):
    bus = EventBus(str(tmp_path / "data"))

    class Gate:
        def __init__(self):
            self.calls = []
            self.failing = True

        def __call__(self, event):
            self.calls.append(event.payload)
            if self.failing:
                raise RuntimeError("boom")

    gate = Gate()
    bus.subscribe("t", gate, name="retryer")
    with pytest.raises(RuntimeError):
        bus.publish("t", "bad")
    assert bus.cursor("retryer") == 0
    assert [event.payload for event in bus.pending("retryer")] == ["bad"]

    with pytest.raises(RuntimeError):
        bus.publish("t", "later")
    assert gate.calls == ["bad", "bad"]
    assert bus.cursor("retryer") == 0

    # once the handler can finish, the backlog is delivered in order
    gate.failing = False
    bus.drain("retryer")
    assert gate.calls == ["bad", "bad", "bad", "later"]
    assert bus.cursor("retryer") == 2
    assert bus.pending("retryer") == ()


def test_one_failing_subscriber_does_not_stop_the_others(tmp_path):
    bus = EventBus(str(tmp_path / "data"))

    def fail(event):
        raise RuntimeError("boom")

    fine = []
    bus.subscribe("t", fail, name="bad")
    bus.subscribe("t", lambda event: fine.append(event.payload), name="fine")
    with pytest.raises(RuntimeError):
        bus.publish("t", "x")
    assert fine == ["x"]
    assert bus.cursor("fine") == 1
    assert bus.cursor("bad") == 0


def test_redelivered_event_is_not_handled_twice(tmp_path):
    data_dir = str(tmp_path / "data")
    bus = EventBus(data_dir)
    bus.subscribe("t", lambda event: None, name="worker")
    bus.publish("t", "one")
    assert _read_offset(data_dir, "worker") == 1

    # cursor regresses, as when a crash lost the acknowledgement but not the log
    with open(os.path.join(data_dir, "cursors", "worker.json"), "w", encoding="utf-8") as handle:
        json.dump({"name": "worker", "offset": 0}, handle)

    rebuilt = EventBus(data_dir)
    seen = []
    rebuilt.subscribe("t", lambda event: seen.append(event.payload), name="worker")
    rebuilt.resume()
    assert seen == ["one"]  # redelivery reaches the handler exactly once


def test_in_memory_bus_keeps_the_old_live_semantics():
    bus = EventBus()
    seen = []
    bus.subscribe("", lambda event: seen.append((event.topic, event.payload)))
    bus.publish("a", 1)
    assert seen == [("a", 1)]
    assert bus.sequence == 1
    assert bus.count("a") == 1
    assert bus.resume() == 0


def test_anonymous_subscriber_is_live_only(tmp_path):
    bus = EventBus(str(tmp_path / "data"))
    bus.publish("t", "before")
    seen = []
    bus.subscribe("t", lambda event: seen.append(event.payload))
    bus.resume()
    assert seen == []
    bus.publish("t", "after")
    assert seen == ["after"]


def test_subscriber_names_are_validated(tmp_path):
    bus = EventBus(str(tmp_path / "data"))
    with pytest.raises(ValueError):
        bus.subscribe("t", lambda event: None, name="a/b")
    with pytest.raises(ValueError):
        bus.drain("nobody")
    with pytest.raises(ValueError):
        bus.pending("nobody")


def test_application_restart_finishes_unprocessed_events_once(config):
    services = build(config)
    run_line(services.console)
    total = services.bus.sequence
    assert total >= 10
    assert all(offset == total for offset in services.bus.offsets().values())

    # rebuild on the same data directory: the backlog must not be counted twice
    restarted = build(config)
    assert restarted.bus.sequence == total
    assert restarted.audit.count("steam.valve.changed") == services.audit.count("steam.valve.changed")
    assert all(offset == total for offset in restarted.bus.offsets().values())

    # service keeps running normally after the restart
    ok(restarted.console, "POST", "/api/steam/drain", {})
    ok(restarted.console, "POST", "/api/steam/valve", {"open": False})
    assert restarted.bus.sequence == total + 2
    assert restarted.bus.offsets()["audit"] == total + 2


def test_lost_audit_cursor_still_counts_once(config):
    services = build(config)
    ok(services.console, "POST", "/api/steam/drain", {})
    ok(services.console, "POST", "/api/steam/valve", {"open": True})
    counted = services.audit.count("steam.valve.changed")

    # crash window: the cursor file never reached disk after the audit row did
    os.remove(os.path.join(config.data_dir, "cursors", "audit.json"))
    restarted = build(config)
    assert restarted.audit.count("steam.valve.changed") == counted
